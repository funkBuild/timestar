#include "aggregator.hpp"

#include "group_key.hpp"
#include "http_query_handler.hpp"  // For SeriesResult
#include "yield_policy.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <utility>

namespace timestar {

// ============================================================================
// DISTRIBUTED AGGREGATION
// ============================================================================

// Yield cadence: kYieldChunkPoints / kYieldChunkStates from
// lib/core/yield_policy.hpp — the single definition shared by every
// yield-chunked loop in the query and serialization paths.

// ============================================================================
// N-WAY MERGE (replaces O(K²N) pairwise merge with O(KN log K) heap merge)
// ============================================================================

// Check whether all K partials share identical timestamp vectors.
// If so, we can use element-wise SIMD combine instead of a merge.
static bool allTimestampsIdentical(const std::vector<PartialAggregationResult*>& partials) {
    if (partials.size() <= 1)
        return true;
    const auto& ref = partials[0]->sortedTimestamps;
    for (size_t i = 1; i < partials.size(); ++i) {
        if (partials[i]->sortedTimestamps != ref)
            return false;
    }
    return true;
}

// True when any partial's raw values contain a NaN.  Chunked scan; `v != v`
// auto-vectorizes.  Used to keep foldAlignedRawValues branch-free: NaN inputs
// (rare) are routed to the NaN-aware heap merge instead.
static seastar::future<bool> anyRawNaN(const std::vector<PartialAggregationResult*>& partials) {
    for (const auto* p : partials) {
        const auto& vals = p->sortedValues;
        for (size_t seg = 0; seg < vals.size(); seg += kYieldChunkPoints) {
            const size_t end = std::min(seg + kYieldChunkPoints, vals.size());
            bool found = false;
            for (size_t i = seg; i < end; ++i) {
                found |= std::isnan(vals[i]);
            }
            if (found) {
                co_return true;
            }
            co_await seastar::coroutine::maybe_yield();
        }
    }
    co_return false;
}

// SIMD element-wise fold for aligned-timestamp raw-value partials.
// Callers must have excluded NaN inputs (see anyRawNaN) — the folds here are
// deliberately branch-free and would poison sums / lose min-max on NaN.
// Combines K value arrays into one using the aggregation method.
// Outputs merged (timestamps, values, counts). O(KN) time, zero merge overhead.
static seastar::future<> foldAlignedRawValues(const std::vector<PartialAggregationResult*>& partials,
                                              AggregationMethod method, std::vector<uint64_t>& outTs,
                                              std::vector<double>& outVals, std::vector<size_t>& outCounts,
                                              bool compactOutput = false) {
    const size_t N = partials[0]->sortedTimestamps.size();
    const size_t K = partials.size();

    outTs = std::move(partials[0]->sortedTimestamps);
    outVals = std::move(partials[0]->sortedValues);
    if (!compactOutput)
        outCounts.assign(N, K);  // Each output point aggregates K input points

    for (size_t s = 1; s < K; ++s) {
        assert(partials[s]->sortedValues.size() == N);
        const double* src = partials[s]->sortedValues.data();
        double* dst = outVals.data();

        // Element-wise fold, in yield-bounded segments.  Within a segment the
        // loops stay branch-free so GCC -O3 auto-vectorizes them well (Highway
        // SIMD dispatch overhead negates benefit for simple add/min/max —
        // measured: no improvement over auto-vectorized scalar).
        for (size_t seg = 0; seg < N; seg += kYieldChunkPoints) {
            const size_t end = std::min(seg + kYieldChunkPoints, N);
            switch (method) {
                case AggregationMethod::AVG:
                case AggregationMethod::SUM:
                    for (size_t i = seg; i < end; ++i)
                        dst[i] += src[i];
                    break;
                case AggregationMethod::COUNT:
                    // COUNT only needs to track the number of contributing partials
                    // per point (handled via outCounts), not accumulate values.
                    break;
                case AggregationMethod::MIN:
                    for (size_t i = seg; i < end; ++i)
                        dst[i] = std::min(dst[i], src[i]);
                    break;
                case AggregationMethod::MAX:
                    for (size_t i = seg; i < end; ++i)
                        dst[i] = std::max(dst[i], src[i]);
                    break;
                case AggregationMethod::LATEST:
                    // Keep existing value (both partials have same timestamp)
                    break;
                case AggregationMethod::FIRST:
                    break;
                // These methods require full AggregationState; should not reach here.
                case AggregationMethod::SPREAD:
                case AggregationMethod::STDDEV:
                case AggregationMethod::STDVAR:
                case AggregationMethod::MEDIAN:
                case AggregationMethod::EXACT_MEDIAN:
                    throw std::logic_error("foldAlignedRawValues called for unsupported method");
                default:
                    throw std::logic_error("foldAlignedRawValues called for unknown method");
            }
            co_await seastar::coroutine::maybe_yield();
        }
    }
}

namespace {

// One stream's head in the N-way merge heap.
struct MergeHeapEntry {
    uint64_t ts;
    size_t partialIdx;
    size_t pos;
    bool operator>(const MergeHeapEntry& o) const { return ts > o.ts; }
};

// Min-heap of stream heads on a fixed array (at most one entry per stream).
// push/pop are std::push_heap/std::pop_heap — exactly what std::priority_queue
// runs — so the order equal timestamps come out in is the same as it always
// was; LATEST/FIRST depend on it.
class MergeHeap {
public:
    explicit MergeHeap(size_t capacity) : entries_(std::make_unique_for_overwrite<MergeHeapEntry[]>(capacity)) {}
    MergeHeap(MergeHeap&& o) noexcept : entries_(std::move(o.entries_)), size_(std::exchange(o.size_, 0)) {}
    MergeHeap& operator=(MergeHeap&& o) noexcept {
        entries_ = std::move(o.entries_);
        size_ = std::exchange(o.size_, 0);
        return *this;
    }

    [[gnu::always_inline]] bool empty() const { return size_ == 0; }
    [[gnu::always_inline]] const MergeHeapEntry& top() const { return entries_[0]; }

    [[gnu::always_inline]] void push(MergeHeapEntry value) {
        entries_[size_++] = value;
        std::push_heap(entries_.get(), entries_.get() + size_, std::greater<MergeHeapEntry>{});
    }

    [[gnu::always_inline]] void pop() {
        std::pop_heap(entries_.get(), entries_.get() + size_, std::greater<MergeHeapEntry>{});
        --size_;
    }

    // Each input stream is sorted: advancing the smallest entry can only move
    // it down the heap. Repair once instead of popping then pushing. Returns
    // true when the entry stayed at the root, i.e. the same stream still has
    // the smallest head.
    [[gnu::always_inline]] bool replaceTop(MergeHeapEntry value) {
        MergeHeapEntry* const entries = entries_.get();
        const size_t size = size_;
        size_t pos = 0;
        for (size_t child = 1; child < size; child = pos * 2 + 1) {
            if (child + 1 < size && entries[child].ts >= entries[child + 1].ts)
                ++child;
            if (value.ts <= entries[child].ts)
                break;
            entries[pos] = entries[child];
            pos = child;
        }
        entries[pos] = value;
        return pos == 0;
    }

    // Smallest head among the streams other than the root's (UINT64_MAX when
    // the root is the only stream left): the root's children are the only
    // candidates for second place.
    [[gnu::always_inline]] uint64_t runnerUp() const {
        uint64_t ts = std::numeric_limits<uint64_t>::max();
        if (size_ > 1)
            ts = entries_[1].ts;
        if (size_ > 2 && entries_[2].ts < ts)
            ts = entries_[2].ts;
        return ts;
    }

private:
    std::unique_ptr<MergeHeapEntry[]> entries_;
    size_t size_ = 0;
};

}  // namespace

// The point-by-point loop of nWayHeapMerge, for up to `budget` input points.
// Returns true once every stream is exhausted.
//
// This is deliberately a plain function rather than part of the coroutine.
// Everything a coroutine keeps across a suspension lives in its heap-allocated
// frame, and the compiler has to treat that frame as memory any store might
// alias: the heap's bounds and the running accumulator were re-read from it on
// every level of every sift, and how badly depended on the compiler (the same
// source ran this loop 1.3-3x slower under GCC 15 than under GCC 14). Here the
// heap and the accumulator are moved into locals that never have their address
// taken, so they stay in registers whatever the compiler, and are handed back
// to the caller's frame only between chunks.
//
// The same goes for what gets inlined into it. Left to the compiler's
// heuristics, whether the heap operations and the per-timestamp callbacks
// were inlined here changed whenever unrelated code in this file changed, and
// with it the speed of every merge (by 25% either way). So the choice is
// stated: the heap operations and the small callbacks are always_inline at
// their definitions, the run path and the state fold are noinline.
template <class Item, class Output, class Init, class Fold, class Finish>
[[gnu::noinline]] static bool heapMergeChunk(const std::vector<PartialAggregationResult*>& partials,
                                             std::vector<uint64_t>& outTs, std::vector<Output>& outItems, Init& init,
                                             Fold& fold, Finish& finish, MergeHeap& heapSlot,
                                             std::optional<Item>& currentSlot, size_t budget, bool replaceRoot) {
    MergeHeap heap = std::move(heapSlot);
    std::optional<Item> current = std::move(currentSlot);
    size_t consumed = 0;

    // Run fast path. A stream whose next timestamp is below every other
    // stream's head (`limit`) supplies, by definition, the next output — and
    // is its only contributor — so those points are emitted straight from the
    // stream with no heap traffic. A series that covers a stretch on its own
    // (series active at different times, a late-starting or backfilled series,
    // the tail once the others are exhausted) then costs a compare per point
    // instead of a sift. init/fold/finish are called in exactly the order the
    // heap would have produced; a timestamp tied with another stream's head is
    // left for the heap. Returns the position of the first point not emitted.
    auto emitRun = [&] [[gnu::noinline]] (size_t pIdx, size_t runPos, uint64_t limit) {
        const auto& stream = partials[pIdx]->sortedTimestamps;
        while (consumed < budget && runPos < stream.size() && stream[runPos] < limit &&
               stream[runPos] != outTs.back()) {
            outItems.push_back(finish(outTs.back(), *current));
            outTs.push_back(stream[runPos]);
            current.emplace(init(*partials[pIdx], runPos));
            ++runPos;
            ++consumed;
        }
        return runPos;
    };

    while (!heap.empty()) {
        auto [ts, pIdx, pos] = heap.top();
        if (!replaceRoot)
            heap.pop();

        if (!outTs.empty() && outTs.back() == ts) {
            fold(*current, *partials[pIdx], pos);
        } else {
            if (current)
                outItems.push_back(finish(outTs.back(), *current));
            outTs.push_back(ts);
            current.emplace(init(*partials[pIdx], pos));
        }

        // Advance this stream. When its next timestamp is still the smallest
        // head the stream is in a run, handled by emitRun above. Whether it is
        // falls out of the heap repair that happens anyway, so series that
        // take turns point by point pay nothing for the fast path.
        const auto& stream = partials[pIdx]->sortedTimestamps;
        const size_t nextPos = pos + 1;
        ++consumed;
        if (nextPos < stream.size()) {
            const MergeHeapEntry next{stream[nextPos], pIdx, nextPos};
            if (replaceRoot) {
                if (heap.replaceTop(next)) [[unlikely]] {
                    // Only worth leaving the loop for when at least two more
                    // points qualify; a lone one is already at the root.
                    const uint64_t limit = heap.runnerUp();
                    if (nextPos + 1 < stream.size() && stream[nextPos + 1] < limit) {
                        const size_t runPos = emitRun(pIdx, nextPos, limit);
                        if (runPos == stream.size())
                            heap.pop();
                        else if (runPos != nextPos)
                            heap.replaceTop({stream[runPos], pIdx, runPos});
                    }
                }
            } else if (heap.empty() || next.ts < heap.top().ts) [[unlikely]] {
                const size_t runPos =
                    emitRun(pIdx, nextPos, heap.empty() ? std::numeric_limits<uint64_t>::max() : heap.top().ts);
                if (runPos < stream.size())
                    heap.push({stream[runPos], pIdx, runPos});
            } else {
                heap.push(next);
            }
        } else if (replaceRoot)
            heap.pop();

        if (consumed >= budget)
            break;
    }

    const bool done = heap.empty();
    heapSlot = std::move(heap);
    currentSlot = std::move(current);
    return done;
}

// Core N-way heap merge over PartialAggregationResult partials.
//
// `init(partial, pos)` is invoked the first time a new timestamp is seen,
// and should produce the initial Item for that timestamp.
// `fold(item, partial, pos)` is invoked for every subsequent duplicate
// timestamp and should mutate `item` to absorb the duplicate.
// `finish(timestamp, item)` emits the final output when that timestamp ends.
//
// Pulls per-partial elements off a min-heap ordered by timestamp, yielding to
// the reactor every `yieldChunk` input points.
template <class Item, class Output, class Init, class Fold, class Finish>
static seastar::future<> nWayHeapMerge(const std::vector<PartialAggregationResult*>& partials,
                                       std::vector<uint64_t>& outTs, std::vector<Output>& outItems, Init init,
                                       Fold fold, Finish finish, size_t yieldChunk = kYieldChunkPoints,
                                       bool replaceRoot = true) {
    const size_t K = partials.size();

    size_t totalSize = 0;
    for (const auto* p : partials)
        totalSize += p->sortedTimestamps.size();
    outTs.reserve(totalSize);
    outItems.reserve(totalSize);

    MergeHeap heap(K);
    for (size_t i = 0; i < K; ++i) {
        if (!partials[i]->sortedTimestamps.empty()) {
            heap.push({partials[i]->sortedTimestamps[0], i, 0});
        }
    }

    // Only the current timestamp needs an accumulator. Finalize it as soon
    // as the heap advances, rather than retaining a full state per output.
    std::optional<Item> current;
    while (!heapMergeChunk<Item, Output>(partials, outTs, outItems, init, fold, finish, heap, current, yieldChunk,
                                         replaceRoot)) {
        co_await seastar::coroutine::maybe_yield();
    }
    if (current)
        outItems.push_back(finish(outTs.back(), *current));
}

// N-way heap merge for raw-value partials with mismatched timestamps.
// On duplicate timestamps the value is folded per `method`; outCounts tracks
// how many NON-NaN partials contributed at each output timestamp.
//
// NaN = missing data (docs/nan_policy.md): a NaN contribution never folds and
// never counts — exactly like AggregationState::addValue, so the raw fold and
// the state fold cannot disagree.  A timestamp whose contributions are all NaN
// keeps value NaN with count 0, mirroring getValue() on an empty state.
static seastar::future<> nWayMergeRawValues(std::vector<PartialAggregationResult*>& partials, AggregationMethod method,
                                            std::vector<uint64_t>& outTs, std::vector<double>& outVals,
                                            std::vector<size_t>& outCounts, bool compactOutput = false) {
    if (!compactOutput) {
        size_t totalSize = 0;
        for (const auto* p : partials)
            totalSize += p->sortedTimestamps.size();
        outCounts.reserve(totalSize);
    }
    auto foldDuplicate = [method] [[gnu::always_inline]] (double existing, double incoming) -> double {
        switch (method) {
            case AggregationMethod::AVG:
            case AggregationMethod::SUM:
            case AggregationMethod::COUNT:
                return existing + incoming;
            case AggregationMethod::MIN:
                return std::min(existing, incoming);
            case AggregationMethod::MAX:
                return std::max(existing, incoming);
            case AggregationMethod::LATEST:
            case AggregationMethod::FIRST:
            case AggregationMethod::SPREAD:
            case AggregationMethod::STDDEV:
            case AggregationMethod::STDVAR:
            case AggregationMethod::MEDIAN:
            case AggregationMethod::EXACT_MEDIAN:
                // These either preserve the existing value (LATEST/FIRST) or
                // require AggregationState and shouldn't have reached this path
                // (the latter group). Behaviour preserved from the original
                // unrolled implementation.
                return existing;
            default:
                return existing;
        }
    };
    // Preserve the previous heap traversal for order-sensitive tie selection.
    const bool replaceRoot = method != AggregationMethod::FIRST && method != AggregationMethod::LATEST;
    if (compactOutput) {
        // Counts are needed only until this timestamp is finalized. In
        // particular, keep "no contribution" separate from an arithmetic NaN
        // (such as +Inf + -Inf), which a later value must not overwrite.
        struct RawAccumulator {
            double value;
            size_t count;
        };
        return nWayHeapMerge<RawAccumulator>(
            partials, outTs, outVals,
            [] [[gnu::always_inline]] (const PartialAggregationResult& p, size_t pos) {
                const double v = p.sortedValues[pos];
                return RawAccumulator{v, std::isnan(v) ? size_t{0} : size_t{1}};
            },
            [foldDuplicate, method] [[gnu::always_inline]] (RawAccumulator & item, const PartialAggregationResult& p,
                                                            size_t pos) {
                const double v = p.sortedValues[pos];
                if (std::isnan(v))
                    return;
                if (item.count == 0)
                    item.value = v;
                else if (method != AggregationMethod::COUNT)
                    item.value = foldDuplicate(item.value, v);
                ++item.count;
            },
            [method] [[gnu::always_inline]] (uint64_t, const RawAccumulator& item) {
                if (item.count == 0)
                    return std::numeric_limits<double>::quiet_NaN();
                if (method == AggregationMethod::COUNT)
                    return static_cast<double>(item.count);
                if (method == AggregationMethod::AVG && item.count > 1)
                    return item.value / item.count;
                return item.value;
            },
            kYieldChunkPoints, replaceRoot);
    }
    return nWayHeapMerge<double>(
        partials, outTs, outVals,
        [&outCounts] [[gnu::always_inline]] (const PartialAggregationResult& p, size_t pos) {
            const double v = p.sortedValues[pos];
            // NaN as the first contribution: keep it as the slot value but
            // count 0 — a later non-NaN contribution replaces it.
            outCounts.push_back(std::isnan(v) ? 0 : 1);
            return v;
        },
        [&outCounts, foldDuplicate] [[gnu::always_inline]] (double& item, const PartialAggregationResult& p,
                                                            size_t pos) {
            const double v = p.sortedValues[pos];
            if (std::isnan(v)) [[unlikely]] {
                return;  // missing — never folds, never counts
            }
            if (outCounts.back() == 0) [[unlikely]] {
                item = v;  // slot held only NaN so far — replace, don't fold
            } else {
                item = foldDuplicate(item, v);
            }
            outCounts.back()++;
        },
        [] [[gnu::always_inline]] (uint64_t, double value) { return value; }, kYieldChunkPoints, replaceRoot);
}

// What a timestamp with exactly one raw contributor finalizes to: precisely
// the result of a fresh AggregationState that had that one value added.
//
// Most timestamps in a no-interval query have one contributor (series rarely
// share exact timestamps), and for those the answer does not need the state:
// a lone finite value is its own avg/min/max/sum/latest/first/median, has
// count 1, and zero spread and variance. Building the 128-byte state just to
// read that back was most of this merge's cost — and whether the compiler
// managed to optimise the state away depended on unrelated code in this file.
// The closed forms cover finite values other than -0.0 at ordinary
// timestamps; everything else (NaN, infinities, -0.0, the sentinel timestamp,
// any method not listed) goes through the state itself, so the two can never
// disagree by more than a test would catch (LoneValueMatchesAStateOfOne).
std::pair<double, size_t> Aggregator::loneValueResult(double value, uint64_t timestamp, AggregationMethod method) {
    if (std::isfinite(value) && !(value == 0.0 && std::signbit(value)) &&
        timestamp != std::numeric_limits<uint64_t>::max()) [[likely]] {
        switch (method) {
            case AggregationMethod::AVG:
            case AggregationMethod::MIN:
            case AggregationMethod::MAX:
            case AggregationMethod::SUM:
            case AggregationMethod::LATEST:
            case AggregationMethod::FIRST:
            case AggregationMethod::MEDIAN:
            case AggregationMethod::EXACT_MEDIAN:
                return {value, 1};
            case AggregationMethod::COUNT:
                return {1.0, 1};
            case AggregationMethod::SPREAD:
            case AggregationMethod::STDDEV:
            case AggregationMethod::STDVAR:
                return {0.0, 1};
            default:
                break;
        }
    }
    AggregationState state;
    state.collectRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
    state.addValue(value, timestamp);
    return {state.getValue(method), state.count};
}

namespace {

// One output timestamp's accumulator in nWayMergeStates. While its only
// contributor is a single raw value it is just that value; a full state is
// built when a second contributor (or a state-carrying partial) arrives.
struct LazyStateItem {
    double value = 0.0;
    uint64_t timestamp = 0;
    std::optional<AggregationState> state;
};

}  // namespace

// N-way heap merge finalizing one AggregationState at a time from MIXED partials: raw
// (sortedValues) and state (sortedStates) partials in the same group.  Raw
// elements build and fold through addValue — which skips NaN, so the raw and
// state routes cannot disagree — and no intermediate sortedStates vector is
// materialized (the old convert-first pre-pass wrote ~420MB of transient
// state for a 3.5M-point STDDEV merge). The state is finalized as the heap
// advances, so neither raw nor merged inputs expand into a full state vector.
// Keep the smaller state yield chunk for the more expensive folds.
template <class Output>
static seastar::future<> nWayMergeStates(std::vector<PartialAggregationResult*>& partials, std::vector<uint64_t>& outTs,
                                         std::vector<Output>& outPoints, AggregationMethod method) {
    const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
    return nWayHeapMerge<LazyStateItem>(
        partials, outTs, outPoints,
        [] [[gnu::always_inline]] (const PartialAggregationResult& p, size_t pos) {
            if (!p.sortedStates.empty()) {
                // The original implementation moved out of partials[pIdx]->sortedStates[pos];
                // const_cast preserves that semantic (callers don't reuse the partials
                // after merge).
                LazyStateItem item;
                item.state.emplace(std::move(const_cast<PartialAggregationResult&>(p).sortedStates[pos]));
                return item;
            }
            return LazyStateItem{p.sortedValues[pos], p.sortedTimestamps[pos], std::nullopt};
        },
        [method, needsRaw] [[gnu::noinline]] (LazyStateItem & item, const PartialAggregationResult& p, size_t pos) {
            if (!item.state) {
                // A second contributor: now the first one needs its state.
                item.state.emplace();
                item.state->collectRaw = needsRaw;
                item.state->addValue(item.value, item.timestamp);
            }
            if (!p.sortedStates.empty()) {
                item.state->mergeForMethod(std::move(const_cast<PartialAggregationResult&>(p).sortedStates[pos]),
                                           method);
            } else {
                item.state->addValue(p.sortedValues[pos], p.sortedTimestamps[pos]);
            }
        },
        [method] [[gnu::always_inline]] (uint64_t timestamp, LazyStateItem& item) {
            double value;
            size_t count;
            if (item.state) {
                value = item.state->getValue(method);
                count = item.state->count;
            } else {
                std::tie(value, count) = Aggregator::loneValueResult(item.value, item.timestamp, method);
            }
            if constexpr (std::is_same_v<Output, double>) {
                (void)timestamp;
                (void)count;
                return value;
            } else {
                return AggregatedPoint{timestamp, value, count};
            }
        },
        kYieldChunkStates, method != AggregationMethod::FIRST && method != AggregationMethod::LATEST);
}

seastar::future<> Aggregator::compactAlignedPartialGroups(std::vector<PartialAggregationResult>& partials,
                                                          AggregationMethod method) {
    // A state is much larger than a raw point. Require enough contributors to
    // reduce both bytes and partial count, and cap each new state vector.
    constexpr size_t minContributors = 16;
    constexpr size_t maxPoints = 256;
    if (partials.size() < minContributors || !isStreamableMethod(method) || method == AggregationMethod::FIRST ||
        method == AggregationMethod::LATEST)
        co_return;

    std::unordered_map<PrehashedString, std::vector<size_t>, PrehashedStringHash, PrehashedStringEqual> groups;
    for (size_t i = 0; i < partials.size(); ++i) {
        const auto& p = partials[i];
        if (!p.sortedTimestamps.empty() && p.sortedTimestamps.size() <= maxPoints &&
            p.sortedValues.size() == p.sortedTimestamps.size() && p.sortedStates.empty() && p.bucketStates.empty() &&
            !p.collapsedState)
            groups[PrehashedString(p.groupKey, p.groupKeyHash)].push_back(i);
        if ((i + 1) % kYieldChunkStates == 0)
            co_await seastar::coroutine::maybe_yield();
    }

    std::vector<bool> consumed(partials.size(), false);
    size_t sinceYield = 0;
    bool changed = false;
    for (const auto& [key, indices] : groups) {
        if (indices.size() < minContributors)
            continue;
        auto& first = partials[indices.front()];
        bool aligned = true;
        for (size_t index : indices) {
            aligned &= partials[index].sortedTimestamps == first.sortedTimestamps;
            sinceYield += first.sortedTimestamps.size();
            if (sinceYield >= kYieldChunkPoints) {
                sinceYield = 0;
                co_await seastar::coroutine::maybe_yield();
            }
            if (!aligned)
                break;
        }
        if (!aligned)
            continue;
        std::vector<AggregationState> states(first.sortedTimestamps.size());
        size_t totalPoints = 0;
        for (size_t index : indices) {
            auto& p = partials[index];
            totalPoints += p.totalPoints;
            for (size_t i = 0; i < states.size(); ++i)
                states[i].addValueForMethod(p.sortedValues[i], first.sortedTimestamps[i], method);
            consumed[index] = index != indices.front();
            sinceYield += states.size();
            if (sinceYield >= kYieldChunkPoints) {
                sinceYield = 0;
                co_await seastar::coroutine::maybe_yield();
            }
        }
        first.sortedStates = std::move(states);
        std::vector<double>{}.swap(first.sortedValues);
        first.totalPoints = totalPoints;
        changed = true;
    }
    if (changed) {
        size_t out = 0;
        for (size_t i = 0; i < partials.size(); ++i) {
            if (!consumed[i]) {
                if (out != i)
                    partials[out] = std::move(partials[i]);
                ++out;
            } else
                partials[i] = PartialAggregationResult{};
            if ((i + 1) % kYieldChunkStates == 0)
                co_await seastar::coroutine::maybe_yield();
        }
        partials.resize(out);
    }
}

seastar::future<std::vector<PartialAggregationResult>> Aggregator::createPartialAggregations(
    std::vector<timestar::SeriesResult> seriesResults, AggregationMethod method, uint64_t interval,
    const std::vector<std::string>& groupByTags, uint64_t bucketAnchor) {
    size_t pointsSinceYield = 0;

    // ── interval == 0: one RAW partial per (series, field) ──────────────────
    //
    // Do NOT fold series that share a group into one partial here.  The pre-fix
    // code merged each additional series into the group's accumulated state
    // vector (re-copying it every time — O(K²·N) for K series in a group, the
    // 980ms-reactor-stall incident of Jul 2026), and materialized a 120-byte
    // AggregationState per point.  Emitting compact raw partials instead makes
    // this pass O(N) copies, and hands the single K-way group merge to
    // mergePartialAggregationsGrouped — exactly how partials produced by the
    // pushdown path already flow.  Raw is correct for every method: the merge
    // folds foldable methods from raw directly (methodCanFoldRaw, NaN-aware —
    // a NaN contribution never folds and never counts, docs/nan_policy.md) and
    // builds AggregationStates itself for the ones that need them (SPREAD/
    // STDDEV/STDVAR/MEDIAN, via addValue which skips NaN), so the result
    // cannot depend on which path produced the partial.
    if (interval == 0) {
        std::vector<PartialAggregationResult> result;
        result.reserve(seriesResults.size());

        for (auto& series : seriesResults) {
            for (auto& [fieldName, fieldData] : series.fields) {
                auto& timestamps = fieldData.first;
                auto& values = fieldData.second;
                if (timestamps.empty()) {
                    continue;
                }

                // Extract numeric values as doubles for aggregation.
                // Handles both native doubles and int64_t (cast to double;
                // precision loss for values > 2^53 is a known trade-off).
                // seriesResults is owned (by-value parameter), so the double
                // and timestamp vectors are moved out rather than copied.
                std::vector<double> doubleValues;
                if (std::holds_alternative<std::vector<double>>(values)) {
                    doubleValues = std::move(std::get<std::vector<double>>(values));
                } else if (std::holds_alternative<std::vector<int64_t>>(values)) {
                    const auto& intValues = std::get<std::vector<int64_t>>(values);
                    doubleValues.reserve(intValues.size());
                    for (int64_t v : intValues) {
                        doubleValues.push_back(static_cast<double>(v));
                    }
                } else {
                    continue;  // Skip non-numeric types (string, bool)
                }

                // Build composite group key directly from allTags + groupByTags,
                // avoiding intermediate std::map allocation for relevantTags
                auto gkr = timestar::buildGroupKeyDirect(series.measurement, fieldName, series.tags, groupByTags);

                PartialAggregationResult partial;
                partial.measurement = series.measurement;
                partial.fieldName = fieldName;
                partial.groupKey = std::move(gkr.key);
                partial.groupKeyHash = gkr.hash;
                partial.cachedTags = std::move(gkr.tags);
                partial.sortedTimestamps = std::move(timestamps);  // sorted per series (from queryTsm)
                partial.sortedValues = std::move(doubleValues);
                partial.totalPoints = partial.sortedTimestamps.size();

                pointsSinceYield += partial.totalPoints;
                result.push_back(std::move(partial));

                if (pointsSinceYield >= kYieldChunkPoints) {
                    pointsSinceYield = 0;
                    co_await seastar::coroutine::maybe_yield();
                }
            }
        }
        co_return result;
    }

    // ── interval > 0: bucketed accumulation, one partial per group ──────────
    // Folding N points into (far fewer) epoch-aligned buckets on the shard is
    // the right trade-off here — the per-point work is O(1) hash upserts, not
    // the quadratic re-merge the interval==0 path had.
    //
    // Use PrehashedString key to compute hash once and avoid redundant hashing
    std::unordered_map<PrehashedString, PartialAggregationResult, PrehashedStringHash, PrehashedStringEqual>
        partialResults;

    for (const auto& series : seriesResults) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            const auto& timestamps = fieldData.first;
            const auto& values = fieldData.second;

            // Extract numeric values as doubles for aggregation.
            std::vector<double> convertedValues;
            const std::vector<double>* doubleValuesPtr = nullptr;

            if (std::holds_alternative<std::vector<double>>(values)) {
                doubleValuesPtr = &std::get<std::vector<double>>(values);
            } else if (std::holds_alternative<std::vector<int64_t>>(values)) {
                const auto& intValues = std::get<std::vector<int64_t>>(values);
                convertedValues.reserve(intValues.size());
                for (int64_t v : intValues) {
                    convertedValues.push_back(static_cast<double>(v));
                }
                doubleValuesPtr = &convertedValues;
            } else {
                continue;  // Skip non-numeric types (string, bool)
            }

            const auto& doubleValues = *doubleValuesPtr;

            // Build composite group key directly from allTags + groupByTags,
            // avoiding intermediate std::map allocation for relevantTags
            auto gkr = timestar::buildGroupKeyDirect(series.measurement, fieldName, series.tags, groupByTags);

            // Hash once via PrehashedString; try_emplace avoids rehashing on lookup
            PrehashedString pkey(std::move(gkr.key), gkr.hash);
            const size_t keyHash = gkr.hash;
            auto [it, inserted] = partialResults.try_emplace(std::move(pkey), PartialAggregationResult{});
            auto& partial = it->second;
            if (inserted) {
                partial.measurement = series.measurement;
                partial.fieldName = fieldName;
                partial.groupKey = it->first.value;        // Read from the emplaced key
                partial.groupKeyHash = keyHash;            // Reuse pre-computed hash
                partial.cachedTags = std::move(gkr.tags);  // Cache parsed tags once
                if (!timestamps.empty()) {
                    // Estimate bucket count from time range / interval rather than
                    // reserving one entry per timestamp (which over-allocates by 3-4 orders
                    // of magnitude for dense data with coarse intervals).
                    uint64_t timeRange = timestamps.back() - timestamps.front();
                    size_t estimatedBuckets = timeRange / interval + 1;
                    partial.bucketStates.reserve(std::min(estimatedBuckets, timestamps.size()));
                }
            }

            // Bucketed aggregation - accumulate into AggregationState per bucket.
            // MEDIAN and EXACT_MEDIAN both use addValue with collectRaw=true.
            const bool needsRaw = (method == AggregationMethod::EXACT_MEDIAN || method == AggregationMethod::MEDIAN);
            for (size_t i = 0; i < timestamps.size(); ++i) {
                // anchor == 0 is the epoch grid; a non-zero anchor shifts to
                // start-aligned buckets.  Range filtering guarantees
                // ts >= anchor; clamp defensively regardless.
                const uint64_t rel = timestamps[i] >= bucketAnchor ? timestamps[i] - bucketAnchor : 0;
                uint64_t bucketTime = bucketAnchor + (rel / interval) * interval;
                auto& state = partial.bucketStates[bucketTime];
                state.collectRaw = needsRaw;
                state.addValue(doubleValues[i], timestamps[i]);
                partial.totalPoints++;

                if (++pointsSinceYield >= kYieldChunkPoints) {
                    pointsSinceYield = 0;
                    // `partial` and the iteration state survive the suspension:
                    // everything lives in this coroutine's frame, and the map is
                    // only mutated from this coroutine.
                    co_await seastar::coroutine::maybe_yield();
                }
            }
        }
    }

    // Convert map to vector
    std::vector<PartialAggregationResult> result;
    result.reserve(partialResults.size());
    for (auto& [hash, partial] : partialResults) {
        result.push_back(std::move(partial));
    }

    co_return result;
}

void foldSeriesBucketsIntoGroup(BucketStateMap& group, BucketStateMap& series, AggregationMethod method, bool consume) {
    auto absorb = [&](AggregationState& target, AggregationState& state) {
        if (consume) {
            target.mergeForMethod(std::move(state), method);
        } else {
            target.mergeForMethod(state, method);
        }
        state.releaseStorage();
    };

    bool unionMerge = false;
    if (!group.empty() && !series.empty() && group.sortedByKey() && series.sortedByKey() &&
        series.size() * 8 >= group.size() && series.begin()->first <= group.back().first) {
        size_t sampled = 0;
        size_t hits = 0;
        for (auto it = series.begin(); it != series.end() && sampled < 64; ++it, ++sampled) {
            hits += group.count(it->first);
        }
        unionMerge = hits * 2 < sampled;
    }

    if (unionMerge) {
        BucketStateMap merged;
        auto g = group.begin();
        const auto groupEnd = group.end();
        auto s = series.begin();
        const auto seriesEnd = series.end();
        while (g != groupEnd || s != seriesEnd) {
            if (s == seriesEnd || (g != groupEnd && g->first < s->first)) {
                merged.try_emplace(g->first, std::move(g->second));
                ++g;
            } else if (g == groupEnd || s->first < g->first) {
                absorb(merged[s->first], s->second);
                ++s;
            } else {
                auto& target = merged.try_emplace(g->first, std::move(g->second)).first->second;
                absorb(target, s->second);
                ++g;
                ++s;
            }
        }
        // Every state in both maps was moved from or absorbed.
        group.releaseWithoutDestroying();
        series.releaseWithoutDestroying();
        group = std::move(merged);
        return;
    }

    for (auto& [bucket, state] : series) {
        absorb(group[bucket], state);
    }
    series.releaseWithoutDestroying();
}

namespace {

struct BucketRef {
    uint64_t bucket;
    AggregationState* state;
};

}  // namespace

// Whether to fold a group's partials into the first partial's map in place
// rather than through a sorted index: worthwhile when they mostly carry the
// same buckets, judged from up to 256 buckets of the second partial looked up
// in the first. Only asked when the first map is in bucket order, where both
// the lookups here and the fold itself need no hash index. It only chooses
// between two merges that produce the same result, so a wrong guess costs
// time, never correctness.
static bool shouldFoldInPlace(const std::vector<PartialAggregationResult*>& partials) {
    const auto& first = partials[0]->bucketStates;
    const auto& second = partials[1]->bucketStates;
    if (!first.sortedByKey()) {
        return false;
    }
    size_t sampled = 0;
    size_t hits = 0;
    for (auto it = second.begin(); it != second.end() && sampled < 256; ++it, ++sampled) {
        hits += first.count(it->first);
    }
    return sampled == 0 || hits * 2 >= sampled;
}

// Turn one map of bucket states into points ordered by bucket, releasing the
// states. A map filled in time order is already in bucket order; only one that
// is not needs sorting.
static seastar::future<> finalizeBucketStates(BucketStateMap& states, AggregationMethod method,
                                              std::vector<AggregatedPoint>& points) {
    const size_t first = points.size();
    points.reserve(first + states.size());
    size_t sinceYield = 0;
    for (auto& [bucket, state] : states) {
        AggregatedPoint point;
        point.timestamp = bucket;
        point.count = state.count;
        point.value = state.getValue(method);
        points.push_back(point);
        state.releaseStorage();
        if (++sinceYield >= kYieldChunkPoints) {
            sinceYield = 0;
            co_await seastar::coroutine::maybe_yield();
        }
    }
    if (!states.sortedByKey()) {
        std::sort(points.begin() + static_cast<ptrdiff_t>(first), points.end(),
                  [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });
    }
    // Every state was visited and owns nothing now.
    states.releaseWithoutDestroying();
}

// Merge one group's bucketed partials, whose buckets mostly differ, into
// points ordered by bucket.
//
// Folding them into one map would be an insert per bucket and then a sort of
// everything. Instead, index the states where they are: a flat array of
// (bucket, state*) per partial, which is already in bucket order when the
// partial's map is (the usual case — see BucketMap), merged across partials
// with stable merges; one pass then folds runs of equal buckets and emits the
// points in order. The index costs 16 bytes per bucket per partial, and there
// is at most one partial per shard here, the shards having merged their own
// series already.
//
// Folding is the same as the map merge's: for a bucket several partials
// carry, the first partial's state absorbs the others in partial order, so
// floating-point sums and LATEST/FIRST tie-breaks come out the same.
static seastar::future<> mergeBucketedPartials(std::vector<PartialAggregationResult*>& partials,
                                               AggregationMethod method, std::vector<AggregatedPoint>& points) {
    size_t total = 0;
    for (const auto* partial : partials) {
        total += partial->bucketStates.size();
    }

    // refs holds one sorted run per entry of `runs` (start offsets, plus the
    // end). Runs keep their partials' order, so the stable merges below keep
    // equal buckets in partial order.
    std::vector<BucketRef> refs;
    refs.reserve(total);
    std::vector<size_t> runs;
    static constexpr size_t kSortChunk = 32768;
    size_t sinceYield = 0;
    auto byBucket = [](const BucketRef& a, const BucketRef& b) { return a.bucket < b.bucket; };

    for (auto* partial : partials) {
        const size_t begin = refs.size();
        for (auto& [bucket, state] : partial->bucketStates) {
            refs.push_back({bucket, &state});
            if (++sinceYield >= kYieldChunkPoints) {
                sinceYield = 0;
                co_await seastar::coroutine::maybe_yield();
            }
        }
        const size_t end = refs.size();
        if (end == begin) {
            continue;
        }
        if (partial->bucketStates.sortedByKey()) {
            runs.push_back(begin);
        } else {
            // Buckets are unique within a partial, so each chunk can be
            // sorted without regard to stability; the merges order the rest.
            for (size_t chunk = begin; chunk < end; chunk += kSortChunk) {
                const size_t chunkEnd = std::min(chunk + kSortChunk, end);
                std::sort(refs.begin() + static_cast<ptrdiff_t>(chunk), refs.begin() + static_cast<ptrdiff_t>(chunkEnd),
                          byBucket);
                runs.push_back(chunk);
                co_await seastar::coroutine::maybe_yield();
            }
        }
    }
    runs.push_back(refs.size());

    // Merge adjacent runs pairwise until one remains (stable: on equal
    // buckets the left run — the earlier partial — comes first).
    if (runs.size() > 2) {
        std::vector<BucketRef> scratch(refs.size());
        std::vector<size_t> merged;
        while (runs.size() > 2) {
            merged.clear();
            for (size_t r = 0; r + 1 < runs.size(); r += 2) {
                const size_t lo = runs[r];
                const size_t mid = runs[r + 1];
                const size_t hi = (r + 2 < runs.size()) ? runs[r + 2] : mid;
                std::merge(refs.begin() + static_cast<ptrdiff_t>(lo), refs.begin() + static_cast<ptrdiff_t>(mid),
                           refs.begin() + static_cast<ptrdiff_t>(mid), refs.begin() + static_cast<ptrdiff_t>(hi),
                           scratch.begin() + static_cast<ptrdiff_t>(lo), byBucket);
                merged.push_back(lo);
                co_await seastar::coroutine::maybe_yield();
            }
            merged.push_back(refs.size());
            refs.swap(scratch);
            runs.swap(merged);
        }
    }

    // Distinct buckets, so the output is sized exactly.
    size_t distinct = 0;
    for (size_t i = 0; i < refs.size(); ++i) {
        distinct += (i == 0 || refs[i].bucket != refs[i - 1].bucket) ? 1 : 0;
    }
    points.reserve(points.size() + distinct);

    for (size_t i = 0; i < refs.size();) {
        const uint64_t bucket = refs[i].bucket;
        AggregationState& state = *refs[i].state;
        for (++i; i < refs.size() && refs[i].bucket == bucket; ++i) {
            state.mergeForMethod(std::move(*refs[i].state), method);
            // Hand the absorbed state's buffer back now, so the next merge's
            // growth can reuse it while it is warm.
            refs[i].state->releaseStorage();
            ++sinceYield;
        }
        AggregatedPoint point;
        point.timestamp = bucket;
        point.count = state.count;
        point.value = state.getValue(method);
        points.push_back(point);
        state.releaseStorage();
        if (++sinceYield >= kYieldChunkPoints) {
            sinceYield = 0;
            co_await seastar::coroutine::maybe_yield();
        }
    }

    // Every state of every partial was visited above and owns nothing now.
    refs = {};
    for (auto* partial : partials) {
        partial->bucketStates.releaseWithoutDestroying();
    }
}

seastar::future<std::vector<GroupedAggregationResult>> Aggregator::mergePartialAggregationsGrouped(
    std::vector<PartialAggregationResult>& partialResults, AggregationMethod method, bool compactOutput) {
    if (partialResults.empty()) {
        co_return std::vector<GroupedAggregationResult>{};
    }
    size_t sinceYield = 0;

    std::vector<GroupedAggregationResult> result;

    // Group partial results by pre-hashed composite key (no re-hashing of groupKey strings)
    std::unordered_map<PrehashedString, std::vector<PartialAggregationResult*>, PrehashedStringHash,
                       PrehashedStringEqual>
        groups;

    for (auto& partial : partialResults) {
        // Reuse the pre-computed groupKeyHash — no re-hashing of the string
        PrehashedString pkey(partial.groupKey, partial.groupKeyHash);
        auto [it, inserted] = groups.try_emplace(std::move(pkey));
        it->second.push_back(&partial);
    }

    // Pre-allocate result
    result.reserve(groups.size());

    // Merge each group and preserve metadata
    for (auto& [groupKey, groupPartials] : groups) {
        if (groupPartials.empty()) {
            continue;
        }

        // Create grouped result with metadata from first partial (all in group share same metadata)
        GroupedAggregationResult groupedResult;
        groupedResult.measurement = std::move(groupPartials[0]->measurement);
        groupedResult.tags = std::move(groupPartials[0]->cachedTags);
        groupedResult.fieldName = std::move(groupPartials[0]->fieldName);

        // Check if we're using buckets or raw timestamps
        bool hasBuckets = !groupPartials[0]->bucketStates.empty();

        if (hasBuckets) {
            if (groupPartials.size() == 1) {
                // One partial: nothing to merge.
                co_await finalizeBucketStates(groupPartials.front()->bucketStates, method, groupedResult.points);
            } else if (shouldFoldInPlace(groupPartials)) {
                // Several partials carrying the same buckets (many series,
                // coarse buckets): fold the others into the first partial's
                // map. Their buckets arrive in the order the first map holds
                // them, so nearly every lookup is the entry after the last.
                auto& mergedStates = groupPartials.front()->bucketStates;
                for (size_t p = 1; p < groupPartials.size(); ++p) {
                    auto& incoming = groupPartials[p]->bucketStates;
                    for (auto& [bucket, state] : incoming) {
                        auto [it, inserted] = mergedStates.try_emplace(bucket, std::move(state));
                        if (!inserted) {
                            it->second.mergeForMethod(std::move(state), method);
                        }
                        state.releaseStorage();
                        if (++sinceYield >= kYieldChunkPoints) {
                            sinceYield = 0;
                            co_await seastar::coroutine::maybe_yield();
                        }
                    }
                    // Every incoming state was moved from or absorbed, and
                    // owns nothing now.
                    incoming.releaseWithoutDestroying();
                }
                co_await finalizeBucketStates(mergedStates, method, groupedResult.points);
            } else {
                // Several partials with mostly different buckets (series
                // active at different times, fine buckets): fold through a
                // sorted index instead. Same fold order, same result.
                co_await mergeBucketedPartials(groupPartials, method, groupedResult.points);
            }

        } else {
            // Non-bucketed merge. First check if all partials carry collapsed
            // states (streaming pushdown) — if so, merge O(K) states directly.
            bool allCollapsed = true;
            for (const auto* p : groupPartials) {
                if (!p->collapsedState.has_value()) {
                    allCollapsed = false;
                    break;
                }
            }

            if (allCollapsed && !groupPartials.empty()) {
                // Fast path: merge collapsed AggregationStates directly.
                AggregationState merged;
                for (const auto* p : groupPartials) {
                    merged.mergeForMethod(*p->collapsedState, method);
                }
                if (merged.count > 0) {
                    groupedResult.points.push_back(
                        {merged.getTimestamp(method), merged.getValue(method), merged.count});
                }
            } else {
                // Not all collapsed: convert any collapsed partials to
                // single-element sortedStates so the existing merge works.
                for (auto* p : groupPartials) {
                    if (p->collapsedState.has_value() && p->sortedValues.empty() && p->sortedStates.empty()) {
                        auto& s = *p->collapsedState;
                        if (s.count > 0) {
                            p->sortedTimestamps.push_back(s.firstTimestamp);
                            p->sortedStates.push_back(std::move(s));
                        }
                        p->collapsedState.reset();
                    }
                }

                // Check if all partials carry compact raw values (from pushdown)
                // — if so, merge without constructing any AggregationState objects.
                bool allRaw = true;
                for (const auto* p : groupPartials) {
                    if (p->sortedValues.empty() && !p->sortedTimestamps.empty()) {
                        allRaw = false;
                        break;
                    }
                }

                // SPREAD/STDDEV/STDVAR/MEDIAN/EXACT_MEDIAN need full AggregationState;
                // they cannot be folded from raw values alone (see
                // timestar::methodCanFoldRaw — the single definition of this rule).
                if (allRaw && methodCanFoldRaw(method) && !groupPartials.empty()) {
                    // Fast path: merge raw (timestamp, value) vectors directly.
                    auto* first = groupPartials[0];

                    if (groupPartials.size() == 1) {
                        // Single partial — zero-copy move, no merge needed.
                        if (method == AggregationMethod::COUNT && !compactOutput) {
                            auto& ts = first->sortedTimestamps;
                            auto& vals = first->sortedValues;
                            groupedResult.points.reserve(ts.size());
                            for (size_t i = 0; i < ts.size(); ++i) {
                                // COUNT counts only non-NaN values; a lone NaN
                                // point is an empty per-timestamp set → NaN
                                // (matches getValue() on a count-0 state).
                                if (std::isnan(vals[i])) [[unlikely]] {
                                    groupedResult.points.push_back(
                                        {ts[i], std::numeric_limits<double>::quiet_NaN(), 0});
                                } else {
                                    groupedResult.points.push_back({ts[i], 1.0, 1});
                                }
                                if (++sinceYield >= kYieldChunkPoints) {
                                    sinceYield = 0;
                                    co_await seastar::coroutine::maybe_yield();
                                }
                            }
                        } else {
                            groupedResult.rawTimestamps = std::move(first->sortedTimestamps);
                            groupedResult.rawValues = std::move(first->sortedValues);
                            if (method == AggregationMethod::COUNT) {
                                for (auto& value : groupedResult.rawValues) {
                                    if (!std::isnan(value))
                                        value = 1.0;
                                    if (++sinceYield >= kYieldChunkPoints) {
                                        sinceYield = 0;
                                        co_await seastar::coroutine::maybe_yield();
                                    }
                                }
                            }
                        }
                    } else {
                        // Multiple partials — use O(KN log K) N-way merge.
                        // Fast path: if all partials share identical timestamps,
                        // use element-wise SIMD fold (O(N), zero merge overhead).
                        std::vector<uint64_t> mergedTs;
                        std::vector<double> mergedVals;
                        std::vector<size_t> mergedCounts;

                        const bool aligned =
                            allTimestampsIdentical(groupPartials) && !co_await anyRawNaN(groupPartials);
                        if (aligned) {
                            co_await foldAlignedRawValues(groupPartials, method, mergedTs, mergedVals, mergedCounts,
                                                          compactOutput);
                        } else {
                            co_await nWayMergeRawValues(groupPartials, method, mergedTs, mergedVals, mergedCounts,
                                                        compactOutput);
                        }

                        if (!compactOutput)
                            groupedResult.points.reserve(mergedTs.size());
                        // Compact heap output is already final. An aligned raw
                        // fold has a constant contributor count, needed only
                        // for AVG normalization or COUNT values.
                        const bool needsFinalization =
                            !compactOutput ||
                            (aligned && (method == AggregationMethod::AVG || method == AggregationMethod::COUNT));
                        for (size_t i = 0; needsFinalization && i < mergedTs.size(); ++i) {
                            const size_t count = compactOutput ? groupPartials.size() : mergedCounts[i];
                            double finalValue = mergedVals[i];
                            if (count == 0) [[unlikely]] {
                                // Every contribution at this timestamp was NaN:
                                // the value slot already holds NaN and no method
                                // computes over an empty set (getValue() on a
                                // count-0 state is NaN) — COUNT included.
                                finalValue = std::numeric_limits<double>::quiet_NaN();
                            } else if (method == AggregationMethod::AVG && count > 1) {
                                finalValue /= count;
                            } else if (method == AggregationMethod::COUNT) {
                                finalValue = static_cast<double>(count);
                            }
                            if (compactOutput)
                                mergedVals[i] = finalValue;
                            else
                                groupedResult.points.push_back({mergedTs[i], finalValue, count});
                            if (++sinceYield >= kYieldChunkPoints) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }
                        if (compactOutput) {
                            groupedResult.rawTimestamps = std::move(mergedTs);
                            groupedResult.rawValues = std::move(mergedVals);
                        }
                    }
                } else {
                    // State-needing methods (SPREAD/STDDEV/STDVAR/MEDIAN), or a
                    // mix of raw and state partials.  nWayMergeStates consumes
                    // raw and state partials directly (raw elements fold via
                    // addValue, which skips NaN) — no conversion pre-pass.
                    const bool needsRaw =
                        (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
                    if (compactOutput && groupPartials.size() > 1) {
                        co_await nWayMergeStates(groupPartials, groupedResult.rawTimestamps, groupedResult.rawValues,
                                                 method);
                    } else if (compactOutput) {
                        auto* first = groupPartials[0];
                        groupedResult.rawTimestamps = std::move(first->sortedTimestamps);
                        if (first->sortedStates.empty())
                            groupedResult.rawValues = std::move(first->sortedValues);
                        else
                            groupedResult.rawValues.reserve(first->sortedStates.size());
                        for (size_t i = 0; i < groupedResult.rawTimestamps.size(); ++i) {
                            if (!first->sortedStates.empty()) {
                                groupedResult.rawValues.push_back(first->sortedStates[i].getValue(method));
                            } else {
                                AggregationState state;
                                state.collectRaw = needsRaw;
                                state.addValue(groupedResult.rawValues[i], groupedResult.rawTimestamps[i]);
                                groupedResult.rawValues[i] = state.getValue(method);
                            }
                            if (++sinceYield >= kYieldChunkPoints) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }
                    } else if (groupPartials.size() == 1 && groupPartials[0]->sortedStates.empty()) {
                        // Single RAW partial: finalize per point through a
                        // singleton state — no merged state vector at all.
                        auto* first = groupPartials[0];
                        groupedResult.points.reserve(first->sortedTimestamps.size());
                        for (size_t i = 0; i < first->sortedTimestamps.size(); ++i) {
                            AggregationState s;
                            s.collectRaw = needsRaw;
                            s.addValue(first->sortedValues[i], first->sortedTimestamps[i]);
                            groupedResult.points.push_back({first->sortedTimestamps[i], s.getValue(method), s.count});
                            if (++sinceYield >= kYieldChunkPoints) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }
                    } else if (groupPartials.size() == 1) {
                        auto* first = groupPartials[0];
                        std::vector<uint64_t> mergedTs = std::move(first->sortedTimestamps);
                        auto& mergedStates = first->sortedStates;
                        groupedResult.points.reserve(mergedTs.size());
                        for (size_t i = 0; i < mergedTs.size(); ++i) {
                            groupedResult.points.push_back(
                                {mergedTs[i], mergedStates[i].getValue(method), mergedStates[i].count});
                            if (++sinceYield >= kYieldChunkPoints) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }
                    } else {
                        std::vector<uint64_t> mergedTs;
                        co_await nWayMergeStates(groupPartials, mergedTs, groupedResult.points, method);
                    }
                }
            }
        }

        result.push_back(std::move(groupedResult));

        // Group boundary: cheap preemption check between groups so that
        // many-group (`by {tag}`) merges cannot monopolize the reactor even
        // when each individual group is small.
        co_await seastar::coroutine::maybe_yield();
    }

    co_return result;
}

// ============================================================================
// LEGACY COMPATIBILITY
// ============================================================================

std::vector<AggregatedPoint> Aggregator::aggregate(const std::vector<uint64_t>& timestamps,
                                                   const std::vector<double>& values, AggregationMethod method,
                                                   uint64_t interval) {
    if (timestamps.empty() || values.empty()) {
        return {};
    }

    const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);

    if (interval > 0) {
        // Bucketed aggregation
        std::unordered_map<uint64_t, AggregationState> buckets;
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            uint64_t bucketTime = (timestamps[i] / interval) * interval;
            auto& state = buckets[bucketTime];
            if (needsRaw)
                state.collectRaw = true;
            state.addValue(values[i], timestamps[i]);
        }

        std::vector<AggregatedPoint> result;
        result.reserve(buckets.size());
        for (const auto& [bucketTime, state] : buckets) {
            result.push_back({bucketTime, state.getValue(method), state.count});
        }
        std::sort(result.begin(), result.end(),
                  [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });
        return result;
    } else {
        // No bucketing - one point per timestamp
        std::unordered_map<uint64_t, AggregationState> states;
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            auto& state = states[timestamps[i]];
            if (needsRaw)
                state.collectRaw = true;
            state.addValue(values[i], timestamps[i]);
        }

        std::vector<AggregatedPoint> result;
        result.reserve(states.size());
        for (const auto& [ts, state] : states) {
            result.push_back({ts, state.getValue(method), state.count});
        }
        std::sort(result.begin(), result.end(),
                  [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });
        return result;
    }
}

std::vector<AggregatedPoint> Aggregator::aggregateMultiple(
    const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& groupData, AggregationMethod method,
    uint64_t interval) {
    if (groupData.empty()) {
        return {};
    }

    // Merge all series into one set of timestamp states
    const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
    std::unordered_map<uint64_t, AggregationState> mergedStates;

    for (const auto& [timestamps, values] : groupData) {
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            uint64_t key = interval > 0 ? (timestamps[i] / interval) * interval : timestamps[i];
            auto& state = mergedStates[key];
            if (needsRaw)
                state.collectRaw = true;
            state.addValue(values[i], timestamps[i]);
        }
    }

    std::vector<AggregatedPoint> result;
    result.reserve(mergedStates.size());
    for (const auto& [ts, state] : mergedStates) {
        result.push_back({ts, state.getValue(method), state.count});
    }
    std::sort(result.begin(), result.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });
    return result;
}

}  // namespace timestar
