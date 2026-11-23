#include "memory_store.hpp"

#include "logger.hpp"
#include "logging_config.hpp"
#include "simd_aggregator.hpp"
#include "util.hpp"
#include "value_type_dispatch.hpp"  // TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <numeric>
#include <ranges>
#include <stdexcept>

// ---------------------------------------------------------------------------
// InMemorySeriesStats::update — maintain running aggregation stats on insert.
//
// Phase 1: sum/min/max via SIMD (no data dependency between iterations)
// Phase 2: Welford mean/m2 via scalar (sequential dependency)
// Phase 3: first/latest timestamp tracking (branch-heavy, not vectorizable)
// ---------------------------------------------------------------------------
void InMemorySeriesStats::update(const double* values, const uint64_t* timestamps, size_t n, bool sorted) {
    if (n == 0)
        return;
    valid = true;

    // Phase 1: FUSED SIMD sum/min/max — one memory pass instead of three
    // separate kernel calls. The fused kernel NaN-skips min/max but the sum
    // propagates NaN, and its all-skipped sentinel collapses ±Inf results to
    // NaN — both cases route to the scalar special-value pass below.
    double batchSum, batchMin, batchMax;
    timestar::simd::SimdAggregator::calculateSumMinMax(values, n, batchSum, batchMin, batchMax);

    // Number of non-NaN values in this batch (canonical: NaN = missing data,
    // never counted/summed — docs/nan_policy.md).
    size_t validN = n;

    if (std::isnan(batchSum) || std::isnan(batchMin) || std::isnan(batchMax)) [[unlikely]] {
        // Special values present: NaN data (skip it), an Inf/-Inf mix (sum is
        // genuinely NaN), or all-±Inf data (kernel sentinel conflation).
        // Recompute scalar with NaN skipped; ±Inf participates arithmetically.
        batchSum = 0.0;
        batchMin = std::numeric_limits<double>::infinity();
        batchMax = -std::numeric_limits<double>::infinity();
        validN = 0;
        for (size_t i = 0; i < n; ++i) {
            const double v = values[i];
            if (std::isnan(v))
                continue;
            ++validN;
            batchSum += v;
            if (v < batchMin)
                batchMin = v;
            if (v > batchMax)
                batchMax = v;
        }
        if (validN == 0)
            return;  // All-NaN batch: nothing to fold (count unchanged).
    }

    // Kahan compensated accumulation of batch sum.
    // Non-finite guard: with a ±Inf sum the compensation term degenerates to
    // NaN and would poison later batches (Inf results silently become NaN).
    double y = batchSum - sumCompensation;
    double t = sum + y;
    sumCompensation = std::isfinite(t) ? (t - sum) - y : 0.0;
    sum = t;

    if (batchMin < min)
        min = batchMin;
    if (batchMax > max)
        max = batchMax;

    // Phase 2: batch mean/M2 via SIMD two-pass + Chan's parallel merge.
    // Replaces the per-value Welford loop, whose `mean += delta / count`
    // put a hardware divide on a loop-carried dependency chain (~20 cy/pt).
    // Two-pass batch variance is numerically as good as sequential Welford.
    {
        const double batchMean = batchSum / static_cast<double>(validN);
        // calculateVariance returns population variance (M2/n).
        // With NaN present the SIMD kernel would return NaN — compute the
        // NaN-skipping variance scalar instead (rare path).
        double batchM2;
        if (validN == n) [[likely]] {
            batchM2 = timestar::simd::SimdAggregator::calculateVariance(values, n, batchMean) * static_cast<double>(n);
        } else {
            batchM2 = 0.0;
            for (size_t i = 0; i < n; ++i) {
                if (std::isnan(values[i]))
                    continue;
                const double delta = values[i] - batchMean;
                batchM2 += delta * delta;
            }
        }
        if (count == 0) {
            mean = batchMean;
            m2 = batchM2;
        } else {
            const double delta = batchMean - mean;
            const double totalCount = static_cast<double>(count) + static_cast<double>(validN);
            m2 += batchM2 + delta * delta * (static_cast<double>(count) * static_cast<double>(validN)) / totalCount;
            mean += delta * (static_cast<double>(validN) / totalCount);
        }
        count += validN;
    }

    // Phase 3: first/latest timestamp tracking.
    // Batches are almost always sorted (chronological ingest), so the
    // endpoints suffice; the sortedness scan is a single predictable
    // compare per element vs. the two-branch tracking loop.
    // When the batch contains NaN (validN < n), first/latest must be the
    // first/last non-NaN VALUES (canonical FIRST/LATEST semantics) — take
    // the NaN-skipping loop instead.
    if (validN == n && (sorted || std::is_sorted(timestamps, timestamps + n))) [[likely]] {
        if (timestamps[0] < firstTimestamp) {
            firstTimestamp = timestamps[0];
            firstValue = values[0];
        }
        if (timestamps[n - 1] >= latestTimestamp) {
            latestTimestamp = timestamps[n - 1];
            latestValue = values[n - 1];
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            if (std::isnan(values[i]))
                continue;
            uint64_t ts = timestamps[i];
            if (ts < firstTimestamp) {
                firstTimestamp = ts;
                firstValue = values[i];
            }
            if (ts >= latestTimestamp) {
                latestTimestamp = ts;
                latestValue = values[i];
            }
        }
    }
}

namespace {
// Validate correction targets before modifying values. Search independent keys
// together so cache misses can overlap; select each half without a data-dependent
// branch. The final comparison also permits returning the one-past-end position.
[[gnu::noinline]] bool findCorrectionTargets(const uint64_t* data, size_t size, const uint64_t* keys, size_t count,
                                             std::vector<size_t>& targets) {
    constexpr size_t width = 8;
    size_t i = 0;
    for (; count - i >= width; i += width) {
        std::array<size_t, width> base{};
        size_t len = size;
        while (len > 1) {
            const size_t half = len / 2;
            for (size_t k = 0; k < width; ++k)
                base[k] = data[base[k] + half] < keys[i + k] ? base[k] + half : base[k];
            len -= half;
        }
        for (size_t k = 0; k < width; ++k) {
            base[k] += data[base[k]] < keys[i + k];
            if (base[k] == size || data[base[k]] != keys[i + k])
                return false;
        }
        targets.insert(targets.end(), base.begin(), base.end());
    }
    for (; i < count; ++i) {
        size_t base = 0, len = size;
        while (len > 1) {
            const size_t half = len / 2;
            base = data[base + half] < keys[i] ? base + half : base;
            len -= half;
        }
        base += data[base] < keys[i];
        if (base == size || data[base] != keys[i])
            return false;
        targets.push_back(base);
    }
    return true;
}

// Small finite corrections can update central moments without reading untouched
// points. Keep the batch's mean/M2 in extended precision; retain the full scan
// for special values, removed extrema and ill-conditioned variance updates.
class OverwriteStatsUpdater {
    InMemorySeriesStats& stats;
    long double mean;
    long double m2;
    const double originalM2;
    bool usable;

    void addToSum(double value) {
        const double y = value - stats.sumCompensation;
        const double next = stats.sum + y;
        stats.sumCompensation = (next - stats.sum) - y;
        stats.sum = next;
    }

public:
    explicit OverwriteStatsUpdater(InMemorySeriesStats& current)
        : stats(current),
          mean(current.mean),
          m2(current.m2),
          originalM2(current.m2),
          usable(current.valid && current.count > 0 && std::isfinite(current.sum) &&
                 std::isfinite(current.sumCompensation) && std::isfinite(current.mean) && std::isfinite(current.m2) &&
                 current.m2 >= 0 && std::abs(current.mean) <= 1e6 * std::sqrt(current.m2 / current.count)) {}

    void replace(double oldValue, double newValue, uint64_t timestamp) {
        if (!usable)
            return;
        if (!std::isfinite(oldValue) || !std::isfinite(newValue) || (oldValue == stats.min && newValue > oldValue) ||
            (oldValue == stats.max && newValue < oldValue)) {
            usable = false;
            return;
        }
        if (oldValue != newValue) {
            const long double delta = static_cast<long double>(newValue) - oldValue;
            const long double nextMean = mean + delta / stats.count;
            m2 +=
                delta * ((static_cast<long double>(oldValue) - mean) + (static_cast<long double>(newValue) - nextMean));
            mean = nextMean;
            // Separate terms retain small new values that would round away in
            // a double-precision (newValue - oldValue) delta.
            addToSum(-oldValue);
            addToSum(newValue);
            stats.min = std::min(stats.min, newValue);
            stats.max = std::max(stats.max, newValue);
        }
        if (timestamp == stats.firstTimestamp)
            stats.firstValue = newValue;
        if (timestamp == stats.latestTimestamp)
            stats.latestValue = newValue;
    }

    bool finish() {
        const double updatedM2 = static_cast<double>(m2);
        if (!usable || m2 < 0 || !std::isfinite(updatedM2) || !std::isfinite(static_cast<double>(mean)) ||
            !std::isfinite(stats.sum) || !std::isfinite(stats.sumCompensation))
            return false;
        // Check binary scale boundaries, not just this batch's relative change.
        // Many small decreases can otherwise amplify the old summary's error
        // without any single batch removing half its M2. Crossing a lower
        // exponent forces a fresh scan, bounding that amplification across
        // successive corrections without storing another per-series counter.
        if (originalM2 > 0 && (updatedM2 == 0 || std::ilogb(updatedM2) < std::ilogb(originalM2)))
            return false;
        stats.mean = static_cast<double>(mean);
        stats.m2 = updatedM2;
        return true;
    }
};
// Keep extended-precision temporaries out of the common insert function's
// register allocation and stack frame, including appends and small-series scans.
[[gnu::noinline]] void overwriteDoubleValues(InMemorySeries<double>& series, const std::vector<double>& incoming,
                                             const std::vector<size_t>& targets) {
    OverwriteStatsUpdater updater(series.stats);
    auto& values = series.values();
    const auto& timestamps = series.timestamps();
    for (size_t i = 0; i < targets.size(); ++i) {
        const size_t target = targets[i];
        updater.replace(values[target], incoming[i], timestamps[target]);
        values[target] = incoming[i];
    }
    if (!updater.finish())
        series.recomputeStats();
}
}  // namespace

template <class T>
void InMemorySeries<T>::insert(TimeStarInsert<T>&& insertRequest) {
    const auto& srcTimestamps = insertRequest.getTimestamps();
    if (srcTimestamps.empty()) [[unlikely]]
        return;

    const auto timestampBatch = insertRequest.sharedTimestampBatch();
    const size_t newSize = srcTimestamps.size();

    // Plain in-order append of a sizeable batch: adopt it as a pending run.
    // Nothing is copied — the value vector is moved in and the timestamps_ are
    // kept by reference when the write's fields share them — and nothing can
    // collide, because every new timestamp is later than everything held and
    // the batch is strictly increasing. (A double series whose stats were
    // invalidated by a deletion takes the general path, which rebuilds them.)
    using timestar::memory_store_detail::RunAdoption;
    const RunAdoption adoption = timestar::memory_store_detail::runAdoption;
    if (newSize >= kMinAdoptedRunPoints && adoption != RunAdoption::Off &&
        (empty() || srcTimestamps.front() > lastTimestamp()) &&
        (timestampBatch ? timestampBatch->strictlyIncreasing
                        : std::adjacent_find(srcTimestamps.begin(), srcTimestamps.end(), std::greater_equal<>()) ==
                              srcTimestamps.end())) {
        bool statsUsable = true;
        if constexpr (std::is_same_v<T, double>) {
            statsUsable = stats.valid || empty();
        }
        if (statsUsable) {
            const bool forceCopy = adoption == RunAdoption::Foreign;
            PendingRun run;
            if (!forceCopy && timestar::isShardLocalMemory(srcTimestamps.data())) {
                if (timestampBatch) {
                    run.sharedTimestamps = timestampBatch->timestamps;
                } else {
                    run.ownedTimestamps = std::move(insertRequest.timestamps);
                }
            } else {
                run.ownedTimestamps.assign(srcTimestamps.begin(), srcTimestamps.end());
            }
            if constexpr (std::is_same_v<T, bool>) {
                // Bit-packed and an eighth of a byte a point: always copied
                // rather than asking the bitset where its words live.
                run.values.assign(insertRequest.values.begin(), insertRequest.values.end());
            } else {
                if (!forceCopy && timestar::isShardLocalMemory(insertRequest.values.data())) {
                    run.values = std::move(insertRequest.values);
                } else {
                    run.values.reserve(newSize);
                    run.values.insert(run.values.end(), std::make_move_iterator(insertRequest.values.begin()),
                                      std::make_move_iterator(insertRequest.values.end()));
                }
            }
            if constexpr (std::is_same_v<T, double>) {
                if (empty()) {
                    stats = InMemorySeriesStats{};
                }
                stats.update(run.values.data(), run.timestamps().data(), newSize, true);
            }
            pendingPoints_ += newSize;
            pending_.push_back(std::move(run));
            return;
        }
    }

    // Everything else works on the consolidated series.
    consolidate();
    const size_t oldSize = timestamps_.size();

    // Last-write-wins invariant: this method never leaves two points with the
    // same timestamp in the series. Duplicates within a batch keep the last
    // occurrence (request order); duplicates against existing data keep the
    // incoming value. Running stats are updated with the surviving new points
    // only. Corrections update finite statistics or rebuild when necessary.

    if (oldSize == 0) [[unlikely]] {
        // Series is empty -- take ownership of the entire vectors via move.
        // takeTimestamps() materializes shared timestamps_ into owned if needed.
        timestamps_ = insertRequest.takeTimestamps();
        values_ = std::move(insertRequest.values);

        // First insert -- check if already sorted using std::ranges::is_sorted.
        // In typical time-series workloads, data arrives chronologically, so this
        // check short-circuits the sort in the common case.
        if (timestampBatch ? !timestampBatch->sorted : !std::ranges::is_sorted(timestamps_)) [[unlikely]] {
            sortPaired(0, timestamps_.size());
        }
        if (!timestampBatch || !timestampBatch->strictlyIncreasing)
            dedupSuffixKeepLast(0);

        if constexpr (std::is_same_v<T, double>) {
            // An emptied series starts over: whatever the summary held
            // describes points that are gone. (MemoryStore erases a series it
            // empties, so this only matters to a series used on its own.)
            stats = InMemorySeriesStats{};
            stats.update(values_.data(), timestamps_.data(), timestamps_.size(), true);
        }
        return;
    }

    if constexpr (std::is_same_v<T, double>) {
        // A deletion invalidates the whole summary. Extending its stale count,
        // extrema or moments and marking it valid again would corrupt pushdown.
        if (!stats.valid)
            recomputeStats();
    }

    if (newSize > SIZE_MAX - oldSize) {
        throw std::runtime_error("InMemorySeries: size overflow");
    }
    const size_t totalSize = oldSize + newSize;
    // Small corrections to existing points need neither a full-vector merge
    // nor a timestamp copy. Locate all targets before mutating anything; a
    // missing timestamp falls through to the normal insert path. Request order
    // is retained, so repeated targets still keep the last incoming value.
    if (newSize <= oldSize / 8 && srcTimestamps.front() <= timestamps_.back()) {
        std::vector<size_t> targets;
        targets.reserve(newSize);
        // Tiny requests cannot overlap a group of searches. Keep the scalar
        // standard search here: warm single-key requests benefit from branch
        // prediction and avoid an out-of-line helper call.
        bool allFound = true;
        if (newSize < 8) {
            for (uint64_t ts : srcTimestamps) {
                const auto it = std::lower_bound(timestamps_.begin(), timestamps_.end(), ts);
                if (it == timestamps_.end() || *it != ts) {
                    allFound = false;
                    break;
                }
                targets.push_back(static_cast<size_t>(it - timestamps_.begin()));
            }
        } else {
            allFound = findCorrectionTargets(timestamps_.data(), oldSize, srcTimestamps.data(), newSize, targets);
        }
        if (allFound) {
            if constexpr (std::is_same_v<T, double>) {
                // For short series, the fused SIMD rebuild is cheaper than
                // updating central moments for each correction individually.
                if (oldSize / newSize < 128) {
                    for (size_t i = 0; i < newSize; ++i)
                        values_[targets[i]] = insertRequest.values[i];
                    recomputeStats();
                } else {
                    overwriteDoubleValues(*this, insertRequest.values, targets);
                }
            } else {
                for (size_t i = 0; i < newSize; ++i)
                    values_[targets[i]] = std::move(insertRequest.values[i]);
            }
            return;
        }
    }
    // Grow geometrically when more capacity is needed. An exact-size reserve
    // here would leave capacity == size after every append, forcing the NEXT
    // append to realloc and memcpy the entire series — O(L) copied bytes per
    // batch instead of amortized O(1).
    if (timestamps_.capacity() < totalSize) {
        const size_t growCap = std::max(totalSize, oldSize * 2);
        timestamps_.reserve(growCap);
        values_.reserve(growCap);
    }

    // Append new data. insert() with iterators is optimized by the STL to
    // perform a single memcpy/memmove for trivially copyable types (uint64_t, double, bool).
    timestamps_.insert(timestamps_.end(), srcTimestamps.begin(), srcTimestamps.end());
    values_.insert(values_.end(), std::make_move_iterator(insertRequest.values.begin()),
                   std::make_move_iterator(insertRequest.values.end()));

    // Sort the suffix if needed. In typical time-series workloads new data
    // arrives in chronological order, so the is_sorted scan short-circuits
    // the (stable) sort in the common case.
    if (timestampBatch
            ? !timestampBatch->sorted
            : !std::ranges::is_sorted(std::ranges::subrange(timestamps_.begin() + oldSize, timestamps_.end())))
        [[unlikely]] {
        sortPaired(oldSize, totalSize);
    }
    // Collapse duplicates within the batch: last write in request order wins.
    if (!timestampBatch || !timestampBatch->strictlyIncreasing)
        dedupSuffixKeepLast(oldSize);

    // Fast path: strictly appending (first new timestamp is later than the
    // old tail). Equal timestamps_ must NOT take this path — that's an
    // overwrite of the old tail point, handled by the merge.
    if (timestamps_[oldSize] > timestamps_[oldSize - 1]) [[likely]] {
        if constexpr (std::is_same_v<T, double>)
            stats.update(values_.data() + oldSize, timestamps_.data() + oldSize, timestamps_.size() - oldSize, true);
        return;
    }

    // Decide whether statistics can be extended before the merge moves the
    // suffix. Avoid calculating incoming stats only to discard them on LWW.
    if constexpr (std::is_same_v<T, double>) {
        bool willOverwrite = false;
        auto old = timestamps_.begin();
        const auto oldEnd = old + oldSize;
        for (size_t i = oldSize; i < timestamps_.size(); ++i) {
            old = std::lower_bound(old, oldEnd, timestamps_[i]);
            if (old != oldEnd && *old == timestamps_[i]) {
                willOverwrite = true;
                break;
            }
        }
        if (!willOverwrite)
            stats.update(values_.data() + oldSize, timestamps_.data() + oldSize, timestamps_.size() - oldSize, true);
    }

    // Both [0, oldSize) and [oldSize, end) are sorted and internally
    // duplicate-free. Merge in O(n); on timestamp collisions the suffix
    // (newer write) wins.
    const bool overwrote = mergePaired(oldSize);
    if (overwrote) {
        if constexpr (std::is_same_v<T, double>) {
            recomputeStats();
        }
    }
}

template <class T>
void InMemorySeries<T>::consolidateSlow() const {
    // Grow geometrically, as the append path does: a series that is read
    // while it is being written consolidates repeatedly, and an exact-size
    // reserve each time would recopy the whole series on every read.
    const size_t total = timestamps_.size() + pendingPoints_;
    if (timestamps_.capacity() < total) {
        const size_t growCap = timestamps_.empty() ? total : std::max(total, timestamps_.size() * 2);
        timestamps_.reserve(growCap);
        values_.reserve(growCap);
    }
    for (auto& run : pending_) {
        const auto& ts = run.timestamps();
        timestamps_.insert(timestamps_.end(), ts.begin(), ts.end());
        values_.insert(values_.end(), std::make_move_iterator(run.values.begin()),
                       std::make_move_iterator(run.values.end()));
    }
    pending_.clear();
    pending_.shrink_to_fit();
    pendingPoints_ = 0;
}

template <class T>
bool InMemorySeries<T>::dedupSuffixKeepLast(size_t from) {
    const size_t n = timestamps_.size();
    if (n - from < 2)
        return false;
    // Fast pre-check: no adjacent equal timestamps_ means nothing to collapse
    // (the suffix is sorted, so duplicates would be adjacent).
    if (std::adjacent_find(timestamps_.begin() + from, timestamps_.end()) == timestamps_.end()) [[likely]] {
        return false;
    }

    // Two-pointer compaction keeping the LAST value of each equal-timestamp
    // run. Stable sort preserved request order, so "last" == newest write.
    size_t w = from;
    for (size_t r = from + 1; r < n; ++r) {
        if (timestamps_[r] == timestamps_[w]) {
            values_[w] = std::move(values_[r]);
        } else {
            ++w;
            if (w != r) {
                timestamps_[w] = timestamps_[r];
                values_[w] = std::move(values_[r]);
            }
        }
    }
    ++w;
    timestamps_.resize(w);
    values_.resize(w);
    return true;
}

template <class T>
void InMemorySeries<T>::recomputeStats() {
    if constexpr (std::is_same_v<T, double>) {
        stats = InMemorySeriesStats{};
        stats.update(values_.data(), timestamps_.data(), timestamps_.size(), true);
    }
}

template <class T>
void InMemorySeries<T>::sortPaired(size_t from, size_t to) {
    const size_t count = to - from;
    if (count <= 1) [[unlikely]]
        return;

    // Build index array for the range [from, to). Sorting indices lets us apply
    // the same permutation to both timestamps_ and values_ in a single pass.
    std::vector<size_t> indices(count);
    std::iota(indices.begin(), indices.end(), from);

    // Stable: equal timestamps_ keep request order so last-write-wins dedup
    // can identify the newest write as the last element of each run.
    std::stable_sort(indices.begin(), indices.end(),
                     [this](size_t a, size_t b) { return timestamps_[a] < timestamps_[b]; });

    // Apply permutation via temporary timestamp vector. Values are applied
    // differently based on type: std::vector<bool> uses proxy references
    // that don't support std::move, so we use copy for that specialization.
    std::vector<uint64_t> sortedTs(count);
    for (size_t i = 0; i < count; ++i) {
        sortedTs[i] = timestamps_[indices[i]];
    }
    std::copy(sortedTs.begin(), sortedTs.end(), timestamps_.begin() + from);

    if constexpr (std::is_same_v<T, bool>) {
        std::vector<bool> sortedVals(count);
        for (size_t i = 0; i < count; ++i) {
            sortedVals[i] = static_cast<bool>(values_[indices[i]]);
        }
        std::copy(sortedVals.begin(), sortedVals.end(), values_.begin() + from);
    } else {
        std::vector<T> sortedVals(count);
        for (size_t i = 0; i < count; ++i) {
            sortedVals[i] = std::move(values_[indices[i]]);
        }
        std::move(sortedVals.begin(), sortedVals.end(), values_.begin() + from);
    }
}

template <class T>
bool InMemorySeries<T>::mergePaired(size_t midpoint) {
    const size_t n = timestamps_.size();
    if (midpoint == 0 || midpoint >= n) [[unlikely]]
        return false;

    // The prefix before the first incoming timestamp cannot change. Leave it
    // in place: late arrivals near the tail should not copy the whole series.
    const size_t mergeStart = static_cast<size_t>(
        std::lower_bound(timestamps_.begin(), timestamps_.begin() + midpoint, timestamps_[midpoint]) -
        timestamps_.begin());

    // Merge two sorted runs: [mergeStart, midpoint) and [midpoint, n).
    // We use a single-pass merge into temporary buffers. This is O(n) time
    // and O(n) space, but avoids the index indirection overhead of the old
    // approach and has better cache locality (sequential reads from both runs,
    // sequential writes to output).
    // Both runs are internally duplicate-free; on equal timestamps_ the suffix
    // run (newer write) wins and the prefix point is dropped (LWW).
    std::vector<uint64_t> mergedTs;
    std::vector<T> mergedVals;
    mergedTs.reserve(n - mergeStart);
    mergedVals.reserve(n - mergeStart);

    bool overwrote = false;
    size_t i = mergeStart, j = midpoint;
    while (i < midpoint && j < n) {
        if (timestamps_[i] < timestamps_[j]) {
            mergedTs.push_back(timestamps_[i]);
            mergedVals.push_back(std::move(values_[i]));
            ++i;
        } else if (timestamps_[j] < timestamps_[i]) {
            mergedTs.push_back(timestamps_[j]);
            mergedVals.push_back(std::move(values_[j]));
            ++j;
        } else {
            // Equal timestamp: keep the incoming value, drop the old point.
            mergedTs.push_back(timestamps_[j]);
            mergedVals.push_back(std::move(values_[j]));
            ++i;
            ++j;
            overwrote = true;
        }
    }
    // Append remaining elements from whichever run is not exhausted
    while (i < midpoint) {
        mergedTs.push_back(timestamps_[i]);
        mergedVals.push_back(std::move(values_[i]));
        ++i;
    }
    while (j < n) {
        mergedTs.push_back(timestamps_[j]);
        mergedVals.push_back(std::move(values_[j]));
        ++j;
    }

    if (mergeStart == 0) {
        timestamps_ = std::move(mergedTs);
        values_ = std::move(mergedVals);
    } else {
        const size_t mergedSize = mergeStart + mergedTs.size();
        std::move(mergedTs.begin(), mergedTs.end(), timestamps_.begin() + mergeStart);
        std::move(mergedVals.begin(), mergedVals.end(), values_.begin() + mergeStart);
        timestamps_.resize(mergedSize);
        values_.resize(mergedSize);
    }
    return overwrote;
}

seastar::future<> MemoryStore::close() {
    if (closed)
        co_return;
    closed = true;

    if (wal) {
        timestar::memory_log.debug("Closing WAL for memory store {}", sequenceNumber);
        co_await wal->close();
    }
}

seastar::future<> MemoryStore::initWAL() {
    wal = std::make_unique<WAL>(sequenceNumber);
    co_await wal->init(this, false);  // false = not recovery, create fresh WAL
}

size_t MemoryStore::walSizeOnDisk() const {
    return wal ? wal->getCurrentSize() : 0;
}

seastar::future<> MemoryStore::removeWAL() {
    if (wal) {
        co_await wal->remove();
        wal.reset();
    }
}

seastar::future<> MemoryStore::initFromWAL(std::string filename) {
    WALReader reader(filename);
    co_await reader.readAll(this);
    // Don't initialize a new WAL here - this memory store is being recovered
    // from an existing WAL that will be converted to TSM and removed
}

bool MemoryStore::isFull() const {
    // Resident bytes are checked independently of the WAL: the WAL estimates
    // below are all about on-disk size; neither sees per-series overhead or
    // uncompressed string payloads, so a high-cardinality or string-heavy
    // store can hold hundreds of MB while they still read as near-zero.
    if (residentBytesEstimate >= residentBytesThreshold()) {
        return true;
    }

    if (!wal) {
        return false;
    }

    // Use the larger of actual WAL size and cumulative estimated size.
    // Compression can make the actual WAL size much smaller than estimates,
    // so relying only on actual size may never trigger rollover.
    size_t walSize = wal->getCurrentSize();
    size_t effectiveSize = std::max(walSize, estimatedAccumulatedSize);
    return effectiveSize >= walSizeThreshold();
}

template <class T>
void MemoryStore::insertMemory(TimeStarInsert<T>&& insertRequest) {
    // In-memory insert
    SeriesId128 seriesId = insertRequest.seriesId128();

    // Account resident cost BEFORE inserting. Point cost is the real in-memory
    // width (timestamp + value, plus the payload for strings), not the compressed
    // WAL estimate; series cost is charged once, when the series first appears.
    const size_t pointCount = insertRequest.getTimestamps().size();
    size_t pointBytes = pointCount * (sizeof(uint64_t) + sizeof(T));
    if constexpr (std::is_same_v<T, std::string>) {
        for (const auto& v : insertRequest.values) {
            pointBytes += v.capacity();
        }
    }
    // robin_map's operator[] returns a mutable reference, creating entry if needed.
    // Default-constructed variant will be InMemorySeries<double> (first alternative).
    // Newness is derived from the size delta — no separate find() probe on the
    // hottest path in the system.
    const size_t sizeBefore = series.size();
    auto& variantSeries = series[seriesId];
    const bool isNewSeries = (series.size() != sizeBefore);
    residentBytesEstimate += pointBytes + (isNewSeries ? PER_SERIES_OVERHEAD_BYTES : 0);

    // Single variant access: std::get_if returns a pointer if the type matches, nullptr otherwise.
    // This replaces the previous 2-3 separate variant accesses
    // (std::visit for empty check + std::holds_alternative for type check + std::get for access).
    auto* typedSeries = std::get_if<InMemorySeries<T>>(&variantSeries);

    if (!typedSeries || typedSeries->empty()) {
        // Either wrong type (e.g. default-constructed double when T != double) or empty.
        // Before overwriting, check whether the current variant has existing data of a different type.
        if (typedSeries == nullptr) {
            bool hasData = std::visit([](const auto& s) { return !s.empty(); }, variantSeries);
            if (hasData) {
                throw std::runtime_error("Type mismatch: series " + insertRequest.seriesKey() +
                                         " already exists with a different type");
            }
        }
        variantSeries = InMemorySeries<T>();
        typedSeries = &std::get<InMemorySeries<T>>(variantSeries);
    }

    // A sizeable batch whose shared timestamps are another shard's memory:
    // hand the series this shard's copy instead, made once per write.
    if (const auto& batch = insertRequest.sharedTimestampBatch();
        batch && batch->timestamps->size() >= InMemorySeries<T>::kMinAdoptedRunPoints &&
        timestar::memory_store_detail::runAdoption == timestar::memory_store_detail::RunAdoption::Auto &&
        !timestar::isShardLocalMemory(batch->timestamps->data())) {
        insertRequest.rebindSharedTimestamps(localTimestampBatch(batch));
    }

    typedSeries->insert(std::move(insertRequest));
}

std::shared_ptr<const SharedTimestampBatch> MemoryStore::localTimestampBatch(
    const std::shared_ptr<const SharedTimestampBatch>& foreign) {
    // Same control block == same batch: the weak reference keeps that block
    // from being reused for another batch while it is remembered here.
    const bool sameBatch =
        !lastForeignTimestamps_.owner_before(foreign) && !foreign.owner_before(lastForeignTimestamps_);
    if (!sameBatch || !lastLocalTimestamps_) {
        auto local = std::make_shared<SharedTimestampBatch>(*foreign);
        local->timestamps = std::make_shared<const std::vector<uint64_t>>(*foreign->timestamps);
        lastLocalTimestamps_ = std::move(local);
        lastForeignTimestamps_ = foreign;
    }
    return lastLocalTimestamps_;
}

template <class T>
bool MemoryStore::wouldExceedThreshold(TimeStarInsert<T>& insertRequest) {
    size_t sz = 0;
    return wouldExceedThreshold(insertRequest, sz);
}

template <class T>
bool MemoryStore::wouldExceedThreshold(TimeStarInsert<T>& insertRequest, size_t& outEstimatedSize) {
    outEstimatedSize = wal ? wal->estimateInsertSize(insertRequest) : 0;

    // Roll over on RESIDENT bytes first — deliberately not gated on the WAL.
    // The WAL estimates below model compressed on-disk size, which wildly
    // under-counts RAM for high-cardinality or string-heavy data (see
    // residentBytesEstimate). The projection uses only the fixed point
    // width — string payloads are charged as they land in insertMemory(),
    // so at worst rollover triggers one insert late.
    const size_t incomingResident = insertRequest.getTimestamps().size() * (sizeof(uint64_t) + sizeof(T));
    if ((residentBytesEstimate + incomingResident) >= residentBytesThreshold()) {
        return true;
    }

    if (!wal) {
        return false;
    }

    // Use the larger of actual WAL size and cumulative estimated size for
    // the base, ensuring rollover triggers even when compression is highly
    // effective and actual WAL size grows slowly.
    size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);

    return (effectiveSize + outEstimatedSize) >= walSizeThreshold();
}

template <class T>
seastar::future<bool> MemoryStore::insert(TimeStarInsert<T>& insertRequest) {
    if (closed)
        throw std::runtime_error("MemoryStore is closed");

    // Check if this insert would exceed the WAL segment threshold.
    // The estimated size is returned via outEstimatedSize to avoid
    // recomputing it below (eliminates double-estimation).
    size_t thisEstimatedSize = 0;
    bool needsRollover = wouldExceedThreshold(insertRequest, thisEstimatedSize);
    if (needsRollover) {
        // Don't insert - signal that rollover is needed
        timestar::memory_log.debug("Insert would exceed WAL segment limit, signaling rollover needed");
        co_return true;
    }

    if (wal) {
        auto walResult = co_await wal->insert(insertRequest);
        if (walResult == WALInsertResult::RolloverNeeded) [[unlikely]] {
            co_return true;
        }
        LOG_INSERT_PATH(timestar::memory_log, trace, "WAL insert completed for series: {}", insertRequest.seriesKey());
    }

    estimatedAccumulatedSize += thisEstimatedSize;
    insertMemory(std::move(insertRequest));

    co_return false;
}

template <class T>
seastar::future<bool> MemoryStore::insertBatch(std::vector<TimeStarInsert<T>>& insertRequests,
                                               size_t preComputedBatchSize) {
    if (closed)
        throw std::runtime_error("MemoryStore is closed");

    if (insertRequests.empty()) {
        co_return false;  // No work to do
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto start_memory_batch = std::chrono::high_resolution_clock::now();
#endif
    LOG_INSERT_PATH(timestar::memory_log, info, "[PERF] [MEMORY] Batch insert started for {} requests",
                    insertRequests.size());

    // Compute the estimated batch size if not pre-computed
    size_t batchEstimate = preComputedBatchSize;
    if (batchEstimate == 0 && wal) {
        for (auto& insertRequest : insertRequests) {
            batchEstimate += wal->estimateInsertSize(insertRequest);
        }
    }

    // Check if this batch would exceed the WAL threshold.
    // Use the larger of actual WAL size and cumulative estimated size as the
    // base. Compression can make the actual WAL size much smaller than the
    // uncompressed estimate, so relying only on actual compressed size may
    // never trigger rollover for highly compressible data.
    bool needsRollover = false;
    if (wal) {
        size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);
        needsRollover = (effectiveSize + batchEstimate) >= walSizeThreshold();
    }
    if (!needsRollover) {
        // Roll over on RESIDENT bytes too — not gated on the WAL. The WAL
        // estimate models compressed on-disk size and under-counts RAM badly
        // for high-cardinality or string-heavy batches (see
        // residentBytesEstimate). Fixed point width only; string payloads are
        // charged as they land in insertMemory().
        size_t batchPoints = 0;
        for (const auto& insertRequest : insertRequests) {
            batchPoints += insertRequest.getTimestamps().size();
        }
        const size_t batchResident = batchPoints * (sizeof(uint64_t) + sizeof(T));
        needsRollover = (residentBytesEstimate + batchResident) >= residentBytesThreshold();
    }
    if (needsRollover) {
        // Don't insert - signal that rollover is needed
        LOG_INSERT_PATH(timestar::memory_log, debug, "Batch insert would exceed WAL limit, signaling rollover needed");
        co_return true;
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto start_wal_insert = std::chrono::high_resolution_clock::now();
#endif
    if (wal) {
        auto walResult = co_await wal->insertBatch(insertRequests);
        if (walResult == WALInsertResult::RolloverNeeded) [[unlikely]] {
            co_return true;
        }
    }

    // Increment AFTER the WAL insert succeeds — if the WAL throws, we must not
    // leak the estimate (there is no corresponding decrement on the exception path).
    estimatedAccumulatedSize += batchEstimate;

#if TIMESTAR_LOG_INSERT_PATH
    auto start_memory_insert = std::chrono::high_resolution_clock::now();
#endif
    for (auto& insertRequest : insertRequests) {
        // Bind the cached key by const-ref: insertMemory() moves out only values and
        // timestamps, never _seriesKey, and insertRequest stays a live lvalue, so the
        // reference is valid in the catch block (the only reader). Avoids a per-point
        // string copy on the happy path.
        const std::string& key = insertRequest.seriesKey();
        try {
            insertMemory(std::move(insertRequest));
        } catch (const std::exception& e) {
            // Do NOT swallow this. The WAL already has the point, so it is
            // durable and will reappear on restart — but it is absent from the
            // memory store, so queries cannot see it until then. Reporting
            // success here told the client the write landed when it had not,
            // which is silent data loss from the caller's point of view.
            //
            // Rethrowing surfaces it through the per-shard partial-failure path
            // in the write handler. A client retry is safe: duplicate points
            // are last-write-wins, so replaying the same point is idempotent.
            timestar::memory_log.error(
                "insertMemory failed for series {}: {} — point is in the WAL but not queryable "
                "until restart; reporting the write as failed",
                key, e.what());
            throw;
        }
    }
#if TIMESTAR_LOG_INSERT_PATH
    auto end_memory_batch = std::chrono::high_resolution_clock::now();
    auto wal_duration = std::chrono::duration_cast<std::chrono::microseconds>(start_memory_insert - start_wal_insert);
    auto mem_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_memory_batch - start_memory_insert);
    LOG_INSERT_PATH(timestar::memory_log, info, "[PERF] [MEMORY] WAL batch: {}μs, memory: {}μs", wal_duration.count(),
                    mem_duration.count());
#endif

    co_return false;
}

std::optional<TSMValueType> MemoryStore::getSeriesType(const SeriesId128& seriesId) const {
    // Compile-time guard: variant order must match TSMValueType enum values.
    using V = decltype(series)::mapped_type;
    static_assert(std::is_same_v<std::variant_alternative_t<0, V>, InMemorySeries<double>>);
    static_assert(std::is_same_v<std::variant_alternative_t<1, V>, InMemorySeries<bool>>);
    static_assert(std::is_same_v<std::variant_alternative_t<2, V>, InMemorySeries<std::string>>);
    static_assert(std::is_same_v<std::variant_alternative_t<3, V>, InMemorySeries<int64_t>>);

    auto it = series.find(seriesId);

    if (it == series.end())
        return {};

    return static_cast<TSMValueType>(it->second.index());
}

// Explicit template instantiations for every TimeStar value type.
// The macro fans out (double, bool, std::string, int64_t) — see
// value_type_dispatch.hpp.
#define INST_INMEM(T)                                             \
    template void InMemorySeries<T>::insert(TimeStarInsert<T>&&); \
    template void InMemorySeries<T>::sortPaired(size_t, size_t);  \
    template bool InMemorySeries<T>::mergePaired(size_t);         \
    template bool InMemorySeries<T>::dedupSuffixKeepLast(size_t); \
    template void InMemorySeries<T>::recomputeStats();
TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(INST_INMEM)
#undef INST_INMEM

#define INST_STORE(T)                                                                                    \
    template seastar::future<bool> MemoryStore::insert<T>(TimeStarInsert<T>&);                           \
    template seastar::future<bool> MemoryStore::insertBatch<T>(std::vector<TimeStarInsert<T>>&, size_t); \
    template void MemoryStore::insertMemory<T>(TimeStarInsert<T>&&);                                     \
    template bool MemoryStore::wouldExceedThreshold<T>(TimeStarInsert<T>&);                              \
    template bool MemoryStore::wouldExceedThreshold<T>(TimeStarInsert<T>&, size_t&);
TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(INST_STORE)
#undef INST_STORE

void MemoryStore::deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    timestar::memory_log.debug("Deleting range for series {} from {} to {}", seriesId.toHex(), startTime, endTime);

    // Find the series in memory
    auto it = series.find(seriesId);
    if (it == series.end()) {
        return;  // No data to delete
    }

    auto& variantSeries = it.value();

    // Actually remove data from memory based on variant type
    std::visit(
        [&](auto& inMemorySeries) {
            auto& timestamps = inMemorySeries.timestamps();
            auto& values = inMemorySeries.values();

            // Binary search for the deletion range on sorted timestamps
            auto startIt = std::lower_bound(timestamps.begin(), timestamps.end(), startTime);
            auto endIt = std::upper_bound(timestamps.begin(), timestamps.end(), endTime);

            if (startIt == endIt) {
                return;  // Nothing to delete
            }

            size_t startIdx = static_cast<size_t>(startIt - timestamps.begin());
            size_t endIdx = static_cast<size_t>(endIt - timestamps.begin());

            timestamps.erase(timestamps.begin() + startIdx, timestamps.begin() + endIdx);
            values.erase(values.begin() + startIdx, values.begin() + endIdx);

            // Invalidate running stats after deletion — they may no longer
            // reflect the actual data (min/max could have been in deleted range).
            inMemorySeries.stats.valid = false;
        },
        variantSeries);

    // If all data was removed, remove the series entirely
    bool isEmpty = std::visit([](const auto& inMemorySeries) { return inMemorySeries.empty(); }, variantSeries);

    if (isEmpty) {
        series.erase(seriesId);
    }
}
