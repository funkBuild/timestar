#include "aggregator.hpp"

#include "group_key.hpp"
#include "http_query_handler.hpp"  // For SeriesResult
#include "yield_policy.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <queue>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <stdexcept>
#include <unordered_map>

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
// Outputs merged (timestamps, values, counts). O(N) time, zero merge overhead.
static seastar::future<> foldAlignedRawValues(const std::vector<PartialAggregationResult*>& partials,
                                              AggregationMethod method, std::vector<uint64_t>& outTs,
                                              std::vector<double>& outVals, std::vector<size_t>& outCounts) {
    const size_t N = partials[0]->sortedTimestamps.size();
    const size_t K = partials.size();

    outTs = std::move(partials[0]->sortedTimestamps);
    outVals = std::move(partials[0]->sortedValues);
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

// Core N-way heap merge over PartialAggregationResult partials.
//
// `init(partial, pos)` is invoked the first time a new timestamp is seen,
// and should produce the initial Item for that timestamp.
// `fold(item, partial, pos)` is invoked for every subsequent duplicate
// timestamp and should mutate `item` to absorb the duplicate.
//
// Pulls per-partial elements off a min-heap ordered by timestamp; same
// semantics as the unrolled nWayMerge* variants below.
template <class Item, class Init, class Fold>
static seastar::future<> nWayHeapMerge(const std::vector<PartialAggregationResult*>& partials,
                                       std::vector<uint64_t>& outTs, std::vector<Item>& outItems, Init init, Fold fold,
                                       size_t yieldChunk = kYieldChunkPoints) {
    const size_t K = partials.size();

    size_t totalSize = 0;
    for (const auto* p : partials)
        totalSize += p->sortedTimestamps.size();
    outTs.reserve(totalSize);
    outItems.reserve(totalSize);

    struct HeapEntry {
        uint64_t ts;
        size_t partialIdx;
        size_t pos;
        bool operator>(const HeapEntry& o) const { return ts > o.ts; }
    };

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;
    for (size_t i = 0; i < K; ++i) {
        if (!partials[i]->sortedTimestamps.empty()) {
            heap.push({partials[i]->sortedTimestamps[0], i, 0});
        }
    }

    size_t sinceYield = 0;
    while (!heap.empty()) {
        auto [ts, pIdx, pos] = heap.top();
        heap.pop();

        if (!outTs.empty() && outTs.back() == ts) {
            fold(outItems.back(), *partials[pIdx], pos);
        } else {
            outTs.push_back(ts);
            outItems.push_back(init(*partials[pIdx], pos));
        }

        const size_t nextPos = pos + 1;
        if (nextPos < partials[pIdx]->sortedTimestamps.size()) {
            heap.push({partials[pIdx]->sortedTimestamps[nextPos], pIdx, nextPos});
        }

        if (++sinceYield >= yieldChunk) {
            sinceYield = 0;
            co_await seastar::coroutine::maybe_yield();
        }
    }
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
                                            std::vector<size_t>& outCounts) {
    {
        size_t totalSize = 0;
        for (const auto* p : partials)
            totalSize += p->sortedTimestamps.size();
        outCounts.reserve(totalSize);
    }
    auto foldDuplicate = [method](double existing, double incoming) -> double {
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
    return nWayHeapMerge<double>(
        partials, outTs, outVals,
        [&outCounts](const PartialAggregationResult& p, size_t pos) {
            const double v = p.sortedValues[pos];
            // NaN as the first contribution: keep it as the slot value but
            // count 0 — a later non-NaN contribution replaces it.
            outCounts.push_back(std::isnan(v) ? 0 : 1);
            return v;
        },
        [&outCounts, foldDuplicate](double& item, const PartialAggregationResult& p, size_t pos) {
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
        });
}

// N-way heap merge producing AggregationStates from MIXED partials: raw
// (sortedValues) and state (sortedStates) partials in the same group.  Raw
// elements build and fold through addValue — which skips NaN, so the raw and
// state routes cannot disagree — and no intermediate sortedStates vector is
// materialized (the old convert-first pre-pass wrote ~420MB of transient
// state for a 3.5M-point STDDEV merge).  Uses the smaller state yield chunk:
// each element moves a 120-byte AggregationState.
static seastar::future<> nWayMergeStates(std::vector<PartialAggregationResult*>& partials, std::vector<uint64_t>& outTs,
                                         std::vector<AggregationState>& outStates, AggregationMethod method) {
    const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
    return nWayHeapMerge<AggregationState>(
        partials, outTs, outStates,
        [needsRaw](const PartialAggregationResult& p, size_t pos) {
            if (!p.sortedStates.empty()) {
                // The original implementation moved out of partials[pIdx]->sortedStates[pos];
                // const_cast preserves that semantic (callers don't reuse the partials
                // after merge).
                return std::move(const_cast<PartialAggregationResult&>(p).sortedStates[pos]);
            }
            AggregationState s;
            s.collectRaw = needsRaw;
            s.addValue(p.sortedValues[pos], p.sortedTimestamps[pos]);
            return s;
        },
        [method](AggregationState& item, const PartialAggregationResult& p, size_t pos) {
            if (!p.sortedStates.empty()) {
                item.mergeForMethod(std::move(const_cast<PartialAggregationResult&>(p).sortedStates[pos]), method);
            } else {
                item.addValue(p.sortedValues[pos], p.sortedTimestamps[pos]);
            }
        },
        kYieldChunkStates);
}

seastar::future<std::vector<PartialAggregationResult>> Aggregator::createPartialAggregations(
    std::vector<timestar::SeriesResult> seriesResults, AggregationMethod method, uint64_t interval,
    const std::vector<std::string>& groupByTags) {
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
                uint64_t bucketTime = (timestamps[i] / interval) * interval;
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

seastar::future<std::vector<GroupedAggregationResult>> Aggregator::mergePartialAggregationsGrouped(
    std::vector<PartialAggregationResult>& partialResults, AggregationMethod method) {
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
            // Merge pre-aggregated states (O(1) per bucket)
            std::unordered_map<uint64_t, AggregationState> mergedStates;

            // Pre-reserve using the sum of all partial bucket counts as an upper bound.
            // The actual bucket count may be lower if partials share buckets, but this
            // avoids rehashing during the merge loop.
            {
                size_t totalBuckets = 0;
                for (const auto* partial : groupPartials) {
                    totalBuckets += partial->bucketStates.size();
                }
                mergedStates.reserve(totalBuckets);
            }

            // Merge all partial states for each bucket; try_emplace avoids
            // default-constructing a state when the bucket already exists.
            // Partials are consumed here, so states are moved rather than
            // copied — for MEDIAN this avoids deep-copying rawValues per bucket.
            for (auto* partial : groupPartials) {
                for (auto& [bucketTime, state] : partial->bucketStates) {
                    auto [it, inserted] = mergedStates.try_emplace(bucketTime, std::move(state));
                    if (!inserted) {
                        it->second.mergeForMethod(std::move(state), method);
                    }
                    if (++sinceYield >= kYieldChunkPoints) {
                        sinceYield = 0;
                        co_await seastar::coroutine::maybe_yield();
                    }
                }
            }

            groupedResult.points.reserve(mergedStates.size());

            // Extract final aggregated values from merged states
            for (const auto& [bucketTime, state] : mergedStates) {
                AggregatedPoint point;
                point.timestamp = bucketTime;
                point.count = state.count;
                point.value = state.getValue(method);
                groupedResult.points.push_back(point);
                if (++sinceYield >= kYieldChunkPoints) {
                    sinceYield = 0;
                    co_await seastar::coroutine::maybe_yield();
                }
            }

            // Sort points by timestamp
            std::sort(groupedResult.points.begin(), groupedResult.points.end(),
                      [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

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
                        if (method == AggregationMethod::COUNT) {
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
                        }
                    } else {
                        // Multiple partials — use O(KN log K) N-way merge.
                        // Fast path: if all partials share identical timestamps,
                        // use element-wise SIMD fold (O(N), zero merge overhead).
                        std::vector<uint64_t> mergedTs;
                        std::vector<double> mergedVals;
                        std::vector<size_t> mergedCounts;

                        if (allTimestampsIdentical(groupPartials) && !co_await anyRawNaN(groupPartials)) {
                            co_await foldAlignedRawValues(groupPartials, method, mergedTs, mergedVals, mergedCounts);
                        } else {
                            co_await nWayMergeRawValues(groupPartials, method, mergedTs, mergedVals, mergedCounts);
                        }

                        groupedResult.points.reserve(mergedTs.size());
                        for (size_t i = 0; i < mergedTs.size(); ++i) {
                            double finalValue = mergedVals[i];
                            if (mergedCounts[i] == 0) [[unlikely]] {
                                // Every contribution at this timestamp was NaN:
                                // the value slot already holds NaN and no method
                                // computes over an empty set (getValue() on a
                                // count-0 state is NaN) — COUNT included.
                                finalValue = std::numeric_limits<double>::quiet_NaN();
                            } else if (method == AggregationMethod::AVG && mergedCounts[i] > 1) {
                                finalValue /= mergedCounts[i];
                            } else if (method == AggregationMethod::COUNT) {
                                finalValue = static_cast<double>(mergedCounts[i]);
                            }
                            groupedResult.points.push_back({mergedTs[i], finalValue, mergedCounts[i]});
                            if (++sinceYield >= kYieldChunkPoints) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }
                    }
                } else {
                    // State-needing methods (SPREAD/STDDEV/STDVAR/MEDIAN), or a
                    // mix of raw and state partials.  nWayMergeStates consumes
                    // raw and state partials directly (raw elements fold via
                    // addValue, which skips NaN) — no conversion pre-pass.
                    const bool needsRaw =
                        (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
                    if (groupPartials.size() == 1 && groupPartials[0]->sortedStates.empty()) {
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
                        std::vector<AggregationState> mergedStates;
                        co_await nWayMergeStates(groupPartials, mergedTs, mergedStates, method);

                        groupedResult.points.reserve(mergedTs.size());
                        for (size_t i = 0; i < mergedTs.size(); ++i) {
                            groupedResult.points.push_back(
                                {mergedTs[i], mergedStates[i].getValue(method), mergedStates[i].count});
                            if (++sinceYield >= kYieldChunkPoints) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }
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