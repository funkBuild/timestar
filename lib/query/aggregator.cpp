#include "aggregator.hpp"

#include "group_key.hpp"
#include "http_query_handler.hpp"  // For SeriesResult
#include "simd_aggregator.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <unordered_map>

namespace timestar {

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

double Aggregator::calculateAvg(const std::vector<double>& values) {
    if (values.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    // Use SIMD when available for better performance
    if (simd::SimdAggregator::isAvx2Available()) {
        return simd::SimdAggregator::calculateAvg(values.data(), values.size());
    }
    double sum = std::accumulate(values.begin(), values.end(), 0.0);
    return sum / values.size();
}

double Aggregator::calculateMin(const std::vector<double>& values) {
    if (values.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    // Use SIMD when available for better performance
    if (simd::SimdAggregator::isAvx2Available()) {
        return simd::SimdAggregator::calculateMin(values.data(), values.size());
    }
    return *std::min_element(values.begin(), values.end());
}

double Aggregator::calculateMax(const std::vector<double>& values) {
    if (values.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    // Use SIMD when available for better performance
    if (simd::SimdAggregator::isAvx2Available()) {
        return simd::SimdAggregator::calculateMax(values.data(), values.size());
    }
    return *std::max_element(values.begin(), values.end());
}

double Aggregator::calculateSum(const std::vector<double>& values) {
    if (values.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    // Use SIMD when available for better performance
    if (simd::SimdAggregator::isAvx2Available()) {
        return simd::SimdAggregator::calculateSum(values.data(), values.size());
    }
    return std::accumulate(values.begin(), values.end(), 0.0);
}

// ============================================================================
// DISTRIBUTED AGGREGATION
// ============================================================================

// Merge sorted raw (timestamps, values) into an existing sorted (timestamps, states).
// Both inputs must be sorted by timestamp. Duplicate timestamps are merged via
// AggregationState::addValue. O(n+m) time, O(n+m) space for the output buffer.
static void mergeSortedRawInto(std::vector<uint64_t>& baseTs, std::vector<AggregationState>& baseStates,
                               const std::vector<uint64_t>& newTs, const std::vector<double>& newValues) {
    // Inherit collectRaw from existing states so new states match
    const bool needsRaw = !baseStates.empty() && baseStates[0].collectRaw;

    std::vector<uint64_t> mergedTs;
    std::vector<AggregationState> mergedStates;
    mergedTs.reserve(baseTs.size() + newTs.size());
    mergedStates.reserve(baseTs.size() + newTs.size());

    size_t i = 0, j = 0;
    while (i < baseTs.size() && j < newTs.size()) {
        if (baseTs[i] < newTs[j]) {
            mergedTs.push_back(baseTs[i]);
            mergedStates.push_back(std::move(baseStates[i]));
            i++;
        } else if (baseTs[i] > newTs[j]) {
            mergedTs.push_back(newTs[j]);
            AggregationState s;
            s.collectRaw = needsRaw;
            s.addValue(newValues[j], newTs[j]);
            mergedStates.push_back(s);
            j++;
        } else {
            // Same timestamp: merge into existing state
            mergedTs.push_back(baseTs[i]);
            AggregationState s = std::move(baseStates[i]);
            s.addValue(newValues[j], newTs[j]);
            mergedStates.push_back(std::move(s));
            i++;
            j++;
        }
    }
    while (i < baseTs.size()) {
        mergedTs.push_back(baseTs[i]);
        mergedStates.push_back(std::move(baseStates[i]));
        i++;
    }
    while (j < newTs.size()) {
        mergedTs.push_back(newTs[j]);
        AggregationState s;
        s.collectRaw = needsRaw;
        s.addValue(newValues[j], newTs[j]);
        mergedStates.push_back(s);
        j++;
    }

    baseTs = std::move(mergedTs);
    baseStates = std::move(mergedStates);
}

// Merge two sorted (timestamps, values) raw-value vectors. Used in the reduce
// phase for pushdown results that carry compact raw doubles instead of
// AggregationState objects. Duplicate timestamps are folded via the provided
// method: for most methods (single-value-per-timestamp), duplicates won't
// occur within a single series pushdown, but can appear when merging partials
// from different shards that share timestamps.
// Returns merged (timestamps, values, counts) suitable for building
// AggregatedPoints directly.
static void mergeSortedRawValuesInto(std::vector<uint64_t>& baseTs, std::vector<double>& baseVals,
                                     std::vector<size_t>& baseCounts, const std::vector<uint64_t>& newTs,
                                     const std::vector<double>& newVals, AggregationMethod method) {
    std::vector<uint64_t> mergedTs;
    std::vector<double> mergedVals;
    std::vector<size_t> mergedCounts;
    mergedTs.reserve(baseTs.size() + newTs.size());
    mergedVals.reserve(baseTs.size() + newTs.size());
    mergedCounts.reserve(baseTs.size() + newTs.size());

    size_t i = 0, j = 0;
    while (i < baseTs.size() && j < newTs.size()) {
        if (baseTs[i] < newTs[j]) {
            mergedTs.push_back(baseTs[i]);
            mergedVals.push_back(baseVals[i]);
            mergedCounts.push_back(baseCounts[i]);
            i++;
        } else if (baseTs[i] > newTs[j]) {
            mergedTs.push_back(newTs[j]);
            mergedVals.push_back(newVals[j]);
            mergedCounts.push_back(1);
            j++;
        } else {
            // Same timestamp: fold using lightweight merge
            mergedTs.push_back(baseTs[i]);
            size_t cnt = baseCounts[i] + 1;
            double v;
            switch (method) {
                case AggregationMethod::AVG:
                case AggregationMethod::SUM:
                    v = baseVals[i] + newVals[j];
                    break;
                case AggregationMethod::MIN:
                    v = std::min(baseVals[i], newVals[j]);
                    break;
                case AggregationMethod::MAX:
                case AggregationMethod::LATEST:
                    v = std::max(baseVals[i], newVals[j]);
                    break;
                case AggregationMethod::FIRST:
                    v = baseVals[i];
                    break;
                default:
                    v = baseVals[i] + newVals[j];
                    break;  // SUM-like for COUNT etc.
            }
            mergedVals.push_back(v);
            mergedCounts.push_back(cnt);
            i++;
            j++;
        }
    }
    while (i < baseTs.size()) {
        mergedTs.push_back(baseTs[i]);
        mergedVals.push_back(baseVals[i]);
        mergedCounts.push_back(baseCounts[i]);
        i++;
    }
    while (j < newTs.size()) {
        mergedTs.push_back(newTs[j]);
        mergedVals.push_back(newVals[j]);
        mergedCounts.push_back(1);
        j++;
    }

    baseTs = std::move(mergedTs);
    baseVals = std::move(mergedVals);
    baseCounts = std::move(mergedCounts);
}

// Merge two sorted (timestamps, states) vectors. Used in the reduce phase to
// combine partial results from different shards. O(n+m) time.
static void mergeSortedStatesInto(std::vector<uint64_t>& baseTs, std::vector<AggregationState>& baseStates,
                                  std::vector<uint64_t>& newTs, std::vector<AggregationState>& newStates) {
    std::vector<uint64_t> mergedTs;
    std::vector<AggregationState> mergedStates;
    mergedTs.reserve(baseTs.size() + newTs.size());
    mergedStates.reserve(baseTs.size() + newTs.size());

    size_t i = 0, j = 0;
    while (i < baseTs.size() && j < newTs.size()) {
        if (baseTs[i] < newTs[j]) {
            mergedTs.push_back(baseTs[i]);
            mergedStates.push_back(std::move(baseStates[i]));
            i++;
        } else if (baseTs[i] > newTs[j]) {
            mergedTs.push_back(newTs[j]);
            mergedStates.push_back(std::move(newStates[j]));
            j++;
        } else {
            mergedTs.push_back(baseTs[i]);
            AggregationState s = std::move(baseStates[i]);
            s.merge(std::move(newStates[j]));
            mergedStates.push_back(std::move(s));
            i++;
            j++;
        }
    }
    while (i < baseTs.size()) {
        mergedTs.push_back(baseTs[i]);
        mergedStates.push_back(std::move(baseStates[i]));
        i++;
    }
    while (j < newTs.size()) {
        mergedTs.push_back(newTs[j]);
        mergedStates.push_back(std::move(newStates[j]));
        j++;
    }

    baseTs = std::move(mergedTs);
    baseStates = std::move(mergedStates);
}

std::vector<PartialAggregationResult> Aggregator::createPartialAggregations(
    const std::vector<timestar::SeriesResult>& seriesResults, AggregationMethod method, uint64_t interval,
    const std::vector<std::string>& groupByTags) {
    auto startTime = std::chrono::high_resolution_clock::now();

    // Use PrehashedString key to compute hash once and avoid redundant hashing
    std::unordered_map<PrehashedString, PartialAggregationResult, PrehashedStringHash, PrehashedStringEqual>
        partialResults;

    for (const auto& series : seriesResults) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            const auto& timestamps = fieldData.first;
            const auto& values = fieldData.second;

            // Extract numeric values as doubles for aggregation.
            // Handles both native doubles and int64_t (cast to double; precision
            // loss for values > 2^53 is a known trade-off).
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
                // Pre-reserve bucket map: at most one bucket per unique timestamp,
                // so timestamps.size() is a safe upper bound that prevents rehashing
                // for the typical case where this is the only series in the group.
                if (interval > 0 && !timestamps.empty()) {
                    // Estimate bucket count from time range / interval rather than
                    // reserving one entry per timestamp (which over-allocates by 3-4 orders
                    // of magnitude for dense data with coarse intervals).
                    uint64_t timeRange = timestamps.back() - timestamps.front();
                    size_t estimatedBuckets = (interval > 0) ? (timeRange / interval + 1) : timestamps.size();
                    partial.bucketStates.reserve(std::min(estimatedBuckets, timestamps.size()));
                }
            }

            // PHASE 1 OPTIMIZATION: Two-phase aggregation - aggregate to states immediately
            // Single pass, no reallocation, O(1) merge later
            const bool needsRaw = (method == AggregationMethod::MEDIAN);
            if (interval > 0) {
                // Bucketed aggregation - accumulate into AggregationState per bucket
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    uint64_t bucketTime = (timestamps[i] / interval) * interval;
                    auto& state = partial.bucketStates[bucketTime];
                    state.collectRaw = needsRaw;
                    state.addValue(doubleValues[i], timestamps[i]);
                    partial.totalPoints++;
                }
            } else {
                // No interval - sorted vector aggregation by timestamp.
                // Input timestamps are sorted (from queryTsm). Use sorted merge
                // to avoid std::map's O(log n) insert + per-node heap allocation.
                partial.totalPoints += timestamps.size();
                if (partial.sortedTimestamps.empty()) {
                    // First series for this group: direct populate (common case)
                    partial.sortedTimestamps.reserve(timestamps.size());
                    partial.sortedStates.reserve(timestamps.size());
                    for (size_t i = 0; i < timestamps.size(); ++i) {
                        partial.sortedTimestamps.push_back(timestamps[i]);
                        AggregationState s;
                        s.collectRaw = needsRaw;
                        s.addValue(doubleValues[i], timestamps[i]);
                        partial.sortedStates.push_back(s);
                    }
                } else {
                    // Multiple series in same group: O(n+m) sorted merge
                    mergeSortedRawInto(partial.sortedTimestamps, partial.sortedStates, timestamps, doubleValues);
                }
            }
        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(endTime - startTime).count();

    // Convert map to vector and record timing
    std::vector<PartialAggregationResult> result;
    result.reserve(partialResults.size());
    for (auto& [hash, partial] : partialResults) {
        partial.partialAggregationTimeMs = duration;
        result.push_back(std::move(partial));
    }

    return result;
}

std::vector<GroupedAggregationResult> Aggregator::mergePartialAggregationsGrouped(
    std::vector<PartialAggregationResult>& partialResults, AggregationMethod method) {
    if (partialResults.empty()) {
        return {};
    }

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
            // default-constructing a state when the bucket already exists
            for (const auto* partial : groupPartials) {
                for (const auto& [bucketTime, state] : partial->bucketStates) {
                    auto [it, inserted] = mergedStates.try_emplace(bucketTime, state);
                    if (!inserted) {
                        it->second.merge(state);
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
                    merged.merge(*p->collapsedState);
                }
                if (merged.count > 0) {
                    groupedResult.points.push_back({merged.firstTimestamp, merged.getValue(method), merged.count});
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

                if (allRaw && !groupPartials.empty()) {
                    // Fast path: merge raw (timestamp, value) vectors directly.
                    auto* first = groupPartials[0];

                    if (groupPartials.size() == 1) {
                        // Single partial — zero-copy move, no merge needed.
                        if (method == AggregationMethod::COUNT) {
                            auto& ts = first->sortedTimestamps;
                            groupedResult.points.reserve(ts.size());
                            for (size_t i = 0; i < ts.size(); ++i) {
                                groupedResult.points.push_back({ts[i], 1.0, 1});
                            }
                        } else {
                            groupedResult.rawTimestamps = std::move(first->sortedTimestamps);
                            groupedResult.rawValues = std::move(first->sortedValues);
                        }
                    } else {
                        // Multiple partials with raw values — sorted merge.
                        std::vector<uint64_t> mergedTs = std::move(first->sortedTimestamps);
                        std::vector<double> mergedVals = std::move(first->sortedValues);
                        std::vector<size_t> mergedCounts(mergedTs.size(), 1);

                        for (size_t i = 1; i < groupPartials.size(); ++i) {
                            mergeSortedRawValuesInto(mergedTs, mergedVals, mergedCounts,
                                                     groupPartials[i]->sortedTimestamps, groupPartials[i]->sortedValues,
                                                     method);
                        }

                        groupedResult.points.reserve(mergedTs.size());
                        for (size_t i = 0; i < mergedTs.size(); ++i) {
                            double finalValue = mergedVals[i];
                            if (method == AggregationMethod::AVG && mergedCounts[i] > 1) {
                                finalValue /= mergedCounts[i];
                            } else if (method == AggregationMethod::COUNT) {
                                finalValue = static_cast<double>(mergedCounts[i]);
                            }
                            groupedResult.points.push_back({mergedTs[i], finalValue, mergedCounts[i]});
                        }
                    }
                } else {
                    // Fallback: some partials have AggregationStates (from the
                    // createPartialAggregations path). Convert any raw-value
                    // partials to states, then merge normally.
                    for (auto* p : groupPartials) {
                        if (!p->sortedValues.empty() && p->sortedStates.empty()) {
                            p->sortedStates.reserve(p->sortedTimestamps.size());
                            for (size_t i = 0; i < p->sortedTimestamps.size(); ++i) {
                                AggregationState s;
                                s.addValue(p->sortedValues[i], p->sortedTimestamps[i]);
                                p->sortedStates.push_back(s);
                            }
                            p->sortedValues.clear();
                            p->sortedValues.shrink_to_fit();
                        }
                    }

                    auto* first = groupPartials[0];
                    std::vector<uint64_t> mergedTs = std::move(first->sortedTimestamps);
                    std::vector<AggregationState> mergedStates = std::move(first->sortedStates);

                    for (size_t i = 1; i < groupPartials.size(); ++i) {
                        mergeSortedStatesInto(mergedTs, mergedStates, groupPartials[i]->sortedTimestamps,
                                              groupPartials[i]->sortedStates);
                    }

                    groupedResult.points.reserve(mergedTs.size());
                    for (size_t i = 0; i < mergedTs.size(); ++i) {
                        AggregatedPoint point;
                        point.timestamp = mergedTs[i];
                        point.count = mergedStates[i].count;
                        point.value = mergedStates[i].getValue(method);
                        groupedResult.points.push_back(point);
                    }
                }
            }
        }

        result.push_back(std::move(groupedResult));
    }

    return result;
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

    if (interval > 0) {
        // Bucketed aggregation
        std::unordered_map<uint64_t, AggregationState> buckets;
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            uint64_t bucketTime = (timestamps[i] / interval) * interval;
            buckets[bucketTime].addValue(values[i], timestamps[i]);
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
            states[timestamps[i]].addValue(values[i], timestamps[i]);
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
    std::unordered_map<uint64_t, AggregationState> mergedStates;

    for (const auto& [timestamps, values] : groupData) {
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            uint64_t key = interval > 0 ? (timestamps[i] / interval) * interval : timestamps[i];
            mergedStates[key].addValue(values[i], timestamps[i]);
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