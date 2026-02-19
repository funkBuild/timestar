#include "aggregator.hpp"
#include "simd_aggregator.hpp"
#include "http_query_handler.hpp"  // For SeriesResult
#include <stdexcept>
#include <cmath>
#include <algorithm>
#include <limits>
#include <unordered_map>
#include <chrono>
#include <fmt/format.h>

namespace tsdb {

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
static void mergeSortedRawInto(
    std::vector<uint64_t>& baseTs, std::vector<AggregationState>& baseStates,
    const std::vector<uint64_t>& newTs, const std::vector<double>& newValues) {

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
        s.addValue(newValues[j], newTs[j]);
        mergedStates.push_back(s);
        j++;
    }

    baseTs = std::move(mergedTs);
    baseStates = std::move(mergedStates);
}

// Merge two sorted (timestamps, states) vectors. Used in the reduce phase to
// combine partial results from different shards. O(n+m) time.
static void mergeSortedStatesInto(
    std::vector<uint64_t>& baseTs, std::vector<AggregationState>& baseStates,
    const std::vector<uint64_t>& newTs, const std::vector<AggregationState>& newStates) {

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
            mergedStates.push_back(newStates[j]);
            j++;
        } else {
            mergedTs.push_back(baseTs[i]);
            AggregationState s = std::move(baseStates[i]);
            s.merge(newStates[j]);
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
        mergedStates.push_back(newStates[j]);
        j++;
    }

    baseTs = std::move(mergedTs);
    baseStates = std::move(mergedStates);
}

std::vector<PartialAggregationResult> Aggregator::createPartialAggregations(
    const std::vector<tsdb::SeriesResult>& seriesResults,
    AggregationMethod method,
    uint64_t interval,
    const std::vector<std::string>& groupByTags) {

    auto startTime = std::chrono::high_resolution_clock::now();

    // Use PrehashedString key to compute hash once and avoid redundant hashing
    std::unordered_map<PrehashedString, PartialAggregationResult,
                       PrehashedStringHash, PrehashedStringEqual> partialResults;

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
                continue; // Skip non-numeric types (string, bool)
            }

            const auto& doubleValues = *doubleValuesPtr;

            // Extract only the groupByTags (tags map is already sorted)
            std::map<std::string, std::string> relevantTags;
            if (!groupByTags.empty()) {
                for (const auto& tagName : groupByTags) {
                    auto it = series.tags.find(tagName);
                    if (it != series.tags.end()) {
                        relevantTags[tagName] = it->second;
                    }
                }
            }

            // Build composite group key: measurement + sorted tag k=v pairs + fieldName
            // separated by '\0' delimiter to guarantee uniqueness (no hash collisions)
            std::string compositeKey;
            compositeKey += series.measurement;
            for (const auto& [k, v] : relevantTags) {
                compositeKey += '\0';
                compositeKey += k;
                compositeKey += '=';
                compositeKey += v;
            }
            compositeKey += '\0';
            compositeKey += fieldName;

            // Hash once via PrehashedString; try_emplace avoids rehashing on lookup
            PrehashedString pkey(std::move(compositeKey));
            const size_t keyHash = pkey.hash;
            auto [it, inserted] = partialResults.try_emplace(std::move(pkey), PartialAggregationResult{});
            auto& partial = it->second;
            if (inserted) {
                partial.measurement = series.measurement;
                partial.fieldName = fieldName;
                partial.groupKey = it->first.value;  // Read from the emplaced key
                partial.groupKeyHash = keyHash;       // Reuse pre-computed hash
                // Pre-reserve bucket map: at most one bucket per unique timestamp,
                // so timestamps.size() is a safe upper bound that prevents rehashing
                // for the typical case where this is the only series in the group.
                if (interval > 0 && !timestamps.empty()) {
                    partial.bucketStates.reserve(timestamps.size());
                }
            }

            // PHASE 1 OPTIMIZATION: Two-phase aggregation - aggregate to states immediately
            // Single pass, no reallocation, O(1) merge later
            if (interval > 0) {
                // Bucketed aggregation - accumulate into AggregationState per bucket
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    uint64_t bucketTime = (timestamps[i] / interval) * interval;
                    partial.bucketStates[bucketTime].addValue(doubleValues[i], timestamps[i]);
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
                        s.addValue(doubleValues[i], timestamps[i]);
                        partial.sortedStates.push_back(s);
                    }
                } else {
                    // Multiple series in same group: O(n+m) sorted merge
                    mergeSortedRawInto(partial.sortedTimestamps, partial.sortedStates,
                                       timestamps, doubleValues);
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
    std::vector<PartialAggregationResult>& partialResults,
    AggregationMethod method) {

    if (partialResults.empty()) {
        return {};
    }

    std::vector<GroupedAggregationResult> result;

    // Group partial results by pre-hashed composite key (no re-hashing of groupKey strings)
    std::unordered_map<PrehashedString, std::vector<PartialAggregationResult*>,
                       PrehashedStringHash, PrehashedStringEqual> groups;

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
        groupedResult.tags = PartialAggregationResult::parseTagsFromGroupKey(groupPartials[0]->groupKey);
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
                [](const AggregatedPoint& a, const AggregatedPoint& b) {
                    return a.timestamp < b.timestamp;
                });

        } else {
            // Non-bucketed: merge sorted parallel vectors via O(n+m) merge.
            // Move from the first partial to avoid a copy, then merge the rest.
            auto* first = groupPartials[0];
            std::vector<uint64_t> mergedTs = std::move(first->sortedTimestamps);
            std::vector<AggregationState> mergedStates = std::move(first->sortedStates);

            for (size_t i = 1; i < groupPartials.size(); ++i) {
                mergeSortedStatesInto(mergedTs, mergedStates,
                                      groupPartials[i]->sortedTimestamps,
                                      groupPartials[i]->sortedStates);
            }

            // Extract final aggregated values (already sorted)
            groupedResult.points.reserve(mergedTs.size());
            for (size_t i = 0; i < mergedTs.size(); ++i) {
                AggregatedPoint point;
                point.timestamp = mergedTs[i];
                point.count = mergedStates[i].count;
                point.value = mergedStates[i].getValue(method);
                groupedResult.points.push_back(point);
            }
        }

        result.push_back(std::move(groupedResult));
    }

    return result;
}

// ============================================================================
// LEGACY COMPATIBILITY
// ============================================================================

std::vector<AggregatedPoint> Aggregator::aggregate(
    const std::vector<uint64_t>& timestamps,
    const std::vector<double>& values,
    AggregationMethod method,
    uint64_t interval) {

    if (timestamps.empty() || values.empty()) {
        return {};
    }

    if (interval > 0) {
        // Bucketed aggregation
        std::map<uint64_t, AggregationState> buckets;
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            uint64_t bucketTime = (timestamps[i] / interval) * interval;
            buckets[bucketTime].addValue(values[i], timestamps[i]);
        }

        std::vector<AggregatedPoint> result;
        result.reserve(buckets.size());
        for (const auto& [bucketTime, state] : buckets) {
            result.push_back({bucketTime, state.getValue(method), state.count});
        }
        return result;
    } else {
        // No bucketing - one point per timestamp
        std::map<uint64_t, AggregationState> states;
        for (size_t i = 0; i < timestamps.size() && i < values.size(); ++i) {
            states[timestamps[i]].addValue(values[i], timestamps[i]);
        }

        std::vector<AggregatedPoint> result;
        result.reserve(states.size());
        for (const auto& [ts, state] : states) {
            result.push_back({ts, state.getValue(method), state.count});
        }
        return result;
    }
}

std::vector<AggregatedPoint> Aggregator::aggregateMultiple(
    const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& groupData,
    AggregationMethod method,
    uint64_t interval) {

    if (groupData.empty()) {
        return {};
    }

    // Merge all series into one set of timestamp states
    std::map<uint64_t, AggregationState> mergedStates;

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
    return result;
}

} // namespace tsdb