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
        return 0.0;
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
    // Use SIMD when available for better performance
    if (simd::SimdAggregator::isAvx2Available()) {
        return simd::SimdAggregator::calculateSum(values.data(), values.size());
    }
    return std::accumulate(values.begin(), values.end(), 0.0);
}

// ============================================================================
// DISTRIBUTED AGGREGATION
// ============================================================================

std::vector<PartialAggregationResult> Aggregator::createPartialAggregations(
    const std::vector<tsdb::SeriesResult>& seriesResults,
    AggregationMethod method,
    uint64_t interval,
    const std::vector<std::string>& groupByTags) {

    auto startTime = std::chrono::high_resolution_clock::now();

    // OPTIMIZATION: Use hash-based keys instead of string concatenation
    std::unordered_map<size_t, PartialAggregationResult> partialResults;

    for (const auto& series : seriesResults) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            const auto& timestamps = fieldData.first;
            const auto& values = fieldData.second;

            // Only handle numeric values for now
            if (!std::holds_alternative<std::vector<double>>(values)) {
                continue;
            }

            const auto& doubleValues = std::get<std::vector<double>>(values);

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

            // Compute hash-based group key
            std::hash<std::string> hasher;
            size_t groupKeyHash = hasher(series.measurement);

            // OPTIMIZATION: Tags are already sorted, no need to re-sort
            for (const auto& [k, v] : relevantTags) {
                groupKeyHash ^= hasher(k) + 0x9e3779b9 + (groupKeyHash << 6) + (groupKeyHash >> 2);
                groupKeyHash ^= hasher(v) + 0x9e3779b9 + (groupKeyHash << 6) + (groupKeyHash >> 2);
            }
            groupKeyHash ^= hasher(fieldName) + 0x9e3779b9 + (groupKeyHash << 6) + (groupKeyHash >> 2);

            // Get or create partial result for this group
            auto& partial = partialResults[groupKeyHash];
            if (partial.measurement.empty()) {
                partial.measurement = series.measurement;
                partial.tags = relevantTags;
                partial.fieldName = fieldName;
                partial.groupKeyHash = groupKeyHash;
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
                // No interval - aggregate by timestamp (per-timestamp aggregation)
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    partial.timestampStates[timestamps[i]].addValue(doubleValues[i], timestamps[i]);
                    partial.totalPoints++;
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
    const std::vector<PartialAggregationResult>& partialResults,
    AggregationMethod method) {

    if (partialResults.empty()) {
        return {};
    }

    std::vector<GroupedAggregationResult> result;

    // OPTIMIZATION: Group partial results by hash instead of string concatenation
    std::unordered_map<size_t, std::vector<const PartialAggregationResult*>> groups;

    for (const auto& partial : partialResults) {
        groups[partial.groupKeyHash].push_back(&partial);
    }

    // Pre-allocate result
    result.reserve(groups.size());

    // Merge each group and preserve metadata
    for (const auto& [groupHash, groupPartials] : groups) {
        if (groupPartials.empty()) {
            continue;
        }

        // Create grouped result with metadata from first partial (all in group share same metadata)
        GroupedAggregationResult groupedResult;
        groupedResult.measurement = groupPartials[0]->measurement;
        groupedResult.tags = groupPartials[0]->tags;
        groupedResult.fieldName = groupPartials[0]->fieldName;

        // Check if we're using buckets or raw timestamps
        bool hasBuckets = !groupPartials[0]->bucketStates.empty();

        if (hasBuckets) {
            // PHASE 1 OPTIMIZATION: Merge pre-aggregated states (O(1) per bucket)
            std::unordered_map<uint64_t, AggregationState> mergedStates;

            // Merge all partial states for each bucket
            for (const auto* partial : groupPartials) {
                for (const auto& [bucketTime, state] : partial->bucketStates) {
                    mergedStates[bucketTime].merge(state);
                }
            }

            // OPTIMIZATION: Pre-allocate points vector
            groupedResult.points.reserve(mergedStates.size());

            // Extract final aggregated values from merged states
            for (const auto& [bucketTime, state] : mergedStates) {
                AggregatedPoint point;
                point.timestamp = bucketTime;
                point.count = state.count;
                point.value = state.getValue(method);  // O(1) extraction
                groupedResult.points.push_back(point);
            }

            // Sort points by timestamp
            std::sort(groupedResult.points.begin(), groupedResult.points.end(),
                [](const AggregatedPoint& a, const AggregatedPoint& b) {
                    return a.timestamp < b.timestamp;
                });

        } else {
            // PHASE 1 OPTIMIZATION: Merge time-aligned states (interval == 0)
            std::map<uint64_t, AggregationState> mergedStates;

            // Merge all partial states for each timestamp
            for (const auto* partial : groupPartials) {
                for (const auto& [timestamp, state] : partial->timestampStates) {
                    mergedStates[timestamp].merge(state);
                }
            }

            // Extract final aggregated values from merged states
            for (const auto& [timestamp, state] : mergedStates) {
                AggregatedPoint point;
                point.timestamp = timestamp;
                point.count = state.count;
                point.value = state.getValue(method);  // O(1) extraction
                groupedResult.points.push_back(point);
            }
        }

        result.push_back(std::move(groupedResult));
    }

    return result;
}

} // namespace tsdb