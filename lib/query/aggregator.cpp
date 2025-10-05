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

std::vector<AggregatedPoint> Aggregator::aggregate(
    const std::vector<uint64_t>& timestamps,
    const std::vector<double>& values,
    AggregationMethod method,
    uint64_t interval) {
    
    if (timestamps.size() != values.size()) {
        throw std::invalid_argument("Timestamps and values must have the same size");
    }
    
    if (timestamps.empty()) {
        return {};
    }
    
    std::vector<AggregatedPoint> result;
    
    if (interval == 0) {
        // No time-based bucketing, aggregate all points into one
        AggregatedPoint point;
        point.count = values.size();
        
        switch (method) {
            case AggregationMethod::AVG:
                point.value = calculateAvg(values);
                point.timestamp = timestamps.front();  // Use first timestamp
                break;
            case AggregationMethod::MIN: {
                point.value = calculateMin(values);
                // Find timestamp of minimum value
                auto minIt = std::min_element(values.begin(), values.end());
                point.timestamp = timestamps[std::distance(values.begin(), minIt)];
                break;
            }
            case AggregationMethod::MAX: {
                point.value = calculateMax(values);
                // Find timestamp of maximum value
                auto maxIt = std::max_element(values.begin(), values.end());
                point.timestamp = timestamps[std::distance(values.begin(), maxIt)];
                break;
            }
            case AggregationMethod::SUM:
                point.value = calculateSum(values);
                point.timestamp = timestamps.front();
                break;
            case AggregationMethod::LATEST: {
                auto latest = getLatest(timestamps, values);
                point.timestamp = latest.first;
                point.value = latest.second;
                break;
            }
        }
        
        result.push_back(point);
    } else {
        // Time-based bucketing
        auto buckets = bucketByTime(timestamps, interval);
        
        for (const auto& [bucketTime, indices] : buckets) {
            if (!indices.empty()) {
                AggregatedPoint point = aggregateBucket(
                    bucketTime, timestamps, values, indices, method);
                result.push_back(point);
            }
        }
    }
    
    return result;
}

std::vector<AggregatedPoint> Aggregator::aggregateMultiple(
    const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& series,
    AggregationMethod method,
    uint64_t interval) {

    if (series.empty()) {
        return {};
    }

    std::vector<AggregatedPoint> result;

    if (interval == 0) {
        // No time bucketing - need to align by timestamp
        // For time-aligned aggregation, we need to group values by timestamp
        // and apply the aggregation function at each timestamp
        std::map<uint64_t, std::vector<double>> timeAlignedValues;

        // Group all values by their timestamps
        for (const auto& [timestamps, values] : series) {
            for (size_t i = 0; i < timestamps.size(); ++i) {
                timeAlignedValues[timestamps[i]].push_back(values[i]);
            }
        }

        if (method == AggregationMethod::LATEST) {
            // For LATEST with no interval, return all original values without aggregation
            for (const auto& [timestamps, values] : series) {
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    AggregatedPoint point;
                    point.timestamp = timestamps[i];
                    point.value = values[i];
                    point.count = 1;
                    result.push_back(point);
                }
            }
            // Sort by timestamp since we may have mixed them from different series
            std::sort(result.begin(), result.end(),
                [](const AggregatedPoint& a, const AggregatedPoint& b) {
                    return a.timestamp < b.timestamp;
                });
        } else {
            // For other aggregations with no interval, aggregate values at each timestamp
            for (const auto& [timestamp, vals] : timeAlignedValues) {
                AggregatedPoint point;
                point.timestamp = timestamp;
                point.count = vals.size();

                switch (method) {
                    case AggregationMethod::AVG:
                        point.value = calculateAvg(vals);
                        break;
                    case AggregationMethod::MIN:
                        point.value = calculateMin(vals);
                        break;
                    case AggregationMethod::MAX:
                        point.value = calculateMax(vals);
                        break;
                    case AggregationMethod::SUM:
                        point.value = calculateSum(vals);
                        break;
                    case AggregationMethod::LATEST:
                        // Shouldn't reach here, handled above
                        point.value = vals.back();
                        break;
                }

                result.push_back(point);
            }
        }
    } else {
        // OPTIMIZED: With time bucketing, go directly to buckets without time-alignment
        // This avoids creating a huge intermediate map for millions of points

        // Use unordered_map for O(1) average bucket lookup instead of O(log n)
        std::unordered_map<uint64_t, std::vector<double>> buckets;

        // Estimate number of buckets to pre-allocate hash table capacity
        if (!series.empty() && !series[0].first.empty()) {
            const auto& firstTimestamps = series[0].first;
            if (firstTimestamps.size() > 1) {
                uint64_t timeSpan = firstTimestamps.back() - firstTimestamps.front();
                size_t estimatedBuckets = (timeSpan / interval) + 1;
                buckets.reserve(estimatedBuckets * 1.2); // 20% buffer for variability
            }
        }

        // Group values directly into time buckets from all series
        for (const auto& [timestamps, values] : series) {
            for (size_t i = 0; i < timestamps.size(); ++i) {
                uint64_t bucketTime = (timestamps[i] / interval) * interval;
                buckets[bucketTime].push_back(values[i]);
            }
        }

        // Aggregate each bucket
        for (const auto& [bucketTime, vals] : buckets) {
            AggregatedPoint point;
            point.timestamp = bucketTime;
            point.count = vals.size();

            switch (method) {
                case AggregationMethod::AVG:
                    point.value = calculateAvg(vals);
                    break;
                case AggregationMethod::MIN:
                    point.value = calculateMin(vals);
                    break;
                case AggregationMethod::MAX:
                    point.value = calculateMax(vals);
                    break;
                case AggregationMethod::SUM:
                    point.value = calculateSum(vals);
                    break;
                case AggregationMethod::LATEST: {
                    // For LATEST in a bucket, take the last value
                    point.value = vals.back();
                    break;
                }
            }

            result.push_back(point);
        }

        // Sort result by timestamp since unordered_map doesn't maintain order
        std::sort(result.begin(), result.end(),
            [](const AggregatedPoint& a, const AggregatedPoint& b) {
                return a.timestamp < b.timestamp;
            });
    }

    return result;
}

std::map<std::string, std::vector<AggregatedPoint>> Aggregator::aggregateGroupBy(
    const std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>>& groups,
    AggregationMethod method,
    uint64_t interval) {
    
    std::map<std::string, std::vector<AggregatedPoint>> result;
    
    for (const auto& [groupKey, groupSeries] : groups) {
        result[groupKey] = aggregateMultiple(groupSeries, method, interval);
    }
    
    return result;
}

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

std::pair<uint64_t, double> Aggregator::getLatest(
    const std::vector<uint64_t>& timestamps,
    const std::vector<double>& values) {
    
    if (timestamps.empty()) {
        return {0, std::numeric_limits<double>::quiet_NaN()};
    }
    
    // Find the index of the maximum timestamp
    auto maxIt = std::max_element(timestamps.begin(), timestamps.end());
    size_t idx = std::distance(timestamps.begin(), maxIt);
    
    return {timestamps[idx], values[idx]};
}

std::map<uint64_t, std::vector<size_t>> Aggregator::bucketByTime(
    const std::vector<uint64_t>& timestamps,
    uint64_t interval,
    uint64_t startTime) {
    
    std::map<uint64_t, std::vector<size_t>> buckets;
    
    if (interval == 0 || timestamps.empty()) {
        return buckets;
    }
    
    // If no start time specified, use the first timestamp
    if (startTime == 0 && !timestamps.empty()) {
        startTime = timestamps.front();
    }
    
    // Align start time to interval boundary
    uint64_t alignedStart = (startTime / interval) * interval;
    
    // Bucket each timestamp
    for (size_t i = 0; i < timestamps.size(); ++i) {
        uint64_t bucketTime = (timestamps[i] / interval) * interval;
        buckets[bucketTime].push_back(i);
    }
    
    return buckets;
}

AggregatedPoint Aggregator::aggregateBucket(
    uint64_t bucketTime,
    const std::vector<uint64_t>& timestamps,
    const std::vector<double>& values,
    const std::vector<size_t>& indices,
    AggregationMethod method) {

    AggregatedPoint point;
    point.timestamp = bucketTime;
    point.count = indices.size();

    // OPTIMIZED: Only copy what we need
    if (method == AggregationMethod::LATEST) {
        // LATEST needs both timestamps and values
        std::vector<double> bucketValues;
        std::vector<uint64_t> bucketTimestamps;
        bucketValues.reserve(indices.size());
        bucketTimestamps.reserve(indices.size());

        for (size_t idx : indices) {
            bucketValues.push_back(values[idx]);
            bucketTimestamps.push_back(timestamps[idx]);
        }

        auto latest = getLatest(bucketTimestamps, bucketValues);
        point.value = latest.second;
    } else {
        // AVG, MIN, MAX, SUM only need values
        std::vector<double> bucketValues;
        bucketValues.reserve(indices.size());

        for (size_t idx : indices) {
            bucketValues.push_back(values[idx]);
        }

        switch (method) {
            case AggregationMethod::AVG:
                point.value = calculateAvg(bucketValues);
                break;
            case AggregationMethod::MIN:
                point.value = calculateMin(bucketValues);
                break;
            case AggregationMethod::MAX:
                point.value = calculateMax(bucketValues);
                break;
            case AggregationMethod::SUM:
                point.value = calculateSum(bucketValues);
                break;
            default:
                break;
        }
    }

    return point;
}

std::pair<std::vector<uint64_t>, std::vector<double>> Aggregator::mergeSeries(
    const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& series) {
    
    std::vector<uint64_t> mergedTimestamps;
    std::vector<double> mergedValues;
    
    // Calculate total size
    size_t totalSize = 0;
    for (const auto& [timestamps, values] : series) {
        totalSize += timestamps.size();
    }
    
    mergedTimestamps.reserve(totalSize);
    mergedValues.reserve(totalSize);
    
    // Use a map to merge and sort by timestamp
    std::map<uint64_t, std::vector<double>> timeMap;
    
    for (const auto& [timestamps, values] : series) {
        for (size_t i = 0; i < timestamps.size(); ++i) {
            timeMap[timestamps[i]].push_back(values[i]);
        }
    }
    
    // Convert map back to vectors, averaging values at same timestamp
    for (const auto& [timestamp, vals] : timeMap) {
        mergedTimestamps.push_back(timestamp);
        // For duplicate timestamps, take the average
        double sum = std::accumulate(vals.begin(), vals.end(), 0.0);
        mergedValues.push_back(sum / vals.size());
    }
    
    return {mergedTimestamps, mergedValues};
}

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

                // OPTIMIZATION: Pre-allocate buckets if using intervals
                if (interval > 0 && !timestamps.empty()) {
                    // Estimate number of buckets from time range
                    uint64_t timeRange = timestamps.back() - timestamps.front();
                    size_t estimatedBuckets = (timeRange / interval) + 1;
                    partial.buckets.reserve(static_cast<size_t>(estimatedBuckets * 1.2)); // 20% buffer
                }
            }

            // Add data to buckets or raw vectors
            if (interval > 0) {
                // OPTIMIZATION: Count points per bucket first, then allocate once
                std::unordered_map<uint64_t, size_t> bucketCounts;
                for (const auto& ts : timestamps) {
                    uint64_t bucketTime = (ts / interval) * interval;
                    bucketCounts[bucketTime]++;
                }

                // Reserve space for each bucket
                for (const auto& [bucketTime, count] : bucketCounts) {
                    partial.buckets[bucketTime].reserve(count);
                }

                // Now fill buckets (no reallocation)
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    uint64_t bucketTime = (timestamps[i] / interval) * interval;
                    partial.buckets[bucketTime].push_back(doubleValues[i]);
                    partial.totalPoints++;
                }
            } else {
                // No interval - keep all timestamps and values for time-aligned aggregation
                partial.timestamps.reserve(partial.timestamps.size() + timestamps.size());
                partial.values.reserve(partial.values.size() + doubleValues.size());
                partial.timestamps.insert(partial.timestamps.end(), timestamps.begin(), timestamps.end());
                partial.values.insert(partial.values.end(), doubleValues.begin(), doubleValues.end());
                partial.totalPoints += doubleValues.size();
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

std::vector<AggregatedPoint> Aggregator::mergePartialAggregations(
    const std::vector<PartialAggregationResult>& partialResults,
    AggregationMethod method) {

    if (partialResults.empty()) {
        return {};
    }

    std::vector<AggregatedPoint> result;

    // OPTIMIZATION: Group partial results by hash instead of string concatenation
    std::unordered_map<size_t, std::vector<const PartialAggregationResult*>> groups;

    for (const auto& partial : partialResults) {
        groups[partial.groupKeyHash].push_back(&partial);
    }

    // Merge each group
    for (const auto& [groupHash, groupPartials] : groups) {
        if (groupPartials.empty()) {
            continue;
        }

        // Check if we're using buckets or raw timestamps
        bool hasBuckets = !groupPartials[0]->buckets.empty();

        if (hasBuckets) {
            // OPTIMIZATION: Pre-count bucket sizes, then allocate once
            std::unordered_map<uint64_t, size_t> bucketSizes;
            for (const auto* partial : groupPartials) {
                for (const auto& [bucketTime, values] : partial->buckets) {
                    bucketSizes[bucketTime] += values.size();
                }
            }

            // OPTIMIZATION: Pre-allocate merged buckets
            std::unordered_map<uint64_t, std::vector<double>> mergedBuckets;
            mergedBuckets.reserve(bucketSizes.size());
            for (const auto& [bucketTime, size] : bucketSizes) {
                mergedBuckets[bucketTime].reserve(size);
            }

            // Merge bucketed data (no reallocation)
            for (const auto* partial : groupPartials) {
                for (const auto& [bucketTime, values] : partial->buckets) {
                    auto& bucket = mergedBuckets[bucketTime];
                    bucket.insert(bucket.end(), values.begin(), values.end());
                }
            }

            // OPTIMIZATION: Pre-allocate result vector
            result.reserve(result.size() + mergedBuckets.size());

            // Apply final aggregation to each bucket
            for (const auto& [bucketTime, values] : mergedBuckets) {
                AggregatedPoint point;
                point.timestamp = bucketTime;
                point.count = values.size();

                switch (method) {
                    case AggregationMethod::AVG:
                        point.value = calculateAvg(values);
                        break;
                    case AggregationMethod::MIN:
                        point.value = calculateMin(values);
                        break;
                    case AggregationMethod::MAX:
                        point.value = calculateMax(values);
                        break;
                    case AggregationMethod::SUM:
                        point.value = calculateSum(values);
                        break;
                    case AggregationMethod::LATEST:
                        point.value = values.back();
                        break;
                }

                result.push_back(point);
            }
        } else {
            // Merge time-aligned data (interval == 0)
            std::map<uint64_t, std::vector<double>> timeAlignedValues;

            for (const auto* partial : groupPartials) {
                for (size_t i = 0; i < partial->timestamps.size(); ++i) {
                    timeAlignedValues[partial->timestamps[i]].push_back(partial->values[i]);
                }
            }

            if (method == AggregationMethod::LATEST) {
                // For LATEST, just return all values
                for (const auto& [timestamp, values] : timeAlignedValues) {
                    for (double value : values) {
                        AggregatedPoint point;
                        point.timestamp = timestamp;
                        point.value = value;
                        point.count = 1;
                        result.push_back(point);
                    }
                }
            } else {
                // Aggregate values at each timestamp
                for (const auto& [timestamp, values] : timeAlignedValues) {
                    AggregatedPoint point;
                    point.timestamp = timestamp;
                    point.count = values.size();

                    switch (method) {
                        case AggregationMethod::AVG:
                            point.value = calculateAvg(values);
                            break;
                        case AggregationMethod::MIN:
                            point.value = calculateMin(values);
                            break;
                        case AggregationMethod::MAX:
                            point.value = calculateMax(values);
                            break;
                        case AggregationMethod::SUM:
                            point.value = calculateSum(values);
                            break;
                        case AggregationMethod::LATEST:
                            point.value = values.back();
                            break;
                    }

                    result.push_back(point);
                }
            }
        }
    }

    // Sort result by timestamp
    std::sort(result.begin(), result.end(),
        [](const AggregatedPoint& a, const AggregatedPoint& b) {
            return a.timestamp < b.timestamp;
        });

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
        bool hasBuckets = !groupPartials[0]->buckets.empty();

        if (hasBuckets) {
            // OPTIMIZATION: Pre-count bucket sizes, then allocate once
            std::unordered_map<uint64_t, size_t> bucketSizes;
            for (const auto* partial : groupPartials) {
                for (const auto& [bucketTime, values] : partial->buckets) {
                    bucketSizes[bucketTime] += values.size();
                }
            }

            // OPTIMIZATION: Pre-allocate merged buckets
            std::unordered_map<uint64_t, std::vector<double>> mergedBuckets;
            mergedBuckets.reserve(bucketSizes.size());
            for (const auto& [bucketTime, size] : bucketSizes) {
                mergedBuckets[bucketTime].reserve(size);
            }

            // Merge bucketed data (no reallocation)
            for (const auto* partial : groupPartials) {
                for (const auto& [bucketTime, values] : partial->buckets) {
                    auto& bucket = mergedBuckets[bucketTime];
                    bucket.insert(bucket.end(), values.begin(), values.end());
                }
            }

            // OPTIMIZATION: Pre-allocate points vector
            groupedResult.points.reserve(mergedBuckets.size());

            // Apply final aggregation to each bucket
            for (const auto& [bucketTime, values] : mergedBuckets) {
                AggregatedPoint point;
                point.timestamp = bucketTime;
                point.count = values.size();

                switch (method) {
                    case AggregationMethod::AVG:
                        point.value = calculateAvg(values);
                        break;
                    case AggregationMethod::MIN:
                        point.value = calculateMin(values);
                        break;
                    case AggregationMethod::MAX:
                        point.value = calculateMax(values);
                        break;
                    case AggregationMethod::SUM:
                        point.value = calculateSum(values);
                        break;
                    case AggregationMethod::LATEST:
                        point.value = values.back();
                        break;
                }

                groupedResult.points.push_back(point);
            }

            // Sort points by timestamp
            std::sort(groupedResult.points.begin(), groupedResult.points.end(),
                [](const AggregatedPoint& a, const AggregatedPoint& b) {
                    return a.timestamp < b.timestamp;
                });

        } else {
            // Merge time-aligned data (interval == 0)
            std::map<uint64_t, std::vector<double>> timeAlignedValues;

            for (const auto* partial : groupPartials) {
                for (size_t i = 0; i < partial->timestamps.size(); ++i) {
                    timeAlignedValues[partial->timestamps[i]].push_back(partial->values[i]);
                }
            }

            if (method == AggregationMethod::LATEST) {
                // For LATEST, just return all values
                for (const auto& [timestamp, values] : timeAlignedValues) {
                    for (double value : values) {
                        AggregatedPoint point;
                        point.timestamp = timestamp;
                        point.value = value;
                        point.count = 1;
                        groupedResult.points.push_back(point);
                    }
                }
            } else {
                // Aggregate values at each timestamp
                for (const auto& [timestamp, values] : timeAlignedValues) {
                    AggregatedPoint point;
                    point.timestamp = timestamp;
                    point.count = values.size();

                    switch (method) {
                        case AggregationMethod::AVG:
                            point.value = calculateAvg(values);
                            break;
                        case AggregationMethod::MIN:
                            point.value = calculateMin(values);
                            break;
                        case AggregationMethod::MAX:
                            point.value = calculateMax(values);
                            break;
                        case AggregationMethod::SUM:
                            point.value = calculateSum(values);
                            break;
                        case AggregationMethod::LATEST:
                            point.value = values.back();
                            break;
                    }

                    groupedResult.points.push_back(point);
                }
            }
        }

        result.push_back(std::move(groupedResult));
    }

    return result;
}

} // namespace tsdb