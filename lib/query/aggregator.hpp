#ifndef __AGGREGATOR_H_INCLUDED__
#define __AGGREGATOR_H_INCLUDED__

#include "query_parser.hpp"
#include <vector>
#include <map>
#include <optional>
#include <limits>
#include <algorithm>
#include <numeric>
#include <unordered_map>

namespace tsdb {

// Forward declarations
struct SeriesResult;

// Structure to hold aggregated data points
struct AggregatedPoint {
    uint64_t timestamp;  // Bucket start time for interval aggregation
    double value;
    size_t count;  // Number of points aggregated
};

// Structure to hold grouped aggregation results with metadata
// Used to preserve grouping information when converting back to SeriesResult
struct GroupedAggregationResult {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string fieldName;
    std::vector<AggregatedPoint> points;
};

// Structure to hold partial aggregation results from a shard
// Used for distributed aggregation to reduce data transfer
struct PartialAggregationResult {
    std::string measurement;
    std::map<std::string, std::string> tags;  // Group-by tags only (already sorted)
    std::string fieldName;

    // Partial aggregation data organized by time bucket
    // Key: bucket timestamp, Value: vector of values in that bucket
    std::unordered_map<uint64_t, std::vector<double>> buckets;

    // For non-bucketed aggregation (interval == 0)
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Statistics
    size_t totalPoints = 0;
    double partialAggregationTimeMs = 0.0;

    // Cached group key hash (computed once, reused multiple times)
    size_t groupKeyHash = 0;

    // Compute hash-based group key (faster than string concatenation)
    void computeGroupKeyHash() {
        std::hash<std::string> hasher;
        groupKeyHash = hasher(measurement);

        // Tags are already sorted in std::map, iterate in order
        for (const auto& [k, v] : tags) {
            // Boost hash_combine pattern
            groupKeyHash ^= hasher(k) + 0x9e3779b9 + (groupKeyHash << 6) + (groupKeyHash >> 2);
            groupKeyHash ^= hasher(v) + 0x9e3779b9 + (groupKeyHash << 6) + (groupKeyHash >> 2);
        }

        groupKeyHash ^= hasher(fieldName) + 0x9e3779b9 + (groupKeyHash << 6) + (groupKeyHash >> 2);
    }
};

class Aggregator {
public:
    // Apply aggregation to time series data
    static std::vector<AggregatedPoint> aggregate(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        AggregationMethod method,
        uint64_t interval = 0);  // 0 means no time-based bucketing
    
    // Aggregate multiple series with the same method
    static std::vector<AggregatedPoint> aggregateMultiple(
        const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& series,
        AggregationMethod method,
        uint64_t interval = 0);
    
    // Group-by aggregation - aggregate within groups
    static std::map<std::string, std::vector<AggregatedPoint>> aggregateGroupBy(
        const std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>>& groups,
        AggregationMethod method,
        uint64_t interval = 0);

    // Distributed aggregation - create partial aggregation on shard
    static std::vector<PartialAggregationResult> createPartialAggregations(
        const std::vector<tsdb::SeriesResult>& seriesResults,
        AggregationMethod method,
        uint64_t interval,
        const std::vector<std::string>& groupByTags);

    // Distributed aggregation - merge partial aggregations from multiple shards
    static std::vector<AggregatedPoint> mergePartialAggregations(
        const std::vector<PartialAggregationResult>& partialResults,
        AggregationMethod method);

    // Distributed aggregation - merge with metadata preserved for easier conversion
    static std::vector<GroupedAggregationResult> mergePartialAggregationsGrouped(
        const std::vector<PartialAggregationResult>& partialResults,
        AggregationMethod method);

    // Core aggregation functions (made public for use by optimized implementations)
    static double calculateAvg(const std::vector<double>& values);
    static double calculateMin(const std::vector<double>& values);
    static double calculateMax(const std::vector<double>& values);
    static double calculateSum(const std::vector<double>& values);

private:
    static std::pair<uint64_t, double> getLatest(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values);
    
    // Time-based bucketing
    static std::map<uint64_t, std::vector<size_t>> bucketByTime(
        const std::vector<uint64_t>& timestamps,
        uint64_t interval,
        uint64_t startTime = 0);
    
    // Apply aggregation to a bucket of values
    static AggregatedPoint aggregateBucket(
        uint64_t bucketTime,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        const std::vector<size_t>& indices,
        AggregationMethod method);
    
    // Merge sorted time series
    static std::pair<std::vector<uint64_t>, std::vector<double>> mergeSeries(
        const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& series);
};

} // namespace tsdb

#endif // __AGGREGATOR_H_INCLUDED__