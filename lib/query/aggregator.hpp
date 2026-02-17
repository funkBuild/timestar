#ifndef AGGREGATOR_H_INCLUDED
#define AGGREGATOR_H_INCLUDED

#include "query_parser.hpp"
#include <vector>
#include <map>
#include <optional>
#include <limits>
#include <algorithm>
#include <numeric>
#include <unordered_map>
#include <string_view>

namespace tsdb {

// Pre-computed hash wrapper: stores a string alongside its hash so that
// unordered_map lookups never re-hash.  Used as key in the aggregation maps.
struct PrehashedString {
    std::string value;
    size_t hash = 0;

    PrehashedString() = default;
    explicit PrehashedString(std::string s)
        : value(std::move(s)), hash(std::hash<std::string>{}(value)) {}
    // Construct with externally-provided hash (avoids double hashing)
    PrehashedString(std::string s, size_t h) : value(std::move(s)), hash(h) {}

    bool operator==(const PrehashedString& other) const { return value == other.value; }
};

struct PrehashedStringHash {
    using is_transparent = void;
    size_t operator()(const PrehashedString& k) const noexcept { return k.hash; }
};

struct PrehashedStringEqual {
    using is_transparent = void;
    bool operator()(const PrehashedString& a, const PrehashedString& b) const {
        return a.value == b.value;
    }
};

// Forward declarations
struct SeriesResult;

// Two-phase aggregation state - holds pre-aggregated values for efficient merging
// Instead of transferring all raw values, we transfer compact state and merge O(1)
struct AggregationState {
    double sum = 0.0;
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::lowest();
    double latest = 0.0;  // Most recent value
    uint64_t latestTimestamp = 0;  // Timestamp of latest value
    size_t count = 0;

    // Add a single value to this state (incremental aggregation)
    void addValue(double value, uint64_t timestamp = 0) {
        sum += value;
        min = std::min(min, value);
        max = std::max(max, value);
        if (timestamp >= latestTimestamp) {
            latest = value;
            latestTimestamp = timestamp;
        }
        count++;
    }

    // Merge another state into this one (commutative & associative)
    void merge(const AggregationState& other) {
        sum += other.sum;
        min = std::min(min, other.min);
        max = std::max(max, other.max);
        if (other.latestTimestamp >= latestTimestamp) {
            latest = other.latest;
            latestTimestamp = other.latestTimestamp;
        }
        count += other.count;
    }

    // Extract final aggregated value based on method
    double getValue(AggregationMethod method) const {
        if (count == 0) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        switch (method) {
            case AggregationMethod::AVG:
                return sum / count;
            case AggregationMethod::MIN:
                return min;
            case AggregationMethod::MAX:
                return max;
            case AggregationMethod::SUM:
                return sum;
            case AggregationMethod::LATEST:
                return latest;
            default:
                return std::numeric_limits<double>::quiet_NaN();
        }
    }
};

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
// OPTIMIZED: Uses AggregationState for two-phase aggregation (8x less data transfer)
// Tags are NOT stored here — they're encoded inside groupKey as
// "measurement\0key1=val1\0key2=val2\0fieldName" and reconstructed lazily
// in the reduce phase, avoiding std::map tree-node allocations per partial.
struct PartialAggregationResult {
    std::string measurement;
    std::string fieldName;

    // TWO-PHASE AGGREGATION: Store pre-aggregated states instead of raw values
    // Key: bucket timestamp, Value: aggregation state for that bucket
    std::unordered_map<uint64_t, AggregationState> bucketStates;

    // For non-bucketed aggregation (interval == 0) - sorted parallel vectors
    // Replaces std::map<uint64_t, AggregationState> to eliminate per-entry tree
    // node allocations (525K+ allocs per series in the benchmark).
    std::vector<uint64_t> sortedTimestamps;
    std::vector<AggregationState> sortedStates;

    // Statistics
    size_t totalPoints = 0;
    double partialAggregationTimeMs = 0.0;

    // Cached group key hash (computed once, reused multiple times)
    size_t groupKeyHash = 0;

    // Composite group key string (guaranteed unique, used as map key)
    // Format: "measurement\0tag1=val1\0tag2=val2\0fieldName"
    std::string groupKey;

    // Parse group-by tags from groupKey (called once per group in reduce phase)
    static std::map<std::string, std::string> parseTagsFromGroupKey(const std::string& gk) {
        std::map<std::string, std::string> tags;
        // Format: measurement\0tag1=val1\0...\0fieldName
        size_t firstNull = gk.find('\0');
        if (firstNull == std::string::npos) return tags;
        size_t lastNull = gk.rfind('\0');
        if (firstNull == lastNull) return tags;  // No tags segment
        // Parse tag entries between first and last \0
        size_t pos = firstNull + 1;
        while (pos < lastNull) {
            size_t nextNull = gk.find('\0', pos);
            if (nextNull == std::string::npos || nextNull > lastNull) {
                nextNull = lastNull;
            }
            size_t eqPos = gk.find('=', pos);
            if (eqPos != std::string::npos && eqPos < nextNull) {
                tags[gk.substr(pos, eqPos - pos)] = gk.substr(eqPos + 1, nextNull - eqPos - 1);
            }
            pos = nextNull + 1;
        }
        return tags;
    }
};

class Aggregator {
public:
    // ========================================================================
    // DISTRIBUTED AGGREGATION (15.6x performance improvement)
    // ========================================================================
    // These functions implement map-reduce style distributed aggregation with
    // O(1) state merging for high-performance query execution.

    // Create partial aggregation on shard (map phase)
    static std::vector<PartialAggregationResult> createPartialAggregations(
        const std::vector<tsdb::SeriesResult>& seriesResults,
        AggregationMethod method,
        uint64_t interval,
        const std::vector<std::string>& groupByTags);

    // Merge partial aggregations with metadata preserved (reduce phase)
    static std::vector<GroupedAggregationResult> mergePartialAggregationsGrouped(
        std::vector<PartialAggregationResult>& partialResults,
        AggregationMethod method);

    // ========================================================================
    // UTILITY FUNCTIONS
    // ========================================================================

    // Core aggregation functions (made public for use by optimized implementations)
    static double calculateAvg(const std::vector<double>& values);
    static double calculateMin(const std::vector<double>& values);
    static double calculateMax(const std::vector<double>& values);
    static double calculateSum(const std::vector<double>& values);

    // ========================================================================
    // LEGACY COMPATIBILITY
    // ========================================================================
    // Simple aggregation over a single series (timestamps + values).
    // Provided for backward compatibility with integration tests.
    static std::vector<AggregatedPoint> aggregate(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        AggregationMethod method,
        uint64_t interval);

    // Aggregate across multiple series (group-by merge).
    static std::vector<AggregatedPoint> aggregateMultiple(
        const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& groupData,
        AggregationMethod method,
        uint64_t interval);
};

} // namespace tsdb

#endif // AGGREGATOR_H_INCLUDED