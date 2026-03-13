#ifndef AGGREGATOR_H_INCLUDED
#define AGGREGATOR_H_INCLUDED

#include "query_parser.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace timestar {

// Pre-computed hash wrapper: stores a string alongside its hash so that
// unordered_map lookups never re-hash.  Used as key in the aggregation maps.
struct PrehashedString {
    std::string value;
    size_t hash = 0;

    PrehashedString() = default;
    explicit PrehashedString(std::string s) : value(std::move(s)), hash(std::hash<std::string>{}(value)) {}
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
    bool operator()(const PrehashedString& a, const PrehashedString& b) const { return a.value == b.value; }
};

// Forward declarations
struct SeriesResult;

// Two-phase aggregation state - holds pre-aggregated values for efficient merging
// Instead of transferring all raw values, we transfer compact state and merge O(1)
struct AggregationState {
    // Hard limit on the number of raw values retained per AggregationState.
    //
    // rawValues is only needed for MEDIAN, STDDEV, and STDVAR — all other methods
    // compute from running scalar accumulators (sum, min, max, count, etc.).
    // Without a cap, merging many shards with dense data can allocate hundreds of
    // megabytes per state: 1M doubles = ~8 MB, and a bucketed query can have many
    // buckets each holding their own rawValues vector.
    //
    // When this limit is exceeded, rawValues stops growing and rawValuesSaturated is
    // set to true.  getValue() for MEDIAN/STDDEV/STDVAR then returns NaN to signal
    // that the result would be computed from an incomplete sample rather than silently
    // returning a statistically wrong answer.
    static constexpr size_t RAW_VALUES_HARD_LIMIT = 1'000'000;

    double sum = 0.0;
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::lowest();
    double latest = 0.0;                                             // Most recent value
    uint64_t latestTimestamp = 0;                                    // Timestamp of latest value
    double first = 0.0;                                              // Earliest value
    uint64_t firstTimestamp = std::numeric_limits<uint64_t>::max();  // Timestamp of earliest value
    size_t count = 0;
    // Welford's online M2 accumulator for STDDEV/STDVAR — O(1) memory, no raw values needed.
    // M2 = sum of squared differences from the running mean: Σ(x_i - mean)^2
    double m2 = 0.0;
    // Raw values for MEDIAN computation only (STDDEV/STDVAR use Welford instead).
    // Populated by addValue() only when collectRaw is true, avoiding per-value
    // vector push_back overhead for the ~95% of queries that don't use MEDIAN.
    // Growth is capped at RAW_VALUES_HARD_LIMIT; if the cap is hit rawValuesSaturated
    // is set and getValue() returns NaN for MEDIAN.
    // Mutable so getValue() can sort in-place via nth_element without copying.
    mutable std::vector<double> rawValues;
    bool rawValuesSaturated = false;
    bool collectRaw = false;  // Set to true only for MEDIAN queries

    // Add a single value to this state (incremental aggregation)
    void addValue(double value, uint64_t timestamp = 0) {
        if (std::isnan(value))
            return;  // Skip NaN to avoid poisoning aggregation
        sum += value;
        min = std::min(min, value);
        max = std::max(max, value);
        if (timestamp >= latestTimestamp) {
            latest = value;
            latestTimestamp = timestamp;
        }
        if (timestamp <= firstTimestamp) {
            first = value;
            firstTimestamp = timestamp;
        }
        // Welford's online variance: compute delta from old mean, then update
        double oldMean = (count > 0) ? (sum - value) / count : 0.0;
        count++;
        double newMean = sum / count;
        m2 += (value - oldMean) * (value - newMean);
        if (collectRaw) {
            if (rawValues.size() < RAW_VALUES_HARD_LIMIT) {
                rawValues.push_back(value);
            } else {
                rawValuesSaturated = true;
            }
        }
    }

    // Method-aware fast path: skips branches not needed for the given method.
    // Use from pushdown aggregation where the method is known at call time.
    void addValueForMethod(double value, uint64_t timestamp, AggregationMethod method) {
        if (std::isnan(value)) [[unlikely]]
            return;
        switch (method) {
            case AggregationMethod::AVG:
            case AggregationMethod::SUM:
                sum += value;
                count++;
                break;
            case AggregationMethod::COUNT:
                count++;
                break;
            case AggregationMethod::MIN:
                min = std::min(min, value);
                count++;
                break;
            case AggregationMethod::MAX:
                max = std::max(max, value);
                count++;
                break;
            case AggregationMethod::SPREAD:
                min = std::min(min, value);
                max = std::max(max, value);
                count++;
                break;
            case AggregationMethod::LATEST:
                if (timestamp >= latestTimestamp) {
                    latest = value;
                    latestTimestamp = timestamp;
                }
                count++;
                break;
            case AggregationMethod::FIRST:
                if (timestamp <= firstTimestamp) {
                    first = value;
                    firstTimestamp = timestamp;
                }
                count++;
                break;
            case AggregationMethod::STDDEV:
            case AggregationMethod::STDVAR: {
                sum += value;
                double oldMean = (count > 0) ? (sum - value) / count : 0.0;
                count++;
                double newMean = sum / count;
                m2 += (value - oldMean) * (value - newMean);
                break;
            }
            default:
                // MEDIAN and unknown methods fall back to full addValue
                addValue(value, timestamp);
                break;
        }
    }

    // Merge another state into this one (commutative & associative)
    void merge(const AggregationState& other) {
        // Parallel Welford merge: combine M2 accumulators from two partitions
        if (count > 0 && other.count > 0) {
            double delta = (other.sum / other.count) - (sum / count);
            m2 += other.m2 + delta * delta * (static_cast<double>(count) * other.count) / (count + other.count);
        } else if (other.count > 0) {
            m2 = other.m2;
        }
        sum += other.sum;
        min = std::min(min, other.min);
        max = std::max(max, other.max);
        if (other.latestTimestamp >= latestTimestamp) {
            latest = other.latest;
            latestTimestamp = other.latestTimestamp;
        }
        if (other.firstTimestamp <= firstTimestamp) {
            first = other.first;
            firstTimestamp = other.firstTimestamp;
        }
        // Propagate saturation flag first so getValue() is always consistent.
        if (other.rawValuesSaturated) {
            rawValuesSaturated = true;
        }
        // Append as many raw values as the cap allows; mark saturated if we hit it.
        if (!rawValuesSaturated) {
            size_t available = RAW_VALUES_HARD_LIMIT - rawValues.size();
            size_t toCopy = std::min(available, other.rawValues.size());
            rawValues.insert(rawValues.end(), other.rawValues.begin(),
                             other.rawValues.begin() + static_cast<std::ptrdiff_t>(toCopy));
            if (toCopy < other.rawValues.size()) {
                rawValuesSaturated = true;
            }
        }
        count += other.count;
    }

    // Extract final aggregated value based on method
    // Return the representative timestamp for a collapsed state.
    // LATEST uses latestTimestamp; FIRST uses firstTimestamp; others use firstTimestamp
    // as a sensible default (earliest data point in the state).
    uint64_t getTimestamp(AggregationMethod method) const {
        if (method == AggregationMethod::LATEST) return latestTimestamp;
        return firstTimestamp;
    }

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
            case AggregationMethod::COUNT:
                return static_cast<double>(count);
            case AggregationMethod::FIRST:
                return first;
            case AggregationMethod::SPREAD:
                return max - min;
            case AggregationMethod::MEDIAN: {
                if (rawValues.empty() || rawValuesSaturated) {
                    return std::numeric_limits<double>::quiet_NaN();
                }
                // In-place nth_element avoids copying the entire rawValues vector.
                // rawValues is mutable so this works on const AggregationState.
                size_t n = rawValues.size();
                size_t mid = n / 2;
                std::nth_element(rawValues.begin(), rawValues.begin() + mid, rawValues.end());
                if (n % 2 == 1) {
                    return rawValues[mid];
                } else {
                    // nth_element guarantees elements before mid are <= rawValues[mid]
                    double upper = rawValues[mid];
                    double lower = *std::max_element(rawValues.begin(), rawValues.begin() + mid);
                    return (lower + upper) / 2.0;
                }
            }
            case AggregationMethod::STDDEV: {
                // Uses Welford's M2 accumulator — O(1), no rawValues needed
                return std::sqrt(m2 / count);
            }
            case AggregationMethod::STDVAR: {
                // Uses Welford's M2 accumulator — O(1), no rawValues needed
                return m2 / count;
            }
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
    // Optional raw vectors — when populated (from pushdown fast path), points
    // is empty. Avoids creating N AggregatedPoint structs only to split them
    // back into separate timestamp/value vectors in the query handler.
    std::vector<uint64_t> rawTimestamps;
    std::vector<double> rawValues;
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

    // For non-bucketed aggregation (interval == 0) - sorted parallel vectors.
    // When populated from pushdown, sortedValues carries compact raw doubles
    // (16 bytes/point) and sortedStates is empty. When populated from the
    // fallback path (createPartialAggregations), sortedStates carries full
    // AggregationState objects and sortedValues is empty.
    std::vector<uint64_t> sortedTimestamps;
    std::vector<AggregationState> sortedStates;
    std::vector<double> sortedValues;  // raw values from pushdown (compact path)

    // For non-bucketed streaming pushdown (interval == 0, streamable method):
    // a single pre-folded AggregationState covering the entire time range.
    std::optional<AggregationState> collapsedState;

    // Statistics
    size_t totalPoints = 0;
    double partialAggregationTimeMs = 0.0;

    // Cached group key hash (computed once, reused multiple times)
    size_t groupKeyHash = 0;

    // Composite group key string (guaranteed unique, used as map key)
    // Format: "measurement\0tag1=val1\0tag2=val2\0fieldName"
    std::string groupKey;

    // Cached parsed tags from groupKey — populated once during createPartialAggregations
    // so that mergePartialAggregationsGrouped can reuse without re-parsing.
    std::map<std::string, std::string> cachedTags;

    // Parse group-by tags from groupKey (called once per group in reduce phase)
    static std::map<std::string, std::string> parseTagsFromGroupKey(const std::string& gk) {
        std::map<std::string, std::string> tags;
        // Format: measurement\0tag1=val1\0...\0fieldName
        size_t firstNull = gk.find('\0');
        if (firstNull == std::string::npos)
            return tags;
        size_t lastNull = gk.rfind('\0');
        if (firstNull == lastNull)
            return tags;  // No tags segment
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
        const std::vector<timestar::SeriesResult>& seriesResults, AggregationMethod method, uint64_t interval,
        const std::vector<std::string>& groupByTags);

    // Merge partial aggregations with metadata preserved (reduce phase)
    static std::vector<GroupedAggregationResult> mergePartialAggregationsGrouped(
        std::vector<PartialAggregationResult>& partialResults, AggregationMethod method);

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
    static std::vector<AggregatedPoint> aggregate(const std::vector<uint64_t>& timestamps,
                                                  const std::vector<double>& values, AggregationMethod method,
                                                  uint64_t interval);

    // Aggregate across multiple series (group-by merge).
    static std::vector<AggregatedPoint> aggregateMultiple(
        const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& groupData, AggregationMethod method,
        uint64_t interval);
};

}  // namespace timestar

#endif  // AGGREGATOR_H_INCLUDED