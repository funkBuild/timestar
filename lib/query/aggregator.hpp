#pragma once

// NaN handling policy: NaN = missing data. All aggregation methods skip NaN
// values (they are excluded from SUM, AVG, COUNT, MIN, MAX, etc.). See
// docs/nan_policy.md for the full cross-subsystem policy.

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
    // rawValues is only needed for MEDIAN — all other methods (STDDEV/STDVAR use Welford,
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
    double sumCompensation = 0.0;  // Kahan compensation term for numerically stable summation
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
    // Running mean for canonical Welford algorithm — avoids catastrophic cancellation
    // that occurs when deriving oldMean from (sum - value) / count for extreme value ranges.
    double mean = 0.0;
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
    void addValue(double value, uint64_t timestamp) {
        if (std::isnan(value))
            return;  // Skip NaN to avoid poisoning aggregation
        // Kahan compensated summation for precision over millions of points
        double y = value - sumCompensation;
        double t = sum + y;
        sumCompensation = (t - sum) - y;
        sum = t;
        min = std::min(min, value);
        max = std::max(max, value);
        // Tie-breaking: last-processed value wins at equal timestamps (non-deterministic across merge orders).
        if (timestamp >= latestTimestamp) {
            latest = value;
            latestTimestamp = timestamp;
        }
        if (timestamp < firstTimestamp) {
            first = value;
            firstTimestamp = timestamp;
        }
        // Canonical Welford's online variance: track mean as a dedicated field to
        // avoid catastrophic cancellation from (sum - value) / count.
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;
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
            case AggregationMethod::SUM: {
                double y = value - sumCompensation;
                double t = sum + y;
                sumCompensation = (t - sum) - y;
                sum = t;
                count++;
                mean = (sum + sumCompensation) / static_cast<double>(count);
                break;
            }
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
                if (timestamp < firstTimestamp) {
                    first = value;
                    firstTimestamp = timestamp;
                }
                count++;
                break;
            case AggregationMethod::STDDEV:
            case AggregationMethod::STDVAR: {
                // Welford only needs mean/m2/count — skip sum update (not used
                // by getValue(STDDEV/STDVAR) and avoids Kahan inconsistency).
                count++;
                double delta = value - mean;
                mean += delta / count;
                double delta2 = value - mean;
                m2 += delta * delta2;
                break;
            }
            default:
                // MEDIAN and unknown methods fall back to full addValue
                addValue(value, timestamp);
                break;
        }
        // Track first/latest timestamps for all methods so getTimestamp()
        // returns a meaningful value. LATEST/FIRST handle this in their
        // own branches; MEDIAN falls through to addValue which tracks too.
        if (method != AggregationMethod::LATEST && method != AggregationMethod::FIRST) {
            if (timestamp < firstTimestamp) {
                firstTimestamp = timestamp;
                first = value;
            }
            if (timestamp >= latestTimestamp) {
                latestTimestamp = timestamp;
                latest = value;
            }
        }
    }

    // Full merge — includes Welford M2 and raw values (for STDDEV/MEDIAN).
    void merge(const AggregationState& other) {
        mergeWelford(other);
        mergeCore(other);
        mergeRawValues(other);
    }

    void merge(AggregationState&& other) {
        mergeWelford(other);
        mergeCore(other);
        mergeRawValuesMove(std::move(other));
    }

    // Method-aware merge — skips expensive Welford variance computation and raw
    // value copying for methods that don't need them (~90% of queries).
    void mergeForMethod(const AggregationState& other, AggregationMethod method) {
        const bool useWelford = (method == AggregationMethod::STDDEV || method == AggregationMethod::STDVAR);
        if (useWelford)
            mergeWelford(other);  // computes numerically stable weighted mean
        mergeCore(other);
        // Update mean from sum/count only when Welford did NOT compute it.
        // Preserving the Welford mean is critical for STDDEV/STDVAR precision.
        if (!useWelford && count > 0)
            mean = (sum + sumCompensation) / static_cast<double>(count);
        if (method == AggregationMethod::MEDIAN)
            mergeRawValues(other);
    }

    // Rvalue overload: allows callers passing std::move() to actually move
    // raw values (for MEDIAN) instead of copying them.
    void mergeForMethod(AggregationState&& other, AggregationMethod method) {
        const bool useWelford = (method == AggregationMethod::STDDEV || method == AggregationMethod::STDVAR);
        if (useWelford)
            mergeWelford(other);
        mergeCore(other);
        if (!useWelford && count > 0)
            mean = (sum + sumCompensation) / static_cast<double>(count);
        if (method == AggregationMethod::MEDIAN)
            mergeRawValuesMove(std::move(other));
    }

private:
    // Parallel Welford merge for M2 and mean. MUST be called BEFORE mergeCore
    // (which updates count) because the merge formula uses pre-merge counts.
    void mergeWelford(const AggregationState& other) {
        if (count > 0 && other.count > 0) {
            double delta = other.mean - mean;
            double totalCount = static_cast<double>(count) + other.count;
            m2 += other.m2 + delta * delta * (static_cast<double>(count) * other.count) / totalCount;
            // Numerically stable weighted mean: avoids overflow of mean*count for large counts
            mean = mean + delta * (static_cast<double>(other.count) / totalCount);
        } else if (other.count > 0) {
            m2 = other.m2;
            mean = other.mean;
        }
    }

    // Core merge: scalar accumulators only (sum, min, max, latest, first, count).
    // No Welford, no raw values. ~3x faster than full merge.
    // CRITICAL: must be called AFTER Welford parallel merge (which uses pre-merge counts).
    // NOTE: Does NOT update `mean` — callers that need an accurate mean must update it
    // themselves. When used with mergeWelford, the Welford-computed mean must be preserved.
    void mergeCore(const AggregationState& other) {
        // Kahan compensated merge: fold both sums and their compensation terms.
        double otherTotal = other.sum + other.sumCompensation;
        double y = otherTotal - sumCompensation;
        double t = sum + y;
        sumCompensation = (t - sum) - y;
        sum = t;
        min = std::min(min, other.min);
        max = std::max(max, other.max);
        if (other.latestTimestamp >= latestTimestamp) {
            latest = other.latest;
            latestTimestamp = other.latestTimestamp;
        }
        if (other.firstTimestamp < firstTimestamp) {
            first = other.first;
            firstTimestamp = other.firstTimestamp;
        }
        count += other.count;
    }

    void mergeRawValues(const AggregationState& other) {
        if (other.rawValuesSaturated) {
            rawValuesSaturated = true;
        }
        if (!rawValuesSaturated) {
            size_t available = RAW_VALUES_HARD_LIMIT - rawValues.size();
            size_t toCopy = std::min(available, other.rawValues.size());
            rawValues.insert(rawValues.end(), other.rawValues.begin(),
                             other.rawValues.begin() + static_cast<std::ptrdiff_t>(toCopy));
            if (toCopy < other.rawValues.size()) {
                rawValuesSaturated = true;
            }
        }
    }

    // Move-aware raw values merge: avoids copying when the source is an rvalue.
    void mergeRawValuesMove(AggregationState&& other) {
        if (other.rawValuesSaturated) {
            rawValuesSaturated = true;
        }
        if (!rawValuesSaturated && !other.rawValues.empty()) {
            size_t available = RAW_VALUES_HARD_LIMIT - rawValues.size();
            if (rawValues.empty()) {
                rawValues = std::move(other.rawValues);
                if (rawValues.size() > RAW_VALUES_HARD_LIMIT) {
                    rawValues.resize(RAW_VALUES_HARD_LIMIT);
                    rawValuesSaturated = true;
                }
            } else {
                size_t toCopy = std::min(available, other.rawValues.size());
                rawValues.insert(
                    rawValues.end(), std::make_move_iterator(other.rawValues.begin()),
                    std::make_move_iterator(other.rawValues.begin() + static_cast<std::ptrdiff_t>(toCopy)));
                if (toCopy < other.rawValues.size())
                    rawValuesSaturated = true;
            }
        }
    }

public:
    // Extract final aggregated value based on method
    // Return the representative timestamp for a collapsed state.
    // LATEST uses latestTimestamp; FIRST uses firstTimestamp; others use firstTimestamp
    // as a sensible default (earliest data point in the state).
    [[nodiscard]] uint64_t getTimestamp(AggregationMethod method) const {
        if (method == AggregationMethod::LATEST)
            return latestTimestamp;
        return firstTimestamp;
    }

    [[nodiscard]] double getValue(AggregationMethod method) const {
        if (count == 0) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        switch (method) {
            case AggregationMethod::AVG:
                return (sum + sumCompensation) / count;
            case AggregationMethod::MIN:
                return min;
            case AggregationMethod::MAX:
                return max;
            case AggregationMethod::SUM:
                return sum + sumCompensation;
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
                // Uses Welford's M2 accumulator — O(1), no rawValues needed.
                // Guard against slightly negative m2 from floating-point error
                // in parallel Welford merge (delta^2 cancellation).
                return std::sqrt(std::max(0.0, m2 / count));
            }
            case AggregationMethod::STDVAR: {
                // Uses Welford's M2 accumulator — O(1), no rawValues needed.
                return std::max(0.0, m2 / count);
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

// Methods that can be computed via streaming fold (addValueForMethod) without
// materialising all raw values. MEDIAN requires nth_element on the full
// dataset so it is NOT streamable.  LATEST and FIRST are streamable because
// they fold into AggregationState via addValueForMethod.
[[nodiscard]] inline bool isStreamableMethod(AggregationMethod method) {
    switch (method) {
        case AggregationMethod::AVG:
        case AggregationMethod::MIN:
        case AggregationMethod::MAX:
        case AggregationMethod::SUM:
        case AggregationMethod::COUNT:
        case AggregationMethod::SPREAD:
        case AggregationMethod::STDDEV:
        case AggregationMethod::STDVAR:
        case AggregationMethod::LATEST:
        case AggregationMethod::FIRST:
            return true;
        default:
            return false;
    }
}

}  // namespace timestar
