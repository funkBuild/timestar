#pragma once

#include "subscription_manager.hpp"
#include "query_parser.hpp"

#include <map>
#include <string>
#include <variant>
#include <vector>
#include <cmath>

namespace tsdb {

// Running aggregation state for a single time bucket of a single series+field.
struct BucketState {
    double sum = 0.0;
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::lowest();
    double latest = 0.0;
    uint64_t latestTimestamp = 0;
    uint64_t count = 0;
    bool isStringOnly = true;   // true until a numeric value is added

    void addDouble(double val, uint64_t ts) {
        isStringOnly = false;   // numeric value seen
        sum += val;
        if (val < min) min = val;
        if (val > max) max = val;
        if (ts >= latestTimestamp) {
            latest = val;
            latestTimestamp = ts;
        }
        ++count;
    }

    void addInt64(int64_t val, uint64_t ts) {
        isStringOnly = false;
        addDouble(static_cast<double>(val), ts);
    }

    double computeResult(AggregationMethod method) const {
        if (count == 0) return std::numeric_limits<double>::quiet_NaN();
        switch (method) {
            case AggregationMethod::AVG: return sum / static_cast<double>(count);
            case AggregationMethod::SUM: return sum;
            case AggregationMethod::MIN: return min;
            case AggregationMethod::MAX: return max;
            case AggregationMethod::LATEST: return latest;
            default: return sum / static_cast<double>(count);
        }
    }
};

// Key for identifying a unique series+field combination in aggregation.
struct SeriesFieldKey {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;

    bool operator<(const SeriesFieldKey& other) const {
        if (measurement != other.measurement) return measurement < other.measurement;
        if (field != other.field) return field < other.field;
        return tags < other.tags;
    }
};

// StreamingAggregator accumulates raw data points into time-bucketed aggregated
// values. It is used inside the SSE handler coroutine when aggregationInterval > 0.
//
// Usage:
//   aggregator.addPoint(point);   // Called for each incoming data point
//   auto batch = aggregator.closeBuckets();  // Called on timer tick
//
class StreamingAggregator {
public:
    StreamingAggregator(uint64_t intervalNs, AggregationMethod method)
        : _intervalNs(intervalNs), _method(method) {}

    // Add a data point to the appropriate time bucket.
    void addPoint(const StreamingDataPoint& pt);

    // Close completed buckets and return aggregated results as a StreamingBatch.
    // Only emits buckets whose time range has fully elapsed (bucketStart + interval <= nowNs).
    // Pass nowNs=0 to close all buckets unconditionally (useful in tests / final flush).
    StreamingBatch closeBuckets(uint64_t nowNs = 0);

    // Check if any data has been accumulated.
    bool hasData() const { return !_buckets.empty(); }

    uint64_t intervalNs() const { return _intervalNs; }

private:
    uint64_t _intervalNs;
    AggregationMethod _method;

    // bucketStart -> (SeriesFieldKey -> BucketState)
    std::map<uint64_t, std::map<SeriesFieldKey, BucketState>> _buckets;

    uint64_t bucketStart(uint64_t timestamp) const {
        return timestamp - (timestamp % _intervalNs);
    }
};

} // namespace tsdb
