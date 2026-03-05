#include "streaming_aggregator.hpp"

namespace tsdb {

void StreamingAggregator::addPoint(const StreamingDataPoint& pt) {
    uint64_t bucket = bucketStart(pt.timestamp);

    SeriesFieldKey key;
    key.measurement = pt.measurement;
    key.tags = pt.tags;
    key.field = pt.field;

    auto& state = _buckets[bucket][key];

    std::visit([&](const auto& val) {
        using VT = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<VT, double>) {
            state.addDouble(val, pt.timestamp);
        } else if constexpr (std::is_same_v<VT, int64_t>) {
            state.addInt64(val, pt.timestamp);
        } else if constexpr (std::is_same_v<VT, bool>) {
            state.addDouble(val ? 1.0 : 0.0, pt.timestamp);
        } else if constexpr (std::is_same_v<VT, std::string>) {
            // String values can't be aggregated numerically; count only
            state.count++;
        }
    }, pt.value);
}

StreamingBatch StreamingAggregator::closeBuckets(uint64_t nowNs) {
    StreamingBatch batch;

    auto it = _buckets.begin();
    while (it != _buckets.end()) {
        // Skip the in-progress bucket (the one that hasn't completed yet).
        // A bucket [start, start+interval) is complete when start+interval <= now.
        // nowNs==0 means "close all" (used in tests or final flush).
        if (nowNs != 0 && it->first + _intervalNs > nowNs) {
            break;  // Map is time-ordered; all subsequent buckets are also in-progress
        }

        for (auto& [key, state] : it->second) {
            if (state.count == 0) continue;
            if (state.isStringOnly) continue;   // skip — no meaningful numeric aggregation

            StreamingDataPoint pt;
            pt.measurement = key.measurement;
            pt.tags = key.tags;
            pt.field = key.field;
            pt.timestamp = it->first;
            pt.value = state.computeResult(_method);
            batch.points.push_back(std::move(pt));
        }
        it = _buckets.erase(it);
    }

    return batch;
}

} // namespace tsdb
