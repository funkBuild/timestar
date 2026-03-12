#include "streaming_aggregator.hpp"

namespace timestar {

void StreamingAggregator::addPoint(const StreamingDataPoint& pt) {
    uint64_t bucket = bucketStart(pt.timestamp);

    SeriesFieldKey key;
    key.measurement = pt.measurement;
    key.tags = pt.tags;
    key.field = pt.field;

    auto& state = _buckets[bucket][key];

    std::visit(
        [&](const auto& val) {
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
        },
        pt.value);
}

StreamingBatch StreamingAggregator::closeBuckets(uint64_t nowNs) {
    StreamingBatch batch;

    // Find the half-open range [begin, eraseEnd) of completed buckets.
    // Since _buckets is ordered by time, completed buckets form a contiguous prefix.
    // Collect results first, then erase the whole prefix in one call.
    auto eraseEnd = _buckets.begin();
    for (auto it = _buckets.begin(); it != _buckets.end(); ++it) {
        // Skip the in-progress bucket (the one that hasn't completed yet).
        // A bucket [start, start+interval) is complete when start+interval <= now.
        // nowNs==0 means "close all" (used in tests or final flush).
        if (nowNs != 0 && it->first + _intervalNs > nowNs) {
            break;
        }

        for (auto& [key, state] : it->second) {
            if (state.count == 0)
                continue;
            if (state.isStringOnly)
                continue;

            StreamingDataPoint pt;
            pt.measurement = key.measurement;
            pt.tags = key.tags;
            pt.field = key.field;
            pt.timestamp = it->first;
            pt.value = state.computeResult(_method);
            batch.points.push_back(std::move(pt));
        }
        eraseEnd = std::next(it);
    }
    // Single range-erase: O(N + log(map_size)) instead of N * O(log(map_size))
    _buckets.erase(_buckets.begin(), eraseEnd);

    return batch;
}

}  // namespace timestar
