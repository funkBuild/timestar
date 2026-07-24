#pragma once

#include "../../storage/tsm.hpp"  // TSMValueType

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace timestar::data {

using ::TSMValueType;  // TSMValueType lives in the global namespace (like SeriesId128)

// The LOSSLESS inter-node write unit (integration plan F.1). Unlike the flat
// DataPoint{SeriesId128, ts, double} -- which cannot carry a real Engine write
// (SeriesId128 is a one-way hash, string fields are unrepresentable, int64 loses
// precision) -- WriteSeries carries the CANONICAL series-key string (whose hash IS
// the series identity, so the receiver's SeriesId128 / virtualShard / core routing
// are guaranteed identical to the sender's) plus a single typed value column. It is
// deliberately isomorphic to TimeStarInsert<T>: the receiver rebuilds it via
// TimeStarInsert<T>::fromSeriesKey(seriesKey) and drives Engine::insertBatch<T>.
struct WriteSeries {
    std::string seriesKey;  // "measurement,tag=val,... field" -- hash(seriesKey) == SeriesId128
    TSMValueType type = TSMValueType::Float;
    std::vector<uint64_t> timestamps;
    // Exactly one alternative is active, selected by `type` (Float->double,
    // Integer->int64, Boolean->bool, String->string). Its size == timestamps.size().
    std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>, std::vector<std::string>>
        values;
    // Per-point replicated revision (ADR 0003), parallel to values. Empty == not
    // yet assigned (RF=1 Engine stamps at insert; RF=3 state machine fills these
    // from the log so the Engine must not re-stamp).
    std::vector<uint64_t> revisions;

    // Whether the active alternative and its size are consistent with `type` and
    // `timestamps`. A decoded record is validated with this before use.
    bool consistent() const;
};

struct WriteBatch {
    std::vector<WriteSeries> series;
    uint64_t schemaVersion = 0;
};

// Wire codec (bounds-checked; decode returns nullopt on ANY malformed/truncated/
// inconsistent input so a hostile frame can never fabricate a batch). FNV-checksum
// trailer, same discipline as data_command.
std::string encodeWriteBatch(const WriteBatch& batch);
std::optional<WriteBatch> decodeWriteBatch(const std::string& bytes);

}  // namespace timestar::data
