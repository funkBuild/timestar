#pragma once

#include "../../core/timestar_value.hpp"  // TimeStarInsert
#include "../data/write_record.hpp"       // WriteSeries, WriteBatch

#include <cstdint>
#include <string>
#include <vector>

namespace timestar::cluster {

// Convert the write handler's already-grouped TimeStarInsert<T> (one per field,
// carrying all timestamps/values/revisions for that series) into the lossless
// inter-node WriteSeries (integration plan M2 write path). The seriesKey string is
// the identity whose hash routes it, so the owner node re-derives the same routing
// -- no lossy DataPoint. One conversion per field-series; the router groups them by
// owner and forwards remote groups as a batched WriteBatch.

template <typename T>
constexpr TSMValueType clusterInsertValueType();
template <>
constexpr TSMValueType clusterInsertValueType<double>() {
    return TSMValueType::Float;
}
template <>
constexpr TSMValueType clusterInsertValueType<int64_t>() {
    return TSMValueType::Integer;
}
template <>
constexpr TSMValueType clusterInsertValueType<bool>() {
    return TSMValueType::Boolean;
}
template <>
constexpr TSMValueType clusterInsertValueType<std::string>() {
    return TSMValueType::String;
}

template <typename T>
data::WriteSeries writeSeriesFromInsert(const TimeStarInsert<T>& ins) {
    data::WriteSeries s;
    s.seriesKey = ins.seriesKey();
    s.type = clusterInsertValueType<T>();
    s.timestamps = ins.getTimestamps();
    s.values = ins.values;        // vector<T> selects the matching WriteSeries variant slot
    s.revisions = ins.revisions;  // pass-through (RF=1: Engine stamps at insert; empty here)
    return s;
}

// Append every insert (all four value types) as a WriteSeries onto `batch`. Called
// after the handler has built its per-field inserts; the resulting WriteBatch is
// handed to NodeWriteRouter, which routes local vs remote by owner.
inline void appendInsertsToBatch(data::WriteBatch& batch, const std::vector<TimeStarInsert<double>>& doubles,
                                 const std::vector<TimeStarInsert<bool>>& bools,
                                 const std::vector<TimeStarInsert<std::string>>& strings,
                                 const std::vector<TimeStarInsert<int64_t>>& integers) {
    batch.series.reserve(batch.series.size() + doubles.size() + bools.size() + strings.size() + integers.size());
    for (const auto& i : doubles)
        batch.series.push_back(writeSeriesFromInsert<double>(i));
    for (const auto& i : bools)
        batch.series.push_back(writeSeriesFromInsert<bool>(i));
    for (const auto& i : strings)
        batch.series.push_back(writeSeriesFromInsert<std::string>(i));
    for (const auto& i : integers)
        batch.series.push_back(writeSeriesFromInsert<int64_t>(i));
}

}  // namespace timestar::cluster
