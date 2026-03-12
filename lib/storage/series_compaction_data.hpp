#ifndef SERIES_COMPACTION_DATA_H_INCLUDED
#define SERIES_COMPACTION_DATA_H_INCLUDED

#include "series_id.hpp"
#include "tsm.hpp"

#include <seastar/core/temporary_buffer.hh>
#include <vector>

// Phase 3: Data structure for parallel series compaction
// Holds the result of processing a single series, ready to write
template <typename T>
struct SeriesCompactionData {
    SeriesId128 seriesId;
    TSMValueType seriesType;

    // Zero-copy path data (compressed blocks)
    struct CompressedBlock {
        seastar::temporary_buffer<uint8_t> data;
        uint64_t minTime;
        uint64_t maxTime;
    };
    std::vector<CompressedBlock> compressedBlocks;
    bool isZeroCopy = false;

    // Slow path data (decompressed and merged)
    std::vector<uint64_t> timestamps;
    std::vector<T> values;

    // Statistics
    uint64_t pointsRead = 0;
    uint64_t pointsWritten = 0;
    uint64_t duplicatesRemoved = 0;

    // Constructor
    SeriesCompactionData(const SeriesId128& id, TSMValueType type) : seriesId(id), seriesType(type) {}

    // Move-only (contains temporary_buffer)
    SeriesCompactionData(SeriesCompactionData&&) = default;
    SeriesCompactionData& operator=(SeriesCompactionData&&) = default;
    SeriesCompactionData(const SeriesCompactionData&) = delete;
    SeriesCompactionData& operator=(const SeriesCompactionData&) = delete;
};

#endif  // SERIES_COMPACTION_DATA_H_INCLUDED
