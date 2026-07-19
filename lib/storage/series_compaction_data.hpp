#pragma once

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

    // Zero-copy path: a REFERENCE to a source block, not its bytes.
    //
    // The block is read from its source file at write time, one at a time, and
    // released immediately after being handed to the writer. Holding the actual
    // compressed bytes for every block here made a single high-volume series
    // pin the whole series' on-disk size in RAM (hundreds of MB at deep tiers),
    // which is one of the allocations that made compaction throw std::bad_alloc.
    // A reference is ~100 bytes regardless of how large the block is.
    struct BlockRef {
        seastar::shared_ptr<TSM> sourceFile;  // keeps the file open until read
        TSMIndexBlock indexBlock;             // offset/size + carried-forward stats
        uint64_t minTime = 0;
        uint64_t maxTime = 0;
    };
    std::vector<BlockRef> blockRefs;
    bool isZeroCopy = false;

    // String series only: dictionary carried from the single source file when
    // the zero-copy path is taken. STR2 blocks store dictionary IDs, so the
    // output file's index entry MUST persist this dictionary — otherwise every
    // value of the series becomes permanently undecodable after compaction.
    std::shared_ptr<const std::vector<std::string>> stringDictionary;

    // Slow path data (decompressed and merged)
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    // True when the merged points were handed off incrementally to a sink, so
    // `timestamps`/`values` hold at most a trailing remnant and must NOT be
    // written again by the caller.
    bool emittedViaSink = false;

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
