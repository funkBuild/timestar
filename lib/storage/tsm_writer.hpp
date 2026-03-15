#ifndef TSM_WRITER_H_INCLUDED
#define TSM_WRITER_H_INCLUDED

#include "aligned_buffer.hpp"
#include "memory_store.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "tsm.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <span>
#include <string>
#include <vector>

// Optimized via benchmark: 3000 provides 25% faster queries with equal insert performance
// 3000 points × 16 bytes = 48KB fits perfectly in L2 cache (256-512KB)
// Configurable via [storage] max_points_per_block in TOML config.
inline size_t MaxPointsPerBlock() {
    return timestar::config().storage.max_points_per_block;
}

class TSMWriter {
private:
    AlignedBuffer buffer;
    // std::map is optimal for this use case (small dataset, maintains sorted order)
    std::map<SeriesId128, TSMIndexEntry> indexEntries;
    std::string filename;
    int compressionLevel_ = 1;  // zstd level: 1=fast (fresh writes), 3=better ratio (compacted)

    void writeHeader();

    // Shared series-processing loop used by both run() and runAsync().
    static void writeAllSeries(TSMWriter& writer, seastar::shared_ptr<MemoryStore> store);

public:
    TSMWriter(std::string _filename);

    // Set zstd compression level for string blocks (higher = better ratio, slower).
    // Call before writing any series. Default is 1 (fast).
    void setCompressionLevel(int level) { compressionLevel_ = level; }

    template <class T>
    void writeSeries(TSMValueType seriesType, const SeriesId128& seriesId, const std::vector<uint64_t>& timestamps,
                     const std::vector<T>& values);
    template <class T>
    void writeBlock(TSMValueType seriesType, const SeriesId128& seriesId, std::span<const uint64_t> timestamps,
                    std::span<const T> values, TSMIndexEntry& indexEntry);

    // Phase 3.2: Move semantics overloads for zero-copy writes
    template <class T>
    void writeSeriesDirect(TSMValueType seriesType, const SeriesId128& seriesId, std::vector<uint64_t>&& timestamps,
                           std::vector<T>&& values);
    template <class T>
    void writeBlockDirect(TSMValueType seriesType, const SeriesId128& seriesId, std::vector<uint64_t>&& timestamps,
                          std::vector<T>&& values, TSMIndexEntry& indexEntry);

    // Phase 2: Write compressed block bytes directly (zero-copy transfer)
    void writeCompressedBlock(TSMValueType seriesType, const SeriesId128& seriesId,
                              seastar::temporary_buffer<uint8_t>&& compressedData, uint64_t minTime, uint64_t maxTime);
    // Write compressed block carrying forward block stats from source file
    void writeCompressedBlockWithStats(TSMValueType seriesType, const SeriesId128& seriesId,
                                       seastar::temporary_buffer<uint8_t>&& compressedData,
                                       const TSMIndexBlock& srcBlock);

    void writeIndex();

    // Phase 4A: Parallel index building
    void writeIndexParallel();

    void writeIndexBlock(std::span<const uint64_t> timestamps, TSMIndexEntry& indexEntry, size_t blockStartOffset);
    void writeIndexBlock(std::span<const uint64_t> timestamps, std::span<const double> values,
                         TSMIndexEntry& indexEntry, size_t blockStartOffset);

    // Blocking close using POSIX I/O (for use in tests or seastar::async contexts)
    void close();

    // Async close using Seastar DMA I/O (non-blocking, for use on reactor thread)
    // Writes buffer to disk using open_file_dma + dma_write + flush + close.
    // Produces byte-identical output to close().
    seastar::future<> closeDMA();

    // Blocking run: builds TSM file in memory, writes via POSIX I/O (for tests)
    static void run(seastar::shared_ptr<MemoryStore> store, std::string filename);

    // Async run: builds TSM file in memory, writes via Seastar DMA I/O (for production)
    static seastar::future<> runAsync(seastar::shared_ptr<MemoryStore> store, std::string filename);
};

#endif
