#pragma once

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

    // Per-instance block size cap, defaulting to the config value. Compaction
    // raises it for deep-tier outputs (see TSMCompactor::blockCapForTier):
    // high-cardinality workloads flush files whose per-series blocks hold a
    // few dozen points, and merges are the only chance to consolidate them --
    // bigger blocks at deeper tiers mean better compression ratios and fewer
    // index entries over data that is rewritten rarely and scanned much.
    size_t maxPointsPerBlock_ = MaxPointsPerBlock();

    // --- Streaming output state ---
    // Bytes already flushed to disk. Every recorded block offset is absolute
    // (flushedBytes_ + buffer.size()), so the buffer can be drained at any block
    // boundary without disturbing the index. Without this the whole output file
    // accumulated in `buffer`, making compaction memory scale with output size.
    uint64_t flushedBytes_ = 0;
    seastar::file streamFile_;
    bool streamFileOpen_ = false;
    // Set by abortStream(). Once aborted the object must not produce a file:
    // reopening would truncate and then write the buffered tail at
    // flushedBytes_, leaving a zero hole that still parses as a valid TSM file.
    bool aborted_ = false;
    // Drain the buffer once it exceeds this. Only whole DMA-alignment multiples
    // are written; the remainder is moved to the front and kept.
    static constexpr size_t FLUSH_THRESHOLD = 8u << 20;  // 8 MB

    void writeHeader();

    template <class T>
    TSMIndexEntry beginSeriesEntry(TSMValueType seriesType, const SeriesId128& seriesId, const std::vector<T>& values);
    template <class T>
    void writeSeriesBlockAt(TSMValueType seriesType, const SeriesId128& seriesId,
                            const std::vector<uint64_t>& timestamps, const std::vector<T>& values, size_t offset,
                            size_t blockSize, TSMIndexEntry& indexEntry);
    // Serialise one series' index entry into the buffer.
    void writeIndexEntryFor(const TSMIndexEntry& indexEntry);

    // Absolute offset of the next byte to be written, i.e. what a block's index
    // entry must record. Use this instead of buffer.size() anywhere an offset is
    // persisted.
    uint64_t currentOffset() const { return flushedBytes_ + buffer.size(); }

    seastar::future<> openStreamFileIfNeeded();
    // Write out as much of the buffer as DMA alignment allows, keeping the tail.
    seastar::future<> drainBuffer(bool finalDrain);

    // Shared series-processing loop used by both run() and runAsync().
    static void writeAllSeries(TSMWriter& writer, seastar::shared_ptr<MemoryStore> store);

public:
    TSMWriter(std::string _filename);

    // Set zstd compression level for string blocks (higher = better ratio, slower).
    // Call before writing any series. Default is 1 (fast).
    void setCompressionLevel(int level) { compressionLevel_ = level; }

    // Override the per-block point cap for this writer's output (compaction
    // passes a tier-scaled cap). Ignores zero to keep a misconfigured caller
    // from producing an unwritable file.
    void setMaxPointsPerBlock(size_t cap) {
        if (cap > 0) {
            maxPointsPerBlock_ = cap;
        }
    }
    size_t maxPointsPerBlock() const { return maxPointsPerBlock_; }

    template <class T>
    void writeSeries(TSMValueType seriesType, const SeriesId128& seriesId, const std::vector<uint64_t>& timestamps,
                     const std::vector<T>& values);

    // Same output as writeSeries(), but drains the buffer between blocks so a
    // single large series cannot pin its whole encoded size in memory. Use this
    // wherever series size is unbounded (compaction's merge path).
    template <class T>
    seastar::future<> writeSeriesStreaming(TSMValueType seriesType, const SeriesId128& seriesId,
                                           const std::vector<uint64_t>& timestamps, const std::vector<T>& values);

    // APPEND one chunk of a series that is being emitted incrementally.
    //
    // writeSeries()/writeSeriesStreaming() ASSIGN indexEntries[seriesId], so
    // calling either more than once for the same series silently discards every
    // earlier chunk's blocks. This appends instead, letting a caller emit a
    // series in bounded pieces without ever holding all of it.
    //
    // Points must be supplied in ascending timestamp order across calls. Blocks
    // from different series may interleave in the file -- the index records each
    // block's absolute offset, so physical ordering carries no meaning.
    //
    // String chunks are written WITHOUT dictionary encoding: a dictionary is
    // per-series-per-file and cannot be built without seeing every value, which
    // is exactly what incremental emission avoids.
    //
    // Takes both vectors BY VALUE: this is a coroutine that suspends on
    // flushIfNeeded(), and callers typically own the points in a closure that is
    // destroyed as soon as it returns a future (e.g. seastar::with_semaphore).
    // Reference parameters would dangle across the first suspension.
    template <class T>
    seastar::future<> appendSeriesChunk(TSMValueType seriesType, const SeriesId128& seriesId,
                                        std::vector<uint64_t> timestamps, std::vector<T> values);
    template <class T>
    void writeBlock(TSMValueType seriesType, const SeriesId128& seriesId, std::span<const uint64_t> timestamps,
                    std::span<const T> values, TSMIndexEntry& indexEntry);

    // Write compressed block carrying forward block stats from source file (zero-copy compaction)
    void writeCompressedBlockWithStats(TSMValueType seriesType, const SeriesId128& seriesId,
                                       seastar::temporary_buffer<uint8_t>&& compressedData,
                                       const TSMIndexBlock& srcBlock);

    // Attach a string dictionary to a series' index entry. Used by the
    // zero-copy compaction carry: STR2 blocks copied verbatim from a source
    // file reference dictionary IDs, so the output file's index must persist
    // the source's dictionary or the blocks become undecodable. Call after
    // writeCompressedBlockWithStats has created the entry for this series.
    void setSeriesStringDictionary(const SeriesId128& seriesId, std::shared_ptr<const std::vector<std::string>> dict) {
        indexEntries[seriesId].stringDictionary = std::move(dict);
    }

    // Drain the output buffer to disk if it has grown past FLUSH_THRESHOLD.
    // Safe to call at any BLOCK boundary (never mid-block: block writes
    // back-patch a length field into bytes still held in the buffer).
    // Callers that write large volumes -- compaction in particular -- must call
    // this as they go, otherwise the whole output file accumulates in memory.
    seastar::future<> flushIfNeeded();

    void writeIndex();

    // Same output as writeIndex(), but drains between series entries. The index
    // is the one part of the file that cannot be emitted incrementally as data
    // is written, and widening the per-series block count to uint32 removed the
    // uint16 cap that used to bound it implicitly -- at 80 bytes per block a
    // series with millions of blocks would otherwise build its whole index in
    // memory in one uninterrupted call.
    seastar::future<> writeIndexStreaming();

    void writeIndexBlock(std::span<const uint64_t> timestamps, TSMIndexEntry& indexEntry, size_t blockStartOffset);
    void writeIndexBlock(std::span<const uint64_t> timestamps, std::span<const double> values,
                         TSMIndexEntry& indexEntry, size_t blockStartOffset);
    void writeIndexBlock(std::span<const uint64_t> timestamps, std::span<const int64_t> values,
                         TSMIndexEntry& indexEntry, size_t blockStartOffset);
    void writeIndexBlock(std::span<const uint64_t> timestamps, const std::vector<bool>& values, size_t valOffset,
                         size_t valCount, TSMIndexEntry& indexEntry, size_t blockStartOffset);

    // Blocking close using POSIX I/O (for use in tests or seastar::async contexts)
    void close();

    // Async close using Seastar DMA I/O (non-blocking, for use on reactor thread)
    // Writes buffer to disk using open_file_dma + dma_write + flush + close.
    // Produces byte-identical output to close().
    seastar::future<> closeDMA();

    // Release the streaming file handle without finalising the file. Used on the
    // compaction failure path, where the partially-written temp file is about to
    // be unlinked: the fd must be closed first, and closeDMA() would otherwise
    // write an index and footer onto output that is being discarded anyway.
    seastar::future<> abortStream();

    // Blocking run: builds TSM file in memory, writes via POSIX I/O (for tests)
    static void run(seastar::shared_ptr<MemoryStore> store, std::string filename);

    // Async run: builds TSM file in memory, writes via Seastar DMA I/O (for production)
    static seastar::future<> runAsync(seastar::shared_ptr<MemoryStore> store, std::string filename);
};
