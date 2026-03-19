#pragma once

#include "block.hpp"
#include "block_cache.hpp"
#include "bloom_filter.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// SSTable file format:
//   [Data Block 0] [Data Block 1] ... [Data Block N]
//     Each block: [uncompressed_size(4)] [compressed_data(N)] [crc32(4)]
//   [Bloom Filter Block]
//   [Index Block]
//   [Footer (64 bytes)]
//
// Footer layout (64 bytes):
//   bloom_offset       (uint64_t)  offset 0
//   bloom_size         (uint64_t)  offset 8
//   index_offset       (uint64_t)  offset 16
//   index_size         (uint64_t)  offset 24
//   entry_count        (uint64_t)  offset 32
//   write_timestamp_ns (uint64_t)  offset 40  — wall-clock ns when SSTable was created
//   reserved           (uint64_t)  offset 48  — for future use
//   magic              (uint32_t)  offset 56  0x54534958 = "TSIX"
//   version            (uint32_t)  offset 60  1

static constexpr uint32_t SSTABLE_MAGIC = 0x54534958;  // "TSIX"
static constexpr uint32_t SSTABLE_VERSION = 1;
static constexpr size_t SSTABLE_FOOTER_SIZE = 64;

struct SSTableMetadata {
    uint64_t fileNumber = 0;
    uint64_t entryCount = 0;
    uint64_t fileSize = 0;
    std::string minKey;
    std::string maxKey;
    int level = 0;
    uint64_t writeTimestamp = 0;  // Wall-clock nanoseconds when SSTable was created
};

struct IndexEntry {
    std::string firstKey;  // First key in the data block
    uint64_t offset;       // File offset of the data block
    uint32_t size;         // Compressed size of the data block
};

// Writes a sorted sequence of key-value pairs to an SSTable file.
// Step 3: Streaming writes — opens file at create(), flushes blocks to disk
// as the write buffer fills, bounding memory to ~256KB instead of unbounded.
class SSTableWriter {
public:
    // Create a writer for a new SSTable file. Opens the file handle.
    // compressionLevel: zstd level (1=fast for L0 flushes, 3=better ratio for compacted L1+).
    static seastar::future<SSTableWriter> create(std::string filename, int blockSize = 16384, int bloomBitsPerKey = 15,
                                                 int compressionLevel = 1);

    // Add a key-value pair. Keys MUST be added in sorted order.
    // Synchronous — buffers data in memory. Call flushPending() periodically
    // to stream accumulated data to disk when the buffer exceeds the threshold.
    void add(std::string_view key, std::string_view value);

    // Stream-flush pending data to disk if the write buffer exceeds the threshold.
    // Call periodically between add() calls to bound memory usage.
    seastar::future<> flushPending();

    // Finalize the SSTable: flush remaining block, write bloom filter,
    // index block, and footer. Returns metadata about the written file.
    seastar::future<SSTableMetadata> finish();

    // Abort without finishing (deletes the partial file).
    seastar::future<> abort();

    size_t entryCount() const { return entryCount_; }

private:
    SSTableWriter() = default;

    void flushBlock();
    seastar::future<> maybeStreamFlush();
    seastar::future<> streamFlush();

    std::string filename_;
    BlockBuilder currentBlock_;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
    std::string pendingData_;   // Bounded write buffer (~256KB)
    size_t pendingOffset_ = 0;  // Offset into pendingData_ (avoids erase(0,N) copies)
    uint64_t fileOffset_ = 0;   // Logical data offset (for index entries)
    uint64_t diskOffset_ = 0;   // Physical write position on disk
    size_t entryCount_ = 0;
    int blockSize_;
    int compressionLevel_;
    std::string firstKey_;
    std::string lastKey_;
    std::string currentBlockFirstKey_;
    uint64_t writeTimestampNs_ = 0;  // Captured at create() time

    // Step 3: Streaming I/O
    seastar::file file_;
    bool fileOpen_ = false;
    size_t dmaAlign_ = 0;
    static constexpr size_t STREAM_FLUSH_THRESHOLD = 256 * 1024;
};

// Move-only container for a raw (compressed) block read from disk.
// Holds the DMA temporary_buffer and the on-disk block size.
struct RawBlock {
    seastar::temporary_buffer<char> data;
    uint32_t size;

    RawBlock() = default;
    RawBlock(seastar::temporary_buffer<char> d, uint32_t s) : data(std::move(d)), size(s) {}

    // Move-only (temporary_buffer is move-only)
    RawBlock(const RawBlock&) = delete;
    RawBlock& operator=(const RawBlock&) = delete;
    RawBlock(RawBlock&&) noexcept = default;
    RawBlock& operator=(RawBlock&&) noexcept = default;
};

// Reads an SSTable file and provides point lookups and range iteration.
// Step 1: Lazy block loading — only footer, index, and bloom filter are parsed
// at open(). Data blocks are decompressed on demand.
// Step 2: Optional shared block cache avoids repeated decompression of hot blocks.
class SSTableReader {
public:
    ~SSTableReader();

    // Non-copyable, non-movable (fd ownership)
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;
    SSTableReader(SSTableReader&&) = delete;
    SSTableReader& operator=(SSTableReader&&) = delete;

    // Open an existing SSTable file. Reads entire file into memory,
    // parses footer/index/bloom, but does NOT decompress data blocks.
    static seastar::future<std::unique_ptr<SSTableReader>> open(std::string filename, BlockCache* cache = nullptr);

    // Point lookup: returns the value for a key, or nullopt if not found.
    // Uses bloom filter to skip unnecessary block reads.
    // Reads compressed block from disk on demand (cached if block cache provided).
    seastar::future<std::optional<std::string>> get(std::string_view key);

    // Step 8: Existence check without copying the value.
    // Returns true if the key exists, false otherwise.
    seastar::future<bool> contains(std::string_view key);

    // Range iteration.
    class Iterator {
    public:
        seastar::future<> seek(std::string_view target);
        seastar::future<> seekToFirst();
        seastar::future<> next();

        bool valid() const { return valid_; }
        std::string_view key() const { return key_; }
        std::string_view value() const { return value_; }

    public:
        explicit Iterator(SSTableReader* reader);

    private:
        SSTableReader* reader_;
        bool valid_ = false;
        size_t blockIndex_ = 0;  // Current index entry
        std::string blockData_;  // Step 1: owned decompressed block data
        std::unique_ptr<BlockReader> blockReader_;
        BlockReader::Iterator blockIter_;
        std::string key_;
        std::string_view value_;

        seastar::future<> loadBlock(size_t idx);
        void updateFromBlockIter();
    };

    std::unique_ptr<Iterator> newIterator();

    const SSTableMetadata& metadata() const { return metadata_; }
    const std::string& filename() const { return filename_; }

    // Step 1: close() releases raw file data.
    // Step 2: Proactively evicts blocks from the shared cache.
    seastar::future<> close();

    // Decompress a single block from the raw file data. Returns the
    // decompressed block content. Does NOT consult the block cache.
    seastar::future<std::string> decompressBlock(size_t blockIndex);

    size_t blockCount() const { return index_.size(); }
    uint64_t cacheId() const { return cacheId_; }

    // Concurrency semaphore: limits concurrent cache-miss block reads per shard.
    // Set by NativeIndex::open() — pointer is per-shard (thread-local in Seastar).
    static seastar::semaphore* blockReadSemaphore_;
    static void setBlockReadSemaphore(seastar::semaphore* sem);

private:
    SSTableReader() = default;

    // I/O-only: read the raw (compressed) block from disk via DMA.
    seastar::future<RawBlock> readRawBlock(size_t blockIndex);

    // CPU-only: verify CRC and decompress a raw block. Pure function, no I/O.
    static std::string decompressRawBlock(const RawBlock& raw, std::string_view filename, size_t blockIndex);

    // Find the block index for a key via binary search on the index.
    // Uses two-phase lookup when summary index is available:
    //   Phase 1: Binary search compact summary_ (~N/64 entries) to narrow range
    //   Phase 2: Binary search within the narrowed range of index_ (~64 entries)
    size_t findBlock(std::string_view key) const;

    // Build the summary index from index_ entries. Called once during open().
    void buildSummary();

    // Get decompressed block data, checking cache first.
    // On cache hit, returns the cached block string (no I/O).
    // On cache miss, reads and decompresses from disk asynchronously.
    seastar::future<std::string> getDecompressedBlock(size_t idx);

    std::string filename_;
    SSTableMetadata metadata_;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;

    // Two-level summary index for faster block lookups at high cardinality.
    // Samples every SUMMARY_INTERVAL-th entry from index_ to narrow binary search.
    // SummaryEntry::key is a string_view into index_[i].firstKey — safe because
    // index_ is immutable after open() and outlives summary_.
    static constexpr size_t SUMMARY_INTERVAL = 64;

    struct SummaryEntry {
        std::string_view key;  // Points into index_[indexPos].firstKey
        size_t indexPos;       // Position in index_ array
    };
    std::vector<SummaryEntry> summary_;

    // On-disk streaming: keep a Seastar DMA file open for async reads of data blocks.
    // Only metadata (bloom + index) is held in memory — data blocks are read on demand
    // and cached via the block cache. This bounds per-SSTable memory to O(bloom + index)
    // instead of O(file_size).
    seastar::file readFile_;
    bool readFileOpen_ = false;

    // Step 2: Block cache (optional, shared across SSTables on this shard)
    BlockCache* blockCache_ = nullptr;
    uint64_t cacheId_ = 0;
};

}  // namespace timestar::index
