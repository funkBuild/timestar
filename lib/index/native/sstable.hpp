#ifndef NATIVE_INDEX_SSTABLE_H_INCLUDED
#define NATIVE_INDEX_SSTABLE_H_INCLUDED

#include "block.hpp"
#include "bloom_filter.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// SSTable file format:
//   [Data Block 0] [Data Block 1] ... [Data Block N]
//   [Bloom Filter Block]
//   [Index Block]
//   [Footer (48 bytes)]
//
// Footer layout (48 bytes):
//   bloom_offset   (uint64_t)
//   bloom_size     (uint64_t)
//   index_offset   (uint64_t)
//   index_size     (uint64_t)
//   entry_count    (uint64_t)
//   magic          (uint32_t)  0x54534958 = "TSIX"
//   version        (uint32_t)  1

static constexpr uint32_t SSTABLE_MAGIC = 0x54534958;  // "TSIX"
static constexpr uint32_t SSTABLE_VERSION = 1;
static constexpr size_t SSTABLE_FOOTER_SIZE = 48;

struct SSTableMetadata {
    uint64_t fileNumber = 0;
    uint64_t entryCount = 0;
    uint64_t fileSize = 0;
    std::string minKey;
    std::string maxKey;
    int level = 0;
};

struct IndexEntry {
    std::string firstKey;  // First key in the data block
    uint64_t offset;       // File offset of the data block
    uint32_t size;         // Compressed size of the data block
};

// Writes a sorted sequence of key-value pairs to an SSTable file.
class SSTableWriter {
public:
    // Create a writer for a new SSTable file.
    static seastar::future<SSTableWriter> create(std::string filename, int blockSize = 16384,
                                                  int bloomBitsPerKey = 15);

    // Add a key-value pair. Keys MUST be added in sorted order.
    // Buffers data in memory — no I/O until finish().
    void add(std::string_view key, std::string_view value);

    // Finalize the SSTable: flush remaining block, write bloom filter,
    // index block, and footer. Returns metadata about the written file.
    seastar::future<SSTableMetadata> finish();

    // Abort without finishing (deletes the partial file).
    seastar::future<> abort();

    size_t entryCount() const { return entryCount_; }

private:
    SSTableWriter() = default;

    void flushBlock();

    std::string filename_;
    BlockBuilder currentBlock_;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
    std::string pendingData_;  // All block data buffered here until finish()
    uint64_t fileOffset_ = 0;
    size_t entryCount_ = 0;
    int blockSize_;
    std::string firstKey_;
    std::string lastKey_;
    std::string currentBlockFirstKey_;
};

// Reads an SSTable file and provides point lookups and range iteration.
class SSTableReader {
public:
    // Open an existing SSTable file.
    static seastar::future<std::unique_ptr<SSTableReader>> open(std::string filename);

    // Point lookup: returns the value for a key, or nullopt if not found.
    // Uses bloom filter to skip unnecessary block reads.
    seastar::future<std::optional<std::string>> get(std::string_view key);

    // Range iteration
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
        std::string blockData_;  // Current decompressed block
        std::unique_ptr<BlockReader> blockReader_;
        BlockReader::Iterator blockIter_;
        std::string key_;
        std::string_view value_;

        seastar::future<> loadBlock(size_t idx);
        void updateFromBlockIter();
    };

    seastar::future<std::unique_ptr<Iterator>> newIterator();

    const SSTableMetadata& metadata() const { return metadata_; }
    const std::string& filename() const { return filename_; }

    seastar::future<> close();

private:
    SSTableReader() = default;

    seastar::future<std::string> readBlock(size_t blockIndex);

    seastar::file file_;
    std::string filename_;
    SSTableMetadata metadata_;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_SSTABLE_H_INCLUDED
