#ifndef NATIVE_INDEX_BLOCK_H_INCLUDED
#define NATIVE_INDEX_BLOCK_H_INCLUDED

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// BlockBuilder creates a block of sorted key-value pairs with prefix compression.
//
// Format:
//   For each entry:
//     shared_prefix_len  (varint32)
//     unshared_key_len   (varint32)
//     value_len          (varint32)
//     key_suffix         (unshared_key_len bytes)
//     value              (value_len bytes)
//
//   At the end:
//     restart_offsets[]   (uint32_t each, little-endian)
//     restart_count       (uint32_t, little-endian)
//
// Restart points are placed every `restart_interval` entries. At a restart point,
// shared_prefix_len is 0 (full key stored). This allows binary search over restart
// points followed by a short linear scan.
class BlockBuilder {
public:
    explicit BlockBuilder(int restart_interval = 16);

    // Add a key-value pair. Keys MUST be added in sorted order.
    void add(std::string_view key, std::string_view value);

    // Finalize the block and return the raw (uncompressed) bytes.
    // After calling finish(), the builder is consumed — call reset() to reuse.
    std::string finish();

    // Approximate current size of the block (for flush threshold decisions).
    size_t currentSize() const;

    // Whether any entries have been added.
    bool empty() const { return entryCount_ == 0; }

    // Reset the builder for reuse.
    void reset();

    // Number of entries added.
    size_t entryCount() const { return entryCount_; }

private:
    int restartInterval_;
    size_t entryCount_ = 0;
    std::string buffer_;
    std::string lastKey_;
    std::vector<uint32_t> restartOffsets_;

    static void appendVarint32(std::string& buf, uint32_t value);
};

// BlockReader reads a block produced by BlockBuilder and provides iteration
// with binary search support via restart points.
class BlockReader {
public:
    // Construct from raw (uncompressed) block data. The data must remain valid
    // for the lifetime of the reader and any iterators created from it.
    explicit BlockReader(std::string_view data);

    // Returns false if the block data is malformed.
    bool valid() const { return valid_; }

    class Iterator {
    public:
        Iterator() = default;

        // Position at the first key >= target.
        void seek(std::string_view target);

        // Position at the first entry in the block.
        void seekToFirst();

        // Advance to the next entry.
        void next();

        bool valid() const { return valid_; }
        std::string_view key() const { return key_; }
        std::string_view value() const { return value_; }

    private:
        friend class BlockReader;
        explicit Iterator(const BlockReader* reader);

        const BlockReader* reader_ = nullptr;
        bool valid_ = false;
        size_t offset_ = 0;  // Current position in block data
        std::string key_;    // Fully reconstructed key
        std::string_view value_;

        // Decode the entry at the current offset, updating key_ and value_.
        // Returns false if the offset is past the data region.
        bool decodeEntry();

        // Seek to restart point index and decode the entry there.
        void seekToRestartPoint(size_t restartIndex);
    };

    Iterator newIterator() const;

private:
    std::string_view data_;         // Raw block bytes (entries region only)
    const uint32_t* restarts_ = nullptr;  // Pointer into data_ for restart offsets
    uint32_t restartCount_ = 0;
    size_t dataSize_ = 0;          // Size of entries region (before restarts)
    bool valid_ = false;

    static uint32_t decodeFixed32(const char* p);
    static bool decodeVarint32(const char*& p, const char* limit, uint32_t& result);
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_BLOCK_H_INCLUDED
