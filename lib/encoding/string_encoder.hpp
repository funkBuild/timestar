#pragma once

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <vector>

class StringEncoder {
private:
    // Write variable-length integer for string lengths
    static void writeVarInt(AlignedBuffer& buffer, uint32_t value);

    // Read variable-length integer
    static uint32_t readVarInt(Slice& slice);

    // Shared encode implementation: validates, builds varint-prefixed buffer, compresses.
    struct CompressedPayload {
        std::vector<char> data;
        uint32_t uncompressedSize;
        uint32_t compressedSize;
        uint32_t count;
    };
    static CompressedPayload compressStrings(std::span<const std::string> values, int compressionLevel);

public:
    StringEncoder() = default;

    // Encode strings with zstd compression.
    // Accepts std::span for zero-copy sub-range encoding; std::vector
    // converts implicitly.
    // Format: [header][compressed_data]
    // Header: magic_number(4) | uncompressed_size(4) | compressed_size(4) | count(4)
    // Data (before compression): [length_prefix][string_data] for each string
    // compressionLevel: zstd level (1=fast for fresh writes, 3=better ratio for compacted data).
    static AlignedBuffer encode(std::span<const std::string> values, int compressionLevel = 1);

    // Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
    // Writes the same format as encode() but directly into the target buffer,
    // eliminating the final result-buffer allocation and copy.
    // Returns the number of bytes written to the target buffer.
    static size_t encodeInto(std::span<const std::string> values, AlignedBuffer& target, int compressionLevel = 1);

    // Decode strings from compressed buffer
    static void decode(AlignedBuffer& encoded, size_t count, std::vector<std::string>& out);

    // Decode from a Slice (for TSM reading)
    static void decode(Slice& encoded, size_t count, std::vector<std::string>& out);

    // Decode with skip/count support - avoids allocating strings outside the [skipCount, skipCount+limitCount) range.
    // zstd decompression still happens on the full block (no random access), but individual string
    // copies are skipped for the first skipCount entries.
    static void decode(AlignedBuffer& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                       std::vector<std::string>& out);
    static void decode(Slice& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                       std::vector<std::string>& out);

    // ==================== Dictionary Encoding (Phase 3) ====================
    // For low-cardinality string series, dictionary encoding stores varint IDs
    // instead of raw strings, then compresses the IDs with zstd.
    // Magic: "STR2" (0x53545232) distinguishes from raw "STRG" (0x53545247).

    // Dictionary limits — fall back to raw encoding if exceeded.
    // Benchmarking showed dictionary encoding is only beneficial for cardinality <= ~50.
    // Above that, the dictionary overhead exceeds zstd's native compression of repeated strings.
    static constexpr size_t MAX_DICT_BYTES = 4 * 1024;  // 4KB max dictionary size
    static constexpr size_t MAX_DICT_ENTRIES = 50;      // 50 max unique strings

    // Build a dictionary from a set of string values.
    // Returns the dictionary (ordered vector of unique strings) and total byte size.
    // Returns empty dictionary if limits are exceeded.
    struct Dictionary {
        std::vector<std::string> entries;  // index -> string
        size_t totalBytes = 0;             // total serialized size
        bool valid = false;                // true if within limits
    };
    static Dictionary buildDictionary(std::span<const std::string> values);

    // Serialize a dictionary to bytes: count(4) + [varint_len + string_data]...
    static AlignedBuffer serializeDictionary(const Dictionary& dict);

    // Deserialize a dictionary from bytes.
    static Dictionary deserializeDictionary(Slice& encoded, size_t dictSize);

    // Encode strings using dictionary: replaces strings with varint IDs, then zstd-compresses.
    // Header: magic("STR2") + uncompressedSize(4) + compressedSize(4) + count(4)
    // Data: zstd-compressed varint IDs
    static AlignedBuffer encodeDictionary(std::span<const std::string> values, const Dictionary& dict,
                                          int compressionLevel = 1);

    // Decode dictionary-encoded block: decompress IDs, look up dictionary.
    static void decodeDictionary(Slice& encoded, size_t count, const Dictionary& dict, std::vector<std::string>& out);

    // Decode dictionary-encoded block with skip/limit support.
    static void decodeDictionary(Slice& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                                 const Dictionary& dict, std::vector<std::string>& out);

    // Check if a block is dictionary-encoded by peeking at the magic bytes.
    static bool isDictionaryEncoded(const uint8_t* data, size_t size);
    static bool isDictionaryEncoded(Slice& slice);
};
