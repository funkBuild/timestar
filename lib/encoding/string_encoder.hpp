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
    static size_t encodeInto(std::span<const std::string> values, AlignedBuffer& target,
                              int compressionLevel = 1);

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
};
