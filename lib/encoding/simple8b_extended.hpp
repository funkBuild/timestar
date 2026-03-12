#ifndef SIMPLE8B_EXTENDED_H_INCLUDED
#define SIMPLE8B_EXTENDED_H_INCLUDED

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <cstdint>
#include <vector>

/**
 * Simple8BExtended - An extension of Simple8B that can handle full 64-bit values
 *
 * This class extends Simple8B with a fallback mechanism for values that exceed
 * the 60-bit limit. It uses a special marker to indicate when raw 64-bit values
 * follow in the stream.
 *
 * Encoding scheme:
 * - Values <= 60 bits: Use standard Simple8B encoding
 * - Values > 60 bits: Write a special marker (selector 0 with all bits set)
 *                     followed by the raw 64-bit value
 */
class Simple8BExtended {
public:
    // Special marker: selector 0 with remaining 60 bits all set to 1
    // This combination is impossible in normal Simple8B (selector 0 is for runs of 1s)
    static constexpr uint64_t OVERFLOW_MARKER = 0x0FFFFFFFFFFFFFFFULL;
    static constexpr uint64_t MAX_SIMPLE8B_VALUE = (1ULL << 60) - 1;

    /**
     * Encode a vector of 64-bit integers with overflow support
     * Values up to 60 bits use Simple8B compression
     * Values over 60 bits are stored uncompressed with a marker
     */
    static AlignedBuffer encode(const std::vector<uint64_t>& values);

    /**
     * Decode a buffer encoded with Simple8BExtended
     * Handles both Simple8B compressed values and overflow values
     */
    static std::vector<uint64_t> decode(Slice& encoded, unsigned int expectedSize = 0);

private:
    // Helper to check if a value needs overflow handling
    static bool needsOverflow(uint64_t value) { return value > MAX_SIMPLE8B_VALUE; }

    // Simple8B selector packing schemes (n values of b bits each)
    struct PackingScheme {
        uint64_t selector;
        uint64_t numValues;
        uint64_t bitsPerValue;
    };

    static constexpr PackingScheme schemes[] = {{0, 240, 0},  // Special: 240 zeros
                                                {1, 120, 0},  // Special: 120 zeros
                                                {2, 60, 1},  {3, 30, 2},  {4, 20, 3},  {5, 15, 4},  {6, 12, 5},
                                                {7, 10, 6},  {8, 8, 7},   {9, 7, 8},   {10, 6, 10}, {11, 5, 12},
                                                {12, 4, 15}, {13, 3, 20}, {14, 2, 30}, {15, 1, 60}};

    static bool canPack(const std::vector<uint64_t>& values, size_t offset, uint64_t n, uint64_t bits);
    static uint64_t pack(const std::vector<uint64_t>& values, size_t& offset, uint64_t selector, uint64_t n,
                         uint64_t bits);
    static void unpack(uint64_t packedValue, std::vector<uint64_t>& out, uint64_t n, uint64_t bits);
};

#endif