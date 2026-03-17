#pragma once

#include "../../storage/aligned_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <span>
#include <vector>

/**
 * IntegerEncoderBasic - Non-AVX implementation of integer encoder
 *
 * Uses delta-of-delta encoding with Simple16 compression
 * Optimized with loop unrolling but no SIMD instructions
 */
class IntegerEncoderBasic {
public:
    static AlignedBuffer encode(std::span<const uint64_t> values);

    // Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
    // Returns the number of bytes written to the target buffer.
    static size_t encodeInto(std::span<const uint64_t> values, AlignedBuffer& target);

    static std::pair<size_t, size_t> decode(Slice& encoded, unsigned int timestampSize, std::vector<uint64_t>& values,
                                            uint64_t startTime = 0, uint64_t maxTime = UINT64_MAX);

    static bool isAvailable() { return true; }  // Always available
};
