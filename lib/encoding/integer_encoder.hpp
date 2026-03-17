#pragma once

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <vector>

/**
 * IntegerEncoder - FFOR (Frame-of-Reference) based integer encoder.
 *
 * Uses delta-of-delta + ZigZag preprocessing followed by block-based
 * FFOR bit-packing with an exception mechanism for outliers.
 * Google Highway SIMD is used for vectorized operations.
 */
class IntegerEncoder {
public:
    /**
     * Encode uint64_t values.
     * Accepts std::span for zero-copy sub-range encoding; std::vector
     * converts implicitly.
     */
    static AlignedBuffer encode(std::span<const uint64_t> values);

    /**
     * Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
     * Returns the number of bytes written to the target buffer.
     */
    static size_t encodeInto(std::span<const uint64_t> values, AlignedBuffer& target);

    /**
     * Decode compressed data back to uint64_t values.
     */
    static std::pair<size_t, size_t> decode(Slice& encoded, unsigned int timestampSize, std::vector<uint64_t>& values,
                                            uint64_t startTime = 0, uint64_t maxTime = UINT64_MAX);

    /**
     * Get the name of the encoder implementation being used.
     */
    static std::string getImplementationName();
};
