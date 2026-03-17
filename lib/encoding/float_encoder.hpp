#pragma once

#include "../storage/compressed_buffer.hpp"
#include "../storage/slice_buffer.hpp"
#include "alp/alp_decoder.hpp"
#include "alp/alp_encoder.hpp"

#include <span>
#include <string>
#include <vector>

class AlignedBuffer;

/**
 * FloatEncoder - ALP (Adaptive Lossless floating-Point) compression.
 *
 * Uses the ALP algorithm (SIGMOD 2024) for efficient lossless compression
 * of double-precision floating-point values.
 */
class FloatEncoder {
public:
    /**
     * Encode doubles using ALP compression.
     * Accepts std::span for zero-copy sub-range encoding; std::vector
     * converts implicitly.
     */
    static CompressedBuffer encode(std::span<const double> values);

    /**
     * Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
     * Returns the number of bytes written to the target buffer.
     */
    static size_t encodeInto(std::span<const double> values, AlignedBuffer& target);

    /**
     * Get the name of the encoder implementation being used.
     */
    static std::string getImplementationName();
};

/**
 * FloatDecoder - ALP (Adaptive Lossless floating-Point) decompression.
 */
class FloatDecoder {
public:
    /**
     * Decode compressed data back to doubles.
     */
    static void decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);

    /**
     * Get the name of the decoder implementation being used.
     */
    static std::string getImplementationName();
};
