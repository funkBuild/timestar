#ifndef FLOAT_ENCODER_SIMD_H_INCLUDED
#define FLOAT_ENCODER_SIMD_H_INCLUDED

#include "../../storage/compressed_buffer.hpp"
#include "float_encoder.hpp"

#include <immintrin.h>

#include <cstdint>
#include <span>
#include <vector>

/**
 * SIMD-optimized float encoder using AVX2 instructions
 * Provides 2-3x speedup for XOR operations
 */
class FloatEncoderSIMD {
public:
    // Check if AVX2 is available at runtime
    static bool isAvailable();

    // SIMD-optimized encoding
    static CompressedBuffer encode(std::span<const double> values);

    // Fallback to regular encoder if SIMD not available
    static CompressedBuffer encodeSafe(std::span<const double> values);

private:
    // Process 4 values at once using AVX2
    static void encodeBlock(const double* values, size_t count, CompressedBuffer& buffer, uint64_t& last_value,
                            int& data_bits, int& prev_lzb, int& prev_tzb);
};

#endif  // FLOAT_ENCODER_SIMD_H_INCLUDED