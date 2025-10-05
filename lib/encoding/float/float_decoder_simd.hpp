#ifndef __FLOAT_DECODER_SIMD_H_INCLUDED__
#define __FLOAT_DECODER_SIMD_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <immintrin.h>
#include "../../storage/slice_buffer.hpp"

/**
 * SIMD-optimized float decoder using AVX2 instructions
 *
 * While decoding is inherently sequential due to variable-length encoding
 * and XOR dependencies, we can still optimize:
 * 1. Batch XOR operations when multiple values share the same pattern
 * 2. Use SIMD for the XOR operations themselves
 * 3. Prefetch data for better cache utilization
 */
class FloatDecoderSIMD {
public:
    // Check if AVX2 is available at runtime
    static bool isAvailable();

    // SIMD-optimized decoding
    static void decode(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out);

    // Fallback to regular decoder if SIMD not available
    static void decodeSafe(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out);
};

#endif // __FLOAT_DECODER_SIMD_H_INCLUDED__