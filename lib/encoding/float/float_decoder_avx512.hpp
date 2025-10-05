#ifndef __FLOAT_DECODER_AVX512_H_INCLUDED__
#define __FLOAT_DECODER_AVX512_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <immintrin.h>
#include "../../storage/slice_buffer.hpp"

/**
 * AVX-512 optimized float decoder
 *
 * While decoding is inherently sequential, AVX-512 provides:
 * 1. More aggressive prefetching capabilities
 * 2. Mask registers for branchless operations
 * 3. Wider registers for potential future optimizations
 */
class FloatDecoderAVX512 {
public:
    // Runtime AVX-512 detection
    static bool isAvailable();

    // Check for specific AVX-512 features we need
    static bool hasAVX512F();  // Foundation
    static bool hasAVX512DQ(); // Double/Quad word instructions

    // AVX-512 optimized decoding
    static void decode(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out);

    // Safe decoding with automatic fallback
    static void decodeSafe(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out);
};

#endif // __FLOAT_DECODER_AVX512_H_INCLUDED__