#ifndef FLOAT_ENCODER_AVX512_H_INCLUDED
#define FLOAT_ENCODER_AVX512_H_INCLUDED

#include "../../storage/compressed_buffer.hpp"
#include "float_encoder.hpp"

#include <cstdint>
#include <span>
#include <vector>

/**
 * AVX-512 optimized float encoder
 * Now delegates to Google Highway which automatically selects the best ISA
 * (AVX-512, AVX2, SSE4, etc.) via foreach_target / HWY_DYNAMIC_DISPATCH.
 * Kept for API compatibility -- all methods are defined in float_encoder_simd.cpp.
 */
class FloatEncoderAVX512 {
public:
    // Always returns true -- Highway handles runtime dispatch internally
    static bool isAvailable();

    // Kept for API compatibility
    static bool hasAVX512F();
    static bool hasAVX512DQ();

    // Delegates to Highway-dispatched encoding
    static CompressedBuffer encode(std::span<const double> values);

    // Safe encoding with automatic fallback
    static CompressedBuffer encodeSafe(std::span<const double> values);
};

#endif  // FLOAT_ENCODER_AVX512_H_INCLUDED
