#pragma once

#include "../../storage/compressed_buffer.hpp"
#include "float_encoder.hpp"

#include <cstdint>
#include <span>
#include <vector>

/**
 * SIMD-optimized float encoder using Google Highway
 * Automatically selects the best available ISA (AVX-512, AVX2, SSE4, etc.)
 * via Highway's foreach_target / HWY_DYNAMIC_DISPATCH mechanism.
 */
class FloatEncoderSIMD {
public:
    // Always available -- Highway handles runtime dispatch internally
    static bool isAvailable();

    // Highway-dispatched encoding (selects best ISA at runtime)
    static CompressedBuffer encode(std::span<const double> values);

    // Fallback to regular encoder if needed (kept for API compatibility)
    static CompressedBuffer encodeSafe(std::span<const double> values);
};
