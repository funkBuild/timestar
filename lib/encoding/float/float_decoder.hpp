#pragma once

#include "../../storage/compressed_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <string>
#include <vector>

/**
 * FloatDecoderBasic - Optimized non-AVX decoder
 *
 * Features:
 * - Pre-allocation with smart buffer sizing
 * - Direct memory writes instead of push_back
 * - Cache prefetching
 * - Optimized bit reading patterns
 */
class FloatDecoderBasic {
public:
    static void decode(CompressedSlice& values, size_t nToSkip, size_t length, std::vector<double>& out);
};
