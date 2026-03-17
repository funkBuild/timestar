#pragma once

#include "../../storage/aligned_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <span>
#include <vector>

/**
 * IntegerEncoderAVX512 - Google Highway optimized implementation
 *
 * Delegates to IntegerEncoderSIMD which uses Highway for portable SIMD dispatch.
 * Highway automatically selects the best available target at runtime
 * (SSE4, AVX2, AVX-512, etc.), so this class produces identical results
 * to IntegerEncoderSIMD.
 */
class IntegerEncoderAVX512 {
public:
    static AlignedBuffer encode(std::span<const uint64_t> values);
    static std::pair<size_t, size_t> decode(Slice& encoded, unsigned int timestampSize, std::vector<uint64_t>& values,
                                            uint64_t startTime = 0, uint64_t maxTime = UINT64_MAX);

    static bool isAvailable();
};
