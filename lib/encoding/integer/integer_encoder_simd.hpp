#ifndef INTEGER_ENCODER_SIMD_H_INCLUDED
#define INTEGER_ENCODER_SIMD_H_INCLUDED

#include <vector>
#include <cstdint>
#include "../../storage/aligned_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

/**
 * IntegerEncoderSIMD - AVX2 optimized implementation
 *
 * Uses AVX2 SIMD instructions for improved performance
 * Falls back to basic implementation if AVX2 not available
 */
class IntegerEncoderSIMD {
public:
    static AlignedBuffer encode(const std::vector<uint64_t> &values);
    static std::pair<size_t, size_t> decode(Slice &encoded, unsigned int timestampSize,
                                           std::vector<uint64_t> &values,
                                           uint64_t startTime = 0,
                                           uint64_t maxTime = UINT64_MAX);

    static bool isAvailable();
};

#endif // INTEGER_ENCODER_SIMD_H_INCLUDED