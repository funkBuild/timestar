#ifndef INTEGER_ENCODER_AVX512_H_INCLUDED
#define INTEGER_ENCODER_AVX512_H_INCLUDED

#include <vector>
#include <cstdint>
#include <span>
#include "../../storage/aligned_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

/**
 * IntegerEncoderAVX512 - AVX-512 optimized implementation
 *
 * Uses AVX-512 SIMD instructions for maximum performance
 * Processes 8x64-bit values in parallel where possible
 */
class IntegerEncoderAVX512 {
public:
    static AlignedBuffer encode(std::span<const uint64_t> values);
    static std::pair<size_t, size_t> decode(Slice &encoded, unsigned int timestampSize,
                                           std::vector<uint64_t> &values,
                                           uint64_t startTime = 0,
                                           uint64_t maxTime = UINT64_MAX);

    static bool isAvailable();
};

#endif // INTEGER_ENCODER_AVX512_H_INCLUDED