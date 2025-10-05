#ifndef __INTEGER_ENCODER_BASE_H_INCLUDED__
#define __INTEGER_ENCODER_BASE_H_INCLUDED__

#include <vector>
#include <cstdint>
#include "../../storage/aligned_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

/**
 * IntegerEncoderBasic - Non-AVX implementation of integer encoder
 *
 * Uses delta-of-delta encoding with Simple16 compression
 * Optimized with loop unrolling but no SIMD instructions
 */
class IntegerEncoderBasic {
public:
    static AlignedBuffer encode(const std::vector<uint64_t> &values);
    static std::pair<size_t, size_t> decode(Slice &encoded, unsigned int timestampSize,
                                           std::vector<uint64_t> &values,
                                           uint64_t startTime = 0,
                                           uint64_t maxTime = UINT64_MAX);

    static bool isAvailable() { return true; } // Always available
};

#endif // __INTEGER_ENCODER_BASE_H_INCLUDED__