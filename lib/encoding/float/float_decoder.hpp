#ifndef __FLOAT_DECODER_H_INCLUDED__
#define __FLOAT_DECODER_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <string>
#include "../../storage/compressed_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

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
    static void decode(CompressedSlice &values, size_t nToSkip, size_t length, std::vector<double> &out);
};

#endif // __FLOAT_DECODER_H_INCLUDED__