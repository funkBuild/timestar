#ifndef FLOAT_DECODER_AVX512_H_INCLUDED
#define FLOAT_DECODER_AVX512_H_INCLUDED

#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <vector>

/**
 * Float decoder (AVX-512 tier)
 *
 * Gorilla/XOR decoding is inherently sequential: each value depends on the
 * previous via XOR, so there is no meaningful SIMD parallelism in the decode
 * loop. This class delegates to FloatDecoderBasic and always reports itself
 * as available so the dispatcher in FloatDecoder works on every platform.
 */
class FloatDecoderAVX512 {
public:
    // Always available -- decoding delegates to the scalar FloatDecoderBasic.
    static bool isAvailable();

    // Kept for API compatibility; always returns true.
    static bool hasAVX512F();
    static bool hasAVX512DQ();

    // Decode (delegates to FloatDecoderBasic).
    static void decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);

    // Safe variant (identical to decode since isAvailable() is always true).
    static void decodeSafe(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);
};

#endif  // FLOAT_DECODER_AVX512_H_INCLUDED
