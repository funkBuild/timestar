#ifndef FLOAT_ENCODER_AUTO_H_INCLUDED
#define FLOAT_ENCODER_AUTO_H_INCLUDED

#include "float_encoder.hpp"
#include "float_encoder_avx512.hpp"
#include "float_encoder_simd.hpp"
#include "storage/compressed_buffer.hpp"

#include <vector>

/**
 * FloatEncoderAuto - Automatically selects the best available encoder
 *
 * Both FloatEncoderSIMD and FloatEncoderAVX512 now delegate to the same
 * Google Highway implementation which automatically selects the best ISA
 * (AVX-512, AVX2, SSE4, etc.) via HWY_DYNAMIC_DISPATCH.
 */
class FloatEncoderAuto {
public:
    /**
     * Encode using the best available implementation.
     * Both SIMD classes delegate to the same Highway dispatch, so the
     * priority order is preserved for API compatibility but both paths
     * produce identical results.
     */
    static CompressedBuffer encode(const std::vector<double>& values) {
        // Highway-dispatched (selects best ISA at runtime)
        if (FloatEncoderAVX512::isAvailable()) {
            return FloatEncoderAVX512::encode(values);
        }

        if (FloatEncoderSIMD::isAvailable()) {
            return FloatEncoderSIMD::encode(values);
        }

        // Fallback to optimized original
        return FloatEncoder::encode(values);
    }

    /**
     * Decode - all encoders produce compatible output
     * Uses the standard FloatDecoder which works with all encoder outputs
     */
    static void decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
        FloatDecoder::decode(encoded, nToSkip, length, out);
    }

    /**
     * Get information about which encoder will be used
     */
    static std::string getEncoderName() {
        return "Highway SIMD (auto-dispatch)";
    }

    /**
     * Check feature availability -- always true with Highway
     */
    static bool hasAVX512() { return FloatEncoderAVX512::isAvailable(); }

    static bool hasAVX2() { return FloatEncoderSIMD::isAvailable(); }
};

#endif  // FLOAT_ENCODER_AUTO_H_INCLUDED