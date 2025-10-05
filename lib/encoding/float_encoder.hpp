#ifndef __FLOAT_ENCODER_MAIN_H_INCLUDED__
#define __FLOAT_ENCODER_MAIN_H_INCLUDED__

#include <vector>
#include <string>
#include "float/float_encoder.hpp"  // Contains FloatEncoderBasic
#include "float/float_encoder_simd.hpp"
#include "float/float_encoder_avx512.hpp"
#include "float/float_decoder.hpp"  // Contains FloatDecoderBasic
#include "float/float_decoder_simd.hpp"
#include "float/float_decoder_avx512.hpp"
#include "../storage/compressed_buffer.hpp"
#include "../storage/slice_buffer.hpp"

/**
 * Main FloatEncoder class that automatically selects the best implementation
 * based on CPU capabilities.
 */
class FloatEncoder {
public:
    /**
     * Encode a vector of doubles using the best available implementation
     */
    static CompressedBuffer encode(const std::vector<double>& values);

    /**
     * Get the name of the encoder implementation being used
     */
    static std::string getImplementationName();

    /**
     * Check which implementations are available
     */
    static bool hasAVX512();
    static bool hasAVX2();

    /**
     * Force a specific implementation (for testing)
     */
    enum Implementation {
        AUTO,     // Automatically select best
        BASIC,    // Force basic implementation
        SIMD,     // Force SIMD AVX2
        AVX512    // Force AVX-512
    };

    static void setImplementation(Implementation impl);

private:
    static Implementation selectBestImplementation();
    static Implementation s_forced_impl;
};

/**
 * Main FloatDecoder class that automatically selects the best implementation
 * based on CPU capabilities.
 *
 * All decoder implementations are compatible with all encoder outputs.
 */
class FloatDecoder {
public:
    /**
     * Decode compressed data back to doubles using the best available implementation
     */
    static void decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);

    /**
     * Get the name of the decoder implementation being used
     */
    static std::string getImplementationName();

    /**
     * Check which implementations are available
     */
    static bool hasAVX512();
    static bool hasAVX2();

    /**
     * Force a specific implementation (for testing)
     */
    enum Implementation {
        AUTO,     // Automatically select best
        BASIC,    // Force basic implementation
        SIMD,     // Force SIMD AVX2
        AVX512    // Force AVX-512
    };

    static void setImplementation(Implementation impl);

private:
    static Implementation selectBestImplementation();
    static Implementation s_forced_impl;
};

#endif // __FLOAT_ENCODER_MAIN_H_INCLUDED__