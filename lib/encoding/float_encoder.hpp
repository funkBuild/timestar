#ifndef FLOAT_ENCODER_MAIN_H_INCLUDED
#define FLOAT_ENCODER_MAIN_H_INCLUDED

#include <vector>
#include <span>
#include <string>
#include "float/float_encoder.hpp"  // Contains FloatEncoderBasic
#include "float/float_encoder_simd.hpp"
#include "float/float_encoder_avx512.hpp"
#include "float/float_decoder.hpp"  // Contains FloatDecoderBasic
#include "float/float_decoder_simd.hpp"
#include "float/float_decoder_avx512.hpp"
#include "alp/alp_encoder.hpp"
#include "alp/alp_decoder.hpp"
#include "../storage/compressed_buffer.hpp"
#include "../storage/slice_buffer.hpp"

/**
 * Compile-time selection of float compression algorithm.
 * Change this constexpr to switch the entire storage layer between algorithms.
 *   GORILLA - Gorilla XOR-delta encoding (original, with AVX-512/AVX2 acceleration)
 *   ALP     - Adaptive Lossless floating-Point compression (SIGMOD 2024)
 */
enum class FloatCompression {
    GORILLA,
    ALP
};

static constexpr FloatCompression FLOAT_COMPRESSION = FloatCompression::ALP;

/**
 * Main FloatEncoder class that automatically selects the best implementation
 * based on CPU capabilities.
 */
class FloatEncoder {
public:
    /**
     * Encode doubles using the best available implementation.
     * Accepts std::span for zero-copy sub-range encoding; std::vector
     * converts implicitly.
     */
    static CompressedBuffer encode(std::span<const double> values);

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

#endif // FLOAT_ENCODER_MAIN_H_INCLUDED