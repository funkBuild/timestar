#ifndef FLOAT_ENCODER_AVX512_H_INCLUDED
#define FLOAT_ENCODER_AVX512_H_INCLUDED

#include <vector>
#include <cstdint>
#include <immintrin.h>
#include "../../storage/compressed_buffer.hpp"
#include "float_encoder.hpp"

/**
 * AVX-512 optimized float encoder
 * Processes 8 double values simultaneously for maximum throughput
 * Expected 3-4x speedup over scalar implementation
 */
class FloatEncoderAVX512 {
public:
    // Runtime AVX-512 detection
    static bool isAvailable();
    
    // Check for specific AVX-512 features we need
    static bool hasAVX512F();  // Foundation
    static bool hasAVX512DQ(); // Double/Quad word instructions
    
    // AVX-512 optimized encoding
    static CompressedBuffer encode(const std::vector<double>& values);

    // Safe encoding with automatic fallback
    static CompressedBuffer encodeSafe(const std::vector<double>& values);
};

#endif // FLOAT_ENCODER_AVX512_H_INCLUDED