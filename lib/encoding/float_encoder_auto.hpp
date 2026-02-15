#ifndef FLOAT_ENCODER_AUTO_H_INCLUDED
#define FLOAT_ENCODER_AUTO_H_INCLUDED

#include <vector>
#include "float_encoder.hpp"
#include "float_encoder_simd.hpp"
#include "float_encoder_avx512.hpp"
#include "storage/compressed_buffer.hpp"

/**
 * FloatEncoderAuto - Automatically selects the best available encoder
 * 
 * Priority order (based on benchmarks):
 * 1. AVX-512 (2.10x speedup on large datasets)
 * 2. AVX2 SIMD (2.4x theoretical speedup)
 * 3. Original optimized (baseline)
 */
class FloatEncoderAuto {
public:
    /**
     * Encode using the best available implementation
     * Automatically detects CPU features and selects optimal encoder
     */
    static CompressedBuffer encode(const std::vector<double>& values) {
        // AVX-512 is fastest when available (2.10x speedup)
        if (FloatEncoderAVX512::isAvailable()) {
            return FloatEncoderAVX512::encode(values);
        }
        
        // AVX2 is second best (2.4x theoretical, not tested on this system)
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
        if (FloatEncoderAVX512::isAvailable()) {
            return "AVX-512 (8x parallel)";
        }
        if (FloatEncoderSIMD::isAvailable()) {
            return "AVX2 SIMD (4x parallel)";
        }
        return "Original (optimized)";
    }
    
    /**
     * Check feature availability
     */
    static bool hasAVX512() {
        return FloatEncoderAVX512::isAvailable();
    }
    
    static bool hasAVX2() {
        return FloatEncoderSIMD::isAvailable();
    }
};

#endif // FLOAT_ENCODER_AUTO_H_INCLUDED