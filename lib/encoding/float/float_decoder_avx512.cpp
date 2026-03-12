#include "float_decoder_avx512.hpp"
#include "float_decoder.hpp"
#include "float_encoder.hpp"
#include <bit>
#include <cpuid.h>
#include <cstring>
#include <stdexcept>

bool FloatDecoderAVX512::hasAVX512F() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 16)) != 0; // AVX512F bit
        }
        return false;
    }();
    return available;
}

bool FloatDecoderAVX512::hasAVX512DQ() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 17)) != 0; // AVX512DQ bit
        }
        return false;
    }();
    return available;
}

bool FloatDecoderAVX512::isAvailable() {
    static const bool available = hasAVX512F() && hasAVX512DQ();
    return available;
}

void FloatDecoderAVX512::decodeSafe(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out) {
    if (isAvailable()) {
        decode(encoded, nToSkip, length, out);
    } else {
        // Fallback to regular decoder
        FloatEncoderBasic::decode(encoded, nToSkip, length, out);
    }
}

void FloatDecoderAVX512::decode(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out) {
    // Delegate to the basic decoder -- the AVX-512 and basic decoders are functionally
    // identical for this variable-length XOR format (the sequential data dependency
    // prevents meaningful SIMD parallelism in the decode loop).
    FloatDecoderBasic::decode(encoded, nToSkip, length, out);
}
