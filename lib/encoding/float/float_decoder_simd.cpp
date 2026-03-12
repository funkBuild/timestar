#include "float_decoder_simd.hpp"

#include "float_decoder.hpp"
#include "float_encoder.hpp"

#include <cpuid.h>

#include <bit>
#include <cstring>
#include <stdexcept>

bool FloatDecoderSIMD::isAvailable() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 5)) != 0;  // AVX2 bit
        }
        return false;
    }();
    return available;
}

void FloatDecoderSIMD::decodeSafe(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    if (isAvailable()) {
        decode(encoded, nToSkip, length, out);
    } else {
        FloatEncoderBasic::decode(encoded, nToSkip, length, out);
    }
}

void FloatDecoderSIMD::decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    // Delegate to the basic decoder -- the SIMD and basic decoders are functionally
    // identical for this variable-length XOR format (the sequential data dependency
    // prevents meaningful SIMD parallelism in the decode loop).
    FloatDecoderBasic::decode(encoded, nToSkip, length, out);
}
