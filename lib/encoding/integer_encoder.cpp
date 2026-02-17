#include "integer_encoder.hpp"
#include "integer/integer_encoder.hpp"       // IntegerEncoderBasic
#include "integer/integer_encoder_simd.hpp"  // IntegerEncoderSIMD
#include "integer/integer_encoder_avx512.hpp" // IntegerEncoderAVX512

// Static member initialization
IntegerEncoder::Implementation IntegerEncoder::s_forced_impl = IntegerEncoder::AUTO;

AlignedBuffer IntegerEncoder::encode(std::span<const uint64_t> values) {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case AVX512:
            if (IntegerEncoderAVX512::isAvailable()) {
                return IntegerEncoderAVX512::encode(values);
            }
            // Fall through if not available

        case SIMD:
            if (IntegerEncoderSIMD::isAvailable()) {
                return IntegerEncoderSIMD::encode(values);
            }
            // Fall through if not available

        case BASIC:
        case AUTO:
        default:
            return IntegerEncoderBasic::encode(values);
    }
}

std::pair<size_t, size_t> IntegerEncoder::decode(Slice &encoded, unsigned int timestampSize,
                                                std::vector<uint64_t> &values,
                                                uint64_t minTime, uint64_t maxTime) {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case AVX512:
            if (IntegerEncoderAVX512::isAvailable()) {
                return IntegerEncoderAVX512::decode(encoded, timestampSize, values, minTime, maxTime);
            }
            // Fall through if not available

        case SIMD:
            if (IntegerEncoderSIMD::isAvailable()) {
                return IntegerEncoderSIMD::decode(encoded, timestampSize, values, minTime, maxTime);
            }
            // Fall through if not available

        case BASIC:
        case AUTO:
        default:
            return IntegerEncoderBasic::decode(encoded, timestampSize, values, minTime, maxTime);
    }
}

IntegerEncoder::Implementation IntegerEncoder::selectBestImplementation() {
    // Based on CPU capabilities: AVX-512 > AVX2 > Basic
    if (IntegerEncoderAVX512::isAvailable()) {
        return AVX512;
    }
    if (IntegerEncoderSIMD::isAvailable()) {
        return SIMD;
    }
    return BASIC;
}

std::string IntegerEncoder::getImplementationName() {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case AVX512:
            if (IntegerEncoderAVX512::isAvailable()) {
                return "AVX-512 (16x parallel)";
            }
            // Fall through

        case SIMD:
            if (IntegerEncoderSIMD::isAvailable()) {
                return "AVX2 SIMD (8x parallel)";
            }
            // Fall through

        case BASIC:
        case AUTO:
        default:
            return "Basic Optimized";
    }
}

bool IntegerEncoder::hasAVX512() {
    return IntegerEncoderAVX512::isAvailable();
}

bool IntegerEncoder::hasAVX2() {
    return IntegerEncoderSIMD::isAvailable();
}

void IntegerEncoder::setImplementation(Implementation impl) {
    s_forced_impl = impl;
}