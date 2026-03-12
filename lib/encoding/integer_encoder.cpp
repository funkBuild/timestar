#include "integer_encoder.hpp"

#include "integer/integer_encoder.hpp"         // IntegerEncoderBasic
#include "integer/integer_encoder_avx512.hpp"  // IntegerEncoderAVX512
#include "integer/integer_encoder_ffor.hpp"    // IntegerEncoderFFOR
#include "integer/integer_encoder_simd.hpp"    // IntegerEncoderSIMD

// Static member initialization
thread_local IntegerEncoder::Implementation IntegerEncoder::s_forced_impl = IntegerEncoder::AUTO;

AlignedBuffer IntegerEncoder::encode(std::span<const uint64_t> values) {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case FFOR:
            return IntegerEncoderFFOR::encode(values);

        case AVX512:
            if (IntegerEncoderAVX512::isAvailable()) {
                return IntegerEncoderAVX512::encode(values);
            }
            // Fall through if not available
            [[fallthrough]];

        case SIMD:
            if (IntegerEncoderSIMD::isAvailable()) {
                return IntegerEncoderSIMD::encode(values);
            }
            // Fall through if not available
            [[fallthrough]];

        case BASIC:
        case AUTO:
        default:
            return IntegerEncoderBasic::encode(values);
    }
}

size_t IntegerEncoder::encodeInto(std::span<const uint64_t> values, AlignedBuffer& target) {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case FFOR:
            return IntegerEncoderFFOR::encodeInto(values, target);

        // SIMD and AVX512 do not have encodeInto(); fall through to BASIC
        // which shares the same Simple16 wire format.
        case AVX512:
        case SIMD:
        case BASIC:
        case AUTO:
        default:
            return IntegerEncoderBasic::encodeInto(values, target);
    }
}

std::pair<size_t, size_t> IntegerEncoder::decode(Slice& encoded, unsigned int timestampSize,
                                                 std::vector<uint64_t>& values, uint64_t minTime, uint64_t maxTime) {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case FFOR:
            return IntegerEncoderFFOR::decode(encoded, timestampSize, values, minTime, maxTime);

        case AVX512:
            if (IntegerEncoderAVX512::isAvailable()) {
                return IntegerEncoderAVX512::decode(encoded, timestampSize, values, minTime, maxTime);
            }
            // Fall through if not available
            [[fallthrough]];

        case SIMD:
            if (IntegerEncoderSIMD::isAvailable()) {
                return IntegerEncoderSIMD::decode(encoded, timestampSize, values, minTime, maxTime);
            }
            // Fall through if not available
            [[fallthrough]];

        case BASIC:
        case AUTO:
        default:
            return IntegerEncoderBasic::decode(encoded, timestampSize, values, minTime, maxTime);
    }
}

IntegerEncoder::Implementation IntegerEncoder::selectBestImplementation() {
    return FFOR;
}

std::string IntegerEncoder::getImplementationName() {
    Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

    switch (impl) {
        case FFOR:
            return "FFOR (Frame-of-Reference)";

        case AVX512:
            if (IntegerEncoderAVX512::isAvailable()) {
                return "AVX-512 (8x parallel)";
            }
            [[fallthrough]];

        case SIMD:
            if (IntegerEncoderSIMD::isAvailable()) {
                return "AVX2 SIMD (4x parallel)";
            }
            [[fallthrough]];

        case BASIC:
        case AUTO:
        default:
            return "Basic (Simple16)";
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