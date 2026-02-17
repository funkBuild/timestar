#include "float_encoder.hpp"

// Static member initialization
FloatEncoder::Implementation FloatEncoder::s_forced_impl = FloatEncoder::AUTO;

CompressedBuffer FloatEncoder::encode(std::span<const double> values) {
    if constexpr (FLOAT_COMPRESSION == FloatCompression::ALP) {
        return ALPEncoder::encode(values);
    } else {
        Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

        switch (impl) {
            case AVX512:
                if (FloatEncoderAVX512::isAvailable()) {
                    return FloatEncoderAVX512::encode(values);
                }
                // Fall through if not available
                [[fallthrough]];

            case SIMD:
                if (FloatEncoderSIMD::isAvailable()) {
                    return FloatEncoderSIMD::encode(values);
                }
                // Fall through if not available
                [[fallthrough]];

            case BASIC:
            case AUTO:
            default:
                return FloatEncoderBasic::encode(values);
        }
    }
}

// FloatDecoder implementation
FloatDecoder::Implementation FloatDecoder::s_forced_impl = FloatDecoder::AUTO;

void FloatDecoder::decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    if constexpr (FLOAT_COMPRESSION == FloatCompression::ALP) {
        ALPDecoder::decode(encoded, nToSkip, length, out);
    } else {
        Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

        switch (impl) {
            case AVX512:
                if (FloatDecoderAVX512::isAvailable()) {
                    FloatDecoderAVX512::decode(encoded, nToSkip, length, out);
                    return;
                }
                // Fall through if not available
                [[fallthrough]];

            case SIMD:
                if (FloatDecoderSIMD::isAvailable()) {
                    FloatDecoderSIMD::decode(encoded, nToSkip, length, out);
                    return;
                }
                // Fall through if not available
                [[fallthrough]];

            case BASIC:
            case AUTO:
            default:
                FloatDecoderBasic::decode(encoded, nToSkip, length, out);
        }
    }
}

FloatDecoder::Implementation FloatDecoder::selectBestImplementation() {
    if (FloatDecoderAVX512::isAvailable()) {
        return AVX512;
    }
    if (FloatDecoderSIMD::isAvailable()) {
        return SIMD;
    }
    return BASIC;
}

std::string FloatDecoder::getImplementationName() {
    if constexpr (FLOAT_COMPRESSION == FloatCompression::ALP) {
        return "ALP (Adaptive Lossless floating-Point)";
    } else {
        Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

        switch (impl) {
            case AVX512:
                if (FloatDecoderAVX512::isAvailable()) {
                    return "AVX-512 Decoder";
                }
                [[fallthrough]];

            case SIMD:
                if (FloatDecoderSIMD::isAvailable()) {
                    return "AVX2 SIMD Decoder";
                }
                [[fallthrough]];

            case BASIC:
            case AUTO:
            default:
                return "Basic Optimized Decoder";
        }
    }
}

bool FloatDecoder::hasAVX512() {
    return FloatDecoderAVX512::isAvailable();
}

bool FloatDecoder::hasAVX2() {
    return FloatDecoderSIMD::isAvailable();
}

void FloatDecoder::setImplementation(Implementation impl) {
    s_forced_impl = impl;
}

std::string FloatEncoder::getImplementationName() {
    if constexpr (FLOAT_COMPRESSION == FloatCompression::ALP) {
        return "ALP (Adaptive Lossless floating-Point)";
    } else {
        Implementation impl = (s_forced_impl == AUTO) ? selectBestImplementation() : s_forced_impl;

        switch (impl) {
            case AVX512:
                if (FloatEncoderAVX512::isAvailable()) {
                    return "AVX-512 (8x parallel)";
                }
                [[fallthrough]];

            case SIMD:
                if (FloatEncoderSIMD::isAvailable()) {
                    return "AVX2 SIMD (4x parallel)";
                }
                [[fallthrough]];

            case BASIC:
            case AUTO:
            default:
                return "Basic Optimized";
        }
    }
}

bool FloatEncoder::hasAVX512() {
    return FloatEncoderAVX512::isAvailable();
}

bool FloatEncoder::hasAVX2() {
    return FloatEncoderSIMD::isAvailable();
}

void FloatEncoder::setImplementation(Implementation impl) {
    s_forced_impl = impl;
}

FloatEncoder::Implementation FloatEncoder::selectBestImplementation() {
    if (FloatEncoderAVX512::isAvailable()) {
        return AVX512;
    }
    if (FloatEncoderSIMD::isAvailable()) {
        return SIMD;
    }
    return BASIC;
}
