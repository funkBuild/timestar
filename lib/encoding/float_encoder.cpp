#include "float_encoder.hpp"

#include "../storage/aligned_buffer.hpp"

CompressedBuffer FloatEncoder::encode(std::span<const double> values) {
    if (values.empty()) [[unlikely]] {
        return {};
    }
    return ALPEncoder::encode(values);
}

size_t FloatEncoder::encodeInto(std::span<const double> values, AlignedBuffer& target) {
    if (values.empty()) [[unlikely]] {
        return 0;
    }
    return ALPEncoder::encodeInto(values, target);
}

std::string FloatEncoder::getImplementationName() {
    return "ALP (Adaptive Lossless floating-Point)";
}

void FloatDecoder::decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    ALPDecoder::decode(encoded, nToSkip, length, out);
}

std::string FloatDecoder::getImplementationName() {
    return "ALP (Adaptive Lossless floating-Point)";
}
