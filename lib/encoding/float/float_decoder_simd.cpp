#include "float_decoder_simd.hpp"

#include "float_decoder.hpp"

bool FloatDecoderSIMD::isAvailable() {
    return true;
}

void FloatDecoderSIMD::decodeSafe(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    decode(encoded, nToSkip, length, out);
}

void FloatDecoderSIMD::decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    FloatDecoderBasic::decode(encoded, nToSkip, length, out);
}
