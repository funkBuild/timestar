#include "float_decoder_avx512.hpp"

#include "float_decoder.hpp"

bool FloatDecoderAVX512::hasAVX512F() {
    return true;
}

bool FloatDecoderAVX512::hasAVX512DQ() {
    return true;
}

bool FloatDecoderAVX512::isAvailable() {
    return true;
}

void FloatDecoderAVX512::decodeSafe(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    decode(encoded, nToSkip, length, out);
}

void FloatDecoderAVX512::decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    FloatDecoderBasic::decode(encoded, nToSkip, length, out);
}
