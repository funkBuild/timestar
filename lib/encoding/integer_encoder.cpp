#include "integer_encoder.hpp"
#include "integer/integer_encoder.hpp"       // IntegerEncoderBasic
#include "integer/integer_encoder_simd.hpp"  // IntegerEncoderSIMD
#include "integer/integer_encoder_avx512.hpp" // IntegerEncoderAVX512
#include "integer/integer_encoder_ffor.hpp"  // IntegerEncoderFFOR

// Static member initialization
IntegerEncoder::Implementation IntegerEncoder::s_forced_impl = IntegerEncoder::AUTO;

AlignedBuffer IntegerEncoder::encode(std::span<const uint64_t> values) {
    return IntegerEncoderFFOR::encode(values);
}

size_t IntegerEncoder::encodeInto(std::span<const uint64_t> values, AlignedBuffer &target) {
    return IntegerEncoderFFOR::encodeInto(values, target);
}

std::pair<size_t, size_t> IntegerEncoder::decode(Slice &encoded, unsigned int timestampSize,
                                                std::vector<uint64_t> &values,
                                                uint64_t minTime, uint64_t maxTime) {
    return IntegerEncoderFFOR::decode(encoded, timestampSize, values, minTime, maxTime);
}

IntegerEncoder::Implementation IntegerEncoder::selectBestImplementation() {
    return FFOR;
}

std::string IntegerEncoder::getImplementationName() {
    return "FFOR (Frame-of-Reference)";
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