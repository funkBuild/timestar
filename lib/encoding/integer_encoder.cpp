#include "integer_encoder.hpp"

#include "integer/integer_encoder_ffor.hpp"

AlignedBuffer IntegerEncoder::encode(std::span<const uint64_t> values) {
    return IntegerEncoderFFOR::encode(values);
}

size_t IntegerEncoder::encodeInto(std::span<const uint64_t> values, AlignedBuffer& target) {
    return IntegerEncoderFFOR::encodeInto(values, target);
}

std::pair<size_t, size_t> IntegerEncoder::decode(Slice& encoded, unsigned int timestampSize,
                                                 std::vector<uint64_t>& values, uint64_t minTime, uint64_t maxTime) {
    return IntegerEncoderFFOR::decode(encoded, timestampSize, values, minTime, maxTime);
}

std::string IntegerEncoder::getImplementationName() {
    return "FFOR (Frame-of-Reference)";
}
