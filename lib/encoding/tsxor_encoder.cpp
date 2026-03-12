#include "tsxor_encoder.hpp"

#include "util.hpp"

#include <bit>
#include <cassert>
#include <cmath>

CompressedBuffer TsxorEncoder::encode(std::vector<double>& values) {
    CompressedBuffer buffer;
    Window window;

    for (size_t i = 0; i < values.size(); i++) {
        uint64_t val = std::bit_cast<uint64_t>(values[i]);

        if (window.contains(val)) {
            auto offset = window.getIndexOf(val);
            buffer.write<8>(static_cast<uint64_t>(offset & 0xFF));
        } else {
            uint64_t candidate = window.getCandidate(val);
            uint64_t xor_value = candidate ^ val;
            auto offset = window.getIndexOf(candidate);

            // WRITE 1
            offset |= 0x80;
            buffer.write<8>(static_cast<uint64_t>(offset & 0xFF));

            auto lzb = getLeadingZeroBits(xor_value);
            const auto tzb = getTrailingZeroBits(xor_value);

            if (lzb > 31)
                lzb = 31;

            uint64_t data_bits = 8 * sizeof(uint64_t) - lzb - tzb;

            buffer.writeFixed<0b11, 2>();
            buffer.write<5>(lzb);
            // Note: data_bits==64 is encoded as 0 in the 6-bit field (same convention as FloatEncoderBasic)
            buffer.write<6>(data_bits);

            buffer.write(xor_value >> tzb, data_bits);
        }

        window.insert(val);
    }

    return buffer;
}