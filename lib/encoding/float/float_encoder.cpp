#include "float_encoder.hpp"
#include "float_decoder.hpp"
#include "../../utils/util.hpp"

#include <bit>
#include <cassert>
#include <cmath>
#include <stdexcept>

CompressedBuffer FloatEncoderBasic::encode(std::span<const double> values){
    CompressedBuffer buffer;

    if (values.empty()) {
        return buffer;
    }

    uint64_t last_value = std::bit_cast<uint64_t>(values[0]);
    int data_bits = 0;
    int prev_lzb = -1;
    int prev_tzb = -1;

    buffer.write<64>(last_value);


    for(size_t i = 1; i < values.size(); i++){
        const uint64_t current_value = std::bit_cast<uint64_t>(values[i]);
        const uint64_t xor_value = current_value ^ last_value;


        if(xor_value == 0){
            buffer.writeFixed<0b0, 1>();
        } else {
            auto lzb = getLeadingZeroBits(xor_value);
            const auto tzb = getTrailingZeroBits(xor_value);


            if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb ){
                buffer.writeFixed<0b01, 2>();
                buffer.write(xor_value >> prev_tzb, data_bits);
            } else {
                if(lzb > 31)
                    lzb = 31;

                data_bits = 8 * sizeof(uint64_t) - lzb - tzb;

                buffer.writeFixed<0b11, 2>();
                buffer.write<5>(lzb);
                buffer.write<6>(data_bits == 64 ? 0 : data_bits);
                buffer.write(xor_value >> tzb, data_bits);

                prev_lzb = lzb;
                prev_tzb = tzb;
            }
        }

        last_value = current_value;
    }


    return buffer;
};


void FloatEncoderBasic::decode(CompressedSlice &values, size_t nToSkip, size_t length, std::vector<double> &out){
    // Delegate to the dedicated decoder (single source of truth)
    FloatDecoderBasic::decode(values, nToSkip, length, out);
}