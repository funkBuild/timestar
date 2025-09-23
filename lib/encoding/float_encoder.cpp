#include "float_encoder.hpp"
#include "util.hpp"

#include <iostream>
#include <cassert>
#include <cmath>

CompressedBuffer FloatEncoder::encode(const std::vector<double> &values){
    CompressedBuffer buffer;

    uint64_t last_value = *((uint64_t*)&values[0]);
    int data_bits = 0;
    int prev_lzb = -1;
    int prev_tzb = -1;

    buffer.write(last_value, 64);


    for(size_t i = 1; i < values.size(); i++){
        const uint64_t current_value = *((uint64_t*)&values[i]);
        const uint64_t xor_value = current_value ^ last_value;


        if(xor_value == 0){
            buffer.writeFixed<0b0, 1>();
        } else {
            auto lzb = getLeadingZeroBits(xor_value);
            const auto tzb = getTrailingZeroBits(xor_value);


            if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb ){
                buffer.writeFixed<0b01, 2>();
            } else {
                if(lzb > 31)
                    lzb = 31;

                data_bits = 8 * sizeof(uint64_t) - lzb - tzb;

                // Handle special case where data_bits = 64
                // 6-bit encoding can only represent 0-63, so we represent 64 as 0
                // and handle it specially in the decoder
                buffer.writeFixed<0b11, 2>();
                buffer.write<5>(lzb);

                if (data_bits == 64) {
                    buffer.write<6>(0); // Encode 64 as 0
                } else {
                    buffer.write<6>(data_bits);
                }

                prev_lzb = lzb;
                prev_tzb = tzb;
            }

            buffer.write(xor_value >> prev_tzb, data_bits);
        }

        last_value = current_value;
    }


    return std::move(buffer);
};


void FloatEncoder::decode(CompressedSlice &values, size_t nToSkip, size_t length, std::vector<double> &out){

    uint64_t last_value = values.readFixed<uint64_t, 64>();
    uint64_t tzb = 0;
    uint64_t data_bits = 0;
    unsigned int count = 0;


    out.push_back(reinterpret_cast<double&>(last_value));

    const size_t totalLength = nToSkip + length;

    while(++count < totalLength){

        if(values.readBit()){
            if(values.readBit()){
                const auto lzb = values.readFixed<uint64_t, 5>();
                data_bits = values.readFixed<uint64_t, 6>();

                // Handle special case: data_bits=0 represents 64 when lzb=0
                if (data_bits == 0 && lzb == 0) {
                    data_bits = 64;
                    tzb = 0;
                } else {
                    tzb = 64 - lzb - data_bits;
                }
            }

            const uint64_t decoded_value = values.read<uint64_t>(data_bits) << tzb;
            last_value = last_value ^ decoded_value;
        }

        if(nToSkip > 0){
            nToSkip--;
        } else {
            out.push_back(reinterpret_cast<double&>(last_value));
        }

    }

}