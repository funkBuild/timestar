#include "float_decoder.hpp"
#include <bit>
#include <cstring>
#include <stdexcept>

void FloatDecoderBasic::decode(CompressedSlice &values, size_t nToSkip, size_t length, std::vector<double> &out) {
    if (length == 0) {
        return;
    }

    // OPTIMIZATION 1: Reserve exact space upfront to avoid reallocations
    const size_t current_size = out.size();
    const size_t required_capacity = current_size + length;

    if (out.capacity() < required_capacity) {
        // Reserve with some extra space to avoid frequent reallocations
        out.reserve(required_capacity + (required_capacity >> 3)); // 12.5% extra
    }

    // Resize to exact size needed for direct memory writes
    out.resize(required_capacity);
    double* output_ptr = out.data() + current_size;

    uint64_t last_value = values.readFixed<uint64_t, 64>();
    uint64_t tzb = 0;
    uint64_t data_bits = 0;
    size_t count = 0;

    // Save original nToSkip for totalLength calculation
    const size_t originalSkip = nToSkip;

    // Handle first value
    if (nToSkip == 0) {
        *output_ptr++ = std::bit_cast<double>(last_value);
    } else {
        nToSkip--;  // Account for the first value if skipping
    }

    // Total values to process from delta stream
    // Must use originalSkip since nToSkip may have been decremented
    const size_t totalLength = originalSkip + length;

    while(++count < totalLength){
        if(values.readBit()){
            if(values.readBit()){
                // 0b11 prefix - new bounds
                const uint64_t control_data = values.read<uint64_t>(11);
                const auto lzb = control_data & 0x1F;  // Lower 5 bits
                data_bits = (control_data >> 5) & 0x3F;  // Upper 6 bits

                // Handle special case: data_bits=0 represents 64 when lzb=0
                if (data_bits == 0 && lzb == 0) {
                    data_bits = 64;
                    tzb = 0;
                } else {
                    if (lzb + data_bits > 64) {
                        throw std::runtime_error("Corrupt float block: lzb + data_bits > 64");
                    }
                    tzb = 64 - lzb - data_bits;
                }

                const uint64_t decoded_value = values.read<uint64_t>(data_bits) << tzb;
                last_value ^= decoded_value;
            } else {
                // 0b01 prefix - reusing previous bounds
                const uint64_t decoded_value = values.read<uint64_t>(data_bits) << tzb;
                last_value ^= decoded_value;
            }
        }
        // else: 0b0 prefix - value unchanged

        if(nToSkip > 0){
            nToSkip--;
        } else {
            *output_ptr++ = std::bit_cast<double>(last_value);
        }
    }
}