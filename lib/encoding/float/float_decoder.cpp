#include "float_decoder.hpp"
#include <cstring>

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

    // OPTIMIZATION 2: Prefetch compressed data for better cache utilization
    const uint64_t* data_ptr = values.data;
    __builtin_prefetch(data_ptr, 0, 3);      // L1 cache
    __builtin_prefetch(data_ptr + 8, 0, 2);  // L2 cache

    uint64_t last_value = values.readFixed<uint64_t, 64>();
    uint64_t tzb = 0;
    uint64_t data_bits = 0;
    unsigned int count = 0;

    // Handle first value
    if (nToSkip == 0) {
        *output_ptr++ = reinterpret_cast<double&>(last_value);
    } else {
        nToSkip--;  // Account for the first value if skipping
    }

    const size_t totalLength = nToSkip + length;

    // OPTIMIZATION 3: Process with better branch prediction
    while(++count < totalLength){
        // Prefetch next cache line periodically
        if ((count & 0x7) == 0 && count < totalLength - 8) {
            __builtin_prefetch(data_ptr + (count >> 2), 0, 3);
        }

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

        // OPTIMIZATION 4: Direct memory write instead of push_back
        if(nToSkip > 0){
            nToSkip--;
        } else {
            *output_ptr++ = reinterpret_cast<double&>(last_value);
        }
    }
}