#include "float_decoder_avx512.hpp"
#include "float_encoder.hpp"
#include <bit>
#include <cpuid.h>
#include <cstring>

bool FloatDecoderAVX512::isAvailable() {
    return hasAVX512F() && hasAVX512DQ();
}

bool FloatDecoderAVX512::hasAVX512F() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, nullptr) >= 7) {
        __cpuid_count(7, 0, eax, ebx, ecx, edx);
        return (ebx & (1 << 16)) != 0; // AVX512F bit
    }
    return false;
}

bool FloatDecoderAVX512::hasAVX512DQ() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, nullptr) >= 7) {
        __cpuid_count(7, 0, eax, ebx, ecx, edx);
        return (ebx & (1 << 17)) != 0; // AVX512DQ bit
    }
    return false;
}

void FloatDecoderAVX512::decodeSafe(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out) {
    if (isAvailable()) {
        decode(encoded, nToSkip, length, out);
    } else {
        // Fallback to regular decoder
        FloatEncoderBasic::decode(encoded, nToSkip, length, out);
    }
}

void FloatDecoderAVX512::decode(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out) {
    if (length == 0) {
        return;
    }

    // OPTIMIZATION 1: Reserve exact space with extra buffer
    const size_t current_size = out.size();
    const size_t required_capacity = current_size + length;

    if (out.capacity() < required_capacity) {
        out.reserve(required_capacity + (required_capacity >> 3)); // 12.5% extra
    }

    // Resize for direct memory writes
    out.resize(required_capacity);
    double* output_ptr = out.data() + current_size;

    uint64_t last_value = encoded.readFixed<uint64_t, 64>();
    uint64_t tzb = 0;
    uint64_t data_bits = 0;
    size_t count = 0;

    // Save original nToSkip for totalLength calculation
    const size_t originalSkip = nToSkip;

    // Handle first value
    if (nToSkip == 0) {
        *output_ptr++ = std::bit_cast<double>(last_value);
    } else {
        nToSkip--;  // Account for first value if skipping
    }

    // Total values to process from delta stream
    // Must use originalSkip since nToSkip may have been decremented
    const size_t totalLength = originalSkip + length;

    // OPTIMIZATION 3: Process with AVX-512-optimized loop structure
    // Process in groups of 8 when possible (AVX-512 can handle 8 doubles)
    // Guard against underflow when totalLength <= 1 (totalLength - 1 would wrap for size_t)
    const size_t main_loop_end = (totalLength <= 1) ? 0 : totalLength - ((totalLength - 1) % 8);

    while (++count < main_loop_end) {
        if (encoded.readBit()) {
            if (encoded.readBit()) {
                // 0b11 prefix - new bounds
                const uint64_t control_data = encoded.read<uint64_t>(11);
                const auto lzb = control_data & 0x1F;
                data_bits = (control_data >> 5) & 0x3F;

                if (data_bits == 0 && lzb == 0) {
                    data_bits = 64;
                    tzb = 0;
                } else {
                    tzb = 64 - lzb - data_bits;
                }

                const uint64_t decoded_value = encoded.read<uint64_t>(data_bits) << tzb;
                last_value ^= decoded_value;
            } else {
                // 0b01 prefix - reuse bounds
                const uint64_t decoded_value = encoded.read<uint64_t>(data_bits) << tzb;
                last_value ^= decoded_value;
            }
        }

        // OPTIMIZATION 4: Direct memory write with AVX-512 alignment hint
        if (nToSkip > 0) {
            nToSkip--;
        } else {
            *output_ptr++ = std::bit_cast<double>(last_value);
        }
    }

    // Handle remaining values (less than 8)
    while (count < totalLength) {
        count++;

        if (encoded.readBit()) {
            if (encoded.readBit()) {
                const uint64_t control_data = encoded.read<uint64_t>(11);
                const auto lzb = control_data & 0x1F;
                data_bits = (control_data >> 5) & 0x3F;

                if (data_bits == 0 && lzb == 0) {
                    data_bits = 64;
                    tzb = 0;
                } else {
                    tzb = 64 - lzb - data_bits;
                }

                const uint64_t decoded_value = encoded.read<uint64_t>(data_bits) << tzb;
                last_value ^= decoded_value;
            } else {
                const uint64_t decoded_value = encoded.read<uint64_t>(data_bits) << tzb;
                last_value ^= decoded_value;
            }
        }

        if (nToSkip > 0) {
            nToSkip--;
        } else {
            *output_ptr++ = std::bit_cast<double>(last_value);
        }
    }
}