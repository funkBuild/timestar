#include "float_decoder_simd.hpp"
#include "float_encoder.hpp"
#include <bit>
#include <cpuid.h>
#include <cstring>
#include <stdexcept>

bool FloatDecoderSIMD::isAvailable() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 5)) != 0; // AVX2 bit
        }
        return false;
    }();
    return available;
}

void FloatDecoderSIMD::decodeSafe(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out) {
    if (isAvailable()) {
        decode(encoded, nToSkip, length, out);
    } else {
        FloatEncoderBasic::decode(encoded, nToSkip, length, out);
    }
}

void FloatDecoderSIMD::decode(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out) {
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

    // OPTIMIZATION 3: Process with AVX2-optimized loop structure
    // Process in groups of 4 when possible (AVX2 can handle 4 doubles)
    // Guard against underflow when totalLength <= 1 (totalLength - 1 would wrap for size_t)
    const size_t main_loop_end = (totalLength <= 1) ? 0 : totalLength - ((totalLength - 1) % 4);

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
                    if (lzb + data_bits > 64) {
                        throw std::runtime_error("Corrupt float block: lzb + data_bits > 64");
                    }
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

        // OPTIMIZATION 4: Direct memory write
        if (nToSkip > 0) {
            nToSkip--;
        } else {
            *output_ptr++ = std::bit_cast<double>(last_value);
        }
    }

    // Handle remaining values (less than 4)
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
                    if (lzb + data_bits > 64) {
                        throw std::runtime_error("Corrupt float block: lzb + data_bits > 64");
                    }
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