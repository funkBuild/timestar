#include "float_encoder_avx512.hpp"
#include <bit>
#include <cpuid.h>
#include <cstring>
#include <iostream>

bool FloatEncoderAVX512::hasAVX512F() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 16)) != 0; // AVX512F bit
        }
        return false;
    }();
    return available;
}

bool FloatEncoderAVX512::hasAVX512DQ() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 17)) != 0; // AVX512DQ bit
        }
        return false;
    }();
    return available;
}

bool FloatEncoderAVX512::isAvailable() {
    static const bool available = hasAVX512F() && hasAVX512DQ();
    return available;
}

CompressedBuffer FloatEncoderAVX512::encodeSafe(std::span<const double> values) {
    if (isAvailable()) {
        return encode(values);
    } else {
        // Fallback to regular encoder
        return FloatEncoderBasic::encode(values);
    }
}

CompressedBuffer FloatEncoderAVX512::encode(std::span<const double> values) {
    if (values.empty()) {
        return CompressedBuffer();
    }

    CompressedBuffer buffer;

    // Pre-allocate with extra space for batch operations
    size_t estimated_bits = values.size() * 66;
    buffer.reserve((estimated_bits + 63) / 64 + 16);

    uint64_t last_value = std::bit_cast<uint64_t>(values[0]);
    int data_bits = 0;
    int prev_lzb = -1;
    int prev_tzb = -1;

    buffer.write<64>(last_value);

    size_t i = 1;

    // Process in blocks of 8 for AVX-512
    if (values.size() >= 9) { // Need at least 8 values after the first
        alignas(64) uint64_t current_values[8];
        alignas(64) uint64_t prev_values[8];
        alignas(64) uint64_t xor_results[8];

        // Pre-compute end boundary
        const size_t full_blocks_end = ((values.size() - 1) / 8) * 8 + 1;

        // Process blocks of 8
        for (; i < full_blocks_end; i += 8) {
            // Aggressive prefetching - 2 blocks ahead
            if (i + 16 < values.size()) {
                _mm_prefetch((const char*)&values[i + 16], _MM_HINT_T0);
                if (i + 24 < values.size()) {
                    _mm_prefetch((const char*)&values[i + 24], _MM_HINT_T1);
                }
            }

            // Load 8 double values directly using AVX-512
            __m512i current = _mm512_loadu_si512((__m512i*)&values[i]);
            _mm512_storeu_si512((__m512i*)current_values, current);

            // Build previous values efficiently using AVX-512 permutation
            // First element is last_value, rest are shifted current values
            prev_values[0] = last_value;
            prev_values[1] = current_values[0];
            prev_values[2] = current_values[1];
            prev_values[3] = current_values[2];
            prev_values[4] = current_values[3];
            prev_values[5] = current_values[4];
            prev_values[6] = current_values[5];
            prev_values[7] = current_values[6];

            __m512i prev = _mm512_loadu_si512((__m512i*)prev_values);

            // Perform XOR using AVX-512
            __m512i xor_vec = _mm512_xor_epi64(current, prev);

            // Use mask to detect zeros
            __mmask8 zero_mask = _mm512_cmpeq_epi64_mask(xor_vec, _mm512_setzero_si512());

            // Store XOR results for extraction
            _mm512_storeu_si512((__m512i*)xor_results, xor_vec);

            // Process each element with optimized batched writes
            for (int j = 0; j < 8; j++) {
                uint64_t xor_val = xor_results[j];

                if (zero_mask & (1 << j)) {
                    // XOR is zero - just write single bit
                    buffer.writeFixed<0b0, 1>();
                } else {
                    int lzb = __builtin_clzll(xor_val);
                    int tzb = __builtin_ctzll(xor_val);

                    if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                        // Reuse previous bounds - combine prefix and data when possible
                        if (data_bits + 2 <= 64) {
                            // Combine 0b01 prefix with data in a single write
                            uint64_t combined = (0b01ULL) | ((xor_val >> prev_tzb) << 2);
                            buffer.write(combined, data_bits + 2);
                        } else {
                            // Write separately if too large
                            buffer.writeFixed<0b01, 2>();
                            buffer.write(xor_val >> prev_tzb, data_bits);
                        }
                    } else {
                        // New bounds needed
                        if (lzb > 31) lzb = 31;
                        data_bits = 64 - lzb - tzb;

                        // Try to combine control block and data if they fit
                        if (data_bits + 13 <= 64) {
                            // Combine control block (13 bits) and data into single write
                            uint64_t control_block = (0b11ULL) |
                                                    (uint64_t(lzb) << 2) |
                                                    (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                            uint64_t combined = control_block | ((xor_val >> tzb) << 13);
                            buffer.write(combined, data_bits + 13);
                        } else {
                            // Write control block and data separately
                            uint64_t control_block = (0b11ULL) |
                                                    (uint64_t(lzb) << 2) |
                                                    (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                            buffer.write(control_block, 13);
                            buffer.write(xor_val >> tzb, data_bits);
                        }

                        prev_lzb = lzb;
                        prev_tzb = tzb;
                    }
                }
            }

            // Update last_value for next iteration
            last_value = current_values[7];
        }
    }

    // Handle remaining values with regular encoding
    for (; i < values.size(); i++) {
        const uint64_t current_value = std::bit_cast<uint64_t>(values[i]);
        const uint64_t xor_value = current_value ^ last_value;

        if (xor_value == 0) {
            buffer.writeFixed<0b0, 1>();
        } else {
            int lzb = __builtin_clzll(xor_value);
            int tzb = __builtin_ctzll(xor_value);

            if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                // Combine when possible
                if (data_bits + 2 <= 64) {
                    uint64_t combined = (0b01ULL) | ((xor_value >> prev_tzb) << 2);
                    buffer.write(combined, data_bits + 2);
                } else {
                    buffer.writeFixed<0b01, 2>();
                    buffer.write(xor_value >> prev_tzb, data_bits);
                }
            } else {
                if (lzb > 31) lzb = 31;
                data_bits = 64 - lzb - tzb;

                // Try to combine control and data
                if (data_bits + 13 <= 64) {
                    uint64_t control_block = (0b11ULL) |
                                            (uint64_t(lzb) << 2) |
                                            (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                    uint64_t combined = control_block | ((xor_value >> tzb) << 13);
                    buffer.write(combined, data_bits + 13);
                } else {
                    uint64_t control_block = (0b11ULL) |
                                            (uint64_t(lzb) << 2) |
                                            (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                    buffer.write(control_block, 13);
                    buffer.write(xor_value >> tzb, data_bits);
                }

                prev_lzb = lzb;
                prev_tzb = tzb;
            }
        }

        last_value = current_value;
    }

    return buffer;
}