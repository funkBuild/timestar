#include "float_encoder_simd.hpp"

#include <cpuid.h>

#include <bit>
#include <cstring>

bool FloatEncoderSIMD::isAvailable() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            return (ebx & (1 << 5)) != 0;  // AVX2 bit
        }
        return false;
    }();
    return available;
}

CompressedBuffer FloatEncoderSIMD::encodeSafe(std::span<const double> values) {
    if (isAvailable()) {
        return encode(values);
    } else {
        return FloatEncoderBasic::encode(values);  // Fallback to regular encoder
    }
}

CompressedBuffer FloatEncoderSIMD::encode(std::span<const double> values) {
    if (values.empty()) {
        return CompressedBuffer();
    }

    CompressedBuffer buffer;

    // Pre-allocate capacity based on worst case (64 bits per value + control bits)
    size_t estimated_bits = values.size() * 66;
    buffer.reserve((estimated_bits + 63) / 64);

    uint64_t last_value = std::bit_cast<uint64_t>(values[0]);
    int data_bits = 0;
    int prev_lzb = -1;
    int prev_tzb = -1;

    buffer.write<64>(last_value);

    size_t i = 1;

    // Process in blocks of 4 for SIMD optimization
    if (values.size() >= 5) {  // Need at least 4 values after the first
        alignas(32) uint64_t xor_results[4];
        alignas(32) uint64_t current_values[4];
        alignas(32) uint64_t prev_values[4];

        // Pre-compute bounds
        const size_t full_blocks_end = ((values.size() - 1) / 4) * 4 + 1;

        for (; i < full_blocks_end; i += 4) {
            // Prefetch next block
            if (i + 8 < values.size()) {
                _mm_prefetch((const char*)&values[i + 8], _MM_HINT_T0);
            }

            // Load 4 double values directly as uint64_t using SIMD
            __m256i current = _mm256_loadu_si256((__m256i*)&values[i]);
            _mm256_storeu_si256((__m256i*)current_values, current);

            // Build previous values using SIMD shuffle
            prev_values[0] = last_value;
            prev_values[1] = current_values[0];
            prev_values[2] = current_values[1];
            prev_values[3] = current_values[2];

            __m256i prev = _mm256_loadu_si256((__m256i*)prev_values);

            // Perform XOR operation
            __m256i xor_vec = _mm256_xor_si256(current, prev);

            // Store XOR results
            _mm256_storeu_si256((__m256i*)xor_results, xor_vec);

            // Use SIMD to detect zeros
            __m256i zero_vec = _mm256_setzero_si256();
            __m256i cmp_result = _mm256_cmpeq_epi64(xor_vec, zero_vec);
            int zero_mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp_result));

            // Process each element with optimized writes
            for (int j = 0; j < 4; j++) {
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
                        if (lzb > 31)
                            lzb = 31;
                        data_bits = 64 - lzb - tzb;

                        // Try to combine control block and data if they fit
                        if (data_bits + 13 <= 64) {
                            // Combine control block (13 bits) and data into single write
                            uint64_t control_block =
                                (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                            uint64_t combined = control_block | ((xor_val >> tzb) << 13);
                            buffer.write(combined, data_bits + 13);
                        } else {
                            // Write control block and data separately
                            uint64_t control_block =
                                (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                            buffer.write(control_block, 13);
                            buffer.write(xor_val >> tzb, data_bits);
                        }

                        prev_lzb = lzb;
                        prev_tzb = tzb;
                    }
                }
            }

            // Update last_value for next iteration
            last_value = current_values[3];
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
                if (lzb > 31)
                    lzb = 31;
                data_bits = 64 - lzb - tzb;

                // Try to combine control and data
                if (data_bits + 13 <= 64) {
                    uint64_t control_block =
                        (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                    uint64_t combined = control_block | ((xor_value >> tzb) << 13);
                    buffer.write(combined, data_bits + 13);
                } else {
                    uint64_t control_block =
                        (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
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