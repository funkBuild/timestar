#include "integer_encoder_simd.hpp"
#include "../zigzag.hpp"
#include "../simple16.hpp"
#include <immintrin.h>
#include <cpuid.h>
#include <cstring>

// Check for AVX2 support
bool IntegerEncoderSIMD::isAvailable() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return (ebx & (1 << 5)) != 0; // AVX2 is bit 5 of EBX
    }
    return false;
}

// SIMD zigzag encode for 4 values at once using AVX2
static inline __m256i zigzagEncode4_avx2(const int64_t* values) {
    // Load 4 int64 values
    __m256i v = _mm256_loadu_si256((__m256i*)values);

    // Shift left by 1: (x << 1)
    __m256i shifted = _mm256_slli_epi64(v, 1);

    // Arithmetic shift right by 63: (x >> 63)
    __m256i sign = _mm256_srai_epi64(v, 63);

    // XOR: ((x << 1) ^ -(x >> 63))
    __m256i result = _mm256_xor_si256(shifted, sign);

    return result;
}

// SIMD zigzag decode for 4 values at once using AVX2
static inline __m256i zigzagDecode4_avx2(const uint64_t* values) {
    // Load 4 uint64 values
    __m256i y = _mm256_loadu_si256((__m256i*)values);

    // y >> 1
    __m256i shifted = _mm256_srli_epi64(y, 1);

    // y & 1
    __m256i one = _mm256_set1_epi64x(1);
    __m256i lsb = _mm256_and_si256(y, one);

    // -(y & 1)
    __m256i zero = _mm256_setzero_si256();
    __m256i neg_lsb = _mm256_sub_epi64(zero, lsb);

    // (y >> 1) ^ -(y & 1)
    __m256i result = _mm256_xor_si256(shifted, neg_lsb);

    return result;
}

AlignedBuffer IntegerEncoderSIMD::encode(const std::vector<uint64_t> &values) {
    if (values.empty()) {
        return AlignedBuffer();
    }

    std::vector<uint64_t> encoded;
    encoded.reserve(values.size() + 2);

    uint64_t start_value = values[0];
    encoded.push_back(start_value);

    if (values.size() == 1) {
        return Simple16::encode(encoded);
    }

    int64_t delta = values[1] - values[0];
    encoded.push_back(ZigZag::zigzagEncode(delta));

    const size_t size = values.size();
    size_t i = 2;

    // Use AVX2 for batch processing with better efficiency
    if (isAvailable() && size >= 10) {
        // Process 8 values at a time - split into 2x4 for AVX2 efficiency
        alignas(32) int64_t deltas[8];
        alignas(32) uint64_t encoded_batch[8];

        for (; i + 7 < size; i += 8) {
            // Calculate delta-of-deltas for 8 values
            // Unroll for better pipeline utilization
            deltas[0] = (values[i+0] - values[i-1]) - (values[i-1] - values[i-2]);
            deltas[1] = (values[i+1] - values[i+0]) - (values[i+0] - values[i-1]);
            deltas[2] = (values[i+2] - values[i+1]) - (values[i+1] - values[i+0]);
            deltas[3] = (values[i+3] - values[i+2]) - (values[i+2] - values[i+1]);
            deltas[4] = (values[i+4] - values[i+3]) - (values[i+3] - values[i+2]);
            deltas[5] = (values[i+5] - values[i+4]) - (values[i+4] - values[i+3]);
            deltas[6] = (values[i+6] - values[i+5]) - (values[i+5] - values[i+4]);
            deltas[7] = (values[i+7] - values[i+6]) - (values[i+6] - values[i+5]);

            // SIMD zigzag encode in two batches of 4
            __m256i enc1 = zigzagEncode4_avx2(&deltas[0]);
            __m256i enc2 = zigzagEncode4_avx2(&deltas[4]);

            // Store results
            _mm256_storeu_si256((__m256i*)&encoded_batch[0], enc1);
            _mm256_storeu_si256((__m256i*)&encoded_batch[4], enc2);

            // Add to output
            for (size_t j = 0; j < 8; j++) {
                encoded.push_back(encoded_batch[j]);
            }
        }
    }

    // Process remaining values with 4x unrolling
    for (; i + 3 < size; i += 4) {
        int64_t d0 = (values[i] - values[i-1]) - (values[i-1] - values[i-2]);
        int64_t d1 = (values[i+1] - values[i]) - (values[i] - values[i-1]);
        int64_t d2 = (values[i+2] - values[i+1]) - (values[i+1] - values[i]);
        int64_t d3 = (values[i+3] - values[i+2]) - (values[i+2] - values[i+1]);

        // Use SIMD for zigzag if available
        if (isAvailable()) {
            alignas(32) int64_t batch[4] = {d0, d1, d2, d3};
            alignas(32) uint64_t result[4];
            __m256i enc = zigzagEncode4_avx2(batch);
            _mm256_storeu_si256((__m256i*)result, enc);

            encoded.push_back(result[0]);
            encoded.push_back(result[1]);
            encoded.push_back(result[2]);
            encoded.push_back(result[3]);
        } else {
            encoded.push_back(ZigZag::zigzagEncode(d0));
            encoded.push_back(ZigZag::zigzagEncode(d1));
            encoded.push_back(ZigZag::zigzagEncode(d2));
            encoded.push_back(ZigZag::zigzagEncode(d3));
        }
    }

    // Handle remaining values
    for (; i < size; i++) {
        int64_t D = (values[i] - values[i-1]) - (values[i-1] - values[i-2]);
        encoded.push_back(ZigZag::zigzagEncode(D));
    }

    return Simple16::encode(encoded);
}

std::pair<size_t, size_t> IntegerEncoderSIMD::decode(Slice &encoded, unsigned int timestampSize,
                                                     std::vector<uint64_t> &values,
                                                     uint64_t minTime, uint64_t maxTime) {
    // Optimized memory allocation
    const size_t current_size = values.size();
    const size_t estimated_new = timestampSize;

    if (values.capacity() < current_size + estimated_new) {
        values.reserve(current_size + estimated_new + (estimated_new >> 2)); // 25% extra
    }

    std::vector<uint64_t> deltaValues = Simple16::decode(encoded, timestampSize);

    size_t nSkipped = 0, nAdded = 0;

    if (deltaValues.empty()) {
        return {nSkipped, nAdded};
    }

    uint64_t last_decoded = deltaValues[0];

    // First value
    if (last_decoded < minTime) {
        nSkipped++;
    } else if (last_decoded <= maxTime) {
        values.push_back(last_decoded);
        nAdded++;
    } else {
        return {nSkipped, nAdded};
    }

    if (deltaValues.size() < 2) {
        return {nSkipped, nAdded};
    }

    // Second value
    int64_t delta = ZigZag::zigzagDecode(deltaValues[1]);
    last_decoded += delta;

    if (last_decoded < minTime) {
        nSkipped++;
    } else if (last_decoded <= maxTime) {
        values.push_back(last_decoded);
        nAdded++;
    } else {
        return {nSkipped, nAdded};
    }

    const size_t size = deltaValues.size();
    size_t i = 2;

    if (isAvailable() && size >= 10) {
        // Process in batches of 8 using SIMD zigzag decode
        alignas(32) int64_t decoded_batch[8];

        for (; i + 7 < size; i += 8) {
            // SIMD zigzag decode in two batches of 4
            __m256i dec1 = zigzagDecode4_avx2(&deltaValues[i]);
            __m256i dec2 = zigzagDecode4_avx2(&deltaValues[i + 4]);

            // Store decoded values
            _mm256_storeu_si256((__m256i*)&decoded_batch[0], dec1);
            _mm256_storeu_si256((__m256i*)&decoded_batch[4], dec2);

            // Sequential reconstruction with prefetching
            #pragma unroll 8
            for (size_t j = 0; j < 8; j++) {
                delta += decoded_batch[j];
                last_decoded += delta;

                // Branchless min/max check
                bool in_range = (last_decoded >= minTime) & (last_decoded <= maxTime);
                if (!in_range) {
                    if (last_decoded < minTime) {
                        nSkipped++;
                    } else {
                        return {nSkipped, nAdded};
                    }
                } else {
                    values.push_back(last_decoded);
                    nAdded++;
                }
            }
        }
    }

    // Process 4 values at a time with SIMD zigzag
    if (isAvailable()) {
        alignas(32) int64_t decoded_batch[4];

        for (; i + 3 < size; i += 4) {
            // SIMD zigzag decode
            __m256i dec = zigzagDecode4_avx2(&deltaValues[i]);
            _mm256_storeu_si256((__m256i*)decoded_batch, dec);

            // Unrolled reconstruction
            delta += decoded_batch[0];
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += decoded_batch[1];
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += decoded_batch[2];
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += decoded_batch[3];
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }
        }
    } else {
        // Fallback to non-SIMD 4x unrolling
        for (; i + 3 < size; i += 4) {
            int64_t dd0 = ZigZag::zigzagDecode(deltaValues[i]);
            int64_t dd1 = ZigZag::zigzagDecode(deltaValues[i+1]);
            int64_t dd2 = ZigZag::zigzagDecode(deltaValues[i+2]);
            int64_t dd3 = ZigZag::zigzagDecode(deltaValues[i+3]);

            delta += dd0;
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += dd1;
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += dd2;
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += dd3;
            last_decoded += delta;
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }
        }
    }

    // Handle remaining values
    for (; i < size; i++) {
        int64_t encD = ZigZag::zigzagDecode(deltaValues[i]);
        delta += encD;
        last_decoded += delta;

        if (last_decoded < minTime) {
            nSkipped++;
            continue;
        }

        if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        }

        values.push_back(last_decoded);
        nAdded++;
    }

    return {nSkipped, nAdded};
}