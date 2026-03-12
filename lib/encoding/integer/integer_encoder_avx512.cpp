#include "integer_encoder_avx512.hpp"

#include "../simple16.hpp"
#include "../zigzag.hpp"

#include <cpuid.h>
#include <immintrin.h>

#include <cstring>

// Check for AVX-512F and AVX-512DQ support (cached - CPUID only called once)
bool IntegerEncoderAVX512::isAvailable() {
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
            bool has_avx512f = (ebx & (1 << 16)) != 0;   // AVX512F is bit 16 of EBX
            bool has_avx512dq = (ebx & (1 << 17)) != 0;  // AVX512DQ is bit 17 of EBX
            return has_avx512f && has_avx512dq;
        }
        return false;
    }();
    return available;
}

// SIMD zigzag encode for 8 values at once using AVX-512
static inline __m512i zigzagEncode8_avx512(const int64_t* values) {
    // Load 8 int64 values
    __m512i v = _mm512_loadu_si512((__m512i*)values);

    // Shift left by 1: (x << 1)
    __m512i shifted = _mm512_slli_epi64(v, 1);

    // Arithmetic shift right by 63: (x >> 63)
    __m512i sign = _mm512_srai_epi64(v, 63);

    // XOR: ((x << 1) ^ -(x >> 63))
    __m512i result = _mm512_xor_si512(shifted, sign);

    return result;
}

// SIMD zigzag decode for 8 values at once using AVX-512
static inline __m512i zigzagDecode8_avx512(const uint64_t* values) {
    // Load 8 uint64 values
    __m512i y = _mm512_loadu_si512((__m512i*)values);

    // y >> 1
    __m512i shifted = _mm512_srli_epi64(y, 1);

    // y & 1
    __m512i one = _mm512_set1_epi64(1);
    __m512i lsb = _mm512_and_si512(y, one);

    // -(y & 1)
    __m512i zero = _mm512_setzero_si512();
    __m512i neg_lsb = _mm512_sub_epi64(zero, lsb);

    // (y >> 1) ^ -(y & 1)
    __m512i result = _mm512_xor_si512(shifted, neg_lsb);

    return result;
}

AlignedBuffer IntegerEncoderAVX512::encode(std::span<const uint64_t> values) {
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

    int64_t delta = static_cast<int64_t>(values[1]) - static_cast<int64_t>(values[0]);
    encoded.push_back(ZigZag::zigzagEncode(delta));

    const size_t size = values.size();
    size_t i = 2;

    // Use AVX-512 for batch processing
    if (isAvailable() && size >= 18) {
        // Process 16 values at a time using AVX-512
        alignas(64) int64_t deltas[16];
        alignas(64) uint64_t encoded_batch[16];

        for (; i + 15 < size; i += 16) {
// Calculate delta-of-deltas for 16 values
// Unroll for better pipeline utilization
#pragma unroll 16
            for (size_t j = 0; j < 16; j++) {
                size_t idx = i + j;
                deltas[j] = (static_cast<int64_t>(values[idx]) - static_cast<int64_t>(values[idx - 1])) -
                            (static_cast<int64_t>(values[idx - 1]) - static_cast<int64_t>(values[idx - 2]));
            }

            // SIMD zigzag encode in two batches of 8
            __m512i enc1 = zigzagEncode8_avx512(&deltas[0]);
            __m512i enc2 = zigzagEncode8_avx512(&deltas[8]);

            // Store results
            _mm512_storeu_si512((__m512i*)&encoded_batch[0], enc1);
            _mm512_storeu_si512((__m512i*)&encoded_batch[8], enc2);

            // Add to output
            for (size_t j = 0; j < 16; j++) {
                encoded.push_back(encoded_batch[j]);
            }
        }
    }

    // Process 8 values at a time with AVX-512
    if (isAvailable()) {
        alignas(64) int64_t deltas[8];
        alignas(64) uint64_t encoded_batch[8];

        for (; i + 7 < size; i += 8) {
// Calculate delta-of-deltas
#pragma unroll 8
            for (size_t j = 0; j < 8; j++) {
                size_t idx = i + j;
                deltas[j] = (static_cast<int64_t>(values[idx]) - static_cast<int64_t>(values[idx - 1])) -
                            (static_cast<int64_t>(values[idx - 1]) - static_cast<int64_t>(values[idx - 2]));
            }

            // SIMD zigzag encode
            __m512i enc = zigzagEncode8_avx512(deltas);
            _mm512_storeu_si512((__m512i*)encoded_batch, enc);

            // Add to output
            for (size_t j = 0; j < 8; j++) {
                encoded.push_back(encoded_batch[j]);
            }
        }
    }

    // Process remaining values with 4x unrolling
    for (; i + 3 < size; i += 4) {
        int64_t d0 = (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1])) -
                     (static_cast<int64_t>(values[i - 1]) - static_cast<int64_t>(values[i - 2]));
        int64_t d1 = (static_cast<int64_t>(values[i + 1]) - static_cast<int64_t>(values[i])) -
                     (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1]));
        int64_t d2 = (static_cast<int64_t>(values[i + 2]) - static_cast<int64_t>(values[i + 1])) -
                     (static_cast<int64_t>(values[i + 1]) - static_cast<int64_t>(values[i]));
        int64_t d3 = (static_cast<int64_t>(values[i + 3]) - static_cast<int64_t>(values[i + 2])) -
                     (static_cast<int64_t>(values[i + 2]) - static_cast<int64_t>(values[i + 1]));

        encoded.push_back(ZigZag::zigzagEncode(d0));
        encoded.push_back(ZigZag::zigzagEncode(d1));
        encoded.push_back(ZigZag::zigzagEncode(d2));
        encoded.push_back(ZigZag::zigzagEncode(d3));
    }

    // Handle remaining values
    for (; i < size; i++) {
        int64_t D = (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1])) -
                    (static_cast<int64_t>(values[i - 1]) - static_cast<int64_t>(values[i - 2]));
        encoded.push_back(ZigZag::zigzagEncode(D));
    }

    return Simple16::encode(encoded);
}

std::pair<size_t, size_t> IntegerEncoderAVX512::decode(Slice& encoded, unsigned int timestampSize,
                                                       std::vector<uint64_t>& values, uint64_t minTime,
                                                       uint64_t maxTime) {
    // Optimized memory allocation with alignment for AVX-512
    const size_t current_size = values.size();
    const size_t estimated_new = timestampSize;

    if (values.capacity() < current_size + estimated_new) {
        values.reserve(current_size + estimated_new + (estimated_new >> 2));  // 25% extra
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
    last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

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

    if (isAvailable() && size >= 18) {
        // Process in batches of 16 for AVX-512
        alignas(64) int64_t decoded_batch[16];

        for (; i + 15 < size; i += 16) {
            // SIMD zigzag decode in two batches of 8
            __m512i dec1 = zigzagDecode8_avx512(&deltaValues[i]);
            __m512i dec2 = zigzagDecode8_avx512(&deltaValues[i + 8]);

            // Store decoded values
            _mm512_storeu_si512((__m512i*)&decoded_batch[0], dec1);
            _mm512_storeu_si512((__m512i*)&decoded_batch[8], dec2);

            // Use AVX-512 mask operations for range checking
            __m512i min_vec = _mm512_set1_epi64(minTime);
            __m512i max_vec = _mm512_set1_epi64(maxTime);

#pragma unroll 16
            for (size_t j = 0; j < 16; j++) {
                delta += decoded_batch[j];
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

                // Efficient range check
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
    }

    // Process 8 values at a time with AVX-512
    if (isAvailable()) {
        alignas(64) int64_t decoded_batch[8];

        for (; i + 7 < size; i += 8) {
            // SIMD zigzag decode
            __m512i dec = zigzagDecode8_avx512(&deltaValues[i]);
            _mm512_storeu_si512((__m512i*)decoded_batch, dec);

// Unrolled reconstruction
#pragma unroll 8
            for (size_t j = 0; j < 8; j++) {
                delta += decoded_batch[j];
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

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
    }

    // Process 4 values at a time
    for (; i + 3 < size; i += 4) {
        int64_t dd0 = ZigZag::zigzagDecode(deltaValues[i]);
        int64_t dd1 = ZigZag::zigzagDecode(deltaValues[i + 1]);
        int64_t dd2 = ZigZag::zigzagDecode(deltaValues[i + 2]);
        int64_t dd3 = ZigZag::zigzagDecode(deltaValues[i + 3]);

        delta += dd0;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }

        delta += dd1;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }

        delta += dd2;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }

        delta += dd3;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }
    }

    // Handle remaining values
    for (; i < size; i++) {
        int64_t encD = ZigZag::zigzagDecode(deltaValues[i]);
        delta += encD;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

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