#pragma once

#include "zigzag_simd.hpp"

#include <algorithm>
#include <cstdint>
#include <span>
#include <vector>

// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers

class ZigZag {
    // Minimum element count to dispatch to Highway SIMD path.
    // Below this threshold, scalar code is faster due to dispatch overhead.
    static constexpr size_t kSimdThreshold = 16;

public:
    ZigZag() {}

    static std::vector<uint64_t> zigzagEncodeVector(std::span<const int64_t> input) {
        std::vector<uint64_t> output;
        output.resize(input.size());

        if (input.size() >= kSimdThreshold) {
            zigzag_simd::zigzagEncodeSIMD(input.data(), output.data(), input.size());
        } else {
            std::transform(input.begin(), input.end(), output.begin(), [](int64_t x) { return zigzagEncode(x); });
        }

        return output;
    }

    static std::vector<uint64_t> zigzagEncodeVector(const std::vector<int64_t>& input) {
        return zigzagEncodeVector(std::span<const int64_t>(input));
    }

    static std::vector<int64_t> zigzagDecodeVector(const std::vector<uint64_t>& input) {
        std::vector<int64_t> output;
        output.resize(input.size());

        if (input.size() >= kSimdThreshold) {
            zigzag_simd::zigzagDecodeSIMD(input.data(), output.data(), input.size());
        } else {
            std::transform(input.begin(), input.end(), output.begin(), [](uint64_t x) { return zigzagDecode(x); });
        }

        return output;
    }

    static inline uint64_t zigzagEncode(int64_t x) {
        return (static_cast<uint64_t>(x) << 1) ^ static_cast<uint64_t>(x >> 63);
    }

    static inline int64_t zigzagDecode(uint64_t y) { return (int64_t)((y >> 1) ^ -(y & 0x1)); }

    // Encode int64 values as zigzag uint64 in-place into a pre-allocated buffer.
    // Avoids the heap allocation of zigzagEncodeVector().
    static void zigzagEncodeInto(std::span<const int64_t> input, uint64_t* output) {
        if (input.size() >= kSimdThreshold) {
            zigzag_simd::zigzagEncodeSIMD(input.data(), output, input.size());
        } else {
            for (size_t i = 0; i < input.size(); ++i) {
                output[i] = zigzagEncode(input[i]);
            }
        }
    }
};
