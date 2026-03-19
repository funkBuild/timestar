#pragma once

#include <cstddef>
#include <cstdint>

// Highway-accelerated batch ZigZag encode/decode.
// These are implemented in zigzag_simd.cpp using Highway's foreach_target
// for automatic runtime ISA dispatch (SSE4, AVX2, AVX-512, NEON, etc.).
namespace zigzag_simd {

// Encode count int64_t values to uint64_t using ZigZag encoding.
// input and output must not overlap. output must have room for count elements.
void zigzagEncodeSIMD(const int64_t* input, uint64_t* output, size_t count);

// Decode count uint64_t values to int64_t using ZigZag decoding.
// input and output must not overlap. output must have room for count elements.
void zigzagDecodeSIMD(const uint64_t* input, int64_t* output, size_t count);

}  // namespace zigzag_simd
