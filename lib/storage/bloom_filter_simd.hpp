#pragma once

#include <cstddef>
#include <cstdint>

namespace timestar {
namespace bloom {
namespace simd {

// Minimum byte count before SIMD dispatch is worthwhile.
// Below this threshold the scalar loop is faster due to dispatch overhead.
inline constexpr size_t kSimdThreshold = 64;

// In-place bitwise AND: dst[i] &= src[i]
void bitwiseAndInplace(uint8_t* dst, const uint8_t* src, size_t count);

// In-place bitwise OR: dst[i] |= src[i]
void bitwiseOrInplace(uint8_t* dst, const uint8_t* src, size_t count);

// In-place bitwise XOR: dst[i] ^= src[i]
void bitwiseXorInplace(uint8_t* dst, const uint8_t* src, size_t count);

}  // namespace simd
}  // namespace bloom
}  // namespace timestar
