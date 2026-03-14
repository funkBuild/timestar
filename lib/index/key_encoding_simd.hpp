#ifndef KEY_ENCODING_SIMD_HPP_INCLUDED
#define KEY_ENCODING_SIMD_HPP_INCLUDED

#include <cstddef>

namespace timestar::index::simd {

// Minimum byte count before SIMD dispatch is worthwhile.
inline constexpr size_t kSimdThreshold = 16;

// Scan a string for escape characters (\, comma, =, space).
// Returns the index of the first escape character, or len if none found.
// Uses SIMD to check 16-32 bytes at a time.
size_t findFirstEscapeChar(const char* data, size_t len);

}  // namespace timestar::index::simd

#endif  // KEY_ENCODING_SIMD_HPP_INCLUDED
