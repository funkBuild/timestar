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

// Population count: returns total number of set bits in the table.
// Uses SIMD-accelerated popcount across all bytes.
uint64_t popcount(const uint8_t* data, size_t count);

// Batch contains check for fixed-size 16-byte keys (SeriesId128).
// Hashes each key with XXH3_128bits, probes k positions in the bloom table,
// and writes 1 (may contain) or 0 (definitely absent) to results[i].
// Returns the number of keys that passed the filter (result == 1).
//
// This is the hot-path optimization: avoids per-key function call overhead
// and keeps the bloom table hot in L1/L2 cache across the batch. The hash
// computation itself uses xxHash's internal SIMD. The SIMD advantage here
// is in the probe-checking phase: we gather all k probe bytes and test
// them with a single SIMD comparison rather than k scalar branches.
size_t batchContains16(const uint8_t* table, size_t tableSizeBits,
                       unsigned int numHashes, uint64_t seed,
                       const uint8_t* keys, size_t numKeys,
                       uint8_t* results);

}  // namespace simd
}  // namespace bloom
}  // namespace timestar
