#pragma once

#include <cstddef>
#include <cstdint>

namespace timestar::index::simd {

// Minimum filter byte count before SIMD dispatch is worthwhile.
// Below this threshold the scalar loop is faster due to dispatch overhead.
inline constexpr size_t kBloomSimdThreshold = 64;

// SIMD-accelerated bit-setting for bloom filter build().
//
// For each of numHashes hashes in the hashes array, computes k probe positions
// using double hashing (h1 + i*h2 for i in [0, k)) via fastRange, and sets
// the corresponding bits in the filter.
//
// The SIMD advantage: for each hash, all k probe positions are computed
// simultaneously in SIMD lanes (SIMD multiply + shift replaces k sequential
// __uint128_t multiplications), then bits are set in a scalar loop.
// For large builds (100K+ keys during compaction), this saves ~30-40% of
// the build() CPU time.
//
// Parameters:
//   filter     - Pointer to the uint64_t word array (bit vector)
//   numBits    - Total number of bits in the filter (filter size in words * 64)
//   k          - Number of hash functions (probe positions per key)
//   hashes     - Array of numHashes 64-bit hash values (lower 32 = h1, upper 32 = h2)
//   numHashes  - Number of hashes to process
void bloomBuildBatch(uint64_t* filter, size_t numBits, int k,
                     const uint64_t* hashes, size_t numHashes);

// SIMD-accelerated probe test for bloom filter mayContain().
//
// Computes k probe positions using double hashing, gathers the probe bytes
// from the filter, and tests all k bits simultaneously with SIMD AND+compare.
// This replaces k sequential getBit() calls with conditional branches
// (which cause branch mispredictions on the final probe) with a single
// branchless SIMD test.
//
// Parameters:
//   filter     - Pointer to the uint64_t word array (treated as uint8_t* for byte gather)
//   numBits    - Total number of bits in the filter
//   k          - Number of hash functions
//   h1         - Lower 32 bits of the hash
//   h2         - Upper 32 bits of the hash
//
// Returns: true if all k probe bits are set (key may be present),
//          false if any probe bit is zero (key definitely absent).
bool bloomMayContain(const uint64_t* filter, size_t numBits, int k,
                     uint32_t h1, uint32_t h2);

// SIMD-accelerated popcount: count total set bits in the filter.
// Uses Highway PopulationCount on uint64_t lanes for maximum throughput.
// Useful for filter density estimation and diagnostics.
uint64_t bloomPopcount(const uint64_t* filter, size_t numWords);

}  // namespace timestar::index::simd
