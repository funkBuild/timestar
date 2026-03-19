// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "storage/bloom_filter_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "bloom_filter_simd.hpp"

#include "hwy/highway.h"
#include "hwy/contrib/algo/copy-inl.h"

#include <xxhash.h>

#include <cstring>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace bloom {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

void BitwiseAndInplaceKernel(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vd = hn::LoadU(d, dst + i);
        auto vs = hn::LoadU(d, src + i);
        hn::StoreU(hn::And(vd, vs), d, dst + i);
    }
    for (; i < count; ++i) {
        dst[i] &= src[i];
    }
}

void BitwiseOrInplaceKernel(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vd = hn::LoadU(d, dst + i);
        auto vs = hn::LoadU(d, src + i);
        hn::StoreU(hn::Or(vd, vs), d, dst + i);
    }
    for (; i < count; ++i) {
        dst[i] |= src[i];
    }
}

void BitwiseXorInplaceKernel(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vd = hn::LoadU(d, dst + i);
        auto vs = hn::LoadU(d, src + i);
        hn::StoreU(hn::Xor(vd, vs), d, dst + i);
    }
    for (; i < count; ++i) {
        dst[i] ^= src[i];
    }
}

// SIMD popcount: count set bits across all bytes.
// Process 8 bytes at a time as uint64_t lanes for efficient popcount.
uint64_t PopcountKernel(const uint8_t* HWY_RESTRICT data, size_t count) {
    // Use 64-bit lanes for popcount — Highway's PopulationCount works on
    // integer vectors, and 64-bit gives the best throughput per lane.
    const hn::ScalableTag<uint64_t> d64;
    const size_t N64 = hn::Lanes(d64);
    const size_t stride = N64 * 8;  // bytes per iteration

    auto vsum = hn::Zero(d64);
    size_t i = 0;

    // Main SIMD loop: process N64 uint64_t values per iteration
    for (; i + stride <= count; i += stride) {
        auto v = hn::LoadU(d64, reinterpret_cast<const uint64_t*>(data + i));
        vsum = hn::Add(vsum, hn::PopulationCount(v));
    }

    // Horizontal sum of SIMD accumulator
    uint64_t total = hn::ReduceSum(d64, vsum);

    // Scalar tail: process remaining bytes
    for (; i < count; ++i) {
        // Use compiler builtin for scalar popcount on each byte
        total += static_cast<uint64_t>(__builtin_popcount(data[i]));
    }

    return total;
}

// Batch bloom filter contains check for 16-byte keys.
//
// For each key, we:
// 1. Hash with XXH3_128bits to get (h1, h2) — xxHash uses internal SIMD
// 2. Compute k probe positions using Kirsch-Mitzenmacker double hashing
// 3. Gather the k probe bytes from the bloom table
// 4. Test all k bits — if any probe bit is 0, the key is absent
//
// The SIMD optimization is in step 3-4: rather than k scalar loads and
// branches (which cause branch mispredictions on the final probe), we
// gather probe bytes into a buffer and test them with SIMD AND/comparison.
// This converts k conditional branches into a single SIMD test.
//
// Additionally, processing keys in batch keeps the bloom table hot in
// L1/L2 cache, which matters when checking hundreds of series IDs against
// a TSM file's bloom filter (the prefetchFullIndexEntries hot path).
size_t BatchContains16Kernel(const uint8_t* HWY_RESTRICT table, size_t tableSizeBits,
                             unsigned int numHashes, uint64_t seed,
                             const uint8_t* HWY_RESTRICT keys, size_t numKeys,
                             uint8_t* HWY_RESTRICT results) {
    // Maximum k (number of hashes) we can handle with stack-allocated probes.
    // The bloom_parameters formula yields k ~ 7 for 1% FPP, k ~ 10 for 0.1%.
    // 32 is generous; beyond that we fall back to scalar per-probe checks.
    static constexpr unsigned int kMaxProbes = 32;

    size_t passed = 0;

    for (size_t ki = 0; ki < numKeys; ++ki) {
        const uint8_t* key = keys + ki * 16;

        // Step 1: Hash the 16-byte key
        XXH128_hash_t h = XXH3_128bits_withSeed(key, 16, seed);
        uint64_t h1 = h.low64;
        uint64_t h2 = h.high64 | 1u;  // force odd for full period

        // Step 2-3: Compute probe positions and gather probe bytes/bits
        if (numHashes <= kMaxProbes) {
            // Gather all k probe bytes and their bit masks into small buffers.
            // This converts k random memory accesses + conditional branches
            // into k gathers followed by a single SIMD test.
            uint8_t probeBytes[kMaxProbes];
            uint8_t probeMasks[kMaxProbes];

            for (unsigned int pi = 0; pi < numHashes; ++pi) {
                size_t pos = (h1 + static_cast<uint64_t>(pi) * h2) % tableSizeBits;
                probeBytes[pi] = table[pos / 8];
                probeMasks[pi] = static_cast<uint8_t>(1u << (pos % 8));
            }

            // Step 4: SIMD test — AND each probe byte with its mask, check all non-zero.
            // If numHashes fits in one SIMD register, this is a single AND + compare.
            const hn::ScalableTag<uint8_t> d;
            const size_t N = hn::Lanes(d);

            bool absent = false;

            if (numHashes <= N) {
                // All probes fit in one SIMD register — ideal case.
                // Load probeBytes and probeMasks, AND them, check all bits set.
                // We need to mask out lanes beyond numHashes to avoid false results.
                auto vBytes = hn::LoadN(d, probeBytes, numHashes);
                auto vMasks = hn::LoadN(d, probeMasks, numHashes);
                auto vTest = hn::And(vBytes, vMasks);

                // Compare test result with masks: if (byte & mask) == mask, bit is set.
                // If any lane has (byte & mask) == 0, the key is absent.
                auto vZero = hn::Zero(d);
                auto isZero = hn::Eq(vTest, vZero);

                // We only care about the first numHashes lanes.
                // Create a mask for valid lanes using FirstN.
                auto validMask = hn::FirstN(d, numHashes);
                auto validZeros = hn::And(isZero, validMask);

                if (!hn::AllFalse(d, validZeros)) {
                    absent = true;
                }
            } else {
                // More probes than one SIMD register — process in chunks.
                unsigned int pi = 0;
                for (; pi + N <= numHashes && !absent; pi += N) {
                    auto vBytes = hn::LoadU(d, probeBytes + pi);
                    auto vMasks = hn::LoadU(d, probeMasks + pi);
                    auto vTest = hn::And(vBytes, vMasks);
                    auto vZero = hn::Zero(d);
                    auto isZero = hn::Eq(vTest, vZero);
                    if (!hn::AllFalse(d, isZero)) {
                        absent = true;
                    }
                }
                // Scalar tail for remaining probes
                for (; pi < numHashes && !absent; ++pi) {
                    if ((probeBytes[pi] & probeMasks[pi]) == 0) {
                        absent = true;
                    }
                }
            }

            results[ki] = absent ? 0 : 1;
            if (!absent) ++passed;
        } else {
            // Fallback: too many probes for stack buffer, use scalar
            bool found = true;
            for (unsigned int pi = 0; pi < numHashes; ++pi) {
                size_t pos = (h1 + static_cast<uint64_t>(pi) * h2) % tableSizeBits;
                if ((table[pos / 8] & (1u << (pos % 8))) == 0) {
                    found = false;
                    break;
                }
            }
            results[ki] = found ? 1 : 0;
            if (found) ++passed;
        }
    }

    return passed;
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace bloom
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE

namespace timestar {
namespace bloom {
namespace simd {

HWY_EXPORT(BitwiseAndInplaceKernel);
HWY_EXPORT(BitwiseOrInplaceKernel);
HWY_EXPORT(BitwiseXorInplaceKernel);
HWY_EXPORT(PopcountKernel);
HWY_EXPORT(BatchContains16Kernel);

void bitwiseAndInplace(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(BitwiseAndInplaceKernel)(dst, src, count);
}

void bitwiseOrInplace(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(BitwiseOrInplaceKernel)(dst, src, count);
}

void bitwiseXorInplace(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(BitwiseXorInplaceKernel)(dst, src, count);
}

uint64_t popcount(const uint8_t* data, size_t count) {
    return HWY_DYNAMIC_DISPATCH(PopcountKernel)(data, count);
}

size_t batchContains16(const uint8_t* table, size_t tableSizeBits,
                       unsigned int numHashes, uint64_t seed,
                       const uint8_t* keys, size_t numKeys,
                       uint8_t* results) {
    return HWY_DYNAMIC_DISPATCH(BatchContains16Kernel)(table, tableSizeBits, numHashes, seed, keys, numKeys, results);
}

}  // namespace simd
}  // namespace bloom
}  // namespace timestar

#endif  // HWY_ONCE
