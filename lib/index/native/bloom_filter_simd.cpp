// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "index/native/bloom_filter_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "bloom_filter_simd.hpp"

#include "hwy/highway.h"

#include <cstring>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace index {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Byte-level access to the uint64_t filter array in MayContainKernel assumes
// little-endian layout (bit N maps to byte[N/8], bit_within_byte = N%8).
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "bloom_filter_simd assumes little-endian byte order");

// Fast modulo replacement: maps a 32-bit value uniformly to [0, n) without division.
// Uses the "fastrange" trick: (uint64_t(x) * n) >> 32.
static HWY_INLINE size_t fastRange(uint32_t x, size_t n) {
    return static_cast<size_t>((static_cast<uint64_t>(x) * n) >> 32);
}

// ---------------------------------------------------------------------------
// BuildBatch: SIMD-accelerated bloom filter build
// ---------------------------------------------------------------------------
// For each hash value, compute all k probe positions and set the corresponding
// bits in the filter. The SIMD optimization is in the probe position computation:
// we compute h1 + i*h2 for all k values of i in SIMD lanes, then use SIMD
// multiply + shift to apply fastRange.
//
// On AVX2: 4 uint64_t lanes, so k=7 takes 2 iterations (4+3).
// On AVX-512: 8 lanes, k=7 fits in a single SIMD operation.
// On SSE4/NEON: 2 lanes, k=7 takes 4 iterations.
//
// The bit-setting itself remains scalar (random scattered writes to the filter
// array are not SIMD-friendly), but the probe position computation — which
// involves k multiplications per hash — is the CPU bottleneck being addressed.
void BuildBatchKernel(uint64_t* HWY_RESTRICT filter, size_t numBits, int k, const uint64_t* HWY_RESTRICT hashes,
                      size_t numHashes) {
    // We use uint64_t lanes to compute: fastRange(h1 + i*h2, numBits)
    // = ((h1 + i*h2) as uint32_t * numBits) >> 32
    // Using 64-bit lanes: multiply the 32-bit combined hash (zero-extended to 64)
    // by numBits, then shift right 32.
    const hn::ScalableTag<uint64_t> d64;
    const size_t N64 = hn::Lanes(d64);

    // Precompute iota vector [0, 1, 2, ...] for probe index i
    // We'll process k probes in chunks of N64 lanes
    static constexpr int kMaxK = 32;

    // Stack buffers for SIMD probe computation
    alignas(64) uint64_t probeHashBuf[HWY_MAX_LANES_D(hn::ScalableTag<uint64_t>)];
    alignas(64) uint64_t probePosBuf[HWY_MAX_LANES_D(hn::ScalableTag<uint64_t>)];

    const auto vNumBits = hn::Set(d64, static_cast<uint64_t>(numBits));
    const auto vMask32 = hn::Set(d64, 0xFFFFFFFFULL);

    for (size_t hi = 0; hi < numHashes; ++hi) {
        uint64_t h = hashes[hi];
        uint32_t h1 = static_cast<uint32_t>(h);
        uint32_t h2 = static_cast<uint32_t>(h >> 32);

        const auto vH1 = hn::Set(d64, static_cast<uint64_t>(h1));
        const auto vH2 = hn::Set(d64, static_cast<uint64_t>(h2));

        int i = 0;
        // Process probes in SIMD-width chunks
        for (; i + static_cast<int>(N64) <= k && i < kMaxK; i += static_cast<int>(N64)) {
            // Build iota: [i, i+1, i+2, ...]
            for (size_t lane = 0; lane < N64; ++lane) {
                probeHashBuf[lane] = static_cast<uint64_t>(static_cast<uint32_t>(i) + static_cast<uint32_t>(lane));
            }
            auto vIdx = hn::Load(d64, probeHashBuf);

            // combined = (h1 + idx * h2) & 0xFFFFFFFF  (keep as uint32_t in 64-bit lane)
            auto vCombined = hn::And(hn::Add(vH1, hn::Mul(vIdx, vH2)), vMask32);

            // fastRange: (combined * numBits) >> 32
            // Highway MulHigh gives the upper 64 bits of 64x64 multiply,
            // but we want (val32 * numBits) >> 32 which is just MulHigh
            // of the zero-extended 32-bit value with numBits.
            // Since vCombined is already masked to 32 bits, Mul gives the
            // lower 64 bits of the product. We need the upper 32 bits of
            // a 32x(up to 64)-bit product. For numBits < 2^32, Mul suffices
            // and we shift right 32.
            auto vProduct = hn::Mul(vCombined, vNumBits);
            auto vBitPos = hn::ShiftRight<32>(vProduct);

            // Extract positions and set bits (scalar — random write pattern)
            hn::Store(vBitPos, d64, probePosBuf);
            for (size_t lane = 0; lane < N64; ++lane) {
                size_t bitPos = probePosBuf[lane];
                filter[bitPos / 64] |= (1ULL << (bitPos % 64));
            }
        }

        // Scalar tail for remaining probes
        for (; i < k; ++i) {
            size_t bitPos = fastRange(h1 + static_cast<uint32_t>(i) * h2, numBits);
            filter[bitPos / 64] |= (1ULL << (bitPos % 64));
        }
    }
}

// ---------------------------------------------------------------------------
// MayContain: SIMD-accelerated probe test
// ---------------------------------------------------------------------------
// Gathers all k probe bytes from the filter and tests them simultaneously
// with SIMD AND+compare. This replaces k sequential getBit() calls (each
// with a conditional branch) with a single branchless SIMD test.
//
// The approach mirrors the storage bloom filter's BatchContains16Kernel:
// 1. Compute all k probe positions (scalar — only k=6-10 values)
// 2. Gather the k probe bytes and their bit masks into small buffers
// 3. SIMD AND each probe byte with its mask
// 4. SIMD compare against zero — if any lane is zero, key is absent
//
// For k <= SIMD width (typically 16-64 on modern CPUs), this is a single
// SIMD AND + compare + AllFalse test.
bool MayContainKernel(const uint64_t* HWY_RESTRICT filter, size_t numBits, int k, uint32_t h1, uint32_t h2) {
    static constexpr int kMaxProbes = 32;

    // Treat the uint64_t array as bytes for byte-level gather.
    // On little-endian (x86, ARM), bit position N maps to:
    //   byte = N / 8,  bit_within_byte = N % 8
    // This is consistent with the uint64_t layout where bit N is at
    //   word = N / 64,  bit_within_word = N % 64
    // because word W byte B contains bits [W*64 + B*8 .. W*64 + B*8 + 7].
    const auto* filterBytes = reinterpret_cast<const uint8_t*>(filter);

    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    if (k <= kMaxProbes) {
        uint8_t probeBytes[kMaxProbes];
        uint8_t probeMasks[kMaxProbes];

        // Gather probe bytes and compute bit masks
        for (int i = 0; i < k; ++i) {
            size_t bitPos = fastRange(h1 + static_cast<uint32_t>(i) * h2, numBits);
            probeBytes[i] = filterBytes[bitPos / 8];
            probeMasks[i] = static_cast<uint8_t>(1u << (bitPos % 8));
        }

        if (static_cast<size_t>(k) <= N) {
            // All probes fit in one SIMD register — ideal case
            auto vBytes = hn::LoadN(d, probeBytes, static_cast<size_t>(k));
            auto vMasks = hn::LoadN(d, probeMasks, static_cast<size_t>(k));
            auto vTest = hn::And(vBytes, vMasks);

            auto vZero = hn::Zero(d);
            auto isZero = hn::Eq(vTest, vZero);
            auto validMask = hn::FirstN(d, static_cast<size_t>(k));
            auto validZeros = hn::And(isZero, validMask);

            return hn::AllFalse(d, validZeros);
        }

        // More probes than one SIMD register — process in chunks
        size_t pi = 0;
        for (; pi + N <= static_cast<size_t>(k); pi += N) {
            auto vBytes = hn::LoadU(d, probeBytes + pi);
            auto vMasks = hn::LoadU(d, probeMasks + pi);
            auto vTest = hn::And(vBytes, vMasks);
            auto vZero = hn::Zero(d);
            auto isZero = hn::Eq(vTest, vZero);
            if (!hn::AllFalse(d, isZero)) {
                return false;
            }
        }

        // Handle remaining probes with masked SIMD
        if (pi < static_cast<size_t>(k)) {
            size_t remaining = static_cast<size_t>(k) - pi;
            auto vBytes = hn::LoadN(d, probeBytes + pi, remaining);
            auto vMasks = hn::LoadN(d, probeMasks + pi, remaining);
            auto vTest = hn::And(vBytes, vMasks);
            auto vZero = hn::Zero(d);
            auto isZero = hn::Eq(vTest, vZero);
            auto validMask = hn::FirstN(d, remaining);
            auto validZeros = hn::And(isZero, validMask);
            if (!hn::AllFalse(d, validZeros)) {
                return false;
            }
        }

        return true;
    }

    // Fallback: too many probes for stack buffer
    for (int i = 0; i < k; ++i) {
        size_t bitPos = fastRange(h1 + static_cast<uint32_t>(i) * h2, numBits);
        if ((filterBytes[bitPos / 8] & (1u << (bitPos % 8))) == 0) {
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Popcount: SIMD-accelerated bit count over the filter
// ---------------------------------------------------------------------------
uint64_t PopcountKernel(const uint64_t* HWY_RESTRICT filter, size_t numWords) {
    const hn::ScalableTag<uint64_t> d64;
    const size_t N64 = hn::Lanes(d64);

    auto vsum = hn::Zero(d64);
    size_t i = 0;

    for (; i + N64 <= numWords; i += N64) {
        auto v = hn::LoadU(d64, filter + i);
        vsum = hn::Add(vsum, hn::PopulationCount(v));
    }

    uint64_t total = hn::ReduceSum(d64, vsum);

    // Scalar tail
    for (; i < numWords; ++i) {
        total += static_cast<uint64_t>(__builtin_popcountll(filter[i]));
    }

    return total;
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace index
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE

namespace timestar {
namespace index {
namespace simd {

HWY_EXPORT(BuildBatchKernel);
HWY_EXPORT(MayContainKernel);
HWY_EXPORT(PopcountKernel);

void bloomBuildBatch(uint64_t* filter, size_t numBits, int k, const uint64_t* hashes, size_t numHashes) {
    HWY_DYNAMIC_DISPATCH(BuildBatchKernel)(filter, numBits, k, hashes, numHashes);
}

bool bloomMayContain(const uint64_t* filter, size_t numBits, int k, uint32_t h1, uint32_t h2) {
    return HWY_DYNAMIC_DISPATCH(MayContainKernel)(filter, numBits, k, h1, h2);
}

uint64_t bloomPopcount(const uint64_t* filter, size_t numWords) {
    return HWY_DYNAMIC_DISPATCH(PopcountKernel)(filter, numWords);
}

}  // namespace simd
}  // namespace index
}  // namespace timestar

#endif  // HWY_ONCE
