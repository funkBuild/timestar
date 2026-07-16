#include "alp/alp_simd.hpp"

#include <gtest/gtest.h>

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

// ============================================================================
// Correctness tests for the Highway-dispatched ALP SIMD kernels
// (lib/encoding/alp/alp_simd.cpp), validated against locally-written scalar
// reference implementations.
//
// alpReconstruct computes out[i] = (double)encoded[i] * frac_val / fact_val
// element-wise. The SIMD kernel performs the exact same IEEE-754 operation
// sequence per lane (convert, multiply, divide — no FMA, no reassociation),
// so the result must be BIT-EXACT against the scalar reference.
//
// alpScaleF0 is validated against a scalar re-implementation of the
// documented scaleValue round-trip semantics, gated on
// alpScaleSimdAvailable() as required by the header contract.
// ============================================================================

namespace {

// Sizes covering: empty, single, lane-1 / lane / lane+1 for every possible
// double lane width (2, 4, 8), the 4xN-style unroll boundaries, and large
// non-multiple-of-lane counts.
const std::vector<size_t> kSizes = {0,  1,  2,  3,  4,  5,  7,  8,    9,   15,
                                    16, 17, 31, 32, 33, 63, 64, 1000, 4097};

// (frac_val, fact_val) pairs. Realistic ALP values are frac = 10^-f,
// fact = 10^e; extreme pairs exercise denormal and overflow-to-inf outputs.
const std::vector<std::pair<double, double>> kFracFactPairs = {
    {1.0, 1.0},
    {1.0, 100.0},
    {0.01, 1.0},
    {1e-14, 1.0},
    {1.0, 1e14},
    {1e-10, 1e10},
    {0.1, 1e17},
    {1e-14, 1e300},  // drives small encoded values into the denormal range
    {1e300, 1.0},    // drives large encoded values to +/-inf
};

// Scalar reference for alpReconstruct.
void refReconstruct(const int64_t* encoded, size_t count, double frac_val, double fact_val, double* out) {
    for (size_t i = 0; i < count; ++i) {
        out[i] = static_cast<double>(encoded[i]) * frac_val / fact_val;
    }
}

// Bit-exact comparison (distinguishes -0.0 from +0.0 and NaN payloads).
bool bitEqual(double a, double b) {
    return std::bit_cast<uint64_t>(a) == std::bit_cast<uint64_t>(b);
}

constexpr double kOutSentinel = -777.125;  // exactly representable

// Runs alpReconstruct against the reference for one input set; also checks
// that the kernel writes exactly `count` outputs (guard region untouched).
void checkReconstruct(const std::vector<int64_t>& encoded, double frac_val, double fact_val) {
    const size_t count = encoded.size();
    constexpr size_t kGuard = 16;

    std::vector<double> simd_out(count + kGuard, kOutSentinel);
    std::vector<double> ref_out(count);

    timestar::alp::simd::alpReconstruct(encoded.data(), count, frac_val, fact_val, simd_out.data());
    refReconstruct(encoded.data(), count, frac_val, fact_val, ref_out.data());

    for (size_t i = 0; i < count; ++i) {
        ASSERT_TRUE(bitEqual(simd_out[i], ref_out[i]))
            << "SIMD/scalar mismatch at i=" << i << " count=" << count << " frac=" << frac_val
            << " fact=" << fact_val << " encoded=" << encoded[i] << " simd=" << simd_out[i]
            << " scalar=" << ref_out[i];
    }
    for (size_t i = count; i < count + kGuard; ++i) {
        ASSERT_TRUE(bitEqual(simd_out[i], kOutSentinel)) << "Kernel wrote past count at offset " << i - count;
    }
}

}  // namespace

class AlpSimdCorrectnessTest : public ::testing::Test {};

// ==================== alpReconstruct ====================

TEST_F(AlpSimdCorrectnessTest, ReconstructSizeSweepMatchesScalarBitExact) {
    std::mt19937 rng(20260716);
    std::uniform_int_distribution<int64_t> smallDist(-1000000, 1000000);

    for (size_t size : kSizes) {
        std::vector<int64_t> encoded(size);
        for (auto& v : encoded)
            v = smallDist(rng);
        for (const auto& [frac, fact] : kFracFactPairs) {
            checkReconstruct(encoded, frac, fact);
        }
    }
}

TEST_F(AlpSimdCorrectnessTest, ReconstructExtremeAndSpecialInputs) {
    // Extremes of the int64 domain, zero, and values near the 2^53 exactness
    // boundary; combined with pairs that push outputs denormal or to inf.
    std::vector<int64_t> encoded = {0,
                                    1,
                                    -1,
                                    2,
                                    -2,
                                    (int64_t(1) << 52) - 1,
                                    (int64_t(1) << 52),
                                    (int64_t(1) << 53) - 1,
                                    (int64_t(1) << 53),
                                    (int64_t(1) << 53) + 1,  // not exactly representable as double
                                    -((int64_t(1) << 53) + 1),
                                    std::numeric_limits<int64_t>::max(),
                                    std::numeric_limits<int64_t>::min(),
                                    std::numeric_limits<int64_t>::max() - 1,
                                    std::numeric_limits<int64_t>::min() + 1,
                                    123456789012345678LL,
                                    -987654321098765432LL};
    for (const auto& [frac, fact] : kFracFactPairs) {
        checkReconstruct(encoded, frac, fact);
    }
}

TEST_F(AlpSimdCorrectnessTest, ReconstructDenormalOutputs) {
    // Small encoded values divided by a huge fact land in the denormal range;
    // the SIMD Div must not flush them to zero (must match scalar exactly).
    std::vector<int64_t> encoded;
    for (int64_t v = -20; v <= 20; ++v)
        encoded.push_back(v);
    checkReconstruct(encoded, 1e-14, 1e300);
    checkReconstruct(encoded, 1.0, 1e308);
}

TEST_F(AlpSimdCorrectnessTest, ReconstructRandomSweep) {
    std::mt19937 rng(424242);
    std::uniform_int_distribution<size_t> sizeDist(0, 300);
    std::uniform_int_distribution<int64_t> fullDist(std::numeric_limits<int64_t>::min(),
                                                    std::numeric_limits<int64_t>::max());
    std::uniform_int_distribution<int64_t> smallDist(-10000000, 10000000);
    std::uniform_int_distribution<int> pairDist(0, static_cast<int>(kFracFactPairs.size()) - 1);

    for (int iter = 0; iter < 100; ++iter) {
        const size_t size = sizeDist(rng);
        std::vector<int64_t> encoded(size);
        const bool fullRange = (iter % 3 == 0);
        for (auto& v : encoded)
            v = fullRange ? fullDist(rng) : smallDist(rng);
        const auto& [frac, fact] = kFracFactPairs[static_cast<size_t>(pairDist(rng))];
        checkReconstruct(encoded, frac, fact);
    }
}

TEST_F(AlpSimdCorrectnessTest, ReconstructIsDeterministicAcrossCalls) {
    // Highway dynamic dispatch must resolve to the same target every call:
    // repeated invocations on identical input must be bit-identical.
    std::mt19937 rng(1337);
    std::uniform_int_distribution<int64_t> dist(-1000000000, 1000000000);
    std::vector<int64_t> encoded(4097);
    for (auto& v : encoded)
        v = dist(rng);

    std::vector<double> out1(encoded.size()), out2(encoded.size());
    timestar::alp::simd::alpReconstruct(encoded.data(), encoded.size(), 0.01, 100.0, out1.data());
    timestar::alp::simd::alpReconstruct(encoded.data(), encoded.size(), 0.01, 100.0, out2.data());
    for (size_t i = 0; i < encoded.size(); ++i) {
        ASSERT_TRUE(bitEqual(out1[i], out2[i])) << "Non-deterministic result at i=" << i;
    }
}

// ==================== alpScaleSimdAvailable gate ====================

TEST_F(AlpSimdCorrectnessTest, ScaleSimdAvailableGateIsConsistent) {
    const bool first = timestar::alp::simd::alpScaleSimdAvailable();
    for (int i = 0; i < 16; ++i) {
        EXPECT_EQ(timestar::alp::simd::alpScaleSimdAvailable(), first) << "Gate flipped on call " << i + 2;
    }
    // alpReconstruct has no gate: it must be callable and correct regardless
    // of what the scale gate reports (verified above); just re-assert the
    // gate did not change as a side effect of running the kernels.
    std::vector<int64_t> enc = {1, 2, 3};
    std::vector<double> out(3);
    timestar::alp::simd::alpReconstruct(enc.data(), enc.size(), 1.0, 10.0, out.data());
    EXPECT_EQ(timestar::alp::simd::alpScaleSimdAvailable(), first);
}

// ==================== alpScaleF0 (gated) ====================

namespace {

struct ScaleRef {
    std::vector<int64_t> encoded;  // meaningful only where exact[i]
    std::vector<bool> exact;
    std::vector<uint16_t> excPositions;
    std::vector<uint64_t> excValues;
    int64_t minVal = std::numeric_limits<int64_t>::max();
    int64_t maxVal = std::numeric_limits<int64_t>::min();
};

// Scalar reference replicating the documented scaleValue round-trip
// semantics for fac == 0 (see alp_simd.hpp / the kernel's scalar tail).
ScaleRef refScaleF0(const std::vector<double>& values, double fact_val) {
    ScaleRef r;
    r.encoded.resize(values.size(), 0);
    r.exact.resize(values.size(), false);
    for (size_t i = 0; i < values.size(); ++i) {
        const double v = values[i];
        const uint64_t bits = std::bit_cast<uint64_t>(v);
        if (bits != 0x8000000000000000ULL && !std::isnan(v) && !std::isinf(v)) {
            const double rounded = std::round(v * fact_val);
            if (rounded <= 9007199254740992.0 && rounded >= -9007199254740992.0) {
                const int64_t e = static_cast<int64_t>(rounded);
                if (static_cast<double>(e) * 1.0 / fact_val == v) {
                    r.encoded[i] = e;
                    r.exact[i] = true;
                    r.minVal = std::min(r.minVal, e);
                    r.maxVal = std::max(r.maxVal, e);
                    continue;
                }
            }
        }
        r.excPositions.push_back(static_cast<uint16_t>(i));
        r.excValues.push_back(bits);
    }
    return r;
}

void checkScaleF0(const std::vector<double>& values, double fact_val) {
    const size_t count = values.size();
    std::vector<int64_t> encoded(count, 0);
    std::vector<uint16_t> excPositions(count == 0 ? 1 : count, 0);
    std::vector<uint64_t> excValues(count == 0 ? 1 : count, 0);
    int64_t minVal = 0, maxVal = 0;

    const size_t nExc = timestar::alp::simd::alpScaleF0(values.data(), count, fact_val, encoded.data(), &minVal, &maxVal,
                                              excPositions.data(), excValues.data());
    const ScaleRef ref = refScaleF0(values, fact_val);

    ASSERT_EQ(nExc, ref.excPositions.size()) << "Exception count mismatch, count=" << count << " fact=" << fact_val;
    for (size_t k = 0; k < nExc; ++k) {
        ASSERT_EQ(excPositions[k], ref.excPositions[k]) << "Exception position mismatch at k=" << k;
        ASSERT_EQ(excValues[k], ref.excValues[k]) << "Exception raw bits mismatch at k=" << k;
    }
    EXPECT_EQ(minVal, ref.minVal) << "min mismatch, count=" << count;
    EXPECT_EQ(maxVal, ref.maxVal) << "max mismatch, count=" << count;
    // Exception slots hold junk by contract; only compare exact slots.
    for (size_t i = 0; i < count; ++i) {
        if (ref.exact[i]) {
            ASSERT_EQ(encoded[i], ref.encoded[i]) << "Encoded mismatch at i=" << i << " v=" << values[i];
        }
    }
}

}  // namespace

TEST_F(AlpSimdCorrectnessTest, ScaleF0MatchesScalarReference) {
    if (!timestar::alp::simd::alpScaleSimdAvailable()) {
        GTEST_SKIP() << "alpScaleF0 is gated off on this target (no AVX-512 DQ)";
    }

    std::mt19937 rng(20260715);
    std::uniform_int_distribution<int64_t> intDist(-1000000000000LL, 1000000000000LL);
    std::uniform_real_distribution<double> realDist(-1e6, 1e6);
    const double fact = 100.0;  // 2 decimal places

    const std::vector<double> specials = {std::numeric_limits<double>::quiet_NaN(),
                                          std::numeric_limits<double>::infinity(),
                                          -std::numeric_limits<double>::infinity(),
                                          -0.0,
                                          0.0,
                                          3.141592653589793,  // never exact at 2 decimals
                                          1e17,               // exceeds 2^53 after scaling
                                          -1e17,
                                          std::numeric_limits<double>::denorm_min(),
                                          std::numeric_limits<double>::max()};

    for (size_t size : kSizes) {
        std::vector<double> values(size);
        for (size_t i = 0; i < size; ++i) {
            switch (i % 4) {
            case 0:
            case 1:
                // Exactly representable at scale: v = k / fact
                values[i] = static_cast<double>(intDist(rng)) / fact;
                break;
            case 2:
                values[i] = specials[i % specials.size()];
                break;
            default:
                values[i] = realDist(rng);  // usually inexact at 2 decimals
                break;
            }
        }
        checkScaleF0(values, fact);
    }

    // All-exception and all-exact blocks (mask fast path both ways).
    checkScaleF0(std::vector<double>(64, std::numeric_limits<double>::quiet_NaN()), fact);
    checkScaleF0(std::vector<double>(64, 12.25), 100.0);

    // Randomized sweep with varying fact values.
    const std::vector<double> facts = {1.0, 10.0, 100.0, 1e6, 1e14};
    std::uniform_int_distribution<size_t> sizeDist(0, 200);
    std::uniform_int_distribution<size_t> factIdx(0, facts.size() - 1);
    std::uniform_int_distribution<size_t> specialIdx(0, specials.size() - 1);
    for (int iter = 0; iter < 100; ++iter) {
        const double f = facts[factIdx(rng)];
        const size_t size = sizeDist(rng);
        std::vector<double> values(size);
        for (size_t i = 0; i < size; ++i) {
            if (i % 5 == 4) {
                values[i] = specials[specialIdx(rng)];
            } else if (i % 2 == 0) {
                values[i] = static_cast<double>(intDist(rng) % 100000000) / f;
            } else {
                values[i] = realDist(rng);
            }
        }
        checkScaleF0(values, f);
    }
}
