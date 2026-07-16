#include "../../../lib/query/transform/transform_functions_simd.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

// ============================================================================
// SIMD-vs-scalar property tests for the Highway-dispatched transform kernels
// (lib/query/transform/transform_functions_simd.cpp), following the pattern of
// test/unit/encoding/alp_simd_correctness_test.cpp:
//   - fixed seed for reproducibility
//   - size sweep covering 0/1, lane-1/lane/lane+1 for all possible double lane
//     widths (2, 4, 8), the scalar-dispatch threshold (SIMD_MIN_SIZE = 8), and
//     large non-multiple-of-lane counts (1000, 4097)
//   - NaN / +-Inf / +-0.0 / denormal injection
//
// The reference implementations are the scalar fallbacks that live in
// timestar::transform::simd::scalar (same header) — the dispatch functions
// route to them for sizes < SIMD_MIN_SIZE, so SIMD body, SIMD tail, and the
// scalar path must all agree.
//
// Exact-equality is used everywhere except:
//   - exp: Highway contrib math Exp is a polynomial approximation, compared
//     with a relative tolerance. NaN propagation is pinned exactly (the kernel
//     needs an explicit NaN re-mask — a known regression risk).
//   - mean_stddev_skipnan: SIMD lane accumulation reassociates the sum, so
//     mean/stddev are compared with a relative tolerance (count is exact).
// ============================================================================

namespace simd = timestar::transform::simd;

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kInf = std::numeric_limits<double>::infinity();

// Sizes: scalar-dispatch path (< 8), lane boundaries for 2/4/8-wide doubles,
// and large non-multiples of any lane count.
const std::vector<size_t> kSizes = {0,  1,  2,  3,  4,  5,  7,  8,   9,   15,  16,   17,
                                    31, 32, 33, 63, 64, 65, 127, 128, 129, 1000, 4097};

// Special values injected into random inputs.
const double kSpecials[] = {
    kNaN,
    kInf,
    -kInf,
    0.0,
    -0.0,
    5e-324,                    // smallest positive denormal
    -5e-324,                   //
    2.2250738585072014e-308,   // smallest positive normal
    1e15,                      // large but well below 2^52 (round-safe)
    -1e15,
};

std::vector<double> makeValues(size_t n, std::mt19937_64& rng) {
    std::uniform_real_distribution<double> dist(-1e6, 1e6);
    std::uniform_int_distribution<int> inject(0, 7);
    std::uniform_int_distribution<size_t> which(0, std::size(kSpecials) - 1);
    std::vector<double> v(n);
    for (auto& x : v) {
        x = (inject(rng) == 0) ? kSpecials[which(rng)] : dist(rng);
    }
    return v;
}

// NaN-aware equality: any NaN equals any NaN; otherwise IEEE == (so -0 == +0).
bool nanEq(double a, double b) {
    if (std::isnan(a) && std::isnan(b))
        return true;
    return a == b;
}

using UnaryFn = std::vector<double> (*)(const std::vector<double>&);

void checkUnarySweep(UnaryFn simdFn, UnaryFn refFn, const char* name) {
    std::mt19937_64 rng(20260716);
    for (size_t size : kSizes) {
        auto values = makeValues(size, rng);
        auto got = simdFn(values);
        auto want = refFn(values);
        ASSERT_EQ(got.size(), want.size()) << name << " size=" << size;
        for (size_t i = 0; i < size; ++i) {
            ASSERT_TRUE(nanEq(got[i], want[i]))
                << name << ": SIMD/scalar mismatch size=" << size << " i=" << i << " in=" << values[i]
                << " simd=" << got[i] << " ref=" << want[i];
        }
    }
}

using UnaryParamFn = std::vector<double> (*)(const std::vector<double>&, double);

void checkUnaryParamSweep(UnaryParamFn simdFn, UnaryParamFn refFn, const char* name) {
    const double params[] = {-100.0, 0.0, 3.7, 1e6};
    std::mt19937_64 rng(20260716);
    for (size_t size : kSizes) {
        auto values = makeValues(size, rng);
        for (double p : params) {
            auto got = simdFn(values, p);
            auto want = refFn(values, p);
            ASSERT_EQ(got.size(), want.size()) << name;
            for (size_t i = 0; i < size; ++i) {
                ASSERT_TRUE(nanEq(got[i], want[i]))
                    << name << ": SIMD/scalar mismatch size=" << size << " param=" << p << " i=" << i
                    << " in=" << values[i] << " simd=" << got[i] << " ref=" << want[i];
            }
        }
    }
}

}  // namespace

class TransformSimdPropertyTest : public ::testing::Test {};

// ==================== element-wise kernels, exact match ====================

TEST_F(TransformSimdPropertyTest, AbsMatchesScalarSweep) {
    checkUnarySweep(&simd::abs, &simd::scalar::abs, "abs");
}

TEST_F(TransformSimdPropertyTest, DefaultZeroMatchesScalarSweep) {
    checkUnarySweep(&simd::default_zero, &simd::scalar::default_zero, "default_zero");
}

TEST_F(TransformSimdPropertyTest, CountNonzeroMatchesScalarSweep) {
    checkUnarySweep(&simd::count_nonzero, &simd::scalar::count_nonzero, "count_nonzero");
}

TEST_F(TransformSimdPropertyTest, CountNotNullMatchesScalarSweep) {
    checkUnarySweep(&simd::count_not_null, &simd::scalar::count_not_null, "count_not_null");
}

TEST_F(TransformSimdPropertyTest, ClampMinMatchesScalarSweep) {
    checkUnaryParamSweep(&simd::clamp_min, &simd::scalar::clamp_min, "clamp_min");
}

TEST_F(TransformSimdPropertyTest, ClampMaxMatchesScalarSweep) {
    checkUnaryParamSweep(&simd::clamp_max, &simd::scalar::clamp_max, "clamp_max");
}

TEST_F(TransformSimdPropertyTest, CutoffMinMatchesScalarSweep) {
    checkUnaryParamSweep(&simd::cutoff_min, &simd::scalar::cutoff_min, "cutoff_min");
}

TEST_F(TransformSimdPropertyTest, CutoffMaxMatchesScalarSweep) {
    checkUnaryParamSweep(&simd::cutoff_max, &simd::scalar::cutoff_max, "cutoff_max");
}

TEST_F(TransformSimdPropertyTest, DiffMatchesScalarSweep) {
    checkUnarySweep(&simd::diff, &simd::scalar::diff, "diff");
}

TEST_F(TransformSimdPropertyTest, MonotonicDiffMatchesScalarSweep) {
    checkUnarySweep(&simd::monotonic_diff, &simd::scalar::monotonic_diff, "monotonic_diff");
}

TEST_F(TransformSimdPropertyTest, SignMatchesScalarSweep) {
    checkUnarySweep(&simd::sign, &simd::scalar::sign, "sign");
}

TEST_F(TransformSimdPropertyTest, SignSpecialValues) {
    // One array > SIMD_MIN_SIZE covering every interesting class in the body.
    std::vector<double> in = {-kInf, -1e6, -5e-324, -0.0, 0.0, 5e-324, 1e6, kInf, kNaN, 2.0, -2.0, 0.0};
    auto out = simd::sign(in);
    ASSERT_EQ(out.size(), in.size());
    EXPECT_DOUBLE_EQ(out[0], -1.0);
    EXPECT_DOUBLE_EQ(out[1], -1.0);
    EXPECT_DOUBLE_EQ(out[2], -1.0) << "negative denormal must be sign -1";
    EXPECT_EQ(out[3], 0.0) << "-0.0 maps to zero";
    EXPECT_EQ(out[4], 0.0);
    EXPECT_DOUBLE_EQ(out[5], 1.0) << "positive denormal must be sign +1";
    EXPECT_DOUBLE_EQ(out[6], 1.0);
    EXPECT_DOUBLE_EQ(out[7], 1.0);
    EXPECT_TRUE(std::isnan(out[8])) << "sign(NaN) must be NaN";
    EXPECT_DOUBLE_EQ(out[9], 1.0);
    EXPECT_DOUBLE_EQ(out[10], -1.0);
    EXPECT_EQ(out[11], 0.0);
}

TEST_F(TransformSimdPropertyTest, MultiplyInplaceMatchesScalarSweep) {
    const double factors[] = {2.5, -1.0, 0.0, 1e-6};
    std::mt19937_64 rng(20260716);
    for (size_t size : kSizes) {
        auto base = makeValues(size, rng);
        for (double f : factors) {
            auto simdVec = base;
            auto refVec = base;
            simd::multiply_inplace(simdVec, f);
            simd::scalar::multiply_inplace(refVec, f);
            ASSERT_EQ(simdVec.size(), refVec.size());
            for (size_t i = 0; i < size; ++i) {
                ASSERT_TRUE(nanEq(simdVec[i], refVec[i]))
                    << "multiply_inplace mismatch size=" << size << " factor=" << f << " i=" << i
                    << " in=" << base[i] << " simd=" << simdVec[i] << " ref=" << refVec[i];
            }
        }
    }
}

TEST_F(TransformSimdPropertyTest, ScaleShiftMatchesScalarSweep) {
    const std::pair<double, double> params[] = {{0.0, 1.0}, {3.5, -2.25}, {1e5, 1e-3}, {-42.0, 1e6}};
    std::mt19937_64 rng(20260716);
    for (size_t size : kSizes) {
        auto values = makeValues(size, rng);
        for (const auto& [sub, mul] : params) {
            auto got = simd::scale_shift(values, sub, mul);
            auto want = simd::scalar::scale_shift(values, sub, mul);
            ASSERT_EQ(got.size(), want.size());
            for (size_t i = 0; i < size; ++i) {
                ASSERT_TRUE(nanEq(got[i], want[i]))
                    << "scale_shift mismatch size=" << size << " sub=" << sub << " mul=" << mul << " i=" << i
                    << " in=" << values[i] << " simd=" << got[i] << " ref=" << want[i];
            }
        }
    }
}

// ==================== round: half-away-from-zero semantics ====================

TEST_F(TransformSimdPropertyTest, RoundMatchesScalarSweep) {
    // Random values stay within +-1e6 and specials within +-1e15, both far
    // below 2^52 where the floor(|x|+0.5) construction is exact.
    checkUnarySweep(&simd::round, &simd::scalar::round, "round");
}

TEST_F(TransformSimdPropertyTest, RoundHalfAwayTies) {
    // Exact .5 ties must round away from zero (std::round semantics), not to
    // even (Highway's native Round). Array is a multiple of all lane widths so
    // every element goes through the SIMD body.
    std::vector<double> in = {0.5,  -0.5,  1.5,  -1.5,  2.5,      -2.5,      3.5,      -3.5,
                              7.5,  -7.5,  0.0,  -0.0,  1e6 + 0.5, -(1e6 + 0.5), 123456.5, -123456.5};
    std::vector<double> want = {1.0, -1.0, 2.0,  -2.0,  3.0,      -3.0,      4.0,      -4.0,
                                8.0, -8.0, 0.0,  -0.0,  1e6 + 1,  -(1e6 + 1),  123457.0, -123457.0};
    auto out = simd::round(in);
    ASSERT_EQ(out.size(), in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        EXPECT_EQ(out[i], want[i]) << "round tie mismatch at i=" << i << " in=" << in[i];
        EXPECT_EQ(out[i], std::round(in[i])) << "round differs from std::round at i=" << i << " in=" << in[i];
    }
    // Signed zero must be preserved (CopySign path).
    EXPECT_TRUE(std::signbit(out[11])) << "round(-0.0) must stay -0.0";
    EXPECT_FALSE(std::signbit(out[10]));
}

TEST_F(TransformSimdPropertyTest, RoundNonFinite) {
    std::vector<double> in = {kNaN, kInf, -kInf, 0.25, -0.25, 0.75, -0.75, 100.0};
    auto out = simd::round(in);
    EXPECT_TRUE(std::isnan(out[0])) << "round(NaN) must be NaN";
    EXPECT_EQ(out[1], kInf);
    EXPECT_EQ(out[2], -kInf);
    EXPECT_EQ(out[3], 0.0);
    EXPECT_EQ(out[4], -0.0);
    EXPECT_EQ(out[5], 1.0);
    EXPECT_EQ(out[6], -1.0);
    EXPECT_EQ(out[7], 100.0);
}

// ==================== exp: tolerance + NaN re-mask regression pin ====================

namespace {

// Comparison for exp: exact class match for NaN/Inf/zero, relative tolerance
// for finite outputs (Highway contrib Exp is a few-ULP polynomial).
void checkExpClose(double in, double got, double want, size_t size, size_t i) {
    if (std::isnan(want)) {
        ASSERT_TRUE(std::isnan(got)) << "exp(" << in << ") should be NaN, got " << got << " (size=" << size
                                     << " i=" << i << ")";
        return;
    }
    if (std::isinf(want) || want == 0.0) {
        ASSERT_EQ(got, want) << "exp(" << in << ") mismatch (size=" << size << " i=" << i << ")";
        return;
    }
    ASSERT_LE(std::abs(got - want), std::abs(want) * 1e-12)
        << "exp(" << in << ") outside tolerance: simd=" << got << " ref=" << want << " (size=" << size << " i=" << i
        << ")";
}

}  // namespace

TEST_F(TransformSimdPropertyTest, ExpMatchesScalarWithTolerance) {
    std::mt19937_64 rng(20260716);
    std::uniform_real_distribution<double> dist(-700.0, 700.0);
    std::uniform_int_distribution<int> inject(0, 7);
    const double expSpecials[] = {kNaN, kInf, -kInf, 0.0, -0.0, 1.0, -1.0};
    std::uniform_int_distribution<size_t> which(0, std::size(expSpecials) - 1);

    for (size_t size : kSizes) {
        std::vector<double> values(size);
        for (auto& v : values)
            v = (inject(rng) == 0) ? expSpecials[which(rng)] : dist(rng);

        auto got = simd::exp(values);
        auto want = simd::scalar::exp(values);
        ASSERT_EQ(got.size(), want.size());
        for (size_t i = 0; i < size; ++i)
            checkExpClose(values[i], got[i], want[i], size, i);
    }
}

// The contrib-math Exp polynomial does NOT propagate NaN by itself; the kernel
// re-masks NaN lanes explicitly. Pin that behavior at every lane position so a
// dropped re-mask cannot regress silently.
TEST_F(TransformSimdPropertyTest, ExpPropagatesNaNAtEveryLanePosition) {
    constexpr size_t kN = 64;  // multiple of all lane widths -> full SIMD body
    for (size_t nanPos = 0; nanPos < kN; ++nanPos) {
        std::vector<double> values(kN, 1.5);
        values[nanPos] = kNaN;
        auto out = simd::exp(values);
        ASSERT_EQ(out.size(), kN);
        for (size_t i = 0; i < kN; ++i) {
            if (i == nanPos) {
                ASSERT_TRUE(std::isnan(out[i])) << "exp must propagate NaN at lane position " << nanPos;
            } else {
                ASSERT_LE(std::abs(out[i] - std::exp(1.5)), std::exp(1.5) * 1e-12)
                    << "NaN lane leaked into neighbor at i=" << i << " (nanPos=" << nanPos << ")";
            }
        }
    }
}

TEST_F(TransformSimdPropertyTest, ExpKnownValues) {
    std::vector<double> in = {0.0, 1.0, -1.0, 2.0, 10.0, -10.0, 700.0, -700.0};
    auto out = simd::exp(in);
    EXPECT_DOUBLE_EQ(out[0], 1.0) << "exp(0) must be exactly 1";
    EXPECT_NEAR(out[1], 2.718281828459045, 1e-12);
    EXPECT_NEAR(out[2], 0.36787944117144233, 1e-13);
    EXPECT_NEAR(out[3], 7.38905609893065, 1e-11);
    EXPECT_NEAR(out[4] / 22026.465794806718, 1.0, 1e-12);
    EXPECT_NEAR(out[5] / 4.5399929762484854e-05, 1.0, 1e-12);
    EXPECT_NEAR(out[6] / 1.0142320547350045e+304, 1.0, 1e-12);
    EXPECT_NEAR(out[7] / 9.859676543759770e-305, 1.0, 1e-12);
}

// ==================== deriv ====================

TEST_F(TransformSimdPropertyTest, DerivMatchesScalarSweep) {
    std::mt19937_64 rng(20260716);
    // Sorted (non-decreasing) timestamps as documented; occasional zero
    // increments exercise the dt == 0 -> NaN mask in the SIMD body.
    const uint64_t increments[] = {0, 1, 1000, 1000000000ULL};
    std::uniform_int_distribution<size_t> incPick(0, std::size(increments) - 1);

    for (size_t size : kSizes) {
        auto values = makeValues(size, rng);
        std::vector<uint64_t> ts(size);
        uint64_t t = 1700000000000000000ULL;
        for (size_t i = 0; i < size; ++i) {
            t += increments[incPick(rng)];
            ts[i] = t;
        }

        auto got = simd::deriv(values, ts);
        auto want = simd::scalar::deriv(values, ts);
        ASSERT_EQ(got.size(), want.size());
        if (size > 0) {
            EXPECT_TRUE(std::isnan(got[0])) << "deriv[0] must be NaN (size=" << size << ")";
        }
        for (size_t i = 0; i < size; ++i) {
            ASSERT_TRUE(nanEq(got[i], want[i]))
                << "deriv mismatch size=" << size << " i=" << i << " v=" << values[i]
                << " dt=" << (i > 0 ? ts[i] - ts[i - 1] : 0) << " simd=" << got[i] << " ref=" << want[i];
        }
    }
}

TEST_F(TransformSimdPropertyTest, DerivKnownSlope) {
    // 1 unit per second -> derivative exactly 1.0 everywhere past index 0.
    constexpr size_t kN = 33;
    std::vector<double> values(kN);
    std::vector<uint64_t> ts(kN);
    for (size_t i = 0; i < kN; ++i) {
        values[i] = static_cast<double>(i);
        ts[i] = 1000000000ULL * i;
    }
    auto out = simd::deriv(values, ts);
    EXPECT_TRUE(std::isnan(out[0]));
    for (size_t i = 1; i < kN; ++i)
        EXPECT_DOUBLE_EQ(out[i], 1.0) << "at i=" << i;
}

TEST_F(TransformSimdPropertyTest, DerivDuplicateTimestampsYieldNaN) {
    constexpr size_t kN = 16;
    std::vector<double> values(kN, 5.0);
    std::vector<uint64_t> ts(kN, 42ULL);  // all identical -> every dt == 0
    auto out = simd::deriv(values, ts);
    for (size_t i = 0; i < kN; ++i)
        EXPECT_TRUE(std::isnan(out[i])) << "dt==0 must yield NaN at i=" << i;
}

// ==================== mean/stddev ====================

TEST_F(TransformSimdPropertyTest, MeanStddevMatchesScalarWithTolerance) {
    std::mt19937_64 rng(20260716);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);
    std::uniform_int_distribution<int> inject(0, 7);

    for (size_t size : kSizes) {
        std::vector<double> values(size);
        for (auto& v : values)
            v = (inject(rng) == 0) ? kNaN : dist(rng);

        double simdMean = -1, simdStd = -1, refMean = -1, refStd = -1;
        size_t simdCount = simd::mean_stddev_skipnan(values, simdMean, simdStd);
        size_t refCount = simd::scalar::mean_stddev_skipnan(values, refMean, refStd);

        ASSERT_EQ(simdCount, refCount) << "non-NaN count mismatch size=" << size;
        if (refCount == 0) {
            EXPECT_EQ(simdMean, 0.0);
            EXPECT_EQ(simdStd, 0.0);
        } else {
            ASSERT_NEAR(simdMean, refMean, std::max(1e-9, std::abs(refMean) * 1e-9))
                << "mean mismatch size=" << size;
            ASSERT_NEAR(simdStd, refStd, std::max(1e-9, std::abs(refStd) * 1e-9)) << "stddev mismatch size=" << size;
        }
    }
}

TEST_F(TransformSimdPropertyTest, MeanStddevAllNaNReturnsZeroCount) {
    std::vector<double> values(32, kNaN);
    double mean = -1, stddev = -1;
    size_t count = simd::mean_stddev_skipnan(values, mean, stddev);
    EXPECT_EQ(count, 0u);
    EXPECT_EQ(mean, 0.0);
    EXPECT_EQ(stddev, 0.0);
}

TEST_F(TransformSimdPropertyTest, MeanStddevConstantArray) {
    // 3.25 is exactly representable; sum/mean/deviation are all exact, so both
    // paths must agree exactly: mean == 3.25, stddev == 0.
    std::vector<double> values(65, 3.25);
    values[7] = kNaN;  // one NaN skipped, count = 64
    double mean = -1, stddev = -1;
    size_t count = simd::mean_stddev_skipnan(values, mean, stddev);
    EXPECT_EQ(count, 64u);
    EXPECT_DOUBLE_EQ(mean, 3.25);
    EXPECT_DOUBLE_EQ(stddev, 0.0);
}

TEST_F(TransformSimdPropertyTest, MeanStddevKnownDistribution) {
    // {1..8} repeated: mean 4.5, population variance 5.25.
    std::vector<double> values(64);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = static_cast<double>(i % 8 + 1);
    double mean = 0, stddev = 0;
    size_t count = simd::mean_stddev_skipnan(values, mean, stddev);
    EXPECT_EQ(count, 64u);
    EXPECT_NEAR(mean, 4.5, 1e-12);
    EXPECT_NEAR(stddev, std::sqrt(5.25), 1e-12);
}
