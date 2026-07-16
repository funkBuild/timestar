#include "forecast/forecast_simd.hpp"

#include <gtest/gtest.h>

#include <bit>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <vector>

namespace fsimd = timestar::forecast::simd;

// ============================================================================
// Correctness tests for the Highway-dispatched forecast kernel
// laggedDeviationDot (lib/query/forecast/forecast_simd.cpp), validated
// against a locally-written scalar reference.
//
// laggedDeviationDot(y, n, lag, mean) = sum over i in [0, n-lag) of
//   (y[i] - mean) * (y[i+lag] - mean)
//
// The SIMD kernel uses FMA and 4 independent vector accumulators, so its
// summation order (and per-term rounding, via FMA) differs from a naive
// scalar loop — the result is NOT bit-exact. The reference is computed in
// long double and the comparison tolerance is relative to the sum of
// absolute term magnitudes (sum_abs): worst-case floating-point error of
// any summation order is O(count * eps * sum_abs), so we allow
//   tol = 2e-12 * sum_abs + 1e-300
// (count <= 4097 here gives a worst-case bound near 9e-13 * sum_abs; the
// tiny absolute floor covers denormal-range accumulations).
// ============================================================================

namespace {

// High-precision scalar reference; also reports sum of |term| for tolerance.
long double refLaggedDeviationDot(const double* y, size_t n, size_t lag, double mean, long double& sumAbsOut) {
    long double sum = 0.0L;
    long double sumAbs = 0.0L;
    if (n == 0 || lag >= n) {
        sumAbsOut = 0.0L;
        return 0.0L;
    }
    const long double m = mean;
    for (size_t i = 0; i + lag < n; ++i) {
        const long double term = (static_cast<long double>(y[i]) - m) * (static_cast<long double>(y[i + lag]) - m);
        sum += term;
        sumAbs += std::fabs(term);
    }
    sumAbsOut = sumAbs;
    return sum;
}

void checkLaggedDot(const std::vector<double>& y, size_t lag, double mean) {
    const size_t n = y.size();
    const double simd = fsimd::laggedDeviationDot(y.data(), n, lag, mean);
    long double sumAbs = 0.0L;
    const long double ref = refLaggedDeviationDot(y.data(), n, lag, mean, sumAbs);

    const double tol = 2e-12 * static_cast<double>(sumAbs) + 1e-300;
    EXPECT_NEAR(simd, static_cast<double>(ref), tol)
        << "SIMD/scalar mismatch: n=" << n << " lag=" << lag << " mean=" << mean << " simd=" << simd
        << " ref=" << static_cast<double>(ref) << " sumAbs=" << static_cast<double>(sumAbs);
}

std::vector<double> randomSeries(std::mt19937& rng, size_t n, double lo, double hi) {
    std::uniform_real_distribution<double> dist(lo, hi);
    std::vector<double> y(n);
    for (auto& v : y)
        v = dist(rng);
    return y;
}

// Effective term counts covering: empty, single, lane-1 / lane / lane+1 for
// every possible double lane width (2, 4, 8), 4xN unroll boundaries
// (8/16/32), and large non-multiples of the lane count.
const std::vector<size_t> kCounts = {0,  1,  2,  3,  4,  5,  7,  8,    9,   15,
                                     16, 17, 31, 32, 33, 63, 64, 1000, 4097};

}  // namespace

class ForecastSimdCorrectnessTest : public ::testing::Test {};

TEST_F(ForecastSimdCorrectnessTest, EdgeCasesReturnZero) {
    std::vector<double> y = {1.0, 2.0, 3.0};
    // Documented API semantics: n == 0 or lag >= n returns 0.0 exactly.
    EXPECT_EQ(fsimd::laggedDeviationDot(nullptr, 0, 0, 5.0), 0.0);
    EXPECT_EQ(fsimd::laggedDeviationDot(y.data(), y.size(), 3, 1.0), 0.0);
    EXPECT_EQ(fsimd::laggedDeviationDot(y.data(), y.size(), 100, 1.0), 0.0);
}

TEST_F(ForecastSimdCorrectnessTest, SizeSweepMatchesScalarRef) {
    std::mt19937 rng(20260716);
    for (size_t count : kCounts) {
        for (size_t lag : {size_t(0), size_t(1), size_t(7)}) {
            const size_t n = count + lag;  // n - lag == count effective terms
            if (n == 0)
                continue;
            auto y = randomSeries(rng, n, -100.0, 100.0);
            checkLaggedDot(y, lag, 3.25);
        }
    }
}

TEST_F(ForecastSimdCorrectnessTest, LagZeroIsSumOfSquaredDeviations) {
    // lag == 0 makes the kernel a variance-style self dot product; the
    // reference must agree and the result must be non-negative.
    std::mt19937 rng(97531);
    auto y = randomSeries(rng, 4097, -50.0, 50.0);
    checkLaggedDot(y, 0, 1.5);
    EXPECT_GE(fsimd::laggedDeviationDot(y.data(), y.size(), 0, 1.5), 0.0);
}

TEST_F(ForecastSimdCorrectnessTest, SpecialMagnitudes) {
    std::mt19937 rng(8675309);

    // Denormals: products underflow; both paths must agree within tolerance.
    {
        std::vector<double> y(1000);
        std::uniform_real_distribution<double> dist(-1.0, 1.0);
        for (auto& v : y)
            v = dist(rng) * std::numeric_limits<double>::denorm_min() * 1e4;
        checkLaggedDot(y, 3, 0.0);
        checkLaggedDot(y, 0, std::numeric_limits<double>::min());
    }

    // Large exponents (products up to ~1e200; sums stay finite).
    {
        std::vector<double> y(1000);
        std::uniform_real_distribution<double> dist(-1e100, 1e100);
        for (auto& v : y)
            v = dist(rng);
        checkLaggedDot(y, 1, 0.0);
        checkLaggedDot(y, 12, 1e99);
    }

    // Heavy cancellation: values clustered near a huge mean.
    {
        std::vector<double> y(4097);
        std::uniform_real_distribution<double> dist(-1.0, 1.0);
        const double mean = 1e8;
        for (auto& v : y)
            v = mean + dist(rng);
        checkLaggedDot(y, 5, mean);
    }

    // All-negative series with negative mean, plus signed zeros.
    {
        std::vector<double> y(129);
        std::uniform_real_distribution<double> dist(-1000.0, -1.0);
        for (auto& v : y)
            v = dist(rng);
        y[7] = -0.0;
        y[64] = 0.0;
        checkLaggedDot(y, 2, -500.0);
    }

    // Constant series equal to the mean: every term is exactly zero.
    {
        std::vector<double> y(1000, 42.5);
        const double result = fsimd::laggedDeviationDot(y.data(), y.size(), 4, 42.5);
        EXPECT_EQ(result, 0.0);
    }
}

TEST_F(ForecastSimdCorrectnessTest, RandomizedSweep) {
    std::mt19937 rng(123456789);
    std::uniform_int_distribution<size_t> nDist(0, 600);
    std::uniform_real_distribution<double> meanDist(-1e4, 1e4);
    std::uniform_real_distribution<double> scaleDist(-6.0, 6.0);

    for (int iter = 0; iter < 100; ++iter) {
        const size_t n = nDist(rng);
        std::uniform_int_distribution<size_t> lagDist(0, n + 2);
        const size_t lag = lagDist(rng);
        const double scale = std::pow(10.0, scaleDist(rng));
        auto y = randomSeries(rng, n, -scale, scale);
        checkLaggedDot(y, lag, meanDist(rng) * scale * 1e-4);
    }
}

TEST_F(ForecastSimdCorrectnessTest, DeterministicAcrossCalls) {
    // Highway dynamic dispatch must resolve to the same target every call:
    // repeated invocations on identical input must be bit-identical.
    std::mt19937 rng(555);
    auto y = randomSeries(rng, 4097, -1000.0, 1000.0);
    const double first = fsimd::laggedDeviationDot(y.data(), y.size(), 24, 12.5);
    for (int i = 0; i < 8; ++i) {
        const double again = fsimd::laggedDeviationDot(y.data(), y.size(), 24, 12.5);
        ASSERT_EQ(std::bit_cast<uint64_t>(first), std::bit_cast<uint64_t>(again))
            << "Non-deterministic result on call " << i + 2;
    }
}
