// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs. clang-format will try to
// alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "query/forecast/forecast_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "forecast_simd.hpp"

#include "hwy/highway.h"

#include <cmath>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace forecast {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Fused LOESS window pass: tricube weight generation + optional user weights
// + five weighted-regression FMA accumulators, in one pass over the window.
// x-coordinates are generated in-register (baseX + lane index) — no scratch
// buffers, unlike the historical 3-phase AVX2 pipeline this replaces.
LoessSums LoessWindowSums(const double* HWY_RESTRICT y, const double* HWY_RESTRICT userWeights, size_t len,
                          double baseX, double invMaxDist) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    const auto vOne = hn::Set(d, 1.0);
    const auto vInvMaxDist = hn::Set(d, invMaxDist);
    const auto vStep = hn::Set(d, static_cast<double>(N));

    // x for lanes 0..N-1 of the first iteration
    auto vX = hn::Add(hn::Set(d, baseX), hn::Iota(d, 0));

    auto sumW = hn::Zero(d);
    auto sumWX = hn::Zero(d);
    auto sumWY = hn::Zero(d);
    auto sumWXX = hn::Zero(d);
    auto sumWXY = hn::Zero(d);

    size_t k = 0;
    for (; k + N <= len; k += N) {
        // Tricube: (1 - u^3)^3 where u = |x| * invMaxDist < 1, else 0
        auto u = hn::Mul(hn::Abs(vX), vInvMaxDist);
        auto u3 = hn::Mul(hn::Mul(u, u), u);
        auto t = hn::Sub(vOne, u3);
        auto t3 = hn::Mul(hn::Mul(t, t), t);
        auto w = hn::IfThenElseZero(hn::Lt(u, vOne), t3);

        if (userWeights != nullptr) {
            w = hn::Mul(w, hn::LoadU(d, userWeights + k));
        }

        auto vy = hn::LoadU(d, y + k);
        auto wx = hn::Mul(w, vX);

        sumW = hn::Add(sumW, w);
        sumWX = hn::Add(sumWX, wx);
        sumWY = hn::MulAdd(w, vy, sumWY);
        sumWXX = hn::MulAdd(wx, vX, sumWXX);
        sumWXY = hn::MulAdd(wx, vy, sumWXY);

        vX = hn::Add(vX, vStep);
    }

    LoessSums sums;
    sums.w = hn::ReduceSum(d, sumW);
    sums.wx = hn::ReduceSum(d, sumWX);
    sums.wy = hn::ReduceSum(d, sumWY);
    sums.wxx = hn::ReduceSum(d, sumWXX);
    sums.wxy = hn::ReduceSum(d, sumWXY);

    // Scalar tail (identical semantics: unconditional user-weight multiply)
    for (; k < len; ++k) {
        double x = baseX + static_cast<double>(k);
        double u = std::abs(x) * invMaxDist;
        double w = 0.0;
        if (u < 1.0) {
            double t = 1.0 - u * u * u;
            w = t * t * t;
        }
        if (userWeights != nullptr) {
            w *= userWeights[k];
        }
        double yk = y[k];
        sums.w += w;
        sums.wx += w * x;
        sums.wy += w * yk;
        sums.wxx += w * x * x;
        sums.wxy += w * x * yk;
    }

    return sums;
}

// Lagged deviation dot product with 4 independent FMA accumulators.
double LaggedDeviationDot(const double* HWY_RESTRICT y, size_t n, size_t lag, double mean) {
    const size_t count = n - lag;
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto vMean = hn::Set(d, mean);

    auto acc0 = hn::Zero(d);
    auto acc1 = hn::Zero(d);
    auto acc2 = hn::Zero(d);
    auto acc3 = hn::Zero(d);

    size_t i = 0;
    for (; i + 4 * N <= count; i += 4 * N) {
        auto a0 = hn::Sub(hn::LoadU(d, y + i), vMean);
        auto b0 = hn::Sub(hn::LoadU(d, y + i + lag), vMean);
        acc0 = hn::MulAdd(a0, b0, acc0);
        auto a1 = hn::Sub(hn::LoadU(d, y + i + N), vMean);
        auto b1 = hn::Sub(hn::LoadU(d, y + i + N + lag), vMean);
        acc1 = hn::MulAdd(a1, b1, acc1);
        auto a2 = hn::Sub(hn::LoadU(d, y + i + 2 * N), vMean);
        auto b2 = hn::Sub(hn::LoadU(d, y + i + 2 * N + lag), vMean);
        acc2 = hn::MulAdd(a2, b2, acc2);
        auto a3 = hn::Sub(hn::LoadU(d, y + i + 3 * N), vMean);
        auto b3 = hn::Sub(hn::LoadU(d, y + i + 3 * N + lag), vMean);
        acc3 = hn::MulAdd(a3, b3, acc3);
    }
    acc0 = hn::Add(hn::Add(acc0, acc1), hn::Add(acc2, acc3));

    for (; i + N <= count; i += N) {
        auto a = hn::Sub(hn::LoadU(d, y + i), vMean);
        auto b = hn::Sub(hn::LoadU(d, y + i + lag), vMean);
        acc0 = hn::MulAdd(a, b, acc0);
    }

    double sum = hn::ReduceSum(d, acc0);

    for (; i < count; ++i) {
        sum += (y[i] - mean) * (y[i + lag] - mean);
    }

    return sum;
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace forecast
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE
namespace timestar {
namespace forecast {
namespace simd {

HWY_EXPORT(LoessWindowSums);
HWY_EXPORT(LaggedDeviationDot);

LoessSums loessWindowSums(const double* y, const double* userWeights, size_t len, double baseX, double invMaxDist) {
    if (len == 0) {
        return {};
    }
    return HWY_DYNAMIC_DISPATCH(LoessWindowSums)(y, userWeights, len, baseX, invMaxDist);
}

double laggedDeviationDot(const double* y, size_t n, size_t lag, double mean) {
    if (n == 0 || lag >= n) {
        return 0.0;
    }
    return HWY_DYNAMIC_DISPATCH(LaggedDeviationDot)(y, n, lag, mean);
}

}  // namespace simd
}  // namespace forecast
}  // namespace timestar
#endif  // HWY_ONCE
