#pragma once

#include <cstddef>

namespace timestar {
namespace forecast {
namespace simd {

// Weighted-regression accumulator sums for one LOESS window.
struct LoessSums {
    double w = 0.0;    // sum of weights
    double wx = 0.0;   // sum of w * x
    double wy = 0.0;   // sum of w * y
    double wxx = 0.0;  // sum of w * x^2
    double wxy = 0.0;  // sum of w * x * y
};

// Fused LOESS window pass: computes tricube weights from the centered
// x-coordinate ((baseX + k) for k in [0, len)), optionally multiplies by
// per-point user weights (robustness weights; pass nullptr for none), and
// accumulates the five weighted-regression sums — all in a single pass with
// no scratch buffers.
//
// Tricube: w = (1 - u^3)^3 for u = |x| * invMaxDist < 1, else 0.
// NOTE: user weights multiply unconditionally (a zero robustness weight
// eliminates the point), matching standard LOESS robustness semantics.
LoessSums loessWindowSums(const double* y, const double* userWeights, size_t len, double baseX, double invMaxDist);

// Lagged deviation dot product: sum over i in [0, n-lag) of
// (y[i] - mean) * (y[i+lag] - mean).  The core of autocorrelation.
double laggedDeviationDot(const double* y, size_t n, size_t lag, double mean);

}  // namespace simd
}  // namespace forecast
}  // namespace timestar
