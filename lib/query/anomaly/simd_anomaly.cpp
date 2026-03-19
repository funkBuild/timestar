// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "query/anomaly/simd_anomaly.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "simd_anomaly.hpp"

#include "hwy/highway.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <numeric>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace anomaly {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// --- Element-wise operations ---

void VectorSubtractKernel(const double* HWY_RESTRICT a, const double* HWY_RESTRICT b, double* HWY_RESTRICT result,
                          size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto va = hn::LoadU(d, &a[i]);
        auto vb = hn::LoadU(d, &b[i]);
        hn::StoreU(hn::Sub(va, vb), d, &result[i]);
    }
    for (; i < count; ++i) {
        result[i] = a[i] - b[i];
    }
}

void VectorAddKernel(const double* HWY_RESTRICT a, const double* HWY_RESTRICT b, double* HWY_RESTRICT result,
                     size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto va = hn::LoadU(d, &a[i]);
        auto vb = hn::LoadU(d, &b[i]);
        hn::StoreU(hn::Add(va, vb), d, &result[i]);
    }
    for (; i < count; ++i) {
        result[i] = a[i] + b[i];
    }
}

void VectorMultiplyKernel(const double* HWY_RESTRICT a, const double* HWY_RESTRICT b, double* HWY_RESTRICT result,
                          size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto va = hn::LoadU(d, &a[i]);
        auto vb = hn::LoadU(d, &b[i]);
        hn::StoreU(hn::Mul(va, vb), d, &result[i]);
    }
    for (; i < count; ++i) {
        result[i] = a[i] * b[i];
    }
}

void VectorScalarMultiplyKernel(const double* HWY_RESTRICT a, double scalar, double* HWY_RESTRICT result,
                                size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto vs = hn::Set(d, scalar);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto va = hn::LoadU(d, &a[i]);
        hn::StoreU(hn::Mul(va, vs), d, &result[i]);
    }
    for (; i < count; ++i) {
        result[i] = a[i] * scalar;
    }
}

// result[i] = a[i] + b[i] * scalar  (fused multiply-add)
void VectorFMAKernel(const double* HWY_RESTRICT a, const double* HWY_RESTRICT b, double scalar,
                     double* HWY_RESTRICT result, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto vs = hn::Set(d, scalar);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto va = hn::LoadU(d, &a[i]);
        auto vb = hn::LoadU(d, &b[i]);
        // MulAdd(b, s, a) = b * s + a
        hn::StoreU(hn::MulAdd(vb, vs, va), d, &result[i]);
    }
    for (; i < count; ++i) {
        result[i] = a[i] + b[i] * scalar;
    }
}

// --- Reduction operations ---

double VectorSumKernel(const double* HWY_RESTRICT values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto acc = hn::Zero(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &values[i]);
        acc = hn::Add(acc, v);
    }

    double sum = hn::ReduceSum(d, acc);
    for (; i < count; ++i) {
        sum += values[i];
    }
    return sum;
}

double VectorSumSquaredDiffKernel(const double* HWY_RESTRICT values, size_t count, double mean) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto vmean = hn::Set(d, mean);

    auto acc = hn::Zero(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &values[i]);
        auto diff = hn::Sub(v, vmean);
        // MulAdd(diff, diff, acc) = diff * diff + acc
        acc = hn::MulAdd(diff, diff, acc);
    }

    double result = hn::ReduceSum(d, acc);
    for (; i < count; ++i) {
        double diff = values[i] - mean;
        result += diff * diff;
    }
    return result;
}

double WeightedSumKernel(const double* HWY_RESTRICT values, const double* HWY_RESTRICT weights, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto acc = hn::Zero(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &values[i]);
        auto w = hn::LoadU(d, &weights[i]);
        // MulAdd(v, w, acc) = v * w + acc
        acc = hn::MulAdd(v, w, acc);
    }

    double sum = hn::ReduceSum(d, acc);
    for (; i < count; ++i) {
        sum += values[i] * weights[i];
    }
    return sum;
}

// --- Bounds and scoring ---

// upper[i] = predictions[i] + bounds * scale[i]
// lower[i] = predictions[i] - bounds * scale[i]
// Uses separate Mul + Add/Sub (NOT MulAdd) to match the non-FMA semantics
// of the original implementation and existing tests.
void ComputeBoundsKernel(const double* HWY_RESTRICT predictions, const double* HWY_RESTRICT scale, double bounds,
                         double* HWY_RESTRICT upper, double* HWY_RESTRICT lower, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto vbounds = hn::Set(d, bounds);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto pred = hn::LoadU(d, &predictions[i]);
        auto sc = hn::LoadU(d, &scale[i]);
        auto margin = hn::Mul(vbounds, sc);
        hn::StoreU(hn::Add(pred, margin), d, &upper[i]);
        hn::StoreU(hn::Sub(pred, margin), d, &lower[i]);
    }
    for (; i < count; ++i) {
        double margin = bounds * scale[i];
        upper[i] = predictions[i] + margin;
        lower[i] = predictions[i] - margin;
    }
}

// score[i] = max(0, val - upper) + max(0, lower - val)
void ComputeAnomalyScoresKernel(const double* HWY_RESTRICT values, const double* HWY_RESTRICT upper,
                                const double* HWY_RESTRICT lower, double* HWY_RESTRICT scores, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto zero = hn::Zero(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto val = hn::LoadU(d, &values[i]);
        auto up = hn::LoadU(d, &upper[i]);
        auto lo = hn::LoadU(d, &lower[i]);

        auto above = hn::Max(zero, hn::Sub(val, up));
        auto below = hn::Max(zero, hn::Sub(lo, val));
        hn::StoreU(hn::Add(above, below), d, &scores[i]);
    }
    for (; i < count; ++i) {
        double above = std::max(0.0, values[i] - upper[i]);
        double below = std::max(0.0, lower[i] - values[i]);
        scores[i] = above + below;
    }

    // Fixup: NaN input values produce NaN scores; clamp to 0
    for (size_t i = 0; i < count; ++i) {
        if (std::isnan(scores[i])) scores[i] = 0.0;
    }
}

// --- LOESS: tricube weights ---
// weight = (1 - |d|^3)^3 where |d| < 1, else 0
void ComputeTricubeWeightsKernel(const double* HWY_RESTRICT distances, double* HWY_RESTRICT weights, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto one = hn::Set(d, 1.0);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto dist = hn::LoadU(d, &distances[i]);
        auto u = hn::Abs(dist);

        // u^3 = u * u * u
        auto u2 = hn::Mul(u, u);
        auto u3 = hn::Mul(u2, u);

        // t = 1 - u^3
        auto t = hn::Sub(one, u3);

        // t^3 = t * t * t
        auto t2 = hn::Mul(t, t);
        auto t3 = hn::Mul(t2, t);

        // Zero out where u >= 1.0
        auto mask = hn::Lt(u, one);
        hn::StoreU(hn::IfThenElseZero(mask, t3), d, &weights[i]);
    }
    for (; i < count; ++i) {
        double u = std::abs(distances[i]);
        if (u >= 1.0) {
            weights[i] = 0.0;
        } else {
            double t = 1.0 - u * u * u;
            weights[i] = t * t * t;
        }
    }
}

// --- Weighted linear regression helper: compute wxx and wxy arrays ---
void WeightedProductsKernel(const double* HWY_RESTRICT x, const double* HWY_RESTRICT y,
                            const double* HWY_RESTRICT weights, double* HWY_RESTRICT wxx, double* HWY_RESTRICT wxy,
                            size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vx = hn::LoadU(d, &x[i]);
        auto vy = hn::LoadU(d, &y[i]);
        auto vw = hn::LoadU(d, &weights[i]);

        auto vwx = hn::Mul(vw, vx);
        hn::StoreU(hn::Mul(vwx, vx), d, &wxx[i]);
        hn::StoreU(hn::Mul(vwx, vy), d, &wxy[i]);
    }
    for (; i < count; ++i) {
        wxx[i] = weights[i] * x[i] * x[i];
        wxy[i] = weights[i] * x[i] * y[i];
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace anomaly
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE

namespace timestar {
namespace anomaly {
namespace simd {

// HWY_EXPORT declarations
HWY_EXPORT(VectorSubtractKernel);
HWY_EXPORT(VectorAddKernel);
HWY_EXPORT(VectorMultiplyKernel);
HWY_EXPORT(VectorScalarMultiplyKernel);
HWY_EXPORT(VectorFMAKernel);
HWY_EXPORT(VectorSumKernel);
HWY_EXPORT(VectorSumSquaredDiffKernel);
HWY_EXPORT(WeightedSumKernel);
HWY_EXPORT(ComputeBoundsKernel);
HWY_EXPORT(ComputeAnomalyScoresKernel);
HWY_EXPORT(ComputeTricubeWeightsKernel);
HWY_EXPORT(WeightedProductsKernel);

// ==================== Vector Operations ====================

void vectorSubtract(const double* a, const double* b, double* result, size_t count) {
    HWY_DYNAMIC_DISPATCH(VectorSubtractKernel)(a, b, result, count);
}

void vectorAdd(const double* a, const double* b, double* result, size_t count) {
    HWY_DYNAMIC_DISPATCH(VectorAddKernel)(a, b, result, count);
}

void vectorMultiply(const double* a, const double* b, double* result, size_t count) {
    HWY_DYNAMIC_DISPATCH(VectorMultiplyKernel)(a, b, result, count);
}

void vectorScalarMultiply(const double* a, double scalar, double* result, size_t count) {
    HWY_DYNAMIC_DISPATCH(VectorScalarMultiplyKernel)(a, scalar, result, count);
}

void vectorFMA(const double* a, const double* b, double scalar, double* result, size_t count) {
    HWY_DYNAMIC_DISPATCH(VectorFMAKernel)(a, b, scalar, result, count);
}

// ==================== Sum and Mean ====================

double vectorSum(const double* values, size_t count) {
    if (count == 0)
        return 0.0;
    return HWY_DYNAMIC_DISPATCH(VectorSumKernel)(values, count);
}

double vectorMean(const double* values, size_t count) {
    if (count == 0)
        return 0.0;
    return vectorSum(values, count) / static_cast<double>(count);
}

// ==================== Variance ====================

double vectorSumSquaredDiff(const double* values, size_t count, double mean) {
    if (count == 0)
        return 0.0;
    return HWY_DYNAMIC_DISPATCH(VectorSumSquaredDiffKernel)(values, count, mean);
}

double vectorVariance(const double* values, size_t count, double mean) {
    if (count <= 1)
        return 0.0;
    return vectorSumSquaredDiff(values, count, mean) / static_cast<double>(count - 1);
}

// ==================== Incremental Rolling Stats ====================

IncrementalRollingStats::IncrementalRollingStats(size_t windowSize)
    : windowSize_(std::max(windowSize, size_t(1))),
      count_(0),
      mean_(0.0),
      m2_(0.0),
      buffer_(windowSize_, std::numeric_limits<double>::quiet_NaN()),
      head_(0),
      bufferFull_(false),
      updatesSinceRecompute_(0) {}

void IncrementalRollingStats::update(double value) {
    if (std::isnan(value))
        return;

    if (bufferFull_) {
        double oldValue = buffer_[head_];
        if (std::isnan(oldValue)) {
            // Old slot was NaN (unused). Add value using Welford (no removal).
            ++count_;
            double delta = value - mean_;
            mean_ += delta / static_cast<double>(count_);
            double delta2 = value - mean_;
            m2_ += delta * delta2;
        } else {
            // Remove oldest value, add new (inverse Welford)
            double oldMean = mean_;
            mean_ = mean_ + (value - oldValue) / static_cast<double>(count_);
            m2_ = m2_ + (value - mean_) * (value - oldMean) - (oldValue - mean_) * (oldValue - oldMean);
        }
    } else {
        // Welford's online algorithm
        ++count_;
        double delta = value - mean_;
        mean_ += delta / static_cast<double>(count_);
        double delta2 = value - mean_;
        m2_ += delta * delta2;
    }

    buffer_[head_] = value;
    head_ = (head_ + 1) % windowSize_;

    if (!bufferFull_ && head_ == 0) {
        bufferFull_ = true;
    }

    // Periodic recomputation to combat floating-point drift from
    // inverse Welford updates. Every windowSize updates after the
    // buffer is full, recompute mean and M2 from scratch.
    ++updatesSinceRecompute_;
    if (bufferFull_ && updatesSinceRecompute_ >= windowSize_) {
        recomputeFromBuffer();
        updatesSinceRecompute_ = 0;
    }
}

double IncrementalRollingStats::variance() const {
    if (count_ < 2)
        return 0.0;
    // Clamp M2 to >= 0 to prevent NaN from floating-point rounding errors
    // in the inverse Welford update (removing oldest value from window).
    return std::max(0.0, m2_) / static_cast<double>(count_ - 1);
}

double IncrementalRollingStats::stddev() const {
    return std::sqrt(variance());
}

void IncrementalRollingStats::recomputeFromBuffer() {
    // Iterate the full buffer, skip NaN slots (gaps from init or removed values).
    // This correctly handles the circular buffer regardless of head_ position.
    double sum = 0.0, c = 0.0;
    size_t validCount = 0;
    for (size_t i = 0; i < windowSize_; ++i) {
        if (std::isnan(buffer_[i])) continue;
        double y = buffer_[i] - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
        ++validCount;
    }
    count_ = validCount;
    if (count_ == 0) {
        mean_ = 0.0;
        m2_ = 0.0;
        return;
    }
    mean_ = sum / static_cast<double>(count_);

    m2_ = 0.0;
    for (size_t i = 0; i < windowSize_; ++i) {
        if (std::isnan(buffer_[i])) continue;
        double diff = buffer_[i] - mean_;
        m2_ += diff * diff;
    }
}

void IncrementalRollingStats::reset() {
    count_ = 0;
    mean_ = 0.0;
    m2_ = 0.0;
    head_ = 0;
    bufferFull_ = false;
    updatesSinceRecompute_ = 0;
    std::fill(buffer_.begin(), buffer_.end(), std::numeric_limits<double>::quiet_NaN());
}

// ==================== Moving Average ====================

void computeMovingAverage(const double* values, size_t count, size_t windowSize, double* result) {
    // The sliding window algorithm is already O(N), inherently serial.
    if (count == 0 || windowSize == 0)
        return;

    size_t halfWindow = windowSize / 2;

    // Use sliding window sum for O(N) instead of O(N*W)
    double windowSum = 0.0;
    size_t windowCount = 0;

    // Initialize first window [0, halfWindow] inclusive — the right half of the
    // centered window around position 0 (the left half is clamped to 0).
    size_t initEnd = std::min(halfWindow + 1, count);
    for (size_t i = 0; i < initEnd; ++i) {
        if (!std::isnan(values[i])) {
            windowSum += values[i];
            ++windowCount;
        }
    }

    for (size_t i = 0; i < count; ++i) {
        // Add new element entering window
        size_t addIdx = i + halfWindow + 1;
        if (addIdx < count && !std::isnan(values[addIdx])) {
            windowSum += values[addIdx];
            ++windowCount;
        }

        // Remove element leaving window
        if (i > halfWindow) {
            size_t removeIdx = i - halfWindow - 1;
            if (!std::isnan(values[removeIdx])) {
                windowSum -= values[removeIdx];
                --windowCount;
            }
        }

        result[i] = (windowCount > 0) ? windowSum / static_cast<double>(windowCount) : 0.0;
    }
}

// ==================== Weighted Operations ====================

double weightedSum(const double* values, const double* weights, size_t count) {
    if (count == 0)
        return 0.0;
    return HWY_DYNAMIC_DISPATCH(WeightedSumKernel)(values, weights, count);
}

double weightedMean(const double* values, const double* weights, size_t count) {
    if (count == 0)
        return 0.0;

    double sumW = vectorSum(weights, count);
    if (sumW < 1e-10)
        return 0.0;

    return weightedSum(values, weights, count) / sumW;
}

// ==================== Bounds Computation ====================

void computeBounds(const double* predictions, const double* scale, double bounds, double* upper, double* lower,
                   size_t count) {
    HWY_DYNAMIC_DISPATCH(ComputeBoundsKernel)(predictions, scale, bounds, upper, lower, count);
}

void computeAnomalyScores(const double* values, const double* upper, const double* lower, double* scores,
                          size_t count) {
    HWY_DYNAMIC_DISPATCH(ComputeAnomalyScoresKernel)(values, upper, lower, scores, count);
}

// ==================== LOESS Helpers ====================

void computeTricubeWeights(const double* distances, double* weights, size_t count) {
    HWY_DYNAMIC_DISPATCH(ComputeTricubeWeightsKernel)(distances, weights, count);
}

LinearFit weightedLinearRegression(const double* x, const double* y, const double* weights, size_t count) {
    LinearFit fit{0.0, 0.0};

    if (count == 0)
        return fit;

    // Compute weighted sums using SIMD where possible
    double sumW = vectorSum(weights, count);
    if (sumW < 1e-10)
        return fit;

    double sumWX = weightedSum(x, weights, count);
    double sumWY = weightedSum(y, weights, count);

    // Compute weighted products via Highway dispatch
    std::vector<double> wxx(count), wxy(count);
    HWY_DYNAMIC_DISPATCH(WeightedProductsKernel)(x, y, weights, wxx.data(), wxy.data(), count);

    double sumWXX = vectorSum(wxx.data(), count);
    double sumWXY = vectorSum(wxy.data(), count);

    // Solve weighted least squares
    double det = sumW * sumWXX - sumWX * sumWX;

    if (std::abs(det) < 1e-10) {
        // Singular - return weighted mean as intercept
        fit.intercept = sumWY / sumW;
        fit.slope = 0.0;
    } else {
        fit.intercept = (sumWY * sumWXX - sumWX * sumWXY) / det;
        fit.slope = (sumW * sumWXY - sumWX * sumWY) / det;
    }

    return fit;
}

}  // namespace simd
}  // namespace anomaly
}  // namespace timestar

#endif  // HWY_ONCE
