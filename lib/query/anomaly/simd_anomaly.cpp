#include "simd_anomaly.hpp"
#include <algorithm>
#include <numeric>
#include <cstring>

#if !TSDB_ANOMALY_DISABLE_SIMD
#include <cpuid.h>
#endif

namespace tsdb {
namespace anomaly {
namespace simd {

// ==================== SIMD Detection ====================

bool isAvx2Available() {
#if TSDB_ANOMALY_DISABLE_SIMD
    return false;
#else
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
            bool avx = (ecx & (1 << 28)) != 0;
            if (avx && __get_cpuid_max(0, nullptr) >= 7) {
                __cpuid_count(7, 0, eax, ebx, ecx, edx);
                return (ebx & (1 << 5)) != 0;  // AVX2
            }
        }
        return false;
    }();
    return available;
#endif
}

bool isAvx512Available() {
#if TSDB_ANOMALY_DISABLE_SIMD
    return false;
#else
    static const bool available = []() {
        unsigned int eax, ebx, ecx, edx;
        if (__get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            bool avx512f = (ebx & (1 << 16)) != 0;
            bool avx512dq = (ebx & (1 << 17)) != 0;
            return avx512f && avx512dq;
        }
        return false;
    }();
    return available;
#endif
}

// ==================== Helper Functions ====================

#if !TSDB_ANOMALY_DISABLE_SIMD
static double hsum_avx(__m256d v) {
    __m128d vlow = _mm256_castpd256_pd128(v);
    __m128d vhigh = _mm256_extractf128_pd(v, 1);
    vlow = _mm_add_pd(vlow, vhigh);
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    return _mm_cvtsd_f64(_mm_add_sd(vlow, high64));
}
#endif

// ==================== Scalar Implementations ====================

namespace scalar {

void vectorSubtract(const double* a, const double* b, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        result[i] = a[i] - b[i];
    }
}

void vectorAdd(const double* a, const double* b, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        result[i] = a[i] + b[i];
    }
}

void vectorMultiply(const double* a, const double* b, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        result[i] = a[i] * b[i];
    }
}

void vectorScalarMultiply(const double* a, double scalar, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        result[i] = a[i] * scalar;
    }
}

void vectorFMA(const double* a, const double* b, double scalar, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        result[i] = a[i] + b[i] * scalar;
    }
}

// Kahan summation for improved precision
double vectorSum(const double* values, size_t count) {
    double sum = 0.0, c = 0.0;
    for (size_t i = 0; i < count; ++i) {
        double y = values[i] - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
    return sum;
}

double vectorMean(const double* values, size_t count) {
    if (count == 0) return 0.0;
    return vectorSum(values, count) / static_cast<double>(count);
}

double vectorSumSquaredDiff(const double* values, size_t count, double mean) {
    double result = 0.0;
    for (size_t i = 0; i < count; ++i) {
        double diff = values[i] - mean;
        result += diff * diff;
    }
    return result;
}

double vectorVariance(const double* values, size_t count, double mean) {
    if (count <= 1) return 0.0;
    return vectorSumSquaredDiff(values, count, mean) / static_cast<double>(count - 1);
}

void computeBounds(
    const double* predictions,
    const double* scale,
    double bounds,
    double* upper,
    double* lower,
    size_t count) {
    for (size_t i = 0; i < count; ++i) {
        double margin = bounds * scale[i];
        upper[i] = predictions[i] + margin;
        lower[i] = predictions[i] - margin;
    }
}

void computeAnomalyScores(
    const double* values,
    const double* upper,
    const double* lower,
    double* scores,
    size_t count) {
    for (size_t i = 0; i < count; ++i) {
        double above = std::max(0.0, values[i] - upper[i]);
        double below = std::max(0.0, lower[i] - values[i]);
        scores[i] = above + below;
    }
}

void computeTricubeWeights(const double* distances, double* weights, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        double u = distances[i];
        if (u >= 1.0) {
            weights[i] = 0.0;
        } else {
            double t = 1.0 - u * u * u;
            weights[i] = t * t * t;
        }
    }
}

void computeMovingAverage(
    const double* values,
    size_t count,
    size_t windowSize,
    double* result) {
    if (count == 0 || windowSize == 0) return;

    size_t halfWindow = windowSize / 2;

    // Use sliding window sum for O(N) instead of O(N*W)
    double windowSum = 0.0;
    size_t windowCount = 0;

    // Initialize first window
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

double weightedSum(const double* values, const double* weights, size_t count) {
    double sum = 0.0;
    for (size_t i = 0; i < count; ++i) {
        sum += values[i] * weights[i];
    }
    return sum;
}

} // namespace scalar

// ==================== Vector Operations ====================

void vectorSubtract(const double* a, const double* b, double* result, size_t count) {
#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::vectorSubtract(a, b, result, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::vectorSubtract(a, b, result, count);
        return;
    }

    size_t simd_end = count - (count % 4);
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_sub_pd(va, vb);
        _mm256_storeu_pd(&result[i], vr);
    }

    for (size_t i = simd_end; i < count; ++i) {
        result[i] = a[i] - b[i];
    }
#endif
}

void vectorAdd(const double* a, const double* b, double* result, size_t count) {
#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::vectorAdd(a, b, result, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::vectorAdd(a, b, result, count);
        return;
    }

    size_t simd_end = count - (count % 4);
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_add_pd(va, vb);
        _mm256_storeu_pd(&result[i], vr);
    }

    for (size_t i = simd_end; i < count; ++i) {
        result[i] = a[i] + b[i];
    }
#endif
}

void vectorMultiply(const double* a, const double* b, double* result, size_t count) {
#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::vectorMultiply(a, b, result, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::vectorMultiply(a, b, result, count);
        return;
    }

    size_t simd_end = count - (count % 4);
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        __m256d vr = _mm256_mul_pd(va, vb);
        _mm256_storeu_pd(&result[i], vr);
    }

    for (size_t i = simd_end; i < count; ++i) {
        result[i] = a[i] * b[i];
    }
#endif
}

void vectorScalarMultiply(const double* a, double scalar, double* result, size_t count) {
#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::vectorScalarMultiply(a, scalar, result, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::vectorScalarMultiply(a, scalar, result, count);
        return;
    }

    __m256d vs = _mm256_set1_pd(scalar);
    size_t simd_end = count - (count % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vr = _mm256_mul_pd(va, vs);
        _mm256_storeu_pd(&result[i], vr);
    }

    for (size_t i = simd_end; i < count; ++i) {
        result[i] = a[i] * scalar;
    }
#endif
}

void vectorFMA(const double* a, const double* b, double scalar, double* result, size_t count) {
#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::vectorFMA(a, b, scalar, result, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::vectorFMA(a, b, scalar, result, count);
        return;
    }

    __m256d vs = _mm256_set1_pd(scalar);
    size_t simd_end = count - (count % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d va = _mm256_loadu_pd(&a[i]);
        __m256d vb = _mm256_loadu_pd(&b[i]);
        // FMA: a + b * scalar
        __m256d vr = _mm256_fmadd_pd(vb, vs, va);
        _mm256_storeu_pd(&result[i], vr);
    }

    for (size_t i = simd_end; i < count; ++i) {
        result[i] = a[i] + b[i] * scalar;
    }
#endif
}

// ==================== Sum and Mean ====================

double vectorSum(const double* values, size_t count) {
    if (count == 0) return 0.0;

#if TSDB_ANOMALY_DISABLE_SIMD
    return scalar::vectorSum(values, count);
#else
    if (isAvx512Available() && count >= 16) {
        __m512d sum_vec = _mm512_setzero_pd();
        size_t simd_end = count - (count % 8);

        for (size_t i = 0; i < simd_end; i += 8) {
            __m512d vals = _mm512_loadu_pd(&values[i]);
            sum_vec = _mm512_add_pd(sum_vec, vals);
        }

        double sum = _mm512_reduce_add_pd(sum_vec);
        for (size_t i = simd_end; i < count; ++i) {
            sum += values[i];
        }
        return sum;
    }

    if (isAvx2Available() && count >= 8) {
        __m256d sum_vec = _mm256_setzero_pd();
        size_t simd_end = count - (count % 4);

        for (size_t i = 0; i < simd_end; i += 4) {
            __m256d vals = _mm256_loadu_pd(&values[i]);
            sum_vec = _mm256_add_pd(sum_vec, vals);
        }

        double sum = hsum_avx(sum_vec);
        for (size_t i = simd_end; i < count; ++i) {
            sum += values[i];
        }
        return sum;
    }

    return scalar::vectorSum(values, count);
#endif
}

double vectorMean(const double* values, size_t count) {
    if (count == 0) return 0.0;
    return vectorSum(values, count) / static_cast<double>(count);
}

// ==================== Variance ====================

double vectorSumSquaredDiff(const double* values, size_t count, double mean) {
    if (count == 0) return 0.0;

#if TSDB_ANOMALY_DISABLE_SIMD
    return scalar::vectorSumSquaredDiff(values, count, mean);
#else
    if (isAvx2Available() && count >= 8) {
        __m256d mean_vec = _mm256_set1_pd(mean);
        __m256d sum_sq = _mm256_setzero_pd();
        size_t simd_end = count - (count % 4);

        for (size_t i = 0; i < simd_end; i += 4) {
            __m256d vals = _mm256_loadu_pd(&values[i]);
            __m256d diff = _mm256_sub_pd(vals, mean_vec);
            __m256d sq = _mm256_mul_pd(diff, diff);
            sum_sq = _mm256_add_pd(sum_sq, sq);
        }

        double result = hsum_avx(sum_sq);
        for (size_t i = simd_end; i < count; ++i) {
            double diff = values[i] - mean;
            result += diff * diff;
        }
        return result;
    }

    return scalar::vectorSumSquaredDiff(values, count, mean);
#endif
}

double vectorVariance(const double* values, size_t count, double mean) {
    if (count <= 1) return 0.0;
    return vectorSumSquaredDiff(values, count, mean) / static_cast<double>(count - 1);
}

// ==================== Incremental Rolling Stats ====================

IncrementalRollingStats::IncrementalRollingStats(size_t windowSize)
    : windowSize_(windowSize)
    , count_(0)
    , mean_(0.0)
    , m2_(0.0)
    , buffer_(windowSize, 0.0)
    , head_(0)
    , bufferFull_(false)
    , updatesSinceRecompute_(0) {}

void IncrementalRollingStats::update(double value) {
    if (std::isnan(value)) return;

    if (bufferFull_) {
        // Remove oldest value from statistics (inverse Welford)
        double oldValue = buffer_[head_];
        double oldMean = mean_;
        mean_ = mean_ + (value - oldValue) / static_cast<double>(count_);
        // Update M2: add new contribution, remove old contribution
        m2_ = m2_ + (value - mean_) * (value - oldMean)
                  - (oldValue - mean_) * (oldValue - oldMean);
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
    if (count_ < 2) return 0.0;
    // Clamp M2 to >= 0 to prevent NaN from floating-point rounding errors
    // in the inverse Welford update (removing oldest value from window).
    return std::max(0.0, m2_) / static_cast<double>(count_ - 1);
}

double IncrementalRollingStats::stddev() const {
    return std::sqrt(variance());
}

void IncrementalRollingStats::recomputeFromBuffer() {
    if (count_ == 0) return;

    // Recompute mean using Kahan summation for precision
    double sum = 0.0, c = 0.0;
    for (size_t i = 0; i < count_; ++i) {
        double y = buffer_[i] - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
    mean_ = sum / static_cast<double>(count_);

    // Recompute M2 (sum of squared differences from mean)
    m2_ = 0.0;
    for (size_t i = 0; i < count_; ++i) {
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
    std::fill(buffer_.begin(), buffer_.end(), 0.0);
}

// ==================== Moving Average ====================

void computeMovingAverage(
    const double* values,
    size_t count,
    size_t windowSize,
    double* result) {
    // The sliding window algorithm is already O(N), so SIMD doesn't help much here
    // Use the scalar implementation which is already optimized
    scalar::computeMovingAverage(values, count, windowSize, result);
}

// ==================== Weighted Operations ====================

double weightedSum(const double* values, const double* weights, size_t count) {
    if (count == 0) return 0.0;

#if TSDB_ANOMALY_DISABLE_SIMD
    return scalar::weightedSum(values, weights, count);
#else
    if (isAvx2Available() && count >= 8) {
        __m256d sum_vec = _mm256_setzero_pd();
        size_t simd_end = count - (count % 4);

        for (size_t i = 0; i < simd_end; i += 4) {
            __m256d vals = _mm256_loadu_pd(&values[i]);
            __m256d wts = _mm256_loadu_pd(&weights[i]);
            __m256d prod = _mm256_mul_pd(vals, wts);
            sum_vec = _mm256_add_pd(sum_vec, prod);
        }

        double sum = hsum_avx(sum_vec);
        for (size_t i = simd_end; i < count; ++i) {
            sum += values[i] * weights[i];
        }
        return sum;
    }

    return scalar::weightedSum(values, weights, count);
#endif
}

double weightedMean(const double* values, const double* weights, size_t count) {
    if (count == 0) return 0.0;

    double sumW = vectorSum(weights, count);
    if (sumW < 1e-10) return 0.0;

    return weightedSum(values, weights, count) / sumW;
}

// ==================== Bounds Computation ====================

void computeBounds(
    const double* predictions,
    const double* scale,
    double bounds,
    double* upper,
    double* lower,
    size_t count) {

#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::computeBounds(predictions, scale, bounds, upper, lower, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::computeBounds(predictions, scale, bounds, upper, lower, count);
        return;
    }

    __m256d bounds_vec = _mm256_set1_pd(bounds);
    size_t simd_end = count - (count % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d pred = _mm256_loadu_pd(&predictions[i]);
        __m256d sc = _mm256_loadu_pd(&scale[i]);
        __m256d margin = _mm256_mul_pd(bounds_vec, sc);

        __m256d up = _mm256_add_pd(pred, margin);
        __m256d lo = _mm256_sub_pd(pred, margin);

        _mm256_storeu_pd(&upper[i], up);
        _mm256_storeu_pd(&lower[i], lo);
    }

    for (size_t i = simd_end; i < count; ++i) {
        double margin = bounds * scale[i];
        upper[i] = predictions[i] + margin;
        lower[i] = predictions[i] - margin;
    }
#endif
}

void computeAnomalyScores(
    const double* values,
    const double* upper,
    const double* lower,
    double* scores,
    size_t count) {

#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::computeAnomalyScores(values, upper, lower, scores, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::computeAnomalyScores(values, upper, lower, scores, count);
        return;
    }

    __m256d zero = _mm256_setzero_pd();
    size_t simd_end = count - (count % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d val = _mm256_loadu_pd(&values[i]);
        __m256d up = _mm256_loadu_pd(&upper[i]);
        __m256d lo = _mm256_loadu_pd(&lower[i]);

        // above = max(0, val - upper)
        __m256d above = _mm256_max_pd(zero, _mm256_sub_pd(val, up));
        // below = max(0, lower - val)
        __m256d below = _mm256_max_pd(zero, _mm256_sub_pd(lo, val));
        // score = above + below
        __m256d score = _mm256_add_pd(above, below);

        _mm256_storeu_pd(&scores[i], score);
    }

    for (size_t i = simd_end; i < count; ++i) {
        double above = std::max(0.0, values[i] - upper[i]);
        double below = std::max(0.0, lower[i] - values[i]);
        scores[i] = above + below;
    }
#endif
}

// ==================== LOESS Helpers ====================

void computeTricubeWeights(const double* distances, double* weights, size_t count) {
#if TSDB_ANOMALY_DISABLE_SIMD
    scalar::computeTricubeWeights(distances, weights, count);
#else
    if (!isAvx2Available() || count < 8) {
        scalar::computeTricubeWeights(distances, weights, count);
        return;
    }

    __m256d one = _mm256_set1_pd(1.0);
    __m256d zero = _mm256_setzero_pd();
    size_t simd_end = count - (count % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d u = _mm256_loadu_pd(&distances[i]);

        // u^3
        __m256d u2 = _mm256_mul_pd(u, u);
        __m256d u3 = _mm256_mul_pd(u2, u);

        // t = 1 - u^3
        __m256d t = _mm256_sub_pd(one, u3);

        // t^3
        __m256d t2 = _mm256_mul_pd(t, t);
        __m256d t3 = _mm256_mul_pd(t2, t);

        // Zero out where u >= 1
        __m256d mask = _mm256_cmp_pd(u, one, _CMP_LT_OQ);
        __m256d result = _mm256_blendv_pd(zero, t3, mask);

        _mm256_storeu_pd(&weights[i], result);
    }

    for (size_t i = simd_end; i < count; ++i) {
        double u = distances[i];
        if (u >= 1.0) {
            weights[i] = 0.0;
        } else {
            double t = 1.0 - u * u * u;
            weights[i] = t * t * t;
        }
    }
#endif
}

LinearFit weightedLinearRegression(
    const double* x,
    const double* y,
    const double* weights,
    size_t count) {

    LinearFit fit{0.0, 0.0};

    if (count == 0) return fit;

    // Compute weighted sums using SIMD where possible
    double sumW = vectorSum(weights, count);
    if (sumW < 1e-10) return fit;

    double sumWX = weightedSum(x, weights, count);
    double sumWY = weightedSum(y, weights, count);

    // Compute weighted products
    std::vector<double> wxx(count), wxy(count);

#if !TSDB_ANOMALY_DISABLE_SIMD
    if (isAvx2Available() && count >= 8) {
        size_t simd_end = count - (count % 4);

        for (size_t i = 0; i < simd_end; i += 4) {
            __m256d vx = _mm256_loadu_pd(&x[i]);
            __m256d vy = _mm256_loadu_pd(&y[i]);
            __m256d vw = _mm256_loadu_pd(&weights[i]);

            __m256d vwx = _mm256_mul_pd(vw, vx);
            __m256d vwxx = _mm256_mul_pd(vwx, vx);
            __m256d vwxy = _mm256_mul_pd(vwx, vy);

            _mm256_storeu_pd(&wxx[i], vwxx);
            _mm256_storeu_pd(&wxy[i], vwxy);
        }

        for (size_t i = simd_end; i < count; ++i) {
            wxx[i] = weights[i] * x[i] * x[i];
            wxy[i] = weights[i] * x[i] * y[i];
        }
    } else
#endif
    {
        for (size_t i = 0; i < count; ++i) {
            wxx[i] = weights[i] * x[i] * x[i];
            wxy[i] = weights[i] * x[i] * y[i];
        }
    }

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

} // namespace simd
} // namespace anomaly
} // namespace tsdb
