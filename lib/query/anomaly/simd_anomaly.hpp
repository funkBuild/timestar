#ifndef SIMD_ANOMALY_H_INCLUDED
#define SIMD_ANOMALY_H_INCLUDED

#include <cmath>
#include <cstddef>
#include <vector>

namespace timestar {
namespace anomaly {
namespace simd {

// SIMD is always available via Highway runtime dispatch.
inline bool isAvx2Available() { return true; }
inline bool isAvx512Available() { return true; }

// ==================== Vector Operations ====================

// Element-wise vector subtraction: result[i] = a[i] - b[i]
void vectorSubtract(const double* a, const double* b, double* result, size_t count);

// Element-wise vector addition: result[i] = a[i] + b[i]
void vectorAdd(const double* a, const double* b, double* result, size_t count);

// Element-wise vector multiply: result[i] = a[i] * b[i]
void vectorMultiply(const double* a, const double* b, double* result, size_t count);

// Vector scalar multiply: result[i] = a[i] * scalar
void vectorScalarMultiply(const double* a, double scalar, double* result, size_t count);

// Fused multiply-add: result[i] = a[i] + b[i] * scalar
void vectorFMA(const double* a, const double* b, double scalar, double* result, size_t count);

// ==================== Sum and Mean ====================

// SIMD-optimized sum
double vectorSum(const double* values, size_t count);

// SIMD-optimized mean
double vectorMean(const double* values, size_t count);

// ==================== Variance and StdDev ====================

// SIMD-optimized variance (sample, using Bessel's correction: N-1 denominator)
double vectorVariance(const double* values, size_t count, double mean);

// SIMD-optimized sum of squared differences
double vectorSumSquaredDiff(const double* values, size_t count, double mean);

// ==================== Rolling Statistics ====================

// Incremental rolling statistics using Welford's algorithm
// Much more efficient than recalculating from scratch each time
class IncrementalRollingStats {
public:
    IncrementalRollingStats(size_t windowSize);

    // Add a new value and get current mean and stddev
    void update(double value);

    // Get current statistics
    double mean() const { return mean_; }
    double variance() const;
    double stddev() const;
    size_t count() const { return count_; }

    // Reset state
    void reset();

private:
    // Recompute mean and M2 from scratch using the circular buffer.
    // Called periodically to prevent floating-point drift from the
    // inverse Welford update when removing the oldest value.
    void recomputeFromBuffer();

    size_t windowSize_;
    size_t count_;
    double mean_;
    double m2_;  // Sum of squared differences from mean

    // Circular buffer for window
    std::vector<double> buffer_;
    size_t head_;
    bool bufferFull_;

    // Counter for periodic recomputation to combat inverse Welford drift
    size_t updatesSinceRecompute_;
};

// ==================== Moving Average ====================

// SIMD-optimized moving average computation
// Returns vector of moving averages for each position
void computeMovingAverage(const double* values, size_t count, size_t windowSize, double* result);

// ==================== Weighted Operations ====================

// SIMD-optimized weighted sum: sum(values[i] * weights[i])
double weightedSum(const double* values, const double* weights, size_t count);

// SIMD-optimized weighted mean
double weightedMean(const double* values, const double* weights, size_t count);

// ==================== Bounds Computation ====================

// Compute upper and lower bounds in one pass using SIMD
// upper[i] = predictions[i] + bounds * scale[i]
// lower[i] = predictions[i] - bounds * scale[i]
void computeBounds(const double* predictions,
                   const double* scale,  // stddev or MAD per point
                   double bounds,        // number of standard deviations
                   double* upper, double* lower, size_t count);

// Compute anomaly scores in one pass
// score[i] = max(0, (value - upper)) + max(0, (lower - value))
void computeAnomalyScores(const double* values, const double* upper, const double* lower, double* scores, size_t count);

// ==================== LOESS Helpers ====================

// Compute tricube weights for an array of distances
void computeTricubeWeights(const double* distances, double* weights, size_t count);

// SIMD-optimized weighted linear regression
// Returns slope and intercept
struct LinearFit {
    double intercept;
    double slope;
};

LinearFit weightedLinearRegression(const double* x, const double* y, const double* weights, size_t count);

}  // namespace simd
}  // namespace anomaly
}  // namespace timestar

#endif  // SIMD_ANOMALY_H_INCLUDED
