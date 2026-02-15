#include "basic_detector.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>

namespace tsdb {
namespace anomaly {

AnomalyOutput BasicDetector::detect(
    const AnomalyInput& input,
    const AnomalyConfig& config
) {
    // Use optimized path for all cases
    return detectOptimized(input, config);
}

AnomalyOutput BasicDetector::detectOptimized(
    const AnomalyInput& input,
    const AnomalyConfig& config
) {
    AnomalyOutput output;

    if (input.empty()) {
        return output;
    }

    size_t n = input.size();
    output.upper.resize(n);
    output.lower.resize(n);
    output.scores.resize(n);
    output.predictions.resize(n);

    size_t windowSize = config.windowSize;
    double bounds = config.bounds;
    size_t minDataPoints = config.minDataPoints;

    // Use incremental rolling stats for O(N) complexity
    simd::IncrementalRollingStats stats(windowSize);

    // Pre-allocate scale vector for SIMD bounds computation
    std::vector<double> scale(n, 1.0);

    // First pass: compute rolling stats incrementally
    for (size_t i = 0; i < n; ++i) {
        double value = input.values[i];

        if (i < minDataPoints) {
            // Not enough data yet - use wide bounds
            output.upper[i] = std::numeric_limits<double>::infinity();
            output.lower[i] = -std::numeric_limits<double>::infinity();
            output.scores[i] = 0.0;
            output.predictions[i] = value;

            // Still update stats for future points
            stats.update(value);
            continue;
        }

        // Get current statistics before updating with this point
        double currentMean = stats.mean();
        double currentStddev = stats.stddev();

        // Use the mean as prediction
        output.predictions[i] = currentMean;

        // Compute effective stddev
        double effectiveStddev = std::max(currentStddev, std::abs(currentMean) * 0.01);
        if (effectiveStddev < 1e-10) {
            effectiveStddev = 1.0;  // Fallback for zero/constant series
        }
        scale[i] = effectiveStddev;

        // Now update stats with current value for next iteration
        stats.update(value);
    }

    // Use SIMD for bounds computation on the portion with valid stats
    if (minDataPoints < n) {
        simd::computeBounds(
            output.predictions.data() + minDataPoints,
            scale.data() + minDataPoints,
            bounds,
            output.upper.data() + minDataPoints,
            output.lower.data() + minDataPoints,
            n - minDataPoints
        );

        // Use SIMD for anomaly score computation
        simd::computeAnomalyScores(
            input.values.data() + minDataPoints,
            output.upper.data() + minDataPoints,
            output.lower.data() + minDataPoints,
            output.scores.data() + minDataPoints,
            n - minDataPoints
        );
    }

    // Count anomalies
    for (size_t i = minDataPoints; i < n; ++i) {
        if (output.scores[i] > 0) {
            ++output.anomalyCount;
        }
    }

    return output;
}

// Legacy implementation kept for reference/validation
BasicDetector::RollingStats BasicDetector::computeRollingStats(
    const std::vector<double>& values,
    size_t endIdx,
    size_t windowSize
) {
    RollingStats stats{0.0, 0.0, 0.0, 0.0, 0.0};

    if (endIdx == 0 || windowSize == 0) {
        return stats;
    }

    size_t start = (endIdx > windowSize) ? endIdx - windowSize : 0;

    // Collect non-NaN values in window
    std::vector<double> windowValues;
    windowValues.reserve(endIdx - start);

    for (size_t i = start; i < endIdx; ++i) {
        if (!std::isnan(values[i])) {
            windowValues.push_back(values[i]);
        }
    }

    if (windowValues.empty()) {
        return stats;
    }

    // Use SIMD for mean computation
    stats.mean = simd::vectorMean(windowValues.data(), windowValues.size());

    // Use SIMD for variance computation
    if (windowValues.size() > 1) {
        double variance = simd::vectorVariance(windowValues.data(), windowValues.size(), stats.mean);
        stats.stddev = std::sqrt(variance);
    }

    // Sort for percentiles (still needed for median/quartiles)
    std::sort(windowValues.begin(), windowValues.end());
    size_t n = windowValues.size();

    // Median
    if (n % 2 == 0) {
        stats.median = (windowValues[n/2 - 1] + windowValues[n/2]) / 2.0;
    } else {
        stats.median = windowValues[n/2];
    }

    // Quartiles
    size_t q1Idx = n / 4;
    size_t q3Idx = (3 * n) / 4;
    stats.q1 = windowValues[q1Idx];
    stats.q3 = windowValues[std::min(q3Idx, n - 1)];

    return stats;
}

} // namespace anomaly
} // namespace tsdb
