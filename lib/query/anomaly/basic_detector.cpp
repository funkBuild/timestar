#include "basic_detector.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace timestar {
namespace anomaly {

AnomalyOutput BasicDetector::detect(const AnomalyInput& input, const AnomalyConfig& config) {
    // Use optimized path for all cases
    return detectOptimized(input, config);
}

AnomalyOutput BasicDetector::detectOptimized(const AnomalyInput& input, const AnomalyConfig& config) {
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

        if (std::isnan(value)) {
            // NaN input: not an anomaly, just missing data
            output.upper[i] = std::numeric_limits<double>::quiet_NaN();
            output.lower[i] = std::numeric_limits<double>::quiet_NaN();
            output.scores[i] = 0.0;
            output.predictions[i] = stats.count() > 0 ? stats.mean() : 0.0;
            // Don't update stats with NaN (IncrementalRollingStats already skips NaN,
            // but we skip the rest of the loop body too)
            continue;
        }

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
        simd::computeBounds(output.predictions.data() + minDataPoints, scale.data() + minDataPoints, bounds,
                            output.upper.data() + minDataPoints, output.lower.data() + minDataPoints,
                            n - minDataPoints);

        // Use SIMD for anomaly score computation
        simd::computeAnomalyScores(input.values.data() + minDataPoints, output.upper.data() + minDataPoints,
                                   output.lower.data() + minDataPoints, output.scores.data() + minDataPoints,
                                   n - minDataPoints);
    }

    // Fix up NaN inputs: SIMD may have produced NaN scores for NaN input values.
    // NaN inputs are missing data, not anomalies -- ensure score is 0.
    for (size_t i = minDataPoints; i < n; ++i) {
        if (std::isnan(input.values[i])) {
            output.scores[i] = 0.0;
        }
    }

    // Count anomalies
    for (size_t i = minDataPoints; i < n; ++i) {
        if (output.scores[i] > 0) {
            ++output.anomalyCount;
        }
    }

    return output;
}

}  // namespace anomaly
}  // namespace timestar
