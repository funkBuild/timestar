#include "agile_detector.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>

namespace timestar {
namespace anomaly {

AgileDetector::HoltWintersState AgileDetector::initializeState(
    const std::vector<double>& values,
    size_t seasonalPeriod
) {
    HoltWintersState state;

    // Initialize level as mean of first period using SIMD
    size_t initSize = std::min(seasonalPeriod, values.size());
    if (initSize == 0) initSize = std::min(size_t(30), values.size());

    // Collect non-NaN values for initial mean
    std::vector<double> validValues;
    validValues.reserve(initSize);
    for (size_t i = 0; i < initSize; ++i) {
        if (!std::isnan(values[i])) {
            validValues.push_back(values[i]);
        }
    }

    state.level = validValues.empty() ? 0.0 :
                  simd::vectorMean(validValues.data(), validValues.size());

    // Initialize trend as zero (no initial trend assumption)
    state.trend = 0.0;

    // Initialize seasonal factors
    state.seasonal.resize(seasonalPeriod > 0 ? seasonalPeriod : 1, 0.0);

    if (seasonalPeriod > 0 && values.size() >= seasonalPeriod) {
        // Compute seasonal deviations from level
        for (size_t i = 0; i < seasonalPeriod && i < values.size(); ++i) {
            if (!std::isnan(values[i])) {
                state.seasonal[i] = values[i] - state.level;
            }
        }

        // Center the seasonal factors using SIMD
        double seasonalMean = simd::vectorMean(state.seasonal.data(), seasonalPeriod);
        for (double& s : state.seasonal) {
            s -= seasonalMean;
        }
    }

    return state;
}

double AgileDetector::predictAndUpdate(
    HoltWintersState& state,
    double actualValue,
    size_t seasonalIndex,
    size_t seasonalPeriod
) {
    // Predict based on current state
    double seasonal = (seasonalPeriod > 0) ? state.seasonal[seasonalIndex] : 0.0;
    double prediction = state.level + state.trend + seasonal;

    if (!std::isnan(actualValue)) {
        // Update state (Holt-Winters equations)
        double prevLevel = state.level;

        // Update level
        state.level = ALPHA * (actualValue - seasonal) +
                      (1 - ALPHA) * (prevLevel + state.trend);

        // Update trend
        state.trend = BETA * (state.level - prevLevel) +
                      (1 - BETA) * state.trend;

        // Update seasonal
        if (seasonalPeriod > 0) {
            state.seasonal[seasonalIndex] =
                GAMMA * (actualValue - state.level) +
                (1 - GAMMA) * seasonal;
        }
    }

    return prediction;
}

double AgileDetector::computeErrorStdDev(
    const std::vector<double>& errors,
    size_t windowSize
) {
    if (errors.empty()) return 1.0;

    size_t start = (errors.size() > windowSize) ? errors.size() - windowSize : 0;
    size_t end = errors.size();

    // Collect finite values from the window (skip NaN entries from missing data)
    double sum = 0.0;
    size_t count = 0;
    for (size_t i = start; i < end; ++i) {
        if (std::isfinite(errors[i])) {
            sum += errors[i];
            ++count;
        }
    }
    if (count < 2) return 1.0;

    double mean = sum / static_cast<double>(count);

    double sumSqDiff = 0.0;
    for (size_t i = start; i < end; ++i) {
        if (std::isfinite(errors[i])) {
            double d = errors[i] - mean;
            sumSqDiff += d * d;
        }
    }
    double variance = sumSqDiff / static_cast<double>(count - 1);

    return (variance > 0) ? std::sqrt(variance) : 1.0;
}

AnomalyOutput AgileDetector::detect(
    const AnomalyInput& input,
    const AnomalyConfig& config
) {
    AnomalyOutput output;

    if (input.empty()) {
        return output;
    }

    size_t n = input.size();

    // Determine seasonal period
    size_t seasonalPeriod = seasonalityToPeriod(
        config.seasonality,
        estimateInterval(input.timestamps)
    );

    // Initialize output vectors
    output.upper.resize(n);
    output.lower.resize(n);
    output.scores.resize(n);
    output.predictions.resize(n);

    // If data length is less than the seasonal period, fall back to
    // non-seasonal detection (effectively basic detector behavior).
    // The Holt-Winters algorithm with seasonality requires at least one full
    // seasonal cycle to initialize the seasonal component meaningfully.
    if (seasonalPeriod > 0 && n < seasonalPeriod) {
        seasonalPeriod = 0;  // Disable seasonality for short data
    }

    // Initialize Holt-Winters state
    HoltWintersState state = initializeState(input.values, seasonalPeriod);

    // Track prediction errors for adaptive bounds
    std::vector<double> errors;
    errors.reserve(n);

    // Pre-allocate scale vector for SIMD bounds computation
    std::vector<double> scale(n, 1.0);

    // Minimum data points before reliable bounds.
    // Use at most 1/4 of the seasonal period to avoid suppressing anomaly
    // detection for the majority of the first season (e.g. 72 vs 288 for
    // daily seasonality with 5-minute intervals).
    size_t seasonalMin = seasonalPeriod > 0 ? std::max<size_t>(seasonalPeriod / 4, 2) : 2;
    size_t minDataPoints = std::max(config.minDataPoints, seasonalMin);

    // First pass: compute predictions and scales
    for (size_t i = 0; i < n; ++i) {
        double value = input.values[i];
        size_t seasonalIdx = (seasonalPeriod > 0) ? i % seasonalPeriod : 0;

        // Get prediction
        double prediction = predictAndUpdate(state, value, seasonalIdx, seasonalPeriod);
        output.predictions[i] = prediction;

        // Track error (always push to maintain 1:1 alignment with data points;
        // NaN values produce NaN errors, skipped by computeErrorStdDev)
        errors.push_back(value - prediction);

        // Compute adaptive bounds based on recent error distribution
        if (i < minDataPoints) {
            // Not enough data - use wide bounds
            output.upper[i] = std::numeric_limits<double>::infinity();
            output.lower[i] = -std::numeric_limits<double>::infinity();
            output.scores[i] = 0.0;
        } else {
            // Use rolling error statistics for bounds
            double errorStd = computeErrorStdDev(errors, config.windowSize);

            // Ensure minimum bound width
            double minStd = std::abs(prediction) * 0.01;
            errorStd = std::max(errorStd, minStd);
            if (errorStd < 1e-10) errorStd = 1.0;

            scale[i] = errorStd;
        }
    }

    // Use SIMD for bounds computation on the portion with valid stats
    if (minDataPoints < n) {
        simd::computeBounds(
            output.predictions.data() + minDataPoints,
            scale.data() + minDataPoints,
            config.bounds,
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

} // namespace anomaly
} // namespace timestar
