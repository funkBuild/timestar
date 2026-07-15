#include "robust_detector.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>

namespace timestar {
namespace anomaly {

void RobustDetector::computeBounds(const STLComponents& stl, std::span<const double> values,
                                   const std::vector<double>& expected, double bounds, std::vector<double>& upper,
                                   std::vector<double>& lower) {
    size_t n = values.size();
    upper.resize(n);
    lower.resize(n);

    // Compute residual statistics (using robust estimators)
    // MAD = median(|x_i - median(x)|), NOT median(|x_i|)
    std::vector<double> residuals;
    residuals.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        if (!std::isnan(stl.residual[i])) {
            residuals.push_back(stl.residual[i]);
        }
    }

    if (residuals.empty()) {
        // No valid residuals - use infinite bounds (never flag anomalies)
        for (size_t i = 0; i < n; ++i) {
            upper[i] = std::numeric_limits<double>::infinity();
            lower[i] = -std::numeric_limits<double>::infinity();
        }
        return;
    }

    // Step 1: Compute median of raw residuals
    size_t sz = residuals.size();
    size_t medianIdx = sz / 2;
    std::nth_element(residuals.begin(), residuals.begin() + static_cast<ptrdiff_t>(medianIdx), residuals.end());
    double residualMedian;
    if (sz % 2 == 0 && sz >= 2) {
        double upperMedian = residuals[medianIdx];
        auto lowerIt = std::max_element(residuals.begin(), residuals.begin() + static_cast<ptrdiff_t>(medianIdx));
        residualMedian = (*lowerIt + upperMedian) / 2.0;
    } else {
        residualMedian = residuals[medianIdx];
    }

    // Step 2: Compute |x_i - median(x)|, reusing the residuals vector in-place
    for (size_t i = 0; i < sz; ++i) {
        residuals[i] = std::abs(residuals[i] - residualMedian);
    }

    // Step 3: MAD = median of the absolute deviations
    std::nth_element(residuals.begin(), residuals.begin() + static_cast<ptrdiff_t>(medianIdx), residuals.end());
    double mad;
    if (sz % 2 == 0 && sz >= 2) {
        double upperMedian = residuals[medianIdx];
        auto lowerIt = std::max_element(residuals.begin(), residuals.begin() + static_cast<ptrdiff_t>(medianIdx));
        mad = (*lowerIt + upperMedian) / 2.0;
    } else {
        mad = residuals[medianIdx];
    }

    // Convert MAD to standard deviation equivalent
    // For normal distribution: sigma ≈ 1.4826 * MAD
    double sigma = 1.4826 * mad;

    // Ensure minimum bound width
    double minSigma = 0.0;
    for (size_t i = 0; i < n; ++i) {
        if (!std::isnan(values[i])) {
            minSigma = std::max(minSigma, std::abs(values[i]) * 0.01);
        }
    }
    sigma = std::max(sigma, minSigma);
    if (sigma < 1e-10)
        sigma = 1.0;

    // Compute bounds around the precomputed expected values (trend + seasonal)
    // using the scalar-scale overload — sigma is uniform, so no N-sized scale
    // vector is needed.
    simd::computeBounds(expected.data(), sigma, bounds, upper.data(), lower.data(), n);
}

AnomalyOutput RobustDetector::detect(const AnomalyInputView& input, const AnomalyConfig& config) {
    AnomalyOutput output;

    if (input.empty()) {
        return output;
    }

    size_t n = input.size();

    // Configure STL decomposition
    STLConfig stlConfig;
    stlConfig.seasonalPeriod = seasonalityToPeriod(config.seasonality, estimateInterval(input.timestamps));
    stlConfig.seasonalWindow = config.stlSeasonalWindow;
    stlConfig.robust = config.stlRobust;

    // Perform STL decomposition (already SIMD-optimized)
    STLComponents stl = STLDecomposition::decompose(input.values, stlConfig);

    // Store predictions (trend + seasonal) using SIMD
    output.predictions.resize(n);
    simd::vectorAdd(stl.trend.data(), stl.seasonal.data(), output.predictions.data(), n);

    // Compute bounds based on residual distribution (SIMD-optimized).
    // output.predictions is the trend+seasonal series computed above — pass it
    // as the expected values instead of recomputing trend+seasonal internally.
    computeBounds(stl, input.values, output.predictions, config.bounds, output.upper, output.lower);

    // Compute anomaly scores using SIMD
    output.scores.resize(n);
    simd::computeAnomalyScores(input.values.data(), output.upper.data(), output.lower.data(), output.scores.data(), n);

    // Fix up NaN inputs: SIMD may have produced NaN scores for NaN input values.
    // NaN inputs are missing data, not anomalies -- ensure score is 0.
    for (size_t i = 0; i < n; ++i) {
        if (std::isnan(input.values[i])) {
            output.scores[i] = 0.0;
        }
    }

    // Count anomalies
    output.anomalyCount = 0;
    for (size_t i = 0; i < n; ++i) {
        if (output.scores[i] > 0) {
            ++output.anomalyCount;
        }
    }

    return output;
}

}  // namespace anomaly
}  // namespace timestar
