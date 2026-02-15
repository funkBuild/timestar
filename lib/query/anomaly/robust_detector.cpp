#include "robust_detector.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>

namespace tsdb {
namespace anomaly {

void RobustDetector::computeBounds(
    const STLComponents& stl,
    const std::vector<double>& values,
    double bounds,
    std::vector<double>& upper,
    std::vector<double>& lower
) {
    size_t n = values.size();
    upper.resize(n);
    lower.resize(n);

    // Compute residual statistics (using robust estimators)
    std::vector<double> absResiduals;
    absResiduals.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        if (!std::isnan(stl.residual[i])) {
            absResiduals.push_back(std::abs(stl.residual[i]));
        }
    }

    if (absResiduals.empty()) {
        // No valid residuals - use wide bounds
        for (size_t i = 0; i < n; ++i) {
            upper[i] = values[i] + 1000.0;
            lower[i] = values[i] - 1000.0;
        }
        return;
    }

    // Use MAD (Median Absolute Deviation) for robust scale estimation
    // nth_element is O(N) vs O(N log N) for full sort
    size_t medianIdx = absResiduals.size() / 2;
    std::nth_element(absResiduals.begin(), absResiduals.begin() + medianIdx, absResiduals.end());
    double mad = absResiduals[medianIdx];

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
    if (sigma < 1e-10) sigma = 1.0;

    // Compute expected values (trend + seasonal) using SIMD
    std::vector<double> expected(n);
    simd::vectorAdd(stl.trend.data(), stl.seasonal.data(), expected.data(), n);

    // Create uniform scale vector
    std::vector<double> scale(n, sigma);

    // Compute bounds using SIMD
    simd::computeBounds(expected.data(), scale.data(), bounds, upper.data(), lower.data(), n);
}

AnomalyOutput RobustDetector::detect(
    const AnomalyInput& input,
    const AnomalyConfig& config
) {
    AnomalyOutput output;

    if (input.empty()) {
        return output;
    }

    size_t n = input.size();

    // Configure STL decomposition
    STLConfig stlConfig;
    stlConfig.seasonalPeriod = seasonalityToPeriod(
        config.seasonality,
        estimateInterval(input.timestamps)
    );
    stlConfig.seasonalWindow = config.stlSeasonalWindow;
    stlConfig.robust = config.stlRobust;

    // Perform STL decomposition (already SIMD-optimized)
    STLComponents stl = STLDecomposition::decompose(input.values, stlConfig);

    // Store predictions (trend + seasonal) using SIMD
    output.predictions.resize(n);
    simd::vectorAdd(stl.trend.data(), stl.seasonal.data(), output.predictions.data(), n);

    // Compute bounds based on residual distribution (SIMD-optimized)
    computeBounds(stl, input.values, config.bounds, output.upper, output.lower);

    // Compute anomaly scores using SIMD
    output.scores.resize(n);
    simd::computeAnomalyScores(
        input.values.data(),
        output.upper.data(),
        output.lower.data(),
        output.scores.data(),
        n
    );

    // Count anomalies
    output.anomalyCount = 0;
    for (size_t i = 0; i < n; ++i) {
        if (output.scores[i] > 0) {
            ++output.anomalyCount;
        }
    }

    return output;
}

} // namespace anomaly
} // namespace tsdb
