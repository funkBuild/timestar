#include "stl_decomposition.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>

namespace timestar {
namespace anomaly {

double STLDecomposition::tricubeWeight(double u) {
    if (u < 0.0)
        u = -u;
    if (u >= 1.0)
        return 0.0;
    double t = 1.0 - u * u * u;
    return t * t * t;
}

double STLDecomposition::median(std::vector<double> v) {
    if (v.empty())
        return 0.0;

    // Remove NaN values
    v.erase(std::remove_if(v.begin(), v.end(), [](double x) { return std::isnan(x); }), v.end());

    if (v.empty())
        return 0.0;

    size_t n = v.size();
    std::nth_element(v.begin(), v.begin() + n / 2, v.end());

    if (n % 2 == 0) {
        double m1 = v[n / 2];
        std::nth_element(v.begin(), v.begin() + n / 2 - 1, v.end());
        return (v[n / 2 - 1] + m1) / 2.0;
    }
    return v[n / 2];
}

double STLDecomposition::iqr(const std::vector<double>& values) {
    std::vector<double> v;
    v.reserve(values.size());

    for (double x : values) {
        if (!std::isnan(x)) {
            v.push_back(x);
        }
    }

    if (v.size() < 4)
        return 1.0;  // Fallback

    // Use nth_element for O(N) quartile extraction instead of O(N log N) sort.
    // Two partial sorts: first find Q3, then find Q1 in the lower partition.
    size_t n = v.size();
    size_t q1Idx = n / 4;
    size_t q3Idx = (3 * n) / 4;

    std::nth_element(v.begin(), v.begin() + q3Idx, v.end());
    double q3 = v[q3Idx];
    std::nth_element(v.begin(), v.begin() + q1Idx, v.begin() + q3Idx);
    double q1 = v[q1Idx];

    return q3 - q1;
}

std::vector<double> STLDecomposition::computeRobustnessWeights(const std::vector<double>& residuals, double threshold) {
    std::vector<double> weights(residuals.size(), 1.0);

    double h = threshold * iqr(residuals);
    if (h < 1e-10)
        h = 1.0;

    // Compute distances for SIMD tricube computation
    std::vector<double> distances(residuals.size());
    for (size_t i = 0; i < residuals.size(); ++i) {
        if (std::isnan(residuals[i])) {
            distances[i] = 2.0;  // > 1 so weight will be 0
        } else {
            distances[i] = std::abs(residuals[i]) / h;
        }
    }

    // Use SIMD for tricube weight computation
    simd::computeTricubeWeights(distances.data(), weights.data(), residuals.size());

    // Zero out NaN positions
    for (size_t i = 0; i < residuals.size(); ++i) {
        if (std::isnan(residuals[i])) {
            weights[i] = 0.0;
        }
    }

    return weights;
}

std::vector<double> STLDecomposition::movingAverage(const std::vector<double>& values, size_t windowSize) {
    if (values.empty() || windowSize == 0) {
        return values;
    }

    std::vector<double> result(values.size());

    // Use SIMD-optimized moving average
    simd::computeMovingAverage(values.data(), values.size(), windowSize, result.data());

    return result;
}

std::vector<double> STLDecomposition::loess(const std::vector<double>& x, const std::vector<double>& y,
                                            size_t windowSize, const std::vector<double>& weights) {
    size_t n = y.size();
    if (n == 0)
        return {};

    std::vector<double> result(n);
    size_t halfWindow = windowSize / 2;
    bool hasWeights = !weights.empty();

    // Pre-allocate work arrays.  Max window span is halfWindow left + 1 center
    // + halfWindow right = windowSize + 1 elements (for odd windowSize, or
    // windowSize for even).  +2 provides a safety margin.
    std::vector<double> localWeights(windowSize + 2);
    std::vector<double> localX(windowSize + 2);
    std::vector<double> localY(windowSize + 2);
    std::vector<double> distances(windowSize + 2);

    for (size_t i = 0; i < n; ++i) {
        // Find window bounds
        size_t start = (i > halfWindow) ? i - halfWindow : 0;
        size_t end = std::min(i + halfWindow + 1, n);
        size_t windowCount = end - start;

        // Compute distances and prepare local arrays
        size_t validCount = 0;
        for (size_t j = start; j < end; ++j) {
            if (std::isnan(y[j]))
                continue;

            double dist =
                std::abs(static_cast<double>(j) - static_cast<double>(i)) / static_cast<double>(halfWindow + 1);
            distances[validCount] = dist;
            localX[validCount] = static_cast<double>(j);
            localY[validCount] = y[j];
            ++validCount;
        }

        if (validCount == 0) {
            result[i] = std::numeric_limits<double>::quiet_NaN();
            continue;
        }

        // Compute tricube weights using SIMD
        simd::computeTricubeWeights(distances.data(), localWeights.data(), validCount);

        // Apply robustness weights if available
        if (hasWeights) {
            size_t k = 0;
            for (size_t j = start; j < end; ++j) {
                if (std::isnan(y[j]))
                    continue;
                localWeights[k] *= weights[j];
                ++k;
            }
        }

        // Use SIMD weighted linear regression
        simd::LinearFit fit =
            simd::weightedLinearRegression(localX.data(), localY.data(), localWeights.data(), validCount);

        result[i] = fit.intercept + fit.slope * static_cast<double>(i);
    }

    return result;
}

std::vector<double> STLDecomposition::computeSeasonalMeans(const std::vector<double>& values, size_t period) {
    if (period == 0 || values.empty()) {
        return std::vector<double>(values.size(), 0.0);
    }

    std::vector<double> result(values.size());

    // Compute mean for each position in the seasonal cycle
    std::vector<std::vector<double>> subseries(period);

    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            subseries[i % period].push_back(values[i]);
        }
    }

    // Use SIMD for subseries mean computation
    std::vector<double> seasonalMeans(period);
    size_t nonEmptyCount = 0;
    double sumOfMeans = 0.0;
    for (size_t p = 0; p < period; ++p) {
        if (!subseries[p].empty()) {
            seasonalMeans[p] = simd::vectorMean(subseries[p].data(), subseries[p].size());
            sumOfMeans += seasonalMeans[p];
            ++nonEmptyCount;
        } else {
            seasonalMeans[p] = 0.0;
        }
    }

    // Center the seasonal means (should sum to zero).
    // Only average over non-empty subseries to avoid bias from all-NaN phases.
    if (nonEmptyCount > 0) {
        double meanOfMeans = sumOfMeans / static_cast<double>(nonEmptyCount);
        for (size_t p = 0; p < period; ++p) {
            if (!subseries[p].empty()) {
                seasonalMeans[p] -= meanOfMeans;
            }
        }
    }

    // Assign seasonal values
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = seasonalMeans[i % period];
    }

    return result;
}

STLComponents STLDecomposition::decompose(const std::vector<double>& values, const STLConfig& config) {
    STLComponents components;
    size_t n = values.size();

    if (n == 0) {
        return components;
    }

    // Initialize
    components.trend.resize(n, 0.0);
    components.seasonal.resize(n, 0.0);
    components.residual.resize(n, 0.0);

    size_t period = config.seasonalPeriod;
    if (period == 0 || period > n / 2) {
        // No seasonality - just extract trend using SIMD moving average
        size_t trendWin = config.trendWindow > 0 ? config.trendWindow : n / 10 + 1;
        simd::computeMovingAverage(values.data(), n, trendWin, components.trend.data());

        // Compute residuals using SIMD
        simd::vectorSubtract(values.data(), components.trend.data(), components.residual.data(), n);
        return components;
    }

    // Set window sizes
    size_t seasonalWindow = config.seasonalWindow;
    if (seasonalWindow % 2 == 0)
        seasonalWindow++;  // Must be odd

    size_t trendWindow = config.trendWindow;
    if (trendWindow == 0) {
        // Default: ceil(1.5 * period / (1 - 1.5/seasonalWindow))
        trendWindow = static_cast<size_t>(1.5 * period / (1.0 - 1.5 / seasonalWindow));
        if (trendWindow % 2 == 0)
            trendWindow++;
    }

    // Initialize deseasonalized series
    std::vector<double> deseasonalized = values;
    std::vector<double> robustnessWeights;

    size_t iterations = config.robust ? config.robustIterations + 1 : 1;

    for (size_t iter = 0; iter < iterations; ++iter) {
        // Step 1: Extract trend
        std::vector<double> trend;
        if (robustnessWeights.empty()) {
            trend.resize(n);
            simd::computeMovingAverage(deseasonalized.data(), n, trendWindow, trend.data());
        } else {
            std::vector<double> x(n);
            for (size_t i = 0; i < n; ++i)
                x[i] = static_cast<double>(i);
            trend = loess(x, deseasonalized, trendWindow, robustnessWeights);
        }

        // Step 2: Detrend to get seasonal + residual (SIMD)
        std::vector<double> detrended(n);
        simd::vectorSubtract(values.data(), trend.data(), detrended.data(), n);

        // Step 3: Extract seasonal component using subseries means
        std::vector<double> seasonal = computeSeasonalMeans(detrended, period);

        // Step 4: Smooth the seasonal component
        std::vector<double> x(n);
        for (size_t i = 0; i < n; ++i)
            x[i] = static_cast<double>(i);
        seasonal = loess(x, seasonal, seasonalWindow, robustnessWeights);

        // Step 5: Deseasonalize for next iteration (SIMD)
        simd::vectorSubtract(values.data(), seasonal.data(), deseasonalized.data(), n);

        // Store components
        components.trend = trend;
        components.seasonal = seasonal;

        // Compute residuals using SIMD: residual = values - trend - seasonal
        std::vector<double> trendPlusSeasonal(n);
        simd::vectorAdd(trend.data(), seasonal.data(), trendPlusSeasonal.data(), n);
        simd::vectorSubtract(values.data(), trendPlusSeasonal.data(), components.residual.data(), n);

        // Update robustness weights for next iteration
        if (config.robust && iter < iterations - 1) {
            robustnessWeights = computeRobustnessWeights(components.residual, config.robustThreshold);
        }
    }

    return components;
}

}  // namespace anomaly
}  // namespace timestar
