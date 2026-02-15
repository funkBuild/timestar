#include "stl_decomposition.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>
#include <limits>

namespace tsdb {
namespace forecast {

// ============================================================================
// Weight Functions
// ============================================================================

double STLDecomposer::tricube(double x) {
    if (x < 0.0) x = -x;
    if (x >= 1.0) return 0.0;
    double tmp = 1.0 - x * x * x;
    return tmp * tmp * tmp;
}

double STLDecomposer::bisquare(double x) {
    if (x < 0.0) x = -x;
    if (x >= 1.0) return 0.0;
    double tmp = 1.0 - x * x;
    return tmp * tmp;
}

// ============================================================================
// LOESS Implementation
// ============================================================================

double STLDecomposer::weightedLocalRegression(
    const std::vector<double>& x,
    const std::vector<double>& y,
    double xeval,
    double span,
    const std::vector<double>& weights
) {
    size_t n = x.size();
    if (n == 0) return 0.0;

    // Determine window size
    size_t window = static_cast<size_t>(std::ceil(span * n));
    if (window < 2) window = 2;
    if (window > n) window = n;

    // Find distances and sort to get nearest neighbors
    std::vector<std::pair<double, size_t>> distances;
    distances.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        double dist = std::abs(x[i] - xeval);
        distances.emplace_back(dist, i);
    }
    std::partial_sort(distances.begin(), distances.begin() + window, distances.end());

    // Maximum distance for tricube weights
    double maxDist = distances[window - 1].first;
    if (maxDist <= 0.0) maxDist = 1e-10;

    // Compute weights and weighted sums for linear regression
    double sumW = 0.0;
    double sumWX = 0.0;
    double sumWY = 0.0;
    double sumWXX = 0.0;
    double sumWXY = 0.0;

    for (size_t i = 0; i < window; ++i) {
        size_t idx = distances[i].second;
        double dist = distances[i].first;

        // Tricube weight based on distance
        double w = tricube(dist / maxDist);

        // Apply additional user weights if provided
        if (!weights.empty() && weights[idx] > 0.0) {
            w *= weights[idx];
        }

        if (w > 0.0) {
            double xi = x[idx] - xeval;  // Center around evaluation point
            double yi = y[idx];

            sumW += w;
            sumWX += w * xi;
            sumWY += w * yi;
            sumWXX += w * xi * xi;
            sumWXY += w * xi * yi;
        }
    }

    if (sumW <= 0.0) {
        // Fallback: return nearest point
        return y[distances[0].second];
    }

    // Solve weighted least squares: y = a + b*x
    double denom = sumW * sumWXX - sumWX * sumWX;
    if (std::abs(denom) < 1e-10) {
        // Degenerate case: return weighted mean
        return sumWY / sumW;
    }

    double a = (sumWXX * sumWY - sumWX * sumWXY) / denom;
    // double b = (sumW * sumWXY - sumWX * sumWY) / denom;

    // Since x is centered at xeval, the prediction at xeval is just the intercept
    return a;
}

std::vector<double> STLDecomposer::loess(
    const std::vector<double>& x,
    const std::vector<double>& y,
    const std::vector<double>& xout,
    double span,
    const std::vector<double>& weights
) {
    if (x.size() != y.size()) {
        throw std::invalid_argument("x and y must have the same length");
    }
    if (x.empty()) {
        throw std::invalid_argument("x and y cannot be empty");
    }
    if (span <= 0.0 || span > 1.0) {
        throw std::invalid_argument("span must be in (0, 1]");
    }
    if (!weights.empty() && weights.size() != x.size()) {
        throw std::invalid_argument("weights must match x and y length");
    }

    std::vector<double> result;
    result.reserve(xout.size());

    for (double xeval : xout) {
        result.push_back(weightedLocalRegression(x, y, xeval, span, weights));
    }

    return result;
}

// ============================================================================
// Cycle-Subseries Smoothing
// ============================================================================

std::vector<double> STLDecomposer::smoothCycleSubseries(
    const std::vector<double>& y,
    size_t period,
    size_t window
) {
    if (y.empty() || period == 0) {
        return std::vector<double>(y.size(), 0.0);
    }

    size_t n = y.size();
    std::vector<double> result(n);

    // Process each position within the cycle
    for (size_t phase = 0; phase < period; ++phase) {
        // Extract subseries for this phase
        std::vector<double> subseries;
        std::vector<size_t> indices;

        for (size_t i = phase; i < n; i += period) {
            subseries.push_back(y[i]);
            indices.push_back(i);
        }

        if (subseries.empty()) continue;

        // Create x values for LOESS (cycle numbers)
        std::vector<double> x(subseries.size());
        for (size_t i = 0; i < x.size(); ++i) {
            x[i] = static_cast<double>(i);
        }

        // Smooth this subseries
        double span = static_cast<double>(window) / static_cast<double>(subseries.size());
        if (span > 1.0) span = 1.0;
        if (span < 0.1) span = 0.1;

        std::vector<double> smoothed = loess(x, subseries, x, span);

        // Place smoothed values back
        for (size_t i = 0; i < indices.size(); ++i) {
            result[indices[i]] = smoothed[i];
        }
    }

    return result;
}

// ============================================================================
// Low-Pass Filter
// ============================================================================

std::vector<double> STLDecomposer::lowPassFilter(
    const std::vector<double>& y,
    size_t period
) {
    if (y.empty()) return {};

    size_t n = y.size();

    // Helper lambda for moving average
    auto movingAverage = [](const std::vector<double>& data, size_t window) -> std::vector<double> {
        if (data.empty() || window == 0) return data;

        size_t n = data.size();
        std::vector<double> result(n, 0.0);

        // Use simple moving average with edge handling
        for (size_t i = 0; i < n; ++i) {
            double sum = 0.0;
            size_t count = 0;

            size_t half = window / 2;
            size_t start = (i >= half) ? (i - half) : 0;
            size_t end = std::min(i + half + 1, n);

            for (size_t j = start; j < end; ++j) {
                sum += data[j];
                count++;
            }

            result[i] = (count > 0) ? (sum / count) : 0.0;
        }

        return result;
    };

    // Apply three moving averages as per STL algorithm
    std::vector<double> filtered = movingAverage(y, period);
    filtered = movingAverage(filtered, period);
    filtered = movingAverage(filtered, 3);

    return filtered;
}

// ============================================================================
// Robustness Weights
// ============================================================================

std::vector<double> STLDecomposer::computeRobustnessWeights(
    const std::vector<double>& residuals
) {
    if (residuals.empty()) return {};

    size_t n = residuals.size();

    // Compute median absolute deviation (MAD)
    std::vector<double> absResiduals(n);
    for (size_t i = 0; i < n; ++i) {
        absResiduals[i] = std::abs(residuals[i]);
    }

    std::vector<double> sortedAbs = absResiduals;
    std::sort(sortedAbs.begin(), sortedAbs.end());

    double mad;
    if (n % 2 == 0) {
        mad = (sortedAbs[n / 2 - 1] + sortedAbs[n / 2]) / 2.0;
    } else {
        mad = sortedAbs[n / 2];
    }

    // Scale factor for MAD to estimate standard deviation
    double scale = 6.0 * mad;
    if (scale < 1e-10) scale = 1e-10;

    // Compute bisquare weights
    std::vector<double> weights(n);
    for (size_t i = 0; i < n; ++i) {
        weights[i] = bisquare(absResiduals[i] / scale);
    }

    return weights;
}

// ============================================================================
// Main STL Decomposition
// ============================================================================

STLResult STLDecomposer::decompose(
    const std::vector<double>& y,
    size_t period,
    size_t seasonalWindow,
    size_t trendWindow,
    bool robust,
    size_t outerIterations,
    size_t innerIterations
) {
    STLResult result;
    result.period = period;
    result.success = false;

    // Validation
    if (y.empty()) {
        return result;
    }

    size_t n = y.size();
    if (n < 2 * period) {
        // Need at least 2 full cycles
        return result;
    }

    if (seasonalWindow < 7 || seasonalWindow % 2 == 0) {
        seasonalWindow = 7;  // Must be odd and >= 7
    }

    // Auto-determine trend window if not specified
    if (trendWindow == 0) {
        double numerator = 1.5 * period;
        double denominator = 1.0 - 1.5 / static_cast<double>(seasonalWindow);
        trendWindow = static_cast<size_t>(std::ceil(numerator / denominator));
        if (trendWindow % 2 == 0) trendWindow++;  // Ensure odd
        if (trendWindow < 3) trendWindow = 3;
    }

    // Ensure trend window is odd
    if (trendWindow % 2 == 0) trendWindow++;

    // Initialize components
    std::vector<double> trend(n, 0.0);
    std::vector<double> seasonal(n, 0.0);
    std::vector<double> residual(n, 0.0);
    std::vector<double> robustnessWeights(n, 1.0);

    if (!robust) {
        outerIterations = 1;
    }

    // Main STL loop
    for (size_t outer = 0; outer < outerIterations; ++outer) {
        for (size_t inner = 0; inner < innerIterations; ++inner) {
            // Step 1: Detrend
            std::vector<double> detrended(n);
            for (size_t i = 0; i < n; ++i) {
                detrended[i] = y[i] - trend[i];
            }

            // Step 2: Cycle-subseries smoothing
            std::vector<double> rawSeasonal = smoothCycleSubseries(detrended, period, seasonalWindow);

            // Step 3: Low-pass filter to get smooth seasonal
            std::vector<double> smoothSeasonal = lowPassFilter(rawSeasonal, period);

            // Step 4: Remove low-frequency from seasonal
            for (size_t i = 0; i < n; ++i) {
                seasonal[i] = rawSeasonal[i] - smoothSeasonal[i];
            }

            // Step 5: Deseasonalize
            std::vector<double> deseasonalized(n);
            for (size_t i = 0; i < n; ++i) {
                deseasonalized[i] = y[i] - seasonal[i];
            }

            // Step 6: Trend smoothing via LOESS
            std::vector<double> x(n);
            for (size_t i = 0; i < n; ++i) {
                x[i] = static_cast<double>(i);
            }

            double trendSpan = static_cast<double>(trendWindow) / static_cast<double>(n);
            if (trendSpan > 1.0) trendSpan = 1.0;

            trend = loess(x, deseasonalized, x, trendSpan, robustnessWeights);
        }

        // Compute residuals and update robustness weights
        for (size_t i = 0; i < n; ++i) {
            residual[i] = y[i] - trend[i] - seasonal[i];
        }

        if (robust && outer < outerIterations - 1) {
            robustnessWeights = computeRobustnessWeights(residual);
        }
    }

    // Final residual calculation
    for (size_t i = 0; i < n; ++i) {
        residual[i] = y[i] - trend[i] - seasonal[i];
    }

    result.trend = std::move(trend);
    result.seasonal = std::move(seasonal);
    result.residual = std::move(residual);
    result.success = true;

    return result;
}

// ============================================================================
// MSTL Decomposition (Multiple Seasonalities)
// ============================================================================

MSTLResult STLDecomposer::decomposeMultiple(
    const std::vector<double>& y,
    std::vector<size_t> periods
) {
    MSTLResult result;
    result.success = false;

    if (y.empty() || periods.empty()) {
        return result;
    }

    // Sort periods in ascending order
    std::sort(periods.begin(), periods.end());

    // Remove duplicates
    periods.erase(std::unique(periods.begin(), periods.end()), periods.end());

    size_t n = y.size();

    // Check that we have enough data for the largest period
    if (n < 2 * periods.back()) {
        return result;
    }

    std::vector<double> remainder = y;
    std::vector<std::vector<double>> seasonals;

    // Extract each seasonal component starting with smallest period
    for (size_t period : periods) {
        // Decompose remainder to extract this seasonal component
        STLResult stlResult = decompose(remainder, period, 7, 0, true, 2, 2);

        if (!stlResult.success) {
            return result;
        }

        // Store this seasonal component
        seasonals.push_back(stlResult.seasonal);

        // Remove this seasonal from remainder
        for (size_t i = 0; i < n; ++i) {
            remainder[i] -= stlResult.seasonal[i];
        }
    }

    // Final decomposition of remainder to get trend
    // Use the largest period for trend extraction
    STLResult finalResult = decompose(remainder, periods.back(), 7, 0, true, 2, 1);

    if (!finalResult.success) {
        return result;
    }

    result.trend = finalResult.trend;
    result.seasonals = std::move(seasonals);
    result.periods = std::move(periods);
    result.residual = finalResult.residual;
    result.success = true;

    return result;
}

} // namespace forecast
} // namespace tsdb
