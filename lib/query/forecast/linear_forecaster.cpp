#include "linear_forecaster.hpp"
#include "../anomaly/simd_anomaly.hpp"
#include <cmath>
#include <algorithm>
#include <numeric>

namespace tsdb {
namespace forecast {

LinearForecaster::LinearFit LinearForecaster::fitLinearRegression(
    const std::vector<double>& x,
    const std::vector<double>& y,
    const std::vector<double>& weights
) {
    LinearFit fit{};
    size_t n = x.size();

    if (n < 2) {
        fit.slope = 0.0;
        fit.intercept = n > 0 ? y[0] : 0.0;
        fit.rSquared = 0.0;
        fit.residualStdDev = 0.0;
        fit.sumSquaredX = 0.0;
        fit.meanX = n > 0 ? x[0] : 0.0;
        return fit;
    }

    // Compute weighted means
    double sumWeights = 0.0;
    double weightedSumX = 0.0;
    double weightedSumY = 0.0;

    for (size_t i = 0; i < n; ++i) {
        double w = weights[i];
        sumWeights += w;
        weightedSumX += w * x[i];
        weightedSumY += w * y[i];
    }

    fit.meanX = weightedSumX / sumWeights;
    double meanY = weightedSumY / sumWeights;

    // Compute weighted sums for regression
    double sumXY = 0.0;
    double sumXX = 0.0;
    double sumYY = 0.0;

    for (size_t i = 0; i < n; ++i) {
        double w = weights[i];
        double dx = x[i] - fit.meanX;
        double dy = y[i] - meanY;
        sumXY += w * dx * dy;
        sumXX += w * dx * dx;
        sumYY += w * dy * dy;
    }

    fit.sumSquaredX = sumXX;

    // Compute slope and intercept
    if (std::abs(sumXX) < 1e-10) {
        // No variation in x - return mean
        fit.slope = 0.0;
        fit.intercept = meanY;
    } else {
        fit.slope = sumXY / sumXX;
        fit.intercept = meanY - fit.slope * fit.meanX;
    }

    // Compute R-squared
    if (std::abs(sumYY) > 1e-10) {
        double ssReg = fit.slope * fit.slope * sumXX;
        fit.rSquared = std::clamp(ssReg / sumYY, 0.0, 1.0);
    } else {
        fit.rSquared = 1.0;  // Perfect fit (all y values are the same)
    }

    // Compute residual standard deviation
    double sse = 0.0;
    for (size_t i = 0; i < n; ++i) {
        double predicted = fit.slope * x[i] + fit.intercept;
        double residual = y[i] - predicted;
        sse += weights[i] * residual * residual;
    }

    if (n > 2) {
        // Use degrees of freedom (n-2) for 2-parameter model (slope + intercept).
        // For weighted regression: s^2 = (n / sumWeights) * (SSE / (n - 2))
        fit.residualStdDev = std::sqrt(sse * n / (sumWeights * (n - 2)));
    } else {
        fit.residualStdDev = 0.0;
    }

    return fit;
}

double LinearForecaster::predictionIntervalWidth(
    const LinearFit& fit,
    double x,
    size_t n,
    double deviations
) {
    if (n < 3 || fit.sumSquaredX < 1e-10) {
        return deviations * fit.residualStdDev;
    }

    // Prediction interval formula:
    // PI = t * s * sqrt(1 + 1/n + (x - x_mean)^2 / sum((x_i - x_mean)^2))
    // We use deviations instead of t-statistic for simplicity

    double dx = x - fit.meanX;
    double term = 1.0 + 1.0 / static_cast<double>(n) + (dx * dx) / fit.sumSquaredX;

    return deviations * fit.residualStdDev * std::sqrt(term);
}

ForecastOutput LinearForecaster::forecast(
    const ForecastInput& input,
    const ForecastConfig& config,
    const std::vector<uint64_t>& forecastTimestamps
) {
    ForecastOutput output;

    size_t n = input.size();
    size_t nForecast = forecastTimestamps.size();

    if (n < config.minDataPoints) {
        // Not enough data - return empty with error indication
        return output;
    }

    output.historicalCount = n;
    output.forecastCount = nForecast;

    // Convert timestamps to normalized x values for numerical stability
    // Use index-based x values: 0, 1, 2, ... n-1
    std::vector<double> x(n);
    std::vector<double> y;
    std::vector<double> weights(n, 1.0);  // Default: uniform weights

    // Apply model-specific weighting and data selection
    size_t startIdx = 0;
    switch (config.linearModel) {
        case LinearModelType::DEFAULT:
            // Standard least-squares: use all data with uniform weights
            y = input.values;
            for (size_t i = 0; i < n; ++i) {
                x[i] = static_cast<double>(i);
            }
            break;

        case LinearModelType::SIMPLE:
            // Less sensitive to recent changes: use only last half of data
            startIdx = n / 2;
            y.assign(input.values.begin() + startIdx, input.values.end());
            x.resize(n - startIdx);
            weights.resize(n - startIdx, 1.0);
            for (size_t i = 0; i < x.size(); ++i) {
                x[i] = static_cast<double>(startIdx + i);
            }
            n = x.size();
            break;

        case LinearModelType::REACTIVE:
            // More sensitive to recent changes: exponential decay weighting
            // w[i] = exp(-lambda * (n-1-i)) where lambda ≈ 0.05
            y = input.values;
            for (size_t i = 0; i < n; ++i) {
                x[i] = static_cast<double>(i);
                // Exponential decay: more weight on recent points
                double lambda = 0.05;
                weights[i] = std::exp(-lambda * static_cast<double>(n - 1 - i));
            }
            break;
    }

    // Fit linear regression with weights
    auto fit = fitLinearRegression(x, y, weights);

    output.slope = fit.slope;
    output.intercept = fit.intercept;
    output.rSquared = fit.rSquared;
    output.residualStdDev = fit.residualStdDev;

    // Generate past values (just copy input)
    output.past = input.values;

    // Generate forecast values and bounds
    output.forecast.resize(nForecast);
    output.upper.resize(nForecast);
    output.lower.resize(nForecast);

    // Calculate time interval from historical data
    uint64_t interval = 0;
    if (n >= 2) {
        interval = (input.timestamps[n - 1] - input.timestamps[0]) / (n - 1);
    }

    for (size_t i = 0; i < nForecast; ++i) {
        // Calculate x position for this forecast point
        double xForecast;
        if (interval > 0) {
            // Use time-based position
            int64_t timeDiff = static_cast<int64_t>(forecastTimestamps[i] - input.timestamps[0]);
            xForecast = static_cast<double>(timeDiff) / static_cast<double>(interval);
        } else {
            // Fallback: extend linearly
            xForecast = static_cast<double>(n + i);
        }

        // Predict value
        double predicted = fit.slope * xForecast + fit.intercept;
        output.forecast[i] = predicted;

        // Compute prediction interval
        double width = predictionIntervalWidth(fit, xForecast, n, config.deviations);
        output.upper[i] = predicted + width;
        output.lower[i] = predicted - width;
    }

    return output;
}

} // namespace forecast
} // namespace tsdb
