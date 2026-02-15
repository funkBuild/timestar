#ifndef LINEAR_FORECASTER_H_INCLUDED
#define LINEAR_FORECASTER_H_INCLUDED

#include "forecast_result.hpp"

namespace tsdb {
namespace forecast {

/**
 * Linear Forecaster
 *
 * Implements simple linear regression for time series forecasting.
 * Uses least squares fitting and prediction intervals for confidence bounds.
 *
 * Algorithm:
 * 1. Fit linear regression: y = mx + b using least squares
 * 2. Compute residual variance for prediction intervals
 * 3. Extrapolate to future timestamps
 * 4. Compute confidence bounds using prediction interval formula:
 *    forecast ± t * σ * sqrt(1 + 1/n + (x-x̄)²/Σ(xᵢ-x̄)²)
 */
class LinearForecaster {
public:
    /**
     * Forecast future values using linear regression
     *
     * @param input Historical time series data
     * @param config Forecast configuration (deviations for bounds)
     * @param forecastTimestamps Timestamps to forecast
     * @return ForecastOutput with past, forecast, upper, lower values
     */
    ForecastOutput forecast(
        const ForecastInput& input,
        const ForecastConfig& config,
        const std::vector<uint64_t>& forecastTimestamps
    );

private:
    // Fit linear regression and get coefficients
    struct LinearFit {
        double slope;
        double intercept;
        double rSquared;
        double residualStdDev;
        double sumSquaredX;  // For prediction intervals
        double meanX;
    };

    LinearFit fitLinearRegression(
        const std::vector<double>& x,
        const std::vector<double>& y,
        const std::vector<double>& weights
    );

    // Compute prediction interval width at a given x value
    double predictionIntervalWidth(
        const LinearFit& fit,
        double x,
        size_t n,
        double deviations
    );
};

} // namespace forecast
} // namespace tsdb

#endif // LINEAR_FORECASTER_H_INCLUDED
