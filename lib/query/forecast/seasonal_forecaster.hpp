#ifndef SEASONAL_FORECASTER_H_INCLUDED
#define SEASONAL_FORECASTER_H_INCLUDED

#include "forecast_result.hpp"
#include "periodicity_detector.hpp"
#include "stl_decomposition.hpp"
#include <vector>

namespace tsdb {
namespace forecast {

/**
 * Seasonal Forecaster using SARIMA
 *
 * Implements Seasonal AutoRegressive Integrated Moving Average forecasting.
 * This model handles both trend and seasonal patterns in time series data.
 *
 * SARIMA(p,d,q)(P,D,Q)s where:
 * - p: Non-seasonal AR order
 * - d: Non-seasonal differencing order
 * - q: Non-seasonal MA order
 * - P: Seasonal AR order
 * - D: Seasonal differencing order
 * - Q: Seasonal MA order
 * - s: Seasonal period
 *
 * Algorithm:
 * 1. Apply seasonal differencing (order D) to remove seasonal effects
 * 2. Apply regular differencing (order d) to make series stationary
 * 3. Fit AR coefficients using Yule-Walker equations
 * 4. Fit MA coefficients using innovation algorithm
 * 5. Forecast by reversing differencing operations
 * 6. Compute prediction intervals from forecast error variance
 */
class SeasonalForecaster {
public:
    /**
     * Forecast future values using SARIMA
     *
     * @param input Historical time series data
     * @param config Forecast configuration
     * @param forecastTimestamps Timestamps to forecast
     * @return ForecastOutput with past, forecast, upper, lower values
     */
    ForecastOutput forecast(
        const ForecastInput& input,
        const ForecastConfig& config,
        const std::vector<uint64_t>& forecastTimestamps
    );

    // Public methods for testing numerical accuracy
    // These are the core mathematical operations that need validation
    // Apply seasonal differencing: y'[t] = y[t] - y[t-s]
    std::vector<double> seasonalDifference(
        const std::vector<double>& y,
        size_t seasonalPeriod
    );

    // Apply regular differencing: y'[t] = y[t] - y[t-1]
    std::vector<double> regularDifference(const std::vector<double>& y);

    // Reverse seasonal differencing for forecasting
    std::vector<double> inverseSeasonalDifference(
        const std::vector<double>& diffed,
        const std::vector<double>& original,
        size_t seasonalPeriod,
        size_t forecastCount
    );

    // Reverse regular differencing
    std::vector<double> inverseRegularDifference(
        const std::vector<double>& diffed,
        double lastValue,
        size_t forecastCount
    );

    // Fit AR coefficients using Yule-Walker equations
    std::vector<double> fitARCoefficients(
        const std::vector<double>& y,
        size_t order
    );

    // Fit seasonal AR coefficients
    std::vector<double> fitSeasonalARCoefficients(
        const std::vector<double>& y,
        size_t order,
        size_t seasonalPeriod
    );

    // Compute autocorrelation at a given lag
    double autoCorrelation(
        const std::vector<double>& y,
        double mean,
        double variance,
        size_t lag
    );

    // Solve Yule-Walker system using Levinson-Durbin recursion
    std::vector<double> levinsonDurbin(
        const std::vector<double>& r,  // Autocorrelations r[0] to r[order]
        size_t order
    );

    // Compute one-step-ahead AR forecast
    double arForecast(
        const std::vector<double>& y,
        const std::vector<double>& arCoeffs,
        double mean
    );

    // Compute one-step-ahead seasonal AR forecast
    double seasonalArForecast(
        const std::vector<double>& y,
        const std::vector<double>& sarCoeffs,
        size_t seasonalPeriod,
        double mean
    );

    // Estimate forecast error variance (for prediction intervals)
    double estimateForecastVariance(
        const std::vector<double>& y,
        const std::vector<double>& arCoeffs,
        double mean,
        size_t horizonSteps
    );

    // Detect seasonal period from data if not specified
    size_t detectSeasonalPeriod(
        const std::vector<double>& y,
        uint64_t dataIntervalNs
    );

    /**
     * Forecast using MSTL decomposition (multiple seasonalities)
     * Used when seasonality='multi'
     */
    ForecastOutput forecastMSTL(
        const ForecastInput& input,
        const ForecastConfig& config,
        const std::vector<uint64_t>& forecastTimestamps
    );

    /**
     * Extrapolate trend component
     */
    std::vector<double> extrapolateTrend(
        const std::vector<double>& trend,
        size_t horizonSteps
    );

    /**
     * Extrapolate seasonal component (repeat last cycle)
     */
    std::vector<double> extrapolateSeasonal(
        const std::vector<double>& seasonal,
        size_t period,
        size_t horizonSteps
    );
};

} // namespace forecast
} // namespace tsdb

#endif // SEASONAL_FORECASTER_H_INCLUDED
