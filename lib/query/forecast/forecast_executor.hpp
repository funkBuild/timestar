#ifndef FORECAST_EXECUTOR_H_INCLUDED
#define FORECAST_EXECUTOR_H_INCLUDED

#include "forecast_result.hpp"
#include "linear_forecaster.hpp"
#include "seasonal_forecaster.hpp"
#include <chrono>

namespace tsdb {
namespace forecast {

/**
 * Forecast Executor
 *
 * Orchestrates time series forecasting. Handles:
 * - Algorithm selection (linear vs seasonal)
 * - Multi-series forecasting with group tags
 * - Result formatting for API responses
 * - Performance timing
 */
class ForecastExecutor {
public:
    /**
     * Execute forecast on a single series
     *
     * @param input Historical time series data
     * @param config Forecast configuration
     * @return ForecastOutput with forecast values and bounds
     */
    ForecastOutput execute(
        const ForecastInput& input,
        const ForecastConfig& config
    );

    /**
     * Execute forecast on multiple series (grouped data)
     *
     * @param timestamps Shared timestamps for all series
     * @param seriesValues Values for each series
     * @param seriesGroupTags Group tags for each series
     * @param config Forecast configuration
     * @return ForecastQueryResult with all series pieces
     */
    ForecastQueryResult executeMulti(
        const std::vector<uint64_t>& timestamps,
        const std::vector<std::vector<double>>& seriesValues,
        const std::vector<std::vector<std::string>>& seriesGroupTags,
        const ForecastConfig& config
    );

    /**
     * Generate forecast timestamps
     *
     * @param historicalTimestamps Original timestamps
     * @param forecastHorizon Number of forecast points (0 = match historical)
     * @return Vector of forecast timestamps
     */
    static std::vector<uint64_t> generateForecastTimestamps(
        const std::vector<uint64_t>& historicalTimestamps,
        size_t forecastHorizon = 0
    );

private:
    LinearForecaster linearForecaster_;
    SeasonalForecaster seasonalForecaster_;

    // Add series pieces to result
    void addSeriesPieces(
        ForecastQueryResult& result,
        const ForecastOutput& output,
        const std::vector<std::string>& groupTags,
        size_t queryIndex = 0
    );
};

} // namespace forecast
} // namespace tsdb

#endif // FORECAST_EXECUTOR_H_INCLUDED
