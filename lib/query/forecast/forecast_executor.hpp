#pragma once

#include "forecast_result.hpp"
#include "linear_forecaster.hpp"
#include "periodicity_detector.hpp"
#include "seasonal_forecaster.hpp"

#include <chrono>

namespace timestar {
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
    ForecastOutput execute(const ForecastInput& input, const ForecastConfig& config);

    /**
     * Execute forecast on multiple series (grouped data)
     *
     * @param timestamps Shared timestamps for all series
     * @param seriesValues Values for each series
     * @param seriesGroupTags Group tags for each series
     * @param config Forecast configuration
     * @return ForecastQueryResult with all series pieces
     */
    ForecastQueryResult executeMulti(const std::vector<uint64_t>& timestamps,
                                     const std::vector<std::vector<double>>& seriesValues,
                                     const std::vector<std::vector<std::string>>& seriesGroupTags,
                                     const ForecastConfig& config);

    /**
     * Generate forecast timestamps
     *
     * @param historicalTimestamps Original timestamps
     * @param forecastHorizon Number of forecast points (0 = match historical)
     * @return Vector of forecast timestamps
     */
    static std::vector<uint64_t> generateForecastTimestamps(const std::vector<uint64_t>& historicalTimestamps,
                                                            size_t forecastHorizon = 0);

    /**
     * Resolve ForecastConfig::forecastHorizon against the AUTO rule.
     *
     * A configured horizon is used verbatim; 0 means auto, which is 20% of the
     * historical length, floored at 50 and capped at 2000 (forecasting a full
     * year of 5-minute points is neither useful nor cheap).
     *
     * Exposed because callers that must SIZE the result before running the
     * forecast -- DerivedQueryExecutor::executeForecast(), which bounds
     * `groups * (historical + horizon)` -- need the same number this class will
     * use, and a second copy of the rule would drift from this one.
     *
     * @param historicalPoints Length of the historical input
     * @param configuredHorizon ForecastConfig::forecastHorizon (0 = auto)
     * @return Number of forecast points that will be produced
     */
    static size_t resolveHorizon(size_t historicalPoints, size_t configuredHorizon);

private:
    LinearForecaster linearForecaster_;
    SeasonalForecaster seasonalForecaster_;

    // Auto-windowing helpers
    static size_t detectMaxPeriodForWindowing(const std::vector<double>& values, uint64_t dataIntervalNs,
                                              const ForecastConfig& config);

    static size_t computeOptimalWindowSize(size_t inputSize, size_t maxPeriod, size_t horizon,
                                           const ForecastConfig& config);

    static size_t windowInput(ForecastInput& input, size_t windowSize);

    // Add series pieces to result
    void addSeriesPieces(ForecastQueryResult& result, const ForecastOutput& output,
                         const std::vector<std::string>& groupTags, size_t queryIndex = 0);
};

}  // namespace forecast
}  // namespace timestar
