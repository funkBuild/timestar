#include "forecast_executor.hpp"
#include <chrono>
#include <algorithm>

namespace tsdb {
namespace forecast {

std::vector<uint64_t> ForecastExecutor::generateForecastTimestamps(
    const std::vector<uint64_t>& historicalTimestamps,
    size_t forecastHorizon
) {
    if (historicalTimestamps.size() < 2) {
        return {};
    }

    // Calculate interval from historical data
    size_t n = historicalTimestamps.size();
    uint64_t interval = (historicalTimestamps[n - 1] - historicalTimestamps[0]) / (n - 1);

    // Default horizon: same number of points as historical
    if (forecastHorizon == 0) {
        forecastHorizon = n;
    }

    // Generate forecast timestamps
    std::vector<uint64_t> forecastTimestamps(forecastHorizon);
    uint64_t lastTime = historicalTimestamps.back();

    for (size_t i = 0; i < forecastHorizon; ++i) {
        forecastTimestamps[i] = lastTime + (i + 1) * interval;
    }

    return forecastTimestamps;
}

ForecastOutput ForecastExecutor::execute(
    const ForecastInput& input,
    const ForecastConfig& config
) {
    if (input.empty()) {
        return ForecastOutput{};
    }

    // Generate forecast timestamps
    size_t horizon = config.forecastHorizon;
    if (horizon == 0) {
        horizon = input.size();  // Default: same as historical
    }

    auto forecastTimestamps = generateForecastTimestamps(input.timestamps, horizon);

    if (forecastTimestamps.empty()) {
        return ForecastOutput{};
    }

    // Execute appropriate forecaster
    ForecastOutput output;

    switch (config.algorithm) {
        case Algorithm::LINEAR:
            output = linearForecaster_.forecast(input, config, forecastTimestamps);
            break;

        case Algorithm::SEASONAL:
            output = seasonalForecaster_.forecast(input, config, forecastTimestamps);
            break;
    }

    return output;
}

void ForecastExecutor::addSeriesPieces(
    ForecastQueryResult& result,
    const ForecastOutput& output,
    const std::vector<std::string>& groupTags,
    size_t queryIndex
) {
    size_t nHistorical = output.historicalCount;
    size_t nForecast = output.forecastCount;
    size_t totalPoints = nHistorical + nForecast;

    // Past piece: actual values for historical, null for forecast
    {
        ForecastSeriesPiece pastPiece;
        pastPiece.piece = "past";
        pastPiece.groupTags = groupTags;
        pastPiece.queryIndex = queryIndex;
        pastPiece.values.resize(totalPoints);

        for (size_t i = 0; i < nHistorical; ++i) {
            pastPiece.values[i] = output.past[i];
        }
        for (size_t i = nHistorical; i < totalPoints; ++i) {
            pastPiece.values[i] = std::nullopt;
        }

        result.series.push_back(std::move(pastPiece));
    }

    // Forecast piece: null (or last point) for historical, predicted for forecast
    {
        ForecastSeriesPiece forecastPiece;
        forecastPiece.piece = "forecast";
        forecastPiece.groupTags = groupTags;
        forecastPiece.queryIndex = queryIndex;
        forecastPiece.values.resize(totalPoints);

        // Include last historical point for continuity
        for (size_t i = 0; i < nHistorical - 1; ++i) {
            forecastPiece.values[i] = std::nullopt;
        }
        if (nHistorical > 0) {
            forecastPiece.values[nHistorical - 1] = output.past.back();
        }
        for (size_t i = 0; i < nForecast; ++i) {
            forecastPiece.values[nHistorical + i] = output.forecast[i];
        }

        result.series.push_back(std::move(forecastPiece));
    }

    // Upper bound piece: null for historical, upper bound for forecast
    {
        ForecastSeriesPiece upperPiece;
        upperPiece.piece = "upper";
        upperPiece.groupTags = groupTags;
        upperPiece.queryIndex = queryIndex;
        upperPiece.values.resize(totalPoints);

        for (size_t i = 0; i < nHistorical; ++i) {
            upperPiece.values[i] = std::nullopt;
        }
        for (size_t i = 0; i < nForecast; ++i) {
            upperPiece.values[nHistorical + i] = output.upper[i];
        }

        result.series.push_back(std::move(upperPiece));
    }

    // Lower bound piece: null for historical, lower bound for forecast
    {
        ForecastSeriesPiece lowerPiece;
        lowerPiece.piece = "lower";
        lowerPiece.groupTags = groupTags;
        lowerPiece.queryIndex = queryIndex;
        lowerPiece.values.resize(totalPoints);

        for (size_t i = 0; i < nHistorical; ++i) {
            lowerPiece.values[i] = std::nullopt;
        }
        for (size_t i = 0; i < nForecast; ++i) {
            lowerPiece.values[nHistorical + i] = output.lower[i];
        }

        result.series.push_back(std::move(lowerPiece));
    }
}

ForecastQueryResult ForecastExecutor::executeMulti(
    const std::vector<uint64_t>& timestamps,
    const std::vector<std::vector<double>>& seriesValues,
    const std::vector<std::vector<std::string>>& seriesGroupTags,
    const ForecastConfig& config
) {
    auto startTime = std::chrono::high_resolution_clock::now();

    ForecastQueryResult result;
    result.success = true;

    if (timestamps.empty() || seriesValues.empty()) {
        result.success = false;
        result.errorMessage = "No input data provided";
        return result;
    }

    // Generate extended timestamps (historical + forecast)
    size_t horizon = config.forecastHorizon;
    if (horizon == 0) {
        horizon = timestamps.size();
    }

    auto forecastTimestamps = generateForecastTimestamps(timestamps, horizon);

    // Combine historical and forecast timestamps
    result.times = timestamps;
    result.times.insert(result.times.end(),
                       forecastTimestamps.begin(),
                       forecastTimestamps.end());
    result.forecastStartIndex = timestamps.size();

    // Process each series
    for (size_t s = 0; s < seriesValues.size(); ++s) {
        ForecastInput input;
        input.timestamps = timestamps;
        input.values = seriesValues[s];

        if (s < seriesGroupTags.size()) {
            input.groupTags = seriesGroupTags[s];
        }

        ForecastOutput output = execute(input, config);

        if (output.empty()) {
            continue;  // Skip failed series
        }

        // Add series pieces
        addSeriesPieces(result, output, input.groupTags, 0);

        // Update statistics from first successful series
        if (result.statistics.historicalPoints == 0) {
            result.statistics.slope = output.slope;
            result.statistics.intercept = output.intercept;
            result.statistics.rSquared = output.rSquared;
            result.statistics.residualStdDev = output.residualStdDev;
            result.statistics.historicalPoints = output.historicalCount;
            result.statistics.forecastPoints = output.forecastCount;
        }
    }

    // Fill in statistics
    result.statistics.algorithm = algorithmToString(config.algorithm);
    result.statistics.deviations = config.deviations;

    if (config.seasonality != Seasonality::NONE) {
        switch (config.seasonality) {
            case Seasonality::HOURLY: result.statistics.seasonality = "hourly"; break;
            case Seasonality::DAILY: result.statistics.seasonality = "daily"; break;
            case Seasonality::WEEKLY: result.statistics.seasonality = "weekly"; break;
            default: result.statistics.seasonality = "none"; break;
        }
    } else {
        result.statistics.seasonality = "none";
    }

    result.statistics.seriesCount = seriesValues.size();

    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs =
        std::chrono::duration<double, std::milli>(endTime - startTime).count();

    return result;
}

} // namespace forecast
} // namespace tsdb
