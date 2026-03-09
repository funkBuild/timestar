#include "forecast_executor.hpp"
#include <chrono>
#include <algorithm>

namespace timestar {
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

    // Guard: if all timestamps are identical, interval == 0 and we cannot
    // generate meaningful forecast timestamps (all would collapse to lastTime,
    // causing division-by-zero in downstream xForecast computation).
    if (interval == 0) {
        return {};
    }

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

// ── Auto-windowing helpers ────────────────────────────────────────────────

size_t ForecastExecutor::detectMaxPeriodForWindowing(
    const std::vector<double>& values,
    uint64_t dataIntervalNs,
    const ForecastConfig& config
) {
    // LINEAR with no seasonality needs no periodicity detection
    if (config.forecastSeasonality == ForecastSeasonality::NONE) {
        return 0;
    }

    // For known seasonality, use the analytical period
    if (config.forecastSeasonality == ForecastSeasonality::HOURLY ||
        config.forecastSeasonality == ForecastSeasonality::DAILY ||
        config.forecastSeasonality == ForecastSeasonality::WEEKLY) {
        return forecastSeasonalityToPeriod(config.forecastSeasonality, dataIntervalNs);
    }

    // For AUTO/MULTI: run lightweight period detection (~1.5ms, downsamples internally)
    if (config.forecastSeasonality == ForecastSeasonality::AUTO ||
        config.forecastSeasonality == ForecastSeasonality::MULTI) {
        PeriodicityDetector detector;
        auto periods = detector.detectPeriods(
            values,
            config.minPeriod,
            config.maxPeriod > 0 ? config.maxPeriod : values.size() / 2,
            config.maxSeasonalComponents,
            config.seasonalThreshold
        );
        if (!periods.empty()) {
            // Return largest detected period
            size_t maxP = 0;
            for (const auto& p : periods) {
                maxP = std::max(maxP, p.period);
            }
            return maxP;
        }
    }

    // LINEAR with no seasonality, or no periods detected
    return 0;
}

size_t ForecastExecutor::computeOptimalWindowSize(
    size_t inputSize,
    size_t maxPeriod,
    size_t horizon,
    const ForecastConfig& config
) {
    size_t windowSize;

    if (maxPeriod > 0) {
        // Seasonal: keep maxHistoryCycles worth of the largest period
        windowSize = config.maxHistoryCycles * maxPeriod;
    } else {
        // Non-seasonal (linear): keep enough for trend estimation
        windowSize = std::max<size_t>(4 * horizon, 500);
    }

    // Floor: at least minDataPoints and 2× horizon
    windowSize = std::max(windowSize, config.minDataPoints);
    windowSize = std::max(windowSize, 2 * horizon);

    // Cap at input size
    windowSize = std::min(windowSize, inputSize);

    return windowSize;
}

size_t ForecastExecutor::windowInput(
    ForecastInput& input,
    size_t windowSize
) {
    if (windowSize >= input.size()) {
        return 0;  // No trimming needed
    }

    size_t trimCount = input.size() - windowSize;
    input.timestamps.erase(input.timestamps.begin(),
                           input.timestamps.begin() + static_cast<ptrdiff_t>(trimCount));
    input.values.erase(input.values.begin(),
                       input.values.begin() + static_cast<ptrdiff_t>(trimCount));
    return trimCount;
}

// ── execute() ─────────────────────────────────────────────────────────────

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
        // Auto horizon: 20% of historical, capped at 2000 points.
        // For large datasets (e.g., 1 year at 5m intervals = 105K points),
        // forecasting the same number of points is excessive and slow.
        horizon = std::min(std::max<size_t>(input.size() / 5, 50), size_t(2000));
    }

    // Auto-windowing: trim old data before expensive computation.
    // Only copy the input if we actually need to trim.
    std::optional<ForecastInput> trimmedInput;
    if (!config.disableAutoWindow && input.size() >= 2) {
        uint64_t dataIntervalNs = (input.timestamps.back() - input.timestamps.front())
                                  / (input.size() - 1);
        size_t maxPeriod = detectMaxPeriodForWindowing(input.values, dataIntervalNs, config);
        size_t windowSize = computeOptimalWindowSize(input.size(), maxPeriod, horizon, config);

        // Only trim if saving >33% of the data
        if (input.size() > windowSize * 3 / 2) {
            trimmedInput.emplace(input);
            windowInput(*trimmedInput, windowSize);
        }
    }
    const ForecastInput& workingInput = trimmedInput ? *trimmedInput : input;

    auto forecastTimestamps = generateForecastTimestamps(workingInput.timestamps, horizon);

    if (forecastTimestamps.empty()) {
        return ForecastOutput{};
    }

    // Execute appropriate forecaster
    ForecastOutput output;

    switch (config.algorithm) {
        case Algorithm::LINEAR:
            output = linearForecaster_.forecast(workingInput, config, forecastTimestamps);
            break;

        case Algorithm::SEASONAL:
            output = seasonalForecaster_.forecast(workingInput, config, forecastTimestamps);
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
        for (size_t i = 0; i + 1 < nHistorical; ++i) {
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

    if (timestamps.size() < 2) {
        result.success = false;
        result.errorMessage = "Insufficient data: at least 2 historical points are required to compute a forecast interval";
        return result;
    }

    // Guard against duplicate timestamps: if all timestamps are identical the
    // sampling interval is 0, which causes division-by-zero in both
    // generateForecastTimestamps and the x-position calculation inside the
    // individual forecasters.
    if (timestamps.back() == timestamps.front()) {
        result.success = false;
        result.errorMessage = "Cannot forecast: all timestamps are identical (zero sampling interval)";
        return result;
    }

    // Generate extended timestamps (historical + forecast)
    size_t horizon = config.forecastHorizon;
    if (horizon == 0) {
        horizon = std::min(std::max<size_t>(timestamps.size() / 5, 50), size_t(2000));
    }

    // Auto-windowing: compute window once, apply to all series
    std::vector<uint64_t> windowedTimestamps = timestamps;
    size_t trimCount = 0;

    if (!config.disableAutoWindow && timestamps.size() >= 2) {
        uint64_t dataIntervalNs = (timestamps.back() - timestamps.front())
                                  / (timestamps.size() - 1);

        // Use first series for period detection (all series share timestamps)
        size_t maxPeriod = detectMaxPeriodForWindowing(
            seriesValues[0], dataIntervalNs, config);
        size_t windowSize = computeOptimalWindowSize(
            timestamps.size(), maxPeriod, horizon, config);

        // Only trim if saving >33%
        if (timestamps.size() > windowSize * 3 / 2) {
            trimCount = timestamps.size() - windowSize;
            windowedTimestamps.erase(
                windowedTimestamps.begin(),
                windowedTimestamps.begin() + static_cast<ptrdiff_t>(trimCount));
        }
    }

    auto forecastTimestamps = generateForecastTimestamps(windowedTimestamps, horizon);

    // Combine windowed historical + forecast timestamps
    result.times = windowedTimestamps;
    result.times.insert(result.times.end(),
                       forecastTimestamps.begin(),
                       forecastTimestamps.end());
    result.forecastStartIndex = windowedTimestamps.size();

    // Record windowing statistics
    result.statistics.originalPoints = timestamps.size();
    result.statistics.windowedPoints = windowedTimestamps.size();

    // Detect and record periods for AUTO/MULTI modes
    if ((config.forecastSeasonality == ForecastSeasonality::AUTO ||
         config.forecastSeasonality == ForecastSeasonality::MULTI) &&
        !windowedTimestamps.empty() && !seriesValues.empty()) {
        PeriodicityDetector detector;
        auto detected = detector.detectPeriods(
            seriesValues[0],
            config.minPeriod,
            config.maxPeriod > 0 ? config.maxPeriod : seriesValues[0].size() / 2,
            config.maxSeasonalComponents,
            config.seasonalThreshold
        );
        for (const auto& d : detected) {
            result.statistics.detectedPeriods.push_back(d.period);
        }
    }

    // Create inner config with auto-window disabled (we already windowed)
    ForecastConfig innerConfig = config;
    innerConfig.disableAutoWindow = true;

    // Process each series
    for (size_t s = 0; s < seriesValues.size(); ++s) {
        ForecastInput input;
        input.timestamps = windowedTimestamps;

        // Trim series values by same offset
        if (trimCount > 0 && seriesValues[s].size() > trimCount) {
            input.values.assign(
                seriesValues[s].begin() + static_cast<ptrdiff_t>(trimCount),
                seriesValues[s].end());
        } else {
            input.values = seriesValues[s];
        }

        if (s < seriesGroupTags.size()) {
            input.groupTags = seriesGroupTags[s];
        }

        ForecastOutput output = execute(input, innerConfig);

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
} // namespace timestar
