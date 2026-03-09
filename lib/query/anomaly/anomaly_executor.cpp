#include "anomaly_executor.hpp"
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace timestar {
namespace anomaly {

double AnomalyExecutor::computeAlertValue(const std::vector<double>& scores) {
    double maxScore = 0.0;
    for (double s : scores) {
        if (!std::isnan(s) && s > maxScore) {
            maxScore = s;
        }
    }
    return maxScore;
}

void AnomalyExecutor::addSeriesPieces(
    AnomalyQueryResult& result,
    const std::vector<double>& rawValues,
    const AnomalyOutput& output,
    const std::vector<std::string>& groupTags,
    size_t queryIndex
) {
    // Raw values piece
    {
        AnomalySeriesPiece piece;
        piece.piece = "raw";
        piece.groupTags = groupTags;
        piece.values = rawValues;
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Upper bound piece
    {
        AnomalySeriesPiece piece;
        piece.piece = "upper";
        piece.groupTags = groupTags;
        piece.values = output.upper;
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Lower bound piece
    {
        AnomalySeriesPiece piece;
        piece.piece = "lower";
        piece.groupTags = groupTags;
        piece.values = output.lower;
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Scores piece
    {
        AnomalySeriesPiece piece;
        piece.piece = "scores";
        piece.groupTags = groupTags;
        piece.values = output.scores;
        piece.alertValue = computeAlertValue(output.scores);
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Predictions piece (optional, useful for debugging)
    if (!output.predictions.empty()) {
        AnomalySeriesPiece piece;
        piece.piece = "predictions";
        piece.groupTags = groupTags;
        piece.values = output.predictions;
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }
}

AnomalyQueryResult AnomalyExecutor::execute(
    const std::vector<uint64_t>& timestamps,
    const std::vector<double>& values,
    const std::vector<std::string>& groupTags,
    const AnomalyConfig& config
) {
    auto startTime = std::chrono::high_resolution_clock::now();

    AnomalyQueryResult result;
    result.times = timestamps;

    if (timestamps.empty() || values.empty()) {
        result.success = true;
        return result;
    }

    // Create detector based on algorithm
    auto detector = createDetector(config.algorithm);

    // Prepare input
    AnomalyInput input;
    input.timestamps = timestamps;
    input.values = values;
    input.groupTags = groupTags;

    try {
        // Run detection
        AnomalyOutput output = detector->detect(input, config);

        // Add series pieces to result
        addSeriesPieces(result, values, output, groupTags, 0);

        // Fill statistics
        result.statistics.algorithm = algorithmToString(config.algorithm);
        result.statistics.bounds = config.bounds;
        result.statistics.seasonality = (config.seasonality == Seasonality::NONE) ? "none" :
            (config.seasonality == Seasonality::HOURLY) ? "hourly" :
            (config.seasonality == Seasonality::DAILY) ? "daily" : "weekly";
        result.statistics.anomalyCount = output.anomalyCount;
        result.statistics.totalPoints = values.size();

        result.success = true;

    } catch (const std::exception& e) {
        result.success = false;
        result.errorMessage = e.what();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs =
        std::chrono::duration<double, std::milli>(endTime - startTime).count();

    return result;
}

AnomalyQueryResult AnomalyExecutor::executeMulti(
    const std::vector<uint64_t>& sharedTimestamps,
    const std::vector<std::vector<double>>& seriesValues,
    const std::vector<std::vector<std::string>>& seriesGroupTags,
    const AnomalyConfig& config
) {
    if (seriesValues.size() != seriesGroupTags.size()) {
        throw std::invalid_argument(
            "seriesValues and seriesGroupTags must have the same size"
        );
    }

    auto startTime = std::chrono::high_resolution_clock::now();

    AnomalyQueryResult result;
    result.times = sharedTimestamps;

    if (sharedTimestamps.empty() || seriesValues.empty()) {
        result.success = true;
        return result;
    }

    // Create detector based on algorithm
    auto detector = createDetector(config.algorithm);

    size_t totalAnomalies = 0;
    size_t totalPoints = 0;

    try {
        // Reuse a single AnomalyInput across iterations to avoid copying
        // sharedTimestamps N times. Timestamps are assigned once; values and
        // groupTags are swapped in and out each iteration.
        AnomalyInput input;
        input.timestamps = sharedTimestamps;

        // Process each series
        for (size_t i = 0; i < seriesValues.size(); ++i) {
            const auto& values = seriesValues[i];
            const auto& groupTags = seriesGroupTags[i];

            if (values.empty()) continue;

            input.values = values;
            input.groupTags = groupTags;

            // Run detection
            AnomalyOutput output = detector->detect(input, config);

            // Add series pieces to result
            addSeriesPieces(result, values, output, groupTags, i);

            totalAnomalies += output.anomalyCount;
            totalPoints += values.size();
        }

        // Fill statistics
        result.statistics.algorithm = algorithmToString(config.algorithm);
        result.statistics.bounds = config.bounds;
        result.statistics.seasonality = (config.seasonality == Seasonality::NONE) ? "none" :
            (config.seasonality == Seasonality::HOURLY) ? "hourly" :
            (config.seasonality == Seasonality::DAILY) ? "daily" : "weekly";
        result.statistics.anomalyCount = totalAnomalies;
        result.statistics.totalPoints = totalPoints;

        result.success = true;

    } catch (const std::exception& e) {
        result.success = false;
        result.errorMessage = e.what();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs =
        std::chrono::duration<double, std::milli>(endTime - startTime).count();

    return result;
}

} // namespace anomaly
} // namespace timestar
