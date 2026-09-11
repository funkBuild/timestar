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

void AnomalyExecutor::addSeriesPieces(AnomalyQueryResult& result, const std::vector<double>& rawValues,
                                      AnomalyOutput&& output, const std::vector<std::string>& groupTags,
                                      size_t queryIndex) {
    // Raw values piece (copied — the caller keeps ownership of rawValues)
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
        piece.values = std::move(output.upper);
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Lower bound piece
    {
        AnomalySeriesPiece piece;
        piece.piece = "lower";
        piece.groupTags = groupTags;
        piece.values = std::move(output.lower);
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Scores piece
    {
        AnomalySeriesPiece piece;
        piece.piece = "scores";
        piece.groupTags = groupTags;
        piece.alertValue = computeAlertValue(output.scores);  // before the move
        piece.values = std::move(output.scores);
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }

    // Predictions piece (optional, useful for debugging)
    if (!output.predictions.empty()) {
        AnomalySeriesPiece piece;
        piece.piece = "predictions";
        piece.groupTags = groupTags;
        piece.values = std::move(output.predictions);
        piece.queryIndex = queryIndex;
        result.series.push_back(std::move(piece));
    }
}

void AnomalyExecutor::detectOneSeries(AnomalyQueryResult& result, AnomalyDetector& detector,
                                      const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                                      const std::vector<std::string>& groupTags, size_t queryIndex,
                                      const AnomalyConfig& config, SeriesTally& tally) {
    if (values.empty()) {
        return;  // nothing resolved here at all — not a decline, just absent
    }

    // Count the points that are REALLY there, not the row width.  Every group
    // may be projected onto one shared time axis and carries NaN wherever it
    // has no sample (docs/nan_policy.md, "NaN = missing"), so values.size() is
    // the axis length and counts another group's timestamps as this group's
    // data: 3 groups holding 66 real points between them, spread over a
    // 30-slot axis, reported 90.  The same rule the aggregation paths already
    // follow -- count counts only non-NaN values.
    size_t finitePoints = 0;
    for (double v : values) {
        if (!std::isnan(v)) {
            ++finitePoints;
        }
    }

    // DECLINE a LONG row that has too few observations to detect against.
    //
    // The detectors' warm-up is indexed by SLOT, not by observation: for BASIC
    // and AGILE the first minDataPoints slots get infinite bounds and a zero
    // score, and every slot after that gets a finite envelope built from
    // whatever the rolling stats have seen.  On a fan-out group that is nearly
    // all NaN, "whatever they have seen" can be a single sample -- and a device
    // with one observation came back with a 30-slot confidence envelope, a
    // prediction at every timestamp and a score at every timestamp, inside a
    // "status":"success" response.  That is the same fabrication the forecast
    // half declines (LinearForecaster::forecast).
    //
    // The gate is `axis longer than the warm-up` AND `too few observations`.
    // Both halves matter:
    //
    //   * the OBSERVATION half is the point: a row must be judged on the
    //     samples it really holds, never on the width of an axis it shares
    //     with other groups.
    //   * the AXIS half exists because a row no longer than minDataPoints
    //     cannot leave a slot-indexed warm-up at all, so for BASIC and AGILE
    //     there is nothing there to fabricate -- declining it would only throw
    //     away the stored values under `raw` and `predictions`.  Below the
    //     warm-up length the gate therefore costs data and buys nothing, so it
    //     does not fire.
    //
    // Gating on `finitePoints < values.size()` (any padding at all) instead
    // cost real data at the boundary: a 10-slot row holding 9 observations was
    // declined outright -- `"series": []`, total_points 0 -- while the same
    // nine observations on a 9-slot axis were answered in full.  One missing
    // slot flipped the answer from the data to nothing, for a row that is
    // entirely inside the warm-up either way.
    //
    // NOT true of ROBUST, which has no warm-up (docs/anomaly-detection.md): it
    // runs an STL decomposition over the whole row and produces a finite
    // envelope at every slot however short the row is -- a dense 3-point robust
    // series comes back with a +/-0.36 band.  The `values.size() >
    // minDataPoints` arm is therefore about what the OTHER two algorithms
    // cannot fabricate; for robust it simply means a short row keeps the same
    // answer it gave before this campaign, which is the standing
    // byte-identity guarantee.
    //
    // NaN is missing (docs/nan_policy.md), so a long single series carrying
    // stored NaNs is genuinely sparse and is gated like any other sparse row.
    if (values.size() > config.minDataPoints && finitePoints < config.minDataPoints) {
        ++tally.declined;
        return;
    }

    // Borrow, do not copy: detectors only read.
    AnomalyInputView input{timestamps, values};

    AnomalyOutput output = detector.detect(input, config);

    tally.anomalies += output.anomalyCount;
    tally.points += finitePoints;

    // Add series pieces to result (moves the output vectors)
    addSeriesPieces(result, values, std::move(output), groupTags, queryIndex);
}

void AnomalyExecutor::fillStatistics(AnomalyQueryResult& result, const AnomalyConfig& config,
                                     const SeriesTally& tally) {
    result.statistics.algorithm = algorithmToString(config.algorithm);
    result.statistics.bounds = config.bounds;
    result.statistics.seasonality = (config.seasonality == Seasonality::NONE)     ? "none"
                                    : (config.seasonality == Seasonality::HOURLY) ? "hourly"
                                    : (config.seasonality == Seasonality::DAILY)  ? "daily"
                                                                                  : "weekly";
    result.statistics.anomalyCount = tally.anomalies;
    result.statistics.totalPoints = tally.points;
    result.statistics.declinedSeriesCount = tally.declined;
}

AnomalyQueryResult AnomalyExecutor::execute(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                                            const std::vector<std::string>& groupTags, const AnomalyConfig& config) {
    auto startTime = std::chrono::high_resolution_clock::now();

    AnomalyQueryResult result;
    result.times = timestamps;

    if (timestamps.empty() || values.empty()) {
        result.success = true;
        return result;
    }

    // Create detector based on algorithm
    auto detector = createDetector(config.algorithm);

    SeriesTally tally;

    try {
        // The SAME per-group routine executeMulti runs, so the two entry points
        // cannot disagree about the finite-point gate or the point accounting.
        // They did: this one reported totalPoints as the row width.
        detectOneSeries(result, *detector, timestamps, values, groupTags, 0, config, tally);
        fillStatistics(result, config, tally);
        result.success = true;

    } catch (const std::exception& e) {
        result.success = false;
        result.errorMessage = e.what();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

    return result;
}

AnomalyQueryResult AnomalyExecutor::executeMulti(const std::vector<uint64_t>& sharedTimestamps,
                                                 const std::vector<std::vector<double>>& seriesValues,
                                                 const std::vector<std::vector<std::string>>& seriesGroupTags,
                                                 const AnomalyConfig& config) {
    if (seriesValues.size() != seriesGroupTags.size()) {
        throw std::invalid_argument("seriesValues and seriesGroupTags must have the same size");
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

    SeriesTally tally;

    try {
        // Process each series
        for (size_t i = 0; i < seriesValues.size(); ++i) {
            detectOneSeries(result, *detector, sharedTimestamps, seriesValues[i], seriesGroupTags[i], i, config, tally);
        }

        fillStatistics(result, config, tally);
        result.success = true;

    } catch (const std::exception& e) {
        result.success = false;
        result.errorMessage = e.what();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

    return result;
}

}  // namespace anomaly
}  // namespace timestar
