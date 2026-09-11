#pragma once

#include "anomaly_detector.hpp"
#include "anomaly_result.hpp"

#include <chrono>
#include <string>
#include <vector>

namespace timestar {
namespace anomaly {

// Executes anomaly detection on query results
// Takes input series and produces multi-piece output (raw, upper, lower, scores)
class AnomalyExecutor {
public:
    AnomalyExecutor() = default;

    // Execute anomaly detection on a single series.
    //
    // Defined as executeMulti() over a one-element series list, and implemented
    // that way rather than restated, so the two entry points cannot drift.
    // They already had: this one counted totalPoints as the ROW WIDTH while
    // executeMulti counted non-NaN observations, so the same series answered
    // through the two doors reported different totals, and the finite-point
    // gate executeMulti grew was missing here entirely.
    AnomalyQueryResult execute(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                               const std::vector<std::string>& groupTags, const AnomalyConfig& config);

    // Execute on multiple series (from multi-group query)
    AnomalyQueryResult executeMulti(const std::vector<uint64_t>& sharedTimestamps,
                                    const std::vector<std::vector<double>>& seriesValues,
                                    const std::vector<std::vector<std::string>>& seriesGroupTags,
                                    const AnomalyConfig& config);

private:
    // Running totals across the groups of one request.
    struct SeriesTally {
        size_t anomalies = 0;
        size_t points = 0;    // finite observations, NOT row width — see detectOneSeries()
        size_t declined = 0;  // groups resolved but not answered
    };

    // Detect against ONE group and append its pieces, or DECLINE it.  The
    // single place both entry points run a group through, so the gate, the
    // point accounting and the piece layout are the same by construction.
    void detectOneSeries(AnomalyQueryResult& result, AnomalyDetector& detector, const std::vector<uint64_t>& timestamps,
                         const std::vector<double>& values, const std::vector<std::string>& groupTags,
                         size_t queryIndex, const AnomalyConfig& config, SeriesTally& tally);

    // Fill the statistics block from the config and the accumulated tally.
    static void fillStatistics(AnomalyQueryResult& result, const AnomalyConfig& config, const SeriesTally& tally);

    // Add series pieces to result. Takes the AnomalyOutput by rvalue reference
    // and moves its vectors into the result pieces (the output is discarded by
    // all callers). rawValues is still copied — the caller keeps ownership.
    void addSeriesPieces(AnomalyQueryResult& result, const std::vector<double>& rawValues, AnomalyOutput&& output,
                         const std::vector<std::string>& groupTags, size_t queryIndex);

    // Compute alert value (maximum anomaly score)
    double computeAlertValue(const std::vector<double>& scores);
};

}  // namespace anomaly
}  // namespace timestar
