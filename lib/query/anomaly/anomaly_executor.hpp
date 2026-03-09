#ifndef ANOMALY_EXECUTOR_H_INCLUDED
#define ANOMALY_EXECUTOR_H_INCLUDED

#include "anomaly_result.hpp"
#include "anomaly_detector.hpp"
#include <vector>
#include <string>
#include <chrono>

namespace timestar {
namespace anomaly {

// Executes anomaly detection on query results
// Takes input series and produces multi-piece output (raw, upper, lower, scores)
class AnomalyExecutor {
public:
    AnomalyExecutor() = default;

    // Execute anomaly detection on a single series
    AnomalyQueryResult execute(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        const std::vector<std::string>& groupTags,
        const AnomalyConfig& config
    );

    // Execute on multiple series (from multi-group query)
    AnomalyQueryResult executeMulti(
        const std::vector<uint64_t>& sharedTimestamps,
        const std::vector<std::vector<double>>& seriesValues,
        const std::vector<std::vector<std::string>>& seriesGroupTags,
        const AnomalyConfig& config
    );

private:
    // Add series pieces to result
    void addSeriesPieces(
        AnomalyQueryResult& result,
        const std::vector<double>& rawValues,
        const AnomalyOutput& output,
        const std::vector<std::string>& groupTags,
        size_t queryIndex
    );

    // Compute alert value (maximum anomaly score)
    double computeAlertValue(const std::vector<double>& scores);
};

} // namespace anomaly
} // namespace timestar

#endif // ANOMALY_EXECUTOR_H_INCLUDED
