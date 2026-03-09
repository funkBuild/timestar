#ifndef SERIES_ALIGNER_H_INCLUDED
#define SERIES_ALIGNER_H_INCLUDED

#include "expression_evaluator.hpp"
#include "derived_query.hpp"
#include <map>
#include <vector>
#include <string>
#include <set>
#include <algorithm>
#include <cmath>
#include <limits>

namespace timestar {

// Strategy for aligning multiple time series
enum class AlignmentStrategy {
    INNER,      // Only keep timestamps present in ALL series (intersection)
    OUTER,      // Keep all timestamps, interpolate missing values
    LEFT,       // Keep timestamps from first series only
    UNION       // Keep all timestamps, use NaN for missing values
};

// Strategy for interpolating missing values
enum class InterpolationMethod {
    LINEAR,     // Linear interpolation between adjacent points
    PREVIOUS,   // Use previous known value (step function)
    NEXT,       // Use next known value
    ZERO,       // Fill with zero
    NAN_FILL    // Fill with NaN
};

// Statistics about alignment operations
struct AlignmentStats {
    size_t inputSeriesCount = 0;
    size_t outputPointCount = 0;
    size_t pointsDropped = 0;
    size_t pointsInterpolated = 0;
    std::vector<size_t> inputPointCounts;
};

// Aligns multiple time series to common timestamps
class SeriesAligner {
public:
    SeriesAligner(AlignmentStrategy strategy = AlignmentStrategy::INNER,
                  InterpolationMethod interpolation = InterpolationMethod::LINEAR)
        : strategy_(strategy), interpolation_(interpolation) {}

    // Align multiple series to common timestamps
    // Input: map of query name -> (timestamps, values)
    // Output: map of query name -> AlignedSeries with matching timestamps
    std::map<std::string, AlignedSeries> align(
        const std::map<std::string, SubQueryResult>& series);

    // Get statistics from the last alignment operation
    const AlignmentStats& getStats() const { return stats_; }

    // Set the target interval for resampling (0 = no resampling)
    void setTargetInterval(uint64_t interval) { targetInterval_ = interval; }

private:
    AlignmentStrategy strategy_;
    InterpolationMethod interpolation_;
    uint64_t targetInterval_ = 0;
    AlignmentStats stats_;

    // Compute the set of output timestamps based on strategy
    std::vector<uint64_t> computeOutputTimestamps(
        const std::map<std::string, SubQueryResult>& series);

    // Compute intersection of all timestamp sets
    std::vector<uint64_t> computeIntersection(
        const std::map<std::string, SubQueryResult>& series);

    // Compute union of all timestamp sets
    std::vector<uint64_t> computeUnion(
        const std::map<std::string, SubQueryResult>& series);

    // Resample timestamps to target interval
    std::vector<uint64_t> resampleTimestamps(
        const std::vector<uint64_t>& timestamps,
        uint64_t interval);

    // Interpolate a single series to target timestamps
    std::vector<double> interpolateSeries(
        const std::vector<uint64_t>& srcTimestamps,
        const std::vector<double>& srcValues,
        const std::vector<uint64_t>& targetTimestamps);

    // Linear interpolation between two points
    double linearInterpolate(uint64_t t, uint64_t t1, double v1,
                            uint64_t t2, double v2);

    // Find the index of the largest timestamp <= target
    size_t findLowerBound(const std::vector<uint64_t>& timestamps, uint64_t target);
};

// Utility functions for time series alignment

// Find common time range across all series
struct TimeRange {
    uint64_t start = 0;
    uint64_t end = 0;
    bool valid = false;
};

inline TimeRange findCommonTimeRange(
    const std::map<std::string, SubQueryResult>& series) {
    TimeRange range;
    range.valid = false;

    for (const auto& [name, result] : series) {
        if (result.timestamps.empty()) continue;

        uint64_t seriesStart = result.timestamps.front();
        uint64_t seriesEnd = result.timestamps.back();

        if (!range.valid) {
            range.start = seriesStart;
            range.end = seriesEnd;
            range.valid = true;
        } else {
            range.start = std::max(range.start, seriesStart);
            range.end = std::min(range.end, seriesEnd);
        }
    }

    // Check if range is valid (start <= end)
    if (range.valid && range.start > range.end) {
        range.valid = false;
    }

    return range;
}

// Generate evenly spaced timestamps within a range
inline std::vector<uint64_t> generateTimestamps(
    uint64_t start, uint64_t end, uint64_t interval) {
    std::vector<uint64_t> timestamps;
    if (interval == 0 || start > end) {
        return timestamps;
    }

    for (uint64_t t = start; t <= end; t += interval) {
        timestamps.push_back(t);
        // Guard against uint64 overflow: if t + interval would wrap around, stop
        if (t > std::numeric_limits<uint64_t>::max() - interval) {
            break;
        }
    }
    return timestamps;
}

} // namespace timestar

#endif // SERIES_ALIGNER_H_INCLUDED
