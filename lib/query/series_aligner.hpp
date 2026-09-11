#pragma once

#include "derived_query.hpp"
#include "expression_evaluator.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace timestar {

// Strategy for aligning multiple time series
enum class AlignmentStrategy {
    INNER,  // Only keep timestamps present in ALL series (intersection)
    OUTER,  // Keep all timestamps, interpolate missing values
    LEFT,   // Keep timestamps from first series only
    UNION   // Keep all timestamps, use NaN for missing values
};

// Strategy for interpolating missing values
enum class InterpolationMethod {
    LINEAR,    // Linear interpolation between adjacent points
    PREVIOUS,  // Use previous known value (step function)
    NEXT,      // Use next known value
    ZERO,      // Fill with zero
    NAN_FILL   // Fill with NaN
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

    // Resampling refuses to build a grid wider than this and falls back to the
    // un-resampled axis; see resampleTimestamps().  Public so that a caller
    // sizing a result BEFORE asking for it (projectedOutputSize() below, and
    // through it the multi-series /derived bound) cannot drift from the rule
    // align() actually applies.
    static constexpr uint64_t kMaxResamplePoints = 10000000;  // 10M points max

    // Align multiple series to common timestamps
    // Input: map of query name -> (timestamps, values)
    // Output: map of query name -> AlignedSeries with matching timestamps
    std::map<std::string, AlignedSeries> align(const std::map<std::string, SubQueryResult>& series);

    // How many points align() would emit for `series`, computed WITHOUT
    // materialising either the axis or the resampled grid.
    //
    // Exists so a caller can refuse an over-large result BEFORE paying for it.
    // The multi-series /derived path used to check its budget only after a group
    // had been aligned, evaluated and pushed, which meant the FIRST group was
    // never checked at all: four stored points at a one-second interval over a
    // thirty-day window materialised 2,592,001 points (10.4x the budget) and
    // stalled the reactor before anything refused them.
    //
    // EXACT for the strategies whose axis is determined by the inputs' contents:
    //   * INNER  -- the axis is the intersection, which is counted by walking the
    //     inputs' sorted timestamps as a merge: O(sum of lengths), copying no
    //     timestamp and (up to eight sub-queries) allocating nothing.
    //   * LEFT   -- exactly the first input.
    // For these the answer equals align()'s output size, resampling and its
    // kMaxResamplePoints fallback included, so a bound built on it refuses
    // exactly what align() would go on to build -- no more and no less.
    //
    // UNION/OUTER over-state, never under-state.  The COUNT is at most the sum
    // of the inputs' lengths (exact when they share no timestamp, high by the
    // number they do share).  The SPAN, [min(first), max(last)], is exact, so
    // with a target interval the GRID SIZE computed from it is exact -- but
    // that does not make the ANSWER exact, because the grid is not always what
    // align() emits.  Past kMaxResamplePoints align() abandons the grid and
    // falls back to the real union axis, while this falls back to the summed
    // axisSize, and the two differ: legs {0, 10000000} x {0, 10000000} at
    // interval 1 project a grid of 10,000,001 (over the ceiling), so align()
    // returns the 2-point union and this returns 4.  An earlier version of this
    // comment claimed UNION/OUTER were "exact too" under a target interval;
    // they are not, and the fallback is where it breaks.
    //
    // The over-statement is harmless -- nothing here ever UNDER-states, so
    // nothing escapes a bound built on it -- and /derived is hard-configured to
    // INNER, so no caller sees it today.  A caller may still keep an
    // after-the-fact check as a backstop (the /derived path does).
    //
    // Sizing INNER by the interval that merely CONTAINS the intersection --
    // length <= the shortest input, span within [max(first), min(last)] -- was
    // both, and is why this walks the data instead: it refused a legitimate
    // 2,000-point result claiming 299,999, and elsewhere admitted a 9,990,001-
    // point one as 1,002 (see the note in the definition).
    size_t projectedOutputSize(const std::map<std::string, SubQueryResult>& series) const;

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
    std::vector<uint64_t> computeOutputTimestamps(const std::map<std::string, SubQueryResult>& series);

    // Compute intersection of all timestamp sets
    std::vector<uint64_t> computeIntersection(const std::map<std::string, SubQueryResult>& series);

    // Compute union of all timestamp sets
    std::vector<uint64_t> computeUnion(const std::map<std::string, SubQueryResult>& series);

    // Resample timestamps to target interval
    std::vector<uint64_t> resampleTimestamps(const std::vector<uint64_t>& timestamps, uint64_t interval);

    // Interpolate a single series to target timestamps
    std::vector<double> interpolateSeries(const std::vector<uint64_t>& srcTimestamps,
                                          const std::vector<double>& srcValues,
                                          const std::vector<uint64_t>& targetTimestamps);

    // Linear interpolation between two points
    double linearInterpolate(uint64_t t, uint64_t t1, double v1, uint64_t t2, double v2);
};

// Utility functions for time series alignment

// Find common time range across all series
struct TimeRange {
    uint64_t start = 0;
    uint64_t end = 0;
    bool valid = false;
};

inline TimeRange findCommonTimeRange(const std::map<std::string, SubQueryResult>& series) {
    TimeRange range;
    range.valid = false;

    for (const auto& [name, result] : series) {
        if (result.timestamps.empty())
            continue;

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
inline std::vector<uint64_t> generateTimestamps(uint64_t start, uint64_t end, uint64_t interval) {
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

}  // namespace timestar
