#include "series_aligner.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace timestar {

std::map<std::string, AlignedSeries> SeriesAligner::align(const std::map<std::string, SubQueryResult>& series) {
    stats_ = AlignmentStats();
    stats_.inputSeriesCount = series.size();

    if (series.empty()) {
        return {};
    }

    // Record input point counts
    for (const auto& [name, result] : series) {
        stats_.inputPointCounts.push_back(result.timestamps.size());
    }

    // Compute output timestamps based on strategy
    std::vector<uint64_t> outputTimestamps = computeOutputTimestamps(series);

    // If target interval is set, resample
    if (targetInterval_ > 0 && !outputTimestamps.empty()) {
        outputTimestamps = resampleTimestamps(outputTimestamps, targetInterval_);
    }

    stats_.outputPointCount = outputTimestamps.size();

    if (outputTimestamps.empty()) {
        return {};
    }

    // Align each series to the output timestamps
    std::map<std::string, AlignedSeries> result;

    // Share one timestamp vector across all aligned series (avoids N copies)
    auto sharedTimestamps = std::make_shared<const std::vector<uint64_t>>(std::move(outputTimestamps));

    for (const auto& [name, subResult] : series) {
        std::vector<double> alignedValues = interpolateSeries(subResult.timestamps, subResult.values, *sharedTimestamps);

        result[name] = AlignedSeries(sharedTimestamps, std::move(alignedValues));
    }

    return result;
}

std::vector<uint64_t> SeriesAligner::computeOutputTimestamps(const std::map<std::string, SubQueryResult>& series) {
    switch (strategy_) {
        case AlignmentStrategy::INNER:
            return computeIntersection(series);

        case AlignmentStrategy::OUTER:
        case AlignmentStrategy::UNION:
            return computeUnion(series);

        case AlignmentStrategy::LEFT: {
            // Use timestamps from the first series
            auto it = series.begin();
            if (it != series.end()) {
                return it->second.timestamps;
            }
            return {};
        }

        default:
            return computeIntersection(series);
    }
}

std::vector<uint64_t> SeriesAligner::computeIntersection(const std::map<std::string, SubQueryResult>& series) {
    if (series.empty()) {
        return {};
    }

    // Start with first series timestamps (already sorted)
    auto it = series.begin();
    std::vector<uint64_t> intersection = it->second.timestamps;

    // Intersect with each subsequent series using std::set_intersection
    // directly on sorted vectors -- O(n) per series, no heap allocations
    for (++it; it != series.end(); ++it) {
        std::vector<uint64_t> temp;
        temp.reserve(std::min(intersection.size(), it->second.timestamps.size()));
        std::set_intersection(intersection.begin(), intersection.end(), it->second.timestamps.begin(),
                              it->second.timestamps.end(), std::back_inserter(temp));

        intersection = std::move(temp);

        if (intersection.empty()) {
            break;
        }
    }

    // Calculate points dropped (guard against underflow)
    for (const auto& [name, result] : series) {
        if (result.timestamps.size() > intersection.size()) {
            stats_.pointsDropped += result.timestamps.size() - intersection.size();
        }
    }

    return intersection;
}

std::vector<uint64_t> SeriesAligner::computeUnion(const std::map<std::string, SubQueryResult>& series) {
    if (series.empty())
        return {};

    // Start with the first series' timestamps (already sorted)
    auto it = series.begin();
    std::vector<uint64_t> merged = it->second.timestamps;

    // Iteratively merge each subsequent sorted series using set_union
    for (++it; it != series.end(); ++it) {
        if (it->second.timestamps.empty())
            continue;
        std::vector<uint64_t> temp;
        temp.reserve(merged.size() + it->second.timestamps.size());
        std::set_union(merged.begin(), merged.end(), it->second.timestamps.begin(), it->second.timestamps.end(),
                       std::back_inserter(temp));
        merged = std::move(temp);
    }

    return merged;
}

std::vector<uint64_t> SeriesAligner::resampleTimestamps(const std::vector<uint64_t>& timestamps, uint64_t interval) {
    if (timestamps.empty() || interval == 0) {
        return timestamps;
    }

    uint64_t start = timestamps.front();
    uint64_t end = timestamps.back();

    // Guard against start > end (shouldn't happen with sorted timestamps, but be safe)
    if (start > end) {
        return {};
    }

    // Align start to interval boundary
    start = (start / interval) * interval;

    std::vector<uint64_t> resampled;

    // Pre-calculate expected count to avoid runaway loops
    // Guard against extremely small intervals that would create too many points
    uint64_t range = end - start;
    uint64_t expectedCount = range / interval + 1;
    static constexpr uint64_t MAX_RESAMPLE_POINTS = 10000000;  // 10M points max
    if (expectedCount > MAX_RESAMPLE_POINTS) {
        // Interval too small for the range; return original timestamps
        return timestamps;
    }

    resampled.reserve(static_cast<size_t>(expectedCount));
    for (uint64_t t = start; t <= end; t += interval) {
        resampled.push_back(t);
        // Guard against uint64 overflow: if t + interval would wrap around, stop
        if (t > std::numeric_limits<uint64_t>::max() - interval) {
            break;
        }
    }

    return resampled;
}

std::vector<double> SeriesAligner::interpolateSeries(const std::vector<uint64_t>& srcTimestamps,
                                                     const std::vector<double>& srcValues,
                                                     const std::vector<uint64_t>& targetTimestamps) {
    std::vector<double> result;
    result.reserve(targetTimestamps.size());

    if (srcTimestamps.size() != srcValues.size()) {
        throw std::invalid_argument("interpolateSeries: srcTimestamps/srcValues size mismatch");
    }
    if (srcTimestamps.empty() || srcValues.empty()) {
        // Fill with NaN if source is empty
        result.resize(targetTimestamps.size(), std::numeric_limits<double>::quiet_NaN());
        return result;
    }

    size_t srcIdx = 0;
    size_t srcSize = srcTimestamps.size();

    for (uint64_t targetTs : targetTimestamps) {
        // Find position in source series
        while (srcIdx < srcSize - 1 && srcTimestamps[srcIdx + 1] <= targetTs) {
            srcIdx++;
        }

        if (srcIdx >= srcSize) {
            // Past the end of source data
            switch (interpolation_) {
                case InterpolationMethod::PREVIOUS:
                    result.push_back(srcValues.back());
                    break;
                case InterpolationMethod::ZERO:
                    result.push_back(0.0);
                    break;
                default:
                    result.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            continue;
        }

        // Exact match
        if (srcTimestamps[srcIdx] == targetTs) {
            result.push_back(srcValues[srcIdx]);
            continue;
        }

        // Target is before first source timestamp
        if (targetTs < srcTimestamps[0]) {
            switch (interpolation_) {
                case InterpolationMethod::NEXT:
                    result.push_back(srcValues[0]);
                    break;
                case InterpolationMethod::ZERO:
                    result.push_back(0.0);
                    break;
                case InterpolationMethod::LINEAR:
                    // Can't extrapolate backwards, use first value
                    result.push_back(srcValues[0]);
                    stats_.pointsInterpolated++;
                    break;
                default:
                    result.push_back(std::numeric_limits<double>::quiet_NaN());
            }
            continue;
        }

        // Need to interpolate between srcIdx and srcIdx+1
        if (srcIdx + 1 < srcSize) {
            switch (interpolation_) {
                case InterpolationMethod::LINEAR: {
                    double interpolated = linearInterpolate(targetTs, srcTimestamps[srcIdx], srcValues[srcIdx],
                                                            srcTimestamps[srcIdx + 1], srcValues[srcIdx + 1]);
                    result.push_back(interpolated);
                    stats_.pointsInterpolated++;
                    break;
                }
                case InterpolationMethod::PREVIOUS:
                    result.push_back(srcValues[srcIdx]);
                    stats_.pointsInterpolated++;
                    break;
                case InterpolationMethod::NEXT:
                    result.push_back(srcValues[srcIdx + 1]);
                    stats_.pointsInterpolated++;
                    break;
                case InterpolationMethod::ZERO:
                    result.push_back(0.0);
                    stats_.pointsInterpolated++;
                    break;
                case InterpolationMethod::NAN_FILL:
                default:
                    result.push_back(std::numeric_limits<double>::quiet_NaN());
                    break;
            }
        } else {
            // At the last point
            result.push_back(srcValues[srcIdx]);
        }
    }

    return result;
}

double SeriesAligner::linearInterpolate(uint64_t t, uint64_t t1, double v1, uint64_t t2, double v2) {
    if (t1 == t2) {
        return v1;
    }

    double ratio = static_cast<double>(t - t1) / static_cast<double>(t2 - t1);
    return v1 + ratio * (v2 - v1);
}

size_t SeriesAligner::findLowerBound(const std::vector<uint64_t>& timestamps, uint64_t target) {
    auto it = std::lower_bound(timestamps.begin(), timestamps.end(), target);
    if (it == timestamps.end()) {
        return timestamps.size() - 1;
    }
    if (*it == target) {
        return std::distance(timestamps.begin(), it);
    }
    if (it == timestamps.begin()) {
        return 0;
    }
    return std::distance(timestamps.begin(), it) - 1;
}

}  // namespace timestar
