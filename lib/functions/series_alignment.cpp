#include "series_alignment.hpp"
#include <cmath>
#include <limits>

namespace tsdb::functions {

// Alignment utilities implementations
namespace alignment_utils {
    
    double DoubleAligner::safeInterpolate(double v1, double v2, double ratio) {
        if (std::isnan(v1) || std::isnan(v2) || std::isnan(ratio)) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        if (ratio < 0.0 || ratio > 1.0) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        return v1 + ratio * (v2 - v1);
    }
    
    bool DoubleAligner::isValidValue(double value) {
        return !std::isnan(value) && !std::isinf(value);
    }
    
    bool BoolAligner::interpolateBoolean(bool v1, bool v2, double ratio) {
        // Simple threshold-based interpolation: < 0.5 = first value, >= 0.5 = second value
        return ratio < 0.5 ? v1 : v2;
    }
    
    std::string StringAligner::interpolateString(const std::string& v1, const std::string& v2, double ratio) {
        // Simple threshold-based interpolation: < 0.5 = first value, >= 0.5 = second value
        return ratio < 0.5 ? v1 : v2;
    }
}

SeriesAlignment::SeriesAlignment() = default;

std::vector<double> SeriesAlignment::alignSeries(const std::vector<double>& values,
                                                const std::vector<uint64_t>& timestamps,
                                                uint64_t targetInterval) {
    // Degenerate cases: return original values unchanged
    if (timestamps.empty() || values.empty()) {
        return {};
    }
    if (targetInterval == 0) {
        return values;
    }
    if (timestamps.size() == 1) {
        // Single point: output is one value at that timestamp
        return {values[0]};
    }

    // Build a regular grid from timestamps.front() to timestamps.back()
    // aligned to multiples of targetInterval starting at the first timestamp.
    const uint64_t start = timestamps.front();
    const uint64_t end   = timestamps.back();

    // Compute output count defensively to avoid huge allocations
    const uint64_t range         = end - start;
    const uint64_t expectedCount = range / targetInterval + 1;
    static constexpr uint64_t MAX_OUTPUT_POINTS = 10'000'000ULL;
    if (expectedCount > MAX_OUTPUT_POINTS) {
        // Interval too fine for the range; return original values unchanged
        return values;
    }

    std::vector<double> result;
    result.reserve(static_cast<size_t>(expectedCount));

    // Linear scan through source series; advance srcIdx as grid time moves forward.
    size_t srcIdx = 0;
    const size_t srcSize = timestamps.size();

    for (uint64_t t = start; t <= end; t += targetInterval) {
        // Advance srcIdx so that srcTimestamps[srcIdx] is the last source
        // timestamp <= t (i.e., the left bracket of the interpolation window).
        while (srcIdx + 1 < srcSize && timestamps[srcIdx + 1] <= t) {
            ++srcIdx;
        }

        // Exact match — emit source value directly
        if (timestamps[srcIdx] == t) {
            result.push_back(values[srcIdx]);
        } else if (srcIdx + 1 < srcSize) {
            // Interpolate between srcIdx (left) and srcIdx+1 (right)
            const uint64_t t1 = timestamps[srcIdx];
            const uint64_t t2 = timestamps[srcIdx + 1];
            const double   v1 = values[srcIdx];
            const double   v2 = values[srcIdx + 1];
            const double ratio = static_cast<double>(t - t1) /
                                 static_cast<double>(t2 - t1);
            result.push_back(alignment_utils::DoubleAligner::safeInterpolate(v1, v2, ratio));
        } else {
            // t is past the last source point — forward-fill last known value
            result.push_back(values[srcIdx]);
        }

        // Guard against uint64_t overflow on the last iteration
        if (t > std::numeric_limits<uint64_t>::max() - targetInterval) {
            break;
        }
    }

    return result;
}

} // namespace tsdb::functions