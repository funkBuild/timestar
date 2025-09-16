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
    // Stub implementation for now
    return values;
}

} // namespace tsdb::functions