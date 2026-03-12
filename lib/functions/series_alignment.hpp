#ifndef SERIES_ALIGNMENT_H_INCLUDED
#define SERIES_ALIGNMENT_H_INCLUDED

#include "function_types.hpp"

#include <cstdint>
#include <vector>

namespace timestar::functions {

// Alignment utilities namespace
namespace alignment_utils {

class DoubleAligner {
public:
    static double safeInterpolate(double v1, double v2, double ratio);
    static bool isValidValue(double value);
};

class BoolAligner {
public:
    static bool interpolateBoolean(bool v1, bool v2, double ratio);
};

class StringAligner {
public:
    static std::string interpolateString(const std::string& v1, const std::string& v2, double ratio);
};
}  // namespace alignment_utils

class SeriesAlignment {
public:
    SeriesAlignment();

    std::vector<double> alignSeries(const std::vector<double>& values, const std::vector<uint64_t>& timestamps,
                                    uint64_t targetInterval);
};

}  // namespace timestar::functions

#endif  // SERIES_ALIGNMENT_H_INCLUDED