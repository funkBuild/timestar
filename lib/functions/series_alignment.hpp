#ifndef __SERIES_ALIGNMENT_H_INCLUDED__
#define __SERIES_ALIGNMENT_H_INCLUDED__

#include "function_types.hpp"
#include <vector>
#include <cstdint>

namespace tsdb::functions {

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
}

class SeriesAlignment {
public:
    SeriesAlignment();
    
    std::vector<double> alignSeries(const std::vector<double>& values, 
                                   const std::vector<uint64_t>& timestamps,
                                   uint64_t targetInterval);
};

} // namespace tsdb::functions

#endif // __SERIES_ALIGNMENT_H_INCLUDED__