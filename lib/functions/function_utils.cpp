#include "function_utils.hpp"

#include <algorithm>
#include <numeric>

namespace timestar::functions {

double calculateMean(const std::vector<double>& values) {
    if (values.empty())
        return 0.0;
    return std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}

}  // namespace timestar::functions
