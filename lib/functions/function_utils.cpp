#include "function_utils.hpp"
#include <algorithm>
#include <numeric>

namespace tsdb::functions {

double calculateMean(const std::vector<double>& values) {
    if (values.empty()) return 0.0;
    return std::accumulate(values.begin(), values.end(), 0.0) / values.size();
}

std::vector<double> simpleMovingAverage(const std::vector<double>& values, int window) {
    std::vector<double> result;
    if (window <= 0 || values.size() < static_cast<size_t>(window)) {
        return result;
    }
    
    for (size_t i = window - 1; i < values.size(); ++i) {
        double sum = 0.0;
        for (int j = 0; j < window; ++j) {
            sum += values[i - j];
        }
        result.push_back(sum / window);
    }
    
    return result;
}

} // namespace tsdb::functions