#include "vectorized_series.hpp"
#include <algorithm>
#include <numeric>

namespace tsdb::functions {

VectorizedSeries::VectorizedSeries() = default;

std::vector<double> VectorizedSeries::add(const std::vector<double>& a, const std::vector<double>& b) {
    size_t minSize = std::min(a.size(), b.size());
    std::vector<double> result(minSize);
    
    for (size_t i = 0; i < minSize; ++i) {
        result[i] = a[i] + b[i];
    }
    
    return result;
}

std::vector<double> VectorizedSeries::multiply(const std::vector<double>& values, double factor) {
    std::vector<double> result(values.size());
    std::transform(values.begin(), values.end(), result.begin(),
                   [factor](double val) { return val * factor; });
    return result;
}

std::vector<double> VectorizedSeries::scale(const std::vector<double>& values, double factor) {
    return multiply(values, factor);
}

} // namespace tsdb::functions