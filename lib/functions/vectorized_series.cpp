#include "vectorized_series.hpp"

#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <string>

namespace timestar::functions {

VectorizedSeries::VectorizedSeries() = default;

std::vector<double> VectorizedSeries::add(const std::vector<double>& a, const std::vector<double>& b) {
    if (a.size() != b.size()) {
        throw std::invalid_argument("Vector size mismatch in VectorizedSeries::add: " + std::to_string(a.size()) +
                                    " vs " + std::to_string(b.size()));
    }
    std::vector<double> result(a.size());
    for (size_t i = 0; i < a.size(); ++i) {
        result[i] = a[i] + b[i];
    }
    return result;
}

std::vector<double> VectorizedSeries::multiply(const std::vector<double>& values, double factor) {
    std::vector<double> result(values.size());
    std::transform(values.begin(), values.end(), result.begin(), [factor](double val) { return val * factor; });
    return result;
}

std::vector<double> VectorizedSeries::scale(const std::vector<double>& values, double factor) {
    return multiply(values, factor);
}

}  // namespace timestar::functions