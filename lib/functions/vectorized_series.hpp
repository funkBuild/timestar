#ifndef VECTORIZED_SERIES_H_INCLUDED
#define VECTORIZED_SERIES_H_INCLUDED

#include "function_types.hpp"
#include <vector>

namespace tsdb::functions {

// Non-template class for backward compatibility and the actual implementation
class VectorizedSeries {
public:
    VectorizedSeries();
    
    std::vector<double> add(const std::vector<double>& a, const std::vector<double>& b);
    std::vector<double> multiply(const std::vector<double>& values, double factor);
    std::vector<double> scale(const std::vector<double>& values, double factor);
};

// Template vectorized series class
template<typename T>
class VectorizedSeriesTemplate {
private:
    std::vector<T> data_;

public:
    explicit VectorizedSeriesTemplate(const std::vector<T>& values) : data_(values) {}
    VectorizedSeriesTemplate() = default;
    
    size_t size() const { return data_.size(); }
    bool empty() const { return data_.empty(); }
    
    T& operator[](size_t index) { return data_[index]; }
    const T& operator[](size_t index) const { return data_[index]; }
    
    std::vector<T> add(const std::vector<T>& a, const std::vector<T>& b) {
        if (a.size() != b.size()) {
            throw std::invalid_argument("Vector size mismatch");
        }
        std::vector<T> result;
        result.reserve(a.size());
        for (size_t i = 0; i < a.size(); ++i) {
            result.push_back(a[i] + b[i]);
        }
        return result;
    }
    
    std::vector<T> multiply(const std::vector<T>& values, T factor) {
        std::vector<T> result;
        result.reserve(values.size());
        for (const auto& val : values) {
            result.push_back(val * factor);
        }
        return result;
    }
    
    std::vector<T> scale(const std::vector<T>& values, T factor) {
        return multiply(values, factor);
    }
};

} // namespace tsdb::functions

#endif // VECTORIZED_SERIES_H_INCLUDED