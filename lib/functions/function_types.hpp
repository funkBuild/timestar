#ifndef __FUNCTION_TYPES_H_INCLUDED__
#define __FUNCTION_TYPES_H_INCLUDED__

#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <map>
#include <variant>
#include <stdexcept>
#include <cstdint>
#include <seastar/core/future.hh>

namespace tsdb::functions {

// Parameter value types for function context
using ParameterValue = std::variant<int64_t, double, std::string, bool>;

// Function categories for organization
enum class FunctionCategory {
    ARITHMETIC,
    SMOOTHING,
    AGGREGATION,
    TRANSFORMATION,
    STATISTICAL,
    OTHER
};

// Forward declarations
template<typename T>
struct FunctionResult;
class FunctionContext;
class IFunction;
class IUnaryFunction;

// Time series view for reading data
template<typename T>
class TimeSeriesView {
public:
    const std::vector<uint64_t>* timestamps;
    const std::vector<T>* values;
    size_t startIndex;
    size_t count;

    // Default constructor for member variable initialization
    TimeSeriesView() : timestamps(nullptr), values(nullptr), startIndex(0), count(0) {}
    
    TimeSeriesView(const std::vector<uint64_t>* ts, const std::vector<T>* vals, 
                   size_t start = 0, size_t cnt = 0)
        : timestamps(ts), values(vals), startIndex(start), 
          count(cnt == 0 ? (vals ? vals->size() - start : 0) : cnt) {}

    size_t size() const { return count; }
    bool empty() const { return count == 0; }
    
    uint64_t timestampAt(size_t index) const {
        return (*timestamps)[startIndex + index];
    }
    
    T valueAt(size_t index) const {
        return (*values)[startIndex + index];
    }

    // Iterator support for range-based loops
    struct iterator {
        const TimeSeriesView* view;
        size_t index;
        
        iterator(const TimeSeriesView* v, size_t idx) : view(v), index(idx) {}
        
        std::pair<uint64_t, T> operator*() const {
            return {view->timestampAt(index), view->valueAt(index)};
        }
        
        iterator& operator++() { ++index; return *this; }
        bool operator!=(const iterator& other) const { return index != other.index; }
    };
    
    iterator begin() const { return iterator(this, 0); }
    iterator end() const { return iterator(this, count); }
};

// Common type aliases
using DoubleSeriesView = TimeSeriesView<double>;
using BoolSeriesView = TimeSeriesView<bool>;
using StringSeriesView = TimeSeriesView<std::string>;

// Function result container
template<typename T>
struct FunctionResult {
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    std::chrono::microseconds executionTime{0};

    size_t size() const { return timestamps.size(); }
    bool empty() const { return timestamps.empty(); }

    TimeSeriesView<T> asView() const {
        return TimeSeriesView<T>(&timestamps, &values);
    }
};

// Function context for parameters
class FunctionContext {
private:
    std::map<std::string, ParameterValue> parameters_;

public:
    void setParameter(const std::string& name, const ParameterValue& value) {
        parameters_[name] = value;
    }

    bool hasParameter(const std::string& name) const {
        return parameters_.find(name) != parameters_.end();
    }

    template<typename T>
    T getParameter(const std::string& name) const {
        auto it = parameters_.find(name);
        if (it == parameters_.end()) {
            throw std::invalid_argument("Parameter not found: " + name);
        }
        return std::get<T>(it->second);
    }

    template<typename T>
    T getParameter(const std::string& name, const T& defaultValue) const {
        auto it = parameters_.find(name);
        if (it == parameters_.end()) {
            return defaultValue;
        }
        return std::get<T>(it->second);
    }
};

// Parameter metadata for function parameters
struct ParameterMetadata {
    std::string name;
    std::string type;
    bool required = false;
    std::string description;
    std::string defaultValue;
};

// Function metadata
struct FunctionMetadata {
    std::string name;
    std::string description;
    FunctionCategory category;
    std::vector<std::string> supportedInputTypes;
    std::string outputType;
    std::vector<ParameterMetadata> parameters; // Changed from map to vector
    
    // Additional fields expected by tests
    bool supportsVectorization = true;
    bool supportsStreaming = true;
    size_t minDataPoints = 1;
    std::vector<std::string> examples;
};

// Exception hierarchy
class FunctionException : public std::runtime_error {
public:
    explicit FunctionException(const std::string& message)
        : std::runtime_error("Function error: " + message) {}
};

class ParameterValidationException : public FunctionException {
public:
    explicit ParameterValidationException(const std::string& message)
        : FunctionException("Parameter validation failed: " + message) {}
};

class InsufficientDataException : public FunctionException {
public:
    explicit InsufficientDataException(const std::string& message)
        : FunctionException("Insufficient data: " + message) {}
};

// Base function interface
class IFunction {
public:
    virtual ~IFunction() = default;
    virtual const FunctionMetadata& getMetadata() const = 0;
    virtual std::unique_ptr<IFunction> clone() const = 0;
    virtual seastar::future<bool> validateParameters(const FunctionContext& context) const = 0;
    
    std::string getName() const { return getMetadata().name; }
};

// Function wrapper that provides registry metadata
class RegistryFunctionWrapper : public IFunction {
private:
    std::unique_ptr<IFunction> wrappedFunction_;
    FunctionMetadata registryMetadata_;

public:
    RegistryFunctionWrapper(std::unique_ptr<IFunction> function, const FunctionMetadata& metadata)
        : wrappedFunction_(std::move(function)), registryMetadata_(metadata) {}

    const FunctionMetadata& getMetadata() const override {
        return registryMetadata_;
    }

    std::unique_ptr<IFunction> clone() const override {
        return std::make_unique<RegistryFunctionWrapper>(wrappedFunction_->clone(), registryMetadata_);
    }

    seastar::future<bool> validateParameters(const FunctionContext& context) const override {
        return wrappedFunction_->validateParameters(context);
    }

    // Get the wrapped function for access to specific functionality
    IFunction* getWrappedFunction() const { return wrappedFunction_.get(); }
};

// Unary function interface (single input series)
class IUnaryFunction : public IFunction {
public:
    virtual seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const = 0;
};

// Unary function wrapper that provides registry metadata
class RegistryUnaryFunctionWrapper : public IUnaryFunction {
private:
    std::unique_ptr<IUnaryFunction> wrappedFunction_;
    FunctionMetadata registryMetadata_;

public:
    RegistryUnaryFunctionWrapper(std::unique_ptr<IUnaryFunction> function, const FunctionMetadata& metadata)
        : wrappedFunction_(std::move(function)), registryMetadata_(metadata) {}

    const FunctionMetadata& getMetadata() const override {
        return registryMetadata_;
    }

    std::unique_ptr<IFunction> clone() const override {
        auto clonedFunction = wrappedFunction_->clone();
        auto castedFunction = dynamic_cast<IUnaryFunction*>(clonedFunction.release());
        if (!castedFunction) {
            throw std::runtime_error("Failed to cast cloned function to IUnaryFunction");
        }
        return std::make_unique<RegistryUnaryFunctionWrapper>(
            std::unique_ptr<IUnaryFunction>(castedFunction), 
            registryMetadata_
        );
    }

    seastar::future<bool> validateParameters(const FunctionContext& context) const override {
        return wrappedFunction_->validateParameters(context);
    }

    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override {
        return wrappedFunction_->execute(input, context);
    }

    // Get the wrapped function for access to specific functionality
    IUnaryFunction* getWrappedFunction() const { return wrappedFunction_.get(); }
};

// Vectorized series for SIMD operations
class VectorizedDoubleSeries {
private:
    std::vector<double> data_;

public:
    explicit VectorizedDoubleSeries(const std::vector<double>& values) : data_(values) {}

    size_t size() const { return data_.size(); }
    bool empty() const { return data_.empty(); }
    bool isSimdAligned() const { return true; } // Simplified for now

    double& operator[](size_t index) { return data_[index]; }
    const double& operator[](size_t index) const { return data_[index]; }

    // Arithmetic operations
    VectorizedDoubleSeries& operator+=(double scalar) {
        for (auto& val : data_) val += scalar;
        return *this;
    }

    VectorizedDoubleSeries& operator*=(double scalar) {
        for (auto& val : data_) val *= scalar;
        return *this;
    }

    VectorizedDoubleSeries& operator+=(const VectorizedDoubleSeries& other) {
        if (data_.size() != other.data_.size()) {
            throw std::invalid_argument("Series size mismatch");
        }
        for (size_t i = 0; i < data_.size(); ++i) {
            data_[i] += other.data_[i];
        }
        return *this;
    }

    VectorizedDoubleSeries& operator-=(const VectorizedDoubleSeries& other) {
        if (data_.size() != other.data_.size()) {
            throw std::invalid_argument("Series size mismatch");
        }
        for (size_t i = 0; i < data_.size(); ++i) {
            data_[i] -= other.data_[i];
        }
        return *this;
    }

    VectorizedDoubleSeries& operator*=(const VectorizedDoubleSeries& other) {
        if (data_.size() != other.data_.size()) {
            throw std::invalid_argument("Series size mismatch");
        }
        for (size_t i = 0; i < data_.size(); ++i) {
            data_[i] *= other.data_[i];
        }
        return *this;
    }

    VectorizedDoubleSeries& operator/=(const VectorizedDoubleSeries& other) {
        if (data_.size() != other.data_.size()) {
            throw std::invalid_argument("Series size mismatch");
        }
        for (size_t i = 0; i < data_.size(); ++i) {
            data_[i] /= other.data_[i];
        }
        return *this;
    }

    // Aggregation operations
    double sum() const {
        double result = 0.0;
        for (const auto& val : data_) result += val;
        return result;
    }

    double average() const {
        return empty() ? 0.0 : sum() / data_.size();
    }

    double minimum() const {
        if (empty()) return 0.0;
        double result = data_[0];
        for (const auto& val : data_) {
            if (val < result) result = val;
        }
        return result;
    }

    double maximum() const {
        if (empty()) return 0.0;
        double result = data_[0];
        for (const auto& val : data_) {
            if (val > result) result = val;
        }
        return result;
    }
};

// Aligned vectorized series (with timestamps)
class AlignedVectorizedSeries {
private:
    std::vector<uint64_t> timestamps_;
    VectorizedDoubleSeries values_;

public:
    AlignedVectorizedSeries(const std::vector<uint64_t>& timestamps,
                           const std::vector<double>& values)
        : timestamps_(timestamps), values_(values) {}

    size_t size() const { return timestamps_.size(); }
    bool empty() const { return timestamps_.empty(); }

    uint64_t timestampAt(size_t index) const { return timestamps_[index]; }
    double valueAt(size_t index) const { return values_[index]; }

    FunctionResult<double> toFunctionResult() const {
        FunctionResult<double> result;
        result.timestamps = timestamps_;
        result.values = std::vector<double>(size());
        for (size_t i = 0; i < size(); ++i) {
            result.values[i] = values_[i];
        }
        return result;
    }

    AlignedVectorizedSeries slice(uint64_t startTime, uint64_t endTime) const {
        std::vector<uint64_t> slicedTimestamps;
        std::vector<double> slicedValues;
        
        for (size_t i = 0; i < size(); ++i) {
            if (timestamps_[i] >= startTime && timestamps_[i] <= endTime) {
                slicedTimestamps.push_back(timestamps_[i]);
                slicedValues.push_back(values_[i]);
            }
        }
        
        return AlignedVectorizedSeries(slicedTimestamps, slicedValues);
    }
};

// Function utilities
class FunctionUtils {
public:
    static void validateMinDataPoints(size_t actual, size_t required, const std::string& functionName) {
        if (actual < required) {
            throw InsufficientDataException(
                functionName + " requires at least " + std::to_string(required) + 
                " data points, but got " + std::to_string(actual)
            );
        }
    }

    template<typename T>
    static T validateAndGetParameter(const FunctionContext& context, const std::string& name) {
        if (!context.hasParameter(name)) {
            throw ParameterValidationException("Required parameter missing: " + name);
        }
        return context.getParameter<T>(name);
    }

    template<typename T>
    static T validateAndGetParameter(const FunctionContext& context, const std::string& name, const T& defaultValue) {
        return context.getParameter<T>(name, defaultValue);
    }

    static std::pair<std::vector<uint64_t>, std::pair<std::vector<size_t>, std::vector<size_t>>> 
    alignTimeSeries(const std::vector<uint64_t>& timestamps1, const std::vector<uint64_t>& timestamps2) {
        std::vector<uint64_t> alignedTimestamps;
        std::vector<size_t> indices1, indices2;

        size_t i1 = 0, i2 = 0;
        while (i1 < timestamps1.size() && i2 < timestamps2.size()) {
            if (timestamps1[i1] == timestamps2[i2]) {
                alignedTimestamps.push_back(timestamps1[i1]);
                indices1.push_back(i1);
                indices2.push_back(i2);
                ++i1;
                ++i2;
            } else if (timestamps1[i1] < timestamps2[i2]) {
                ++i1;
            } else {
                ++i2;
            }
        }

        return {alignedTimestamps, {indices1, indices2}};
    }

    static FunctionResult<double> interpolateLinear(const DoubleSeriesView& input, 
                                                  const std::vector<uint64_t>& targetTimestamps) {
        FunctionResult<double> result;
        result.timestamps = targetTimestamps;
        result.values.reserve(targetTimestamps.size());

        for (uint64_t targetTime : targetTimestamps) {
            // Find surrounding points for interpolation
            size_t i = 0;
            while (i + 1 < input.size() && input.timestampAt(i + 1) < targetTime) {
                ++i;
            }

            if (i + 1 >= input.size()) {
                // Extrapolate from last point
                result.values.push_back(input.valueAt(input.size() - 1));
            } else {
                // Linear interpolation
                uint64_t t1 = input.timestampAt(i);
                uint64_t t2 = input.timestampAt(i + 1);
                double v1 = input.valueAt(i);
                double v2 = input.valueAt(i + 1);

                if (t1 == t2) {
                    result.values.push_back(v1);
                } else {
                    double ratio = static_cast<double>(targetTime - t1) / static_cast<double>(t2 - t1);
                    double interpolated = v1 + ratio * (v2 - v1);
                    result.values.push_back(interpolated);
                }
            }
        }

        return result;
    }
};

} // namespace tsdb::functions

#endif // __FUNCTION_TYPES_H_INCLUDED__