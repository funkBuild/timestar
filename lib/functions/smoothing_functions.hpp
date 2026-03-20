#pragma once

#include "function_types.hpp"

#include <vector>

namespace timestar::functions {

// Base smoothing function with edge handling
class SmoothingFunction {
public:
    enum class EdgeHandling {
        TRUNCATE,     // Truncate results where window is incomplete
        PAD_NEAREST,  // Pad with nearest values
        PAD_ZEROS,    // Pad with zeros
        EXTRAPOLATE   // Extrapolate based on trend
    };
};

// Simple Moving Average Function
class SMAFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Exponential Moving Average Function
class EMAFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Gaussian Smoothing Function
class GaussianSmoothFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Legacy functions for backward compatibility
std::vector<double> simpleMovingAverage(const std::vector<double>& values, int window);
std::vector<double> exponentialMovingAverage(const std::vector<double>& values, double alpha);

}  // namespace timestar::functions
