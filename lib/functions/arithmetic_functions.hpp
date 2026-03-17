#pragma once

#include "function_types.hpp"

#include <vector>

namespace timestar::functions {

// Add Function
class AddFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Subtract Function
class SubtractFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Multiply Function
class MultiplyFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Divide Function
class DivideFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Scale Function (alias for multiply)
class ScaleFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Offset Function (alias for add)
class OffsetFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;

    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(const DoubleSeriesView& input,
                                                    const FunctionContext& context) const override;
};

// Legacy functions for backward compatibility
std::vector<double> add(const std::vector<double>& values, double operand);
std::vector<double> multiply(const std::vector<double>& values, double factor);
std::vector<double> scale(const std::vector<double>& values, double factor);
std::vector<double> offset(const std::vector<double>& values, double offset_value);

}  // namespace timestar::functions
