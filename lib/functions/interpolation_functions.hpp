#ifndef INTERPOLATION_FUNCTIONS_H_INCLUDED
#define INTERPOLATION_FUNCTIONS_H_INCLUDED

#include "function_types.hpp"
#include <vector>
#include <cstdint>

namespace timestar::functions {

// Linear Interpolation Function
class LinearInterpolationFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;
    
    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override;
};

// Spline Interpolation Function
class SplineInterpolationFunction : public IUnaryFunction {
public:
    static const FunctionMetadata metadata_;
    
    const FunctionMetadata& getMetadata() const override;
    std::unique_ptr<IFunction> clone() const override;
    seastar::future<bool> validateParameters(const FunctionContext& context) const override;
    seastar::future<FunctionResult<double>> execute(
        const DoubleSeriesView& input,
        const FunctionContext& context
    ) const override;
};

} // namespace timestar::functions

#endif // INTERPOLATION_FUNCTIONS_H_INCLUDED