#ifndef INTERPOLATION_FUNCTIONS_H_INCLUDED
#define INTERPOLATION_FUNCTIONS_H_INCLUDED

#include "function_types.hpp"
#include <vector>
#include <cstdint>

namespace tsdb::functions {

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

// Legacy function for backward compatibility
std::vector<double> linearInterpolate(const std::vector<double>& values, 
                                     const std::vector<uint64_t>& timestamps,
                                     uint64_t targetInterval);

} // namespace tsdb::functions

#endif // INTERPOLATION_FUNCTIONS_H_INCLUDED