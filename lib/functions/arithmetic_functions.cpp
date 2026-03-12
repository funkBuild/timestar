#include "arithmetic_functions.hpp"

#include <algorithm>
#include <numeric>

namespace timestar::functions {

// AddFunction implementation
const FunctionMetadata AddFunction::metadata_ = {
    .name = "add",
    .description = "Add a constant value to each point in the series",
    .category = FunctionCategory::ARITHMETIC,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "operand", .type = "double", .required = true, .description = "Value to add to each point"},
                   {.name = "alignment",
                    .type = "string",
                    .required = false,
                    .description = "Series alignment method",
                    .defaultValue = "inner"}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"add(5.0)", "add(-2.5)"}};

const FunctionMetadata& AddFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> AddFunction::clone() const {
    return std::make_unique<AddFunction>();
}

seastar::future<bool> AddFunction::validateParameters(const FunctionContext& context) const {
    try {
        context.getParameter<double>("operand");
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> AddFunction::execute(const DoubleSeriesView& input,
                                                             const FunctionContext& context) const {
    FunctionResult<double> result;

    double operand = context.getParameter<double>("operand");

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    // Tight loop on contiguous memory enables compiler auto-vectorization (AVX2/SSE)
    const double* src = input.values->data() + input.startIndex;
    double* dst = result.values.data();
    for (size_t i = 0; i < input.count; ++i) {
        dst[i] = src[i] + operand;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// SubtractFunction implementation
const FunctionMetadata SubtractFunction::metadata_ = {
    .name = "subtract",
    .description = "Subtract a constant value from each point in the series",
    .category = FunctionCategory::ARITHMETIC,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters =
        {{.name = "operand", .type = "double", .required = true, .description = "Value to subtract from each point"}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"subtract(5.0)", "subtract(-2.5)"}};

const FunctionMetadata& SubtractFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> SubtractFunction::clone() const {
    return std::make_unique<SubtractFunction>();
}

seastar::future<bool> SubtractFunction::validateParameters(const FunctionContext& context) const {
    try {
        context.getParameter<double>("operand");
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> SubtractFunction::execute(const DoubleSeriesView& input,
                                                                  const FunctionContext& context) const {
    FunctionResult<double> result;

    double operand = context.getParameter<double>("operand");

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    const double* src = input.values->data() + input.startIndex;
    double* dst = result.values.data();
    for (size_t i = 0; i < input.count; ++i) {
        dst[i] = src[i] - operand;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// MultiplyFunction implementation
const FunctionMetadata MultiplyFunction::metadata_ = {
    .name = "multiply",
    .description = "Multiply each point in the series by a constant value",
    .category = FunctionCategory::ARITHMETIC,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters =
        {{.name = "factor", .type = "double", .required = true, .description = "Value to multiply each point by"}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"multiply(2.0)", "multiply(0.5)"}};

const FunctionMetadata& MultiplyFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> MultiplyFunction::clone() const {
    return std::make_unique<MultiplyFunction>();
}

seastar::future<bool> MultiplyFunction::validateParameters(const FunctionContext& context) const {
    try {
        context.getParameter<double>("factor");
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> MultiplyFunction::execute(const DoubleSeriesView& input,
                                                                  const FunctionContext& context) const {
    FunctionResult<double> result;

    double factor = context.getParameter<double>("factor");

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    // Tight loop on contiguous memory enables compiler auto-vectorization (AVX2/SSE)
    const double* src = input.values->data() + input.startIndex;
    double* dst = result.values.data();
    for (size_t i = 0; i < input.count; ++i) {
        dst[i] = src[i] * factor;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// DivideFunction implementation
const FunctionMetadata DivideFunction::metadata_ = {
    .name = "divide",
    .description = "Divide each point in the series by a constant value",
    .category = FunctionCategory::ARITHMETIC,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters =
        {{.name = "divisor", .type = "double", .required = true, .description = "Value to divide each point by"}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"divide(2.0)", "divide(0.5)"}};

const FunctionMetadata& DivideFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> DivideFunction::clone() const {
    return std::make_unique<DivideFunction>();
}

seastar::future<bool> DivideFunction::validateParameters(const FunctionContext& context) const {
    try {
        double divisor = context.getParameter<double>("divisor");
        return seastar::make_ready_future<bool>(divisor != 0.0);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> DivideFunction::execute(const DoubleSeriesView& input,
                                                                const FunctionContext& context) const {
    FunctionResult<double> result;

    double divisor = context.getParameter<double>("divisor");
    if (divisor == 0.0) {
        throw ParameterValidationException("Division by zero");
    }

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    // Tight loop on contiguous memory enables compiler auto-vectorization (AVX2/SSE)
    const double* src = input.values->data() + input.startIndex;
    double* dst = result.values.data();
    for (size_t i = 0; i < input.count; ++i) {
        dst[i] = src[i] / divisor;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// ScaleFunction implementation (alias for multiply)
const FunctionMetadata ScaleFunction::metadata_ = {.name = "scale",
                                                   .description = "Scale each point in the series by a constant factor",
                                                   .category = FunctionCategory::ARITHMETIC,
                                                   .supportedInputTypes = {"double"},
                                                   .outputType = "double",
                                                   .parameters = {{.name = "factor",
                                                                   .type = "double",
                                                                   .required = true,
                                                                   .description = "Value to scale each point by"},
                                                                  {.name = "alignment",
                                                                   .type = "string",
                                                                   .required = false,
                                                                   .description = "Series alignment method",
                                                                   .defaultValue = "inner"}},
                                                   .supportsVectorization = true,
                                                   .supportsStreaming = true,
                                                   .minDataPoints = 1,
                                                   .examples = {"scale(2.0)", "scale(0.5)"}};

const FunctionMetadata& ScaleFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> ScaleFunction::clone() const {
    return std::make_unique<ScaleFunction>();
}

seastar::future<bool> ScaleFunction::validateParameters(const FunctionContext& context) const {
    try {
        context.getParameter<double>("factor");
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> ScaleFunction::execute(const DoubleSeriesView& input,
                                                               const FunctionContext& context) const {
    FunctionResult<double> result;

    double factor = context.getParameter<double>("factor");

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    const double* src = input.values->data() + input.startIndex;
    double* dst = result.values.data();
    for (size_t i = 0; i < input.count; ++i) {
        dst[i] = src[i] * factor;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// OffsetFunction implementation (alias for add)
const FunctionMetadata OffsetFunction::metadata_ = {
    .name = "offset",
    .description = "Offset each point in the series by a constant value",
    .category = FunctionCategory::ARITHMETIC,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "value", .type = "double", .required = true, .description = "Value to add to each point"},
                   {.name = "alignment",
                    .type = "string",
                    .required = false,
                    .description = "Series alignment method",
                    .defaultValue = "inner"}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"offset(5.0)", "offset(-2.5)"}};

const FunctionMetadata& OffsetFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> OffsetFunction::clone() const {
    return std::make_unique<OffsetFunction>();
}

seastar::future<bool> OffsetFunction::validateParameters(const FunctionContext& context) const {
    try {
        context.getParameter<double>("value");
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> OffsetFunction::execute(const DoubleSeriesView& input,
                                                                const FunctionContext& context) const {
    FunctionResult<double> result;

    double value = context.getParameter<double>("value");

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    const double* src = input.values->data() + input.startIndex;
    double* dst = result.values.data();
    for (size_t i = 0; i < input.count; ++i) {
        dst[i] = src[i] + value;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// Legacy functions for backward compatibility
std::vector<double> add(const std::vector<double>& values, double operand) {
    std::vector<double> result(values.size());
    std::transform(values.begin(), values.end(), result.begin(), [operand](double val) { return val + operand; });
    return result;
}

std::vector<double> multiply(const std::vector<double>& values, double factor) {
    std::vector<double> result(values.size());
    std::transform(values.begin(), values.end(), result.begin(), [factor](double val) { return val * factor; });
    return result;
}

std::vector<double> scale(const std::vector<double>& values, double factor) {
    return multiply(values, factor);
}

std::vector<double> offset(const std::vector<double>& values, double offset_value) {
    return add(values, offset_value);
}

}  // namespace timestar::functions