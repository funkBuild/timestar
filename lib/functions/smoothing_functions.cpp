#include "smoothing_functions.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace timestar::functions {

// SMAFunction implementation
const FunctionMetadata SMAFunction::metadata_ = {
    .name = "sma",
    .description = "Simple Moving Average - calculates the average value over a sliding window",
    .category = FunctionCategory::SMOOTHING,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "window",
                    .type = "int",
                    .required = false,
                    .description = "Window size for moving average",
                    .defaultValue = "5"},
                   {.name = "edge_handling",
                    .type = "string",
                    .required = false,
                    .description = "Edge handling mode: truncate, pad_nearest, pad_zeros",
                    .defaultValue = "truncate"}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"sma(5)", "sma(10, \"pad_nearest\")"}};

const FunctionMetadata& SMAFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> SMAFunction::clone() const {
    return std::make_unique<SMAFunction>();
}

seastar::future<bool> SMAFunction::validateParameters(const FunctionContext& context) const {
    try {
        int64_t window = context.getParameter<int64_t>("window", 5);
        if (window <= 0 || window > 1000) {  // Add reasonable upper limit
            return seastar::make_ready_future<bool>(false);
        }

        std::string edgeHandling = context.getParameter<std::string>("edge_handling", "truncate");
        if (edgeHandling != "truncate" && edgeHandling != "pad_nearest" && edgeHandling != "pad_zeros") {
            return seastar::make_ready_future<bool>(false);
        }

        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> SMAFunction::execute(const DoubleSeriesView& input,
                                                             const FunctionContext& context) const {
    FunctionResult<double> result;

    int64_t window = context.getParameter<int64_t>("window", 5);
    std::string edgeHandling = context.getParameter<std::string>("edge_handling", "truncate");

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    if (window <= 0) {
        throw InsufficientDataException("SMA window size must be positive");
    }

    if (input.count == 0) {
        throw InsufficientDataException("SMA requires at least one data point");
    }

    // For truncate mode, we need at least window data points
    if (edgeHandling == "truncate" && input.count < static_cast<size_t>(window)) {
        throw InsufficientDataException("SMA requires at least " + std::to_string(window) + " data points");
    }

    if (edgeHandling == "truncate") {
        // O(n) sliding window - compute initial sum, then slide
        size_t outputCount = input.count - (window - 1);
        result.values.resize(outputCount);
        result.timestamps.resize(outputCount);

        // Kahan-compensated sliding window to prevent drift over long series
        double sum = 0.0;
        double comp = 0.0;
        for (int64_t j = 0; j < window; ++j) {
            double y = input.valueAt(j) - comp;
            double t = sum + y;
            comp = (t - sum) - y;
            sum = t;
        }
        result.values[0] = sum / window;
        result.timestamps[0] = input.timestampAt(window - 1);

        for (size_t i = 1; i < outputCount; ++i) {
            double delta = input.valueAt(i + window - 1) - input.valueAt(i - 1);
            double y = delta - comp;
            double t = sum + y;
            comp = (t - sum) - y;
            sum = t;
            result.values[i] = sum / window;
            result.timestamps[i] = input.timestampAt(i + window - 1);
        }
    } else if (edgeHandling == "pad_nearest") {
        // Pad with nearest values - return same size as input
        result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                                 input.timestamps->begin() + input.startIndex + input.count);
        result.values.resize(input.count);

        // O(n) sliding window with Kahan summation.
        // Initial sum: window copies of values[0] (left-edge padding fills entire window).
        double sum = static_cast<double>(window) * input.valueAt(0);
        double comp = 0.0;
        result.values[0] = sum / window;

        // Phase 1: Ramp-up -- each step replaces one padding (values[0]) with values[i]
        size_t rampEnd = std::min(static_cast<size_t>(window), input.count);
        for (size_t i = 1; i < rampEnd; ++i) {
            double delta = input.valueAt(i) - input.valueAt(0);
            double y = delta - comp;
            double t = sum + y;
            comp = (t - sum) - y;
            sum = t;
            result.values[i] = sum / window;
        }

        // Phase 2: Steady-state -- full window, standard slide
        for (size_t i = rampEnd; i < input.count; ++i) {
            double delta = input.valueAt(i) - input.valueAt(i - window);
            double y = delta - comp;
            double t = sum + y;
            comp = (t - sum) - y;
            sum = t;
            result.values[i] = sum / window;
        }
    } else if (edgeHandling == "pad_zeros") {
        // Pad with zeros - return same size as input
        result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                                 input.timestamps->begin() + input.startIndex + input.count);
        result.values.resize(input.count);

        // O(n) sliding window with Kahan summation.
        // Phase 1: Ramp-up -- window extends beyond left edge, missing values are 0
        double sum = 0.0;
        double comp = 0.0;
        size_t rampEnd = std::min(static_cast<size_t>(window), input.count);
        for (size_t i = 0; i < rampEnd; ++i) {
            double y = input.valueAt(i) - comp;
            double t = sum + y;
            comp = (t - sum) - y;
            sum = t;
            result.values[i] = sum / window;
        }

        // Phase 2: Steady-state -- full window within data bounds
        for (size_t i = rampEnd; i < input.count; ++i) {
            double delta = input.valueAt(i) - input.valueAt(i - window);
            double y = delta - comp;
            double t = sum + y;
            comp = (t - sum) - y;
            sum = t;
            result.values[i] = sum / window;
        }
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// EMAFunction implementation
const FunctionMetadata EMAFunction::metadata_ = {
    .name = "ema",
    .description = "Exponential Moving Average - calculates weighted average with exponentially decreasing weights",
    .category = FunctionCategory::SMOOTHING,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "alpha",
                    .type = "double",
                    .required = false,
                    .description = "Smoothing factor (0 < alpha <= 1)",
                    .defaultValue = "0.1"},
                   {.name = "window",
                    .type = "int",
                    .required = false,
                    .description = "Window size for EMA calculation, alpha = 2/(window+1)",
                    .defaultValue = ""},
                   {.name = "half_life",
                    .type = "double",
                    .required = false,
                    .description = "Half-life for EMA calculation, alpha = 1 - exp(-ln(2)/half_life)",
                    .defaultValue = ""}},
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"ema(0.1)", "ema(window=10)", "ema(half_life=5.0)"}};

const FunctionMetadata& EMAFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> EMAFunction::clone() const {
    return std::make_unique<EMAFunction>();
}

seastar::future<bool> EMAFunction::validateParameters(const FunctionContext& context) const {
    try {
        bool hasAlpha = context.hasParameter("alpha");
        bool hasWindow = context.hasParameter("window");
        bool hasHalfLife = context.hasParameter("half_life");

        // Only one parameter should be provided at a time
        int paramCount = (hasAlpha ? 1 : 0) + (hasWindow ? 1 : 0) + (hasHalfLife ? 1 : 0);
        if (paramCount > 1) {
            return seastar::make_ready_future<bool>(false);
        }

        if (hasAlpha) {
            double alpha = context.getParameter<double>("alpha");
            return seastar::make_ready_future<bool>(alpha > 0.0 && alpha <= 1.0);
        } else if (hasWindow) {
            int64_t window = context.getParameter<int64_t>("window");
            return seastar::make_ready_future<bool>(window > 0);
        } else if (hasHalfLife) {
            double halfLife = context.getParameter<double>("half_life");
            return seastar::make_ready_future<bool>(halfLife > 0.0);
        } else {
            // Default alpha is valid
            return seastar::make_ready_future<bool>(true);
        }
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> EMAFunction::execute(const DoubleSeriesView& input,
                                                             const FunctionContext& context) const {
    FunctionResult<double> result;

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    // Calculate alpha based on the provided parameter
    double alpha = 0.1;  // default

    if (context.hasParameter("alpha")) {
        alpha = context.getParameter<double>("alpha");
    } else if (context.hasParameter("window")) {
        int64_t window = context.getParameter<int64_t>("window");
        alpha = 2.0 / (window + 1.0);
    } else if (context.hasParameter("half_life")) {
        double halfLife = context.getParameter<double>("half_life");
        alpha = 1.0 - std::exp(-std::log(2.0) / halfLife);
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.reserve(input.count);

    // First value is unchanged
    result.values.push_back(input.valueAt(0));

    // Calculate EMA for subsequent values
    for (size_t i = 1; i < input.count; ++i) {
        double ema = alpha * input.valueAt(i) + (1.0 - alpha) * result.values.back();
        result.values.push_back(ema);
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// GaussianSmoothFunction implementation
const FunctionMetadata GaussianSmoothFunction::metadata_ = {
    .name = "gaussian",
    .description = "Gaussian smoothing filter with configurable sigma and window",
    .category = FunctionCategory::SMOOTHING,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "sigma",
                    .type = "double",
                    .required = false,
                    .description = "Standard deviation for Gaussian kernel",
                    .defaultValue = "1.0"},
                   {.name = "window",
                    .type = "int",
                    .required = false,
                    .description = "Window size for Gaussian kernel, sigma = window/3.0",
                    .defaultValue = ""}},
    .supportsVectorization = true,
    .supportsStreaming = false,
    .minDataPoints = 3,
    .examples = {"gaussian(1.0)", "gaussian(window=7)", "gaussian(2.5)"}};

const FunctionMetadata& GaussianSmoothFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> GaussianSmoothFunction::clone() const {
    return std::make_unique<GaussianSmoothFunction>();
}

seastar::future<bool> GaussianSmoothFunction::validateParameters(const FunctionContext& context) const {
    try {
        // Both sigma and window may be provided independently:
        // sigma controls the Gaussian kernel width, window controls the kernel array size.
        if (context.hasParameter("sigma")) {
            double sigma = context.getParameter<double>("sigma");
            if (sigma <= 0.0) {
                return seastar::make_ready_future<bool>(false);
            }
        }

        if (context.hasParameter("window")) {
            int64_t window = context.getParameter<int64_t>("window");
            if (window <= 0 || window % 2 == 0) {  // Window should be positive and odd
                return seastar::make_ready_future<bool>(false);
            }
        }

        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> GaussianSmoothFunction::execute(const DoubleSeriesView& input,
                                                                        const FunctionContext& context) const {
    FunctionResult<double> result;

    if (input.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    if (input.count < 3) {
        throw InsufficientDataException("Gaussian smoothing requires at least 3 data points");
    }

    // Inline parameter validation (avoids calling .get() on a future, which
    // would block the Seastar reactor thread and risk deadlock).
    // Both sigma and window may be provided: sigma controls kernel width,
    // window controls kernel array size.
    {
        if (context.hasParameter("sigma")) {
            double s = context.getParameter<double>("sigma");
            if (s <= 0.0) {
                throw std::invalid_argument("GaussianSmoothFunction: sigma must be positive");
            }
        }
        if (context.hasParameter("window")) {
            int64_t w = context.getParameter<int64_t>("window");
            if (w <= 0 || w % 2 == 0) {
                throw std::invalid_argument("GaussianSmoothFunction: window must be a positive odd integer");
            }
        }
    }

    // Calculate sigma based on the provided parameter
    double sigma = 1.0;  // default

    if (context.hasParameter("sigma")) {
        sigma = context.getParameter<double>("sigma");
    } else if (context.hasParameter("window")) {
        int64_t window = context.getParameter<int64_t>("window");
        sigma = window / 3.0;  // Rule of thumb: window = 3*sigma for good coverage
    }

    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                             input.timestamps->begin() + input.startIndex + input.count);
    result.values.resize(input.count);

    // Calculate kernel size - ensure it's odd and reasonable
    int kernelSize;
    if (context.hasParameter("window")) {
        kernelSize = static_cast<int>(context.getParameter<int64_t>("window"));
        // Ensure kernel size is odd
        if (kernelSize % 2 == 0) {
            kernelSize++;
        }
    } else {
        kernelSize = static_cast<int>(3 * sigma) * 2 + 1;
    }

    std::vector<double> kernel(kernelSize);
    double sum = 0.0;

    // Generate Gaussian kernel
    for (int i = 0; i < kernelSize; ++i) {
        int x = i - kernelSize / 2;
        kernel[i] = std::exp(-(x * x) / (2.0 * sigma * sigma));
        sum += kernel[i];
    }

    // Normalize kernel
    for (double& k : kernel) {
        k /= sum;
    }

    // Apply convolution with proper boundary handling
    // Use int for loop variable to avoid repeated size_t-to-int casts
    int inputCountInt = static_cast<int>(input.count);
    int halfKernel = kernelSize / 2;
    for (int i = 0; i < inputCountInt; ++i) {
        double smoothed = 0.0;
        double weightSum = 0.0;

        for (int j = 0; j < kernelSize; ++j) {
            int idx = i + j - halfKernel;
            if (idx >= 0 && idx < inputCountInt) {
                smoothed += input.valueAt(idx) * kernel[j];
                weightSum += kernel[j];
            }
        }

        if (weightSum > 0.0) {
            smoothed /= weightSum;
        }
        result.values[i] = smoothed;
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// Smoothing utilities implementation
namespace smoothing_utils {
std::vector<uint64_t> generateSmoothedTimestamps(const std::vector<uint64_t>& original, size_t window,
                                                 SmoothingFunction::EdgeHandling edgeHandling) {
    std::vector<uint64_t> result;

    switch (edgeHandling) {
        case SmoothingFunction::EdgeHandling::TRUNCATE:
            if (original.size() >= window) {
                result.assign(original.begin() + window - 1, original.end());
            }
            break;

        case SmoothingFunction::EdgeHandling::PAD_NEAREST:
            result = original;
            break;

        case SmoothingFunction::EdgeHandling::PAD_ZEROS:
        case SmoothingFunction::EdgeHandling::EXTRAPOLATE:
            result = original;
            break;
    }

    return result;
}
}  // namespace smoothing_utils

// Legacy functions for backward compatibility
std::vector<double> simpleMovingAverage(const std::vector<double>& values, int window) {
    std::vector<double> result;
    if (window <= 0 || values.size() < static_cast<size_t>(window)) {
        return result;
    }

    size_t outputCount = values.size() - (window - 1);
    result.resize(outputCount);

    double sum = 0.0;
    for (int j = 0; j < window; ++j) {
        sum += values[j];
    }
    result[0] = sum / window;

    for (size_t i = 1; i < outputCount; ++i) {
        sum -= values[i - 1];
        sum += values[i + window - 1];
        result[i] = sum / window;
    }

    return result;
}

std::vector<double> exponentialMovingAverage(const std::vector<double>& values, double alpha) {
    std::vector<double> result;
    if (values.empty() || alpha <= 0.0 || alpha > 1.0) {
        return result;
    }

    result.push_back(values[0]);
    for (size_t i = 1; i < values.size(); ++i) {
        double ema = alpha * values[i] + (1.0 - alpha) * result.back();
        result.push_back(ema);
    }

    return result;
}

}  // namespace timestar::functions