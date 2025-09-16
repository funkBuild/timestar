#include "smoothing_functions.hpp"
#include <algorithm>
#include <numeric>
#include <cmath>

namespace tsdb::functions {

// SMAFunction implementation
const FunctionMetadata SMAFunction::metadata_ = {
    .name = "sma",
    .description = "Simple Moving Average - calculates the average value over a sliding window",
    .category = FunctionCategory::SMOOTHING,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {
        {.name = "window", .type = "int", .required = false, .description = "Window size for moving average", .defaultValue = "5"},
        {.name = "edge_handling", .type = "string", .required = false, .description = "Edge handling mode: truncate, pad_nearest, pad_zeros", .defaultValue = "truncate"}
    },
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"sma(5)", "sma(10, \"pad_nearest\")"}
};

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

seastar::future<FunctionResult<double>> SMAFunction::execute(
    const DoubleSeriesView& input,
    const FunctionContext& context
) const {
    FunctionResult<double> result;
    
    int64_t window = context.getParameter<int64_t>("window", 5);
    std::string edgeHandling = context.getParameter<std::string>("edge_handling", "truncate");
    
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
        // Original truncate behavior - only return complete windows
        for (size_t i = window - 1; i < input.count; ++i) {
            double sum = 0.0;
            for (int64_t j = 0; j < window; ++j) {
                sum += input.valueAt(i - j);
            }
            result.values.push_back(sum / window);
            result.timestamps.push_back(input.timestampAt(i));
        }
    } else if (edgeHandling == "pad_nearest") {
        // Pad with nearest values - return same size as input
        result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                                input.timestamps->begin() + input.startIndex + input.count);
        result.values.reserve(input.count);
        
        for (size_t i = 0; i < input.count; ++i) {
            double sum = 0.0;
            int64_t validCount = 0;
            
            // For each position in the window
            for (int64_t j = 0; j < window; ++j) {
                int64_t dataIdx = static_cast<int64_t>(i) - j;
                
                if (dataIdx >= 0 && dataIdx < static_cast<int64_t>(input.count)) {
                    // Valid data point within bounds
                    sum += input.valueAt(dataIdx);
                    validCount++;
                } else if (dataIdx < 0) {
                    // Pad with first value (nearest)
                    sum += input.valueAt(0);
                    validCount++;
                } else {
                    // dataIdx >= input.count, pad with last value (nearest)
                    sum += input.valueAt(input.count - 1);
                    validCount++;
                }
            }
            
            result.values.push_back(validCount > 0 ? sum / validCount : input.valueAt(i));
        }
    } else if (edgeHandling == "pad_zeros") {
        // Pad with zeros - return same size as input
        result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                                input.timestamps->begin() + input.startIndex + input.count);
        result.values.reserve(input.count);
        
        for (size_t i = 0; i < input.count; ++i) {
            double sum = 0.0;
            int64_t validCount = 0;
            
            // For each position in the window
            for (int64_t j = 0; j < window; ++j) {
                int64_t dataIdx = static_cast<int64_t>(i) - j;
                
                if (dataIdx >= 0 && dataIdx < static_cast<int64_t>(input.count)) {
                    // Valid data point within bounds
                    sum += input.valueAt(dataIdx);
                    validCount++;
                }
                // Outside bounds: add 0 (implicit), don't increment validCount
            }
            
            result.values.push_back(validCount > 0 ? sum / window : 0.0);  // Divide by full window for pad_zeros
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
    .parameters = {
        {.name = "alpha", .type = "double", .required = false, .description = "Smoothing factor (0 < alpha <= 1)", .defaultValue = "0.1"},
        {.name = "window", .type = "int", .required = false, .description = "Window size for EMA calculation, alpha = 2/(window+1)", .defaultValue = ""},
        {.name = "half_life", .type = "double", .required = false, .description = "Half-life for EMA calculation, alpha = 1 - exp(-ln(2)/half_life)", .defaultValue = ""}
    },
    .supportsVectorization = true,
    .supportsStreaming = true,
    .minDataPoints = 1,
    .examples = {"ema(0.1)", "ema(window=10)", "ema(half_life=5.0)"}
};

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

seastar::future<FunctionResult<double>> EMAFunction::execute(
    const DoubleSeriesView& input,
    const FunctionContext& context
) const {
    FunctionResult<double> result;
    
    if (input.count == 0) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }
    
    // Calculate alpha based on the provided parameter
    double alpha = 0.1; // default
    
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
    .parameters = {
        {.name = "sigma", .type = "double", .required = false, .description = "Standard deviation for Gaussian kernel", .defaultValue = "1.0"},
        {.name = "window", .type = "int", .required = false, .description = "Window size for Gaussian kernel, sigma = window/3.0", .defaultValue = ""}
    },
    .supportsVectorization = true,
    .supportsStreaming = false,
    .minDataPoints = 3,
    .examples = {"gaussian(1.0)", "gaussian(window=7)", "gaussian(2.5)"}
};

const FunctionMetadata& GaussianSmoothFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> GaussianSmoothFunction::clone() const {
    return std::make_unique<GaussianSmoothFunction>();
}

seastar::future<bool> GaussianSmoothFunction::validateParameters(const FunctionContext& context) const {
    try {
        bool hasSigma = context.hasParameter("sigma");
        bool hasWindow = context.hasParameter("window");
        
        // Only one parameter should be provided at a time
        if (hasSigma && hasWindow) {
            return seastar::make_ready_future<bool>(false);
        }
        
        if (hasSigma) {
            double sigma = context.getParameter<double>("sigma");
            return seastar::make_ready_future<bool>(sigma > 0.0);
        } else if (hasWindow) {
            int64_t window = context.getParameter<int64_t>("window");
            return seastar::make_ready_future<bool>(window > 0 && window % 2 == 1); // Window should be odd
        } else {
            // Default sigma is valid
            return seastar::make_ready_future<bool>(true);
        }
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> GaussianSmoothFunction::execute(
    const DoubleSeriesView& input,
    const FunctionContext& context
) const {
    FunctionResult<double> result;
    
    if (input.count < 3) {
        throw InsufficientDataException("Gaussian smoothing requires at least 3 data points");
    }
    
    // Calculate sigma based on the provided parameter
    double sigma = 1.0; // default
    
    if (context.hasParameter("sigma")) {
        sigma = context.getParameter<double>("sigma");
    } else if (context.hasParameter("window")) {
        int64_t window = context.getParameter<int64_t>("window");
        sigma = window / 3.0; // Rule of thumb: window = 3*sigma for good coverage
    }
    
    result.timestamps.assign(input.timestamps->begin() + input.startIndex,
                            input.timestamps->begin() + input.startIndex + input.count);
    result.values.reserve(input.count);
    
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
    for (size_t i = 0; i < input.count; ++i) {
        double smoothed = 0.0;
        
        for (int j = 0; j < kernelSize; ++j) {
            int idx = static_cast<int>(i) + j - kernelSize / 2;
            if (idx >= 0 && idx < static_cast<int>(input.count)) {
                smoothed += input.valueAt(idx) * kernel[j];
            }
        }
        
        result.values.push_back(smoothed);
    }
    
    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// Smoothing utilities implementation
namespace smoothing_utils {
    std::vector<uint64_t> generateSmoothedTimestamps(
        const std::vector<uint64_t>& original,
        size_t window,
        SmoothingFunction::EdgeHandling edgeHandling
    ) {
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
}

// Legacy functions for backward compatibility
std::vector<double> simpleMovingAverage(const std::vector<double>& values, int window) {
    std::vector<double> result;
    if (window <= 0 || values.size() < static_cast<size_t>(window)) {
        return result;
    }
    
    for (size_t i = window - 1; i < values.size(); ++i) {
        double sum = 0.0;
        for (int j = 0; j < window; ++j) {
            sum += values[i - j];
        }
        result.push_back(sum / window);
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

} // namespace tsdb::functions