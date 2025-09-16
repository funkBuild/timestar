#include "interpolation_functions.hpp"
#include <algorithm>
#include <sstream>
#include <limits>

namespace tsdb::functions {

// LinearInterpolationFunction implementation
const FunctionMetadata LinearInterpolationFunction::metadata_ = {
    .name = "linear_interpolate",
    .description = "Linear interpolation to fill gaps or resample data at regular intervals",
    .category = FunctionCategory::TRANSFORMATION,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {
        {.name = "target_interval", .type = "int", .required = false, .description = "Target interval between points in nanoseconds"},
        {.name = "target_timestamps", .type = "string", .required = false, .description = "Comma-separated list of target timestamps"},
        {.name = "boundary", .type = "string", .required = false, .description = "Boundary handling: clamp, nan, nan_fill, extrapolate", .defaultValue = "clamp"}
    },
    .supportsVectorization = true,
    .supportsStreaming = false,
    .minDataPoints = 2,
    .examples = {"linear_interpolate(interval=1000)", "linear_interpolate(timestamps=\"1000,2000,3000\")"}
};

const FunctionMetadata& LinearInterpolationFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> LinearInterpolationFunction::clone() const {
    return std::make_unique<LinearInterpolationFunction>();
}

seastar::future<bool> LinearInterpolationFunction::validateParameters(const FunctionContext& context) const {
    try {
        bool hasInterval = context.hasParameter("target_interval");
        bool hasTimestamps = context.hasParameter("target_timestamps");
        
        // Must have either interval or timestamps, but not both
        if (hasInterval && hasTimestamps) {
            return seastar::make_ready_future<bool>(false);
        }
        if (!hasInterval && !hasTimestamps) {
            return seastar::make_ready_future<bool>(false);
        }
        
        if (hasInterval) {
            int64_t interval = context.getParameter<int64_t>("target_interval");
            if (interval <= 0) {
                return seastar::make_ready_future<bool>(false);
            }
        }
        
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> LinearInterpolationFunction::execute(
    const DoubleSeriesView& input,
    const FunctionContext& context
) const {
    FunctionResult<double> result;
    
    if (input.count < 2) {
        throw InsufficientDataException("Linear interpolation requires at least 2 data points");
    }
    
    std::string boundary = context.getParameter<std::string>("boundary", "clamp");
    std::vector<uint64_t> targetTimestamps;
    
    if (context.hasParameter("target_interval")) {
        int64_t interval = context.getParameter<int64_t>("target_interval");
        uint64_t startTime = input.timestampAt(0);
        uint64_t endTime = input.timestampAt(input.count - 1);
        
        for (uint64_t t = startTime; t <= endTime; t += interval) {
            targetTimestamps.push_back(t);
        }
    } else {
        std::string timestampStr = context.getParameter<std::string>("target_timestamps");
        std::istringstream ss(timestampStr);
        std::string token;
        
        while (std::getline(ss, token, ',')) {
            if (!token.empty()) {
                targetTimestamps.push_back(std::stoull(token));
            }
        }
    }
    
    if (targetTimestamps.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }
    
    result.timestamps = targetTimestamps;
    result.values.reserve(targetTimestamps.size());
    
    // Perform linear interpolation
    for (uint64_t targetTime : targetTimestamps) {
        // Find the two points to interpolate between
        size_t i = 0;
        while (i + 1 < input.count && input.timestampAt(i + 1) < targetTime) {
            ++i;
        }
        
        if (targetTime < input.timestampAt(0)) {
            // Before first point
            if (boundary == "clamp") {
                result.values.push_back(input.valueAt(0));
            } else if (boundary == "nan" || boundary == "nan_fill") {
                result.values.push_back(std::numeric_limits<double>::quiet_NaN());
            } else { // extrapolate
                if (input.count >= 2) {
                    double slope = (input.valueAt(1) - input.valueAt(0)) / 
                                  static_cast<double>(input.timestampAt(1) - input.timestampAt(0));
                    double extrapolated = input.valueAt(0) + slope * 
                                         static_cast<double>(targetTime - input.timestampAt(0));
                    result.values.push_back(extrapolated);
                } else {
                    result.values.push_back(input.valueAt(0));
                }
            }
        } else if (targetTime > input.timestampAt(input.count - 1)) {
            // After last point
            if (boundary == "clamp") {
                result.values.push_back(input.valueAt(input.count - 1));
            } else if (boundary == "nan" || boundary == "nan_fill") {
                result.values.push_back(std::numeric_limits<double>::quiet_NaN());
            } else { // extrapolate
                if (input.count >= 2) {
                    size_t lastIdx = input.count - 1;
                    double slope = (input.valueAt(lastIdx) - input.valueAt(lastIdx - 1)) / 
                                  static_cast<double>(input.timestampAt(lastIdx) - input.timestampAt(lastIdx - 1));
                    double extrapolated = input.valueAt(lastIdx) + slope * 
                                         static_cast<double>(targetTime - input.timestampAt(lastIdx));
                    result.values.push_back(extrapolated);
                } else {
                    result.values.push_back(input.valueAt(input.count - 1));
                }
            }
        } else if (targetTime == input.timestampAt(i)) {
            // Exact match
            result.values.push_back(input.valueAt(i));
        } else if (i + 1 < input.count) {
            // Linear interpolation between two points
            uint64_t t1 = input.timestampAt(i);
            uint64_t t2 = input.timestampAt(i + 1);
            double v1 = input.valueAt(i);
            double v2 = input.valueAt(i + 1);
            
            double ratio = static_cast<double>(targetTime - t1) / static_cast<double>(t2 - t1);
            double interpolated = v1 + ratio * (v2 - v1);
            result.values.push_back(interpolated);
        } else {
            // Edge case - use last point
            result.values.push_back(input.valueAt(input.count - 1));
        }
    }
    
    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// SplineInterpolationFunction implementation
const FunctionMetadata SplineInterpolationFunction::metadata_ = {
    .name = "spline_interpolate", 
    .description = "Cubic spline interpolation for smooth curves",
    .category = FunctionCategory::TRANSFORMATION,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {
        {.name = "target_interval", .type = "int", .required = false, .description = "Target interval between points in nanoseconds"},
        {.name = "target_timestamps", .type = "string", .required = false, .description = "Comma-separated list of target timestamps"}
    },
    .supportsVectorization = true,
    .supportsStreaming = false,
    .minDataPoints = 4,
    .examples = {"spline_interpolation(interval=1000)"}
};

const FunctionMetadata& SplineInterpolationFunction::getMetadata() const {
    return metadata_;
}

std::unique_ptr<IFunction> SplineInterpolationFunction::clone() const {
    return std::make_unique<SplineInterpolationFunction>();
}

seastar::future<bool> SplineInterpolationFunction::validateParameters(const FunctionContext& context) const {
    try {
        bool hasInterval = context.hasParameter("target_interval");
        bool hasTimestamps = context.hasParameter("target_timestamps");
        
        if (!hasInterval && !hasTimestamps) {
            return seastar::make_ready_future<bool>(false);
        }
        
        if (hasInterval) {
            int64_t interval = context.getParameter<int64_t>("target_interval");
            if (interval <= 0) {
                return seastar::make_ready_future<bool>(false);
            }
        }
        
        return seastar::make_ready_future<bool>(true);
    } catch (...) {
        return seastar::make_ready_future<bool>(false);
    }
}

seastar::future<FunctionResult<double>> SplineInterpolationFunction::execute(
    const DoubleSeriesView& input,
    const FunctionContext& context
) const {
    FunctionResult<double> result;
    
    if (input.count < 4) {
        throw InsufficientDataException("Spline interpolation requires at least 4 data points");
    }
    
    // For now, fall back to linear interpolation
    // A full cubic spline implementation would be more complex
    std::vector<uint64_t> targetTimestamps;
    
    if (context.hasParameter("target_interval")) {
        int64_t interval = context.getParameter<int64_t>("target_interval");
        uint64_t startTime = input.timestampAt(0);
        uint64_t endTime = input.timestampAt(input.count - 1);
        
        for (uint64_t t = startTime; t <= endTime; t += interval) {
            targetTimestamps.push_back(t);
        }
    } else {
        std::string timestampStr = context.getParameter<std::string>("target_timestamps");
        std::istringstream ss(timestampStr);
        std::string token;
        
        while (std::getline(ss, token, ',')) {
            if (!token.empty()) {
                targetTimestamps.push_back(std::stoull(token));
            }
        }
    }
    
    result.timestamps = targetTimestamps;
    result.values.reserve(targetTimestamps.size());
    
    // Simple linear interpolation fallback
    for (uint64_t targetTime : targetTimestamps) {
        size_t i = 0;
        while (i + 1 < input.count && input.timestampAt(i + 1) < targetTime) {
            ++i;
        }
        
        if (i + 1 < input.count) {
            uint64_t t1 = input.timestampAt(i);
            uint64_t t2 = input.timestampAt(i + 1);
            double v1 = input.valueAt(i);
            double v2 = input.valueAt(i + 1);
            
            double ratio = static_cast<double>(targetTime - t1) / static_cast<double>(t2 - t1);
            double interpolated = v1 + ratio * (v2 - v1);
            result.values.push_back(interpolated);
        } else {
            result.values.push_back(input.valueAt(input.count - 1));
        }
    }
    
    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// Legacy function for backward compatibility
std::vector<double> linearInterpolate(const std::vector<double>& values, 
                                    const std::vector<uint64_t>& timestamps,
                                    uint64_t targetInterval) {
    // Stub implementation for now
    return values;
}

} // namespace tsdb::functions