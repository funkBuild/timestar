#include "interpolation_functions.hpp"

#include <algorithm>
#include <limits>
#include <sstream>

namespace timestar::functions {

static constexpr size_t MAX_INTERPOLATION_POINTS = 1'000'000;

// LinearInterpolationFunction implementation
const FunctionMetadata LinearInterpolationFunction::metadata_ = {
    .name = "linear_interpolate",
    .description = "Linear interpolation to fill gaps or resample data at regular intervals",
    .category = FunctionCategory::TRANSFORMATION,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "target_interval",
                    .type = "int",
                    .required = false,
                    .description = "Target interval between points in nanoseconds"},
                   {.name = "target_timestamps",
                    .type = "string",
                    .required = false,
                    .description = "Comma-separated list of target timestamps"},
                   {.name = "boundary",
                    .type = "string",
                    .required = false,
                    .description = "Boundary handling: clamp, nan, nan_fill, extrapolate",
                    .defaultValue = "clamp"}},
    .supportsVectorization = true,
    .supportsStreaming = false,
    .minDataPoints = 2,
    .examples = {"linear_interpolate(interval=1000)", "linear_interpolate(timestamps=\"1000,2000,3000\")"}};

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

seastar::future<FunctionResult<double>> LinearInterpolationFunction::execute(const DoubleSeriesView& input,
                                                                             const FunctionContext& context) const {
    FunctionResult<double> result;

    if (input.count < 2) {
        throw InsufficientDataException("Linear interpolation requires at least 2 data points");
    }

    std::string boundary = context.getParameter<std::string>("boundary", "clamp");
    std::vector<uint64_t> targetTimestamps;

    if (context.hasParameter("target_interval")) {
        int64_t interval = context.getParameter<int64_t>("target_interval");
        if (interval <= 0) {
            throw ParameterValidationException("target_interval must be positive");
        }
        uint64_t startTime = input.timestampAt(0);
        uint64_t endTime = input.timestampAt(input.count - 1);

        uint64_t uinterval = static_cast<uint64_t>(interval);
        size_t estimatedPoints = (endTime > startTime) ? ((endTime - startTime) / uinterval + 1) : 1;
        if (estimatedPoints > MAX_INTERPOLATION_POINTS) {
            throw ParameterValidationException("target_interval too small: would generate " +
                                               std::to_string(estimatedPoints) + " points (max " +
                                               std::to_string(MAX_INTERPOLATION_POINTS) + ")");
        }

        for (uint64_t t = startTime; t <= endTime; t += uinterval) {
            targetTimestamps.push_back(t);
            if (t > UINT64_MAX - uinterval)
                break;
        }
    } else {
        std::string timestampStr = context.getParameter<std::string>("target_timestamps");
        std::istringstream ss(timestampStr);
        std::string token;

        while (std::getline(ss, token, ',')) {
            if (!token.empty()) {
                try {
                    targetTimestamps.push_back(std::stoull(token));
                } catch (const std::exception& e) {
                    throw ParameterValidationException("Invalid timestamp value \"" + token + "\": " + e.what());
                }
                if (targetTimestamps.size() > MAX_INTERPOLATION_POINTS) {
                    throw ParameterValidationException("Too many target_timestamps: exceeds limit of " +
                                                       std::to_string(MAX_INTERPOLATION_POINTS));
                }
            }
        }
    }

    if (targetTimestamps.empty()) {
        return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
    }

    result.timestamps = targetTimestamps;
    result.values.reserve(targetTimestamps.size());

    // Perform linear interpolation using two-pointer approach.
    // Both input timestamps and target timestamps are sorted, so we maintain
    // the search position across iterations for O(n+m) instead of O(n*m).
    size_t searchPos = 0;
    for (uint64_t targetTime : targetTimestamps) {
        // Advance searchPos to find the two points to interpolate between.
        // Since targets are sorted, we never need to go backwards.
        while (searchPos + 1 < input.count && input.timestampAt(searchPos + 1) < targetTime) {
            ++searchPos;
        }
        size_t i = searchPos;

        if (targetTime < input.timestampAt(0)) {
            // Before first point
            if (boundary == "clamp") {
                result.values.push_back(input.valueAt(0));
            } else if (boundary == "nan" || boundary == "nan_fill") {
                result.values.push_back(std::numeric_limits<double>::quiet_NaN());
            } else {  // extrapolate
                if (input.count >= 2) {
                    double slope = (input.valueAt(1) - input.valueAt(0)) /
                                   static_cast<double>(input.timestampAt(1) - input.timestampAt(0));
                    // Bug #22 fix: Cast both operands to double before subtraction
                    // to prevent unsigned underflow when targetTime < timestampAt(0)
                    double extrapolated = input.valueAt(0) + slope * (static_cast<double>(targetTime) -
                                                                      static_cast<double>(input.timestampAt(0)));
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
            } else {  // extrapolate
                if (input.count >= 2) {
                    size_t lastIdx = input.count - 1;
                    double slope = (input.valueAt(lastIdx) - input.valueAt(lastIdx - 1)) /
                                   static_cast<double>(input.timestampAt(lastIdx) - input.timestampAt(lastIdx - 1));
                    double extrapolated =
                        input.valueAt(lastIdx) + slope * static_cast<double>(targetTime - input.timestampAt(lastIdx));
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
// WARNING: This function currently falls back to linear interpolation.
// Cubic spline interpolation is not yet implemented. The function name
// and metadata description are aspirational.
const FunctionMetadata SplineInterpolationFunction::metadata_ = {
    .name = "spline_interpolate",
    .description = "Cubic spline interpolation for smooth curves",
    .category = FunctionCategory::TRANSFORMATION,
    .supportedInputTypes = {"double"},
    .outputType = "double",
    .parameters = {{.name = "target_interval",
                    .type = "int",
                    .required = false,
                    .description = "Target interval between points in nanoseconds"},
                   {.name = "target_timestamps",
                    .type = "string",
                    .required = false,
                    .description = "Comma-separated list of target timestamps"}},
    .supportsVectorization = true,
    .supportsStreaming = false,
    .minDataPoints = 4,
    .examples = {"spline_interpolation(interval=1000)"}};

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

seastar::future<FunctionResult<double>> SplineInterpolationFunction::execute(const DoubleSeriesView& input,
                                                                             const FunctionContext& context) const {
    FunctionResult<double> result;

    if (input.count < 4) {
        throw InsufficientDataException("Spline interpolation requires at least 4 data points");
    }

    // For now, fall back to linear interpolation
    // A full cubic spline implementation would be more complex
    std::vector<uint64_t> targetTimestamps;

    if (context.hasParameter("target_interval")) {
        int64_t interval = context.getParameter<int64_t>("target_interval");
        if (interval <= 0) {
            throw ParameterValidationException("target_interval must be positive");
        }
        uint64_t startTime = input.timestampAt(0);
        uint64_t endTime = input.timestampAt(input.count - 1);

        uint64_t uinterval = static_cast<uint64_t>(interval);
        size_t estimatedPoints = (endTime > startTime) ? ((endTime - startTime) / uinterval + 1) : 1;
        if (estimatedPoints > MAX_INTERPOLATION_POINTS) {
            throw ParameterValidationException("target_interval too small: would generate " +
                                               std::to_string(estimatedPoints) + " points (max " +
                                               std::to_string(MAX_INTERPOLATION_POINTS) + ")");
        }

        for (uint64_t t = startTime; t <= endTime; t += uinterval) {
            targetTimestamps.push_back(t);
            if (t > UINT64_MAX - uinterval)
                break;
        }
    } else {
        std::string timestampStr = context.getParameter<std::string>("target_timestamps");
        std::istringstream ss(timestampStr);
        std::string token;

        while (std::getline(ss, token, ',')) {
            if (!token.empty()) {
                try {
                    targetTimestamps.push_back(std::stoull(token));
                } catch (const std::exception& e) {
                    throw ParameterValidationException("Invalid timestamp value \"" + token + "\": " + e.what());
                }
                if (targetTimestamps.size() > MAX_INTERPOLATION_POINTS) {
                    throw ParameterValidationException("Too many target_timestamps: exceeds limit of " +
                                                       std::to_string(MAX_INTERPOLATION_POINTS));
                }
            }
        }
    }

    result.timestamps = targetTimestamps;
    result.values.reserve(targetTimestamps.size());

    // Simple linear interpolation fallback using two-pointer approach.
    // Both input and target timestamps are sorted, so maintain search position
    // across iterations for O(n+m) instead of O(n*m).
    size_t searchPos = 0;
    for (uint64_t targetTime : targetTimestamps) {
        while (searchPos + 1 < input.count && input.timestampAt(searchPos + 1) < targetTime) {
            ++searchPos;
        }
        size_t i = searchPos;

        if (i + 1 < input.count) {
            uint64_t t1 = input.timestampAt(i);
            uint64_t t2 = input.timestampAt(i + 1);
            double v1 = input.valueAt(i);
            double v2 = input.valueAt(i + 1);

            if (t1 == t2) {
                result.values.push_back(v1);
            } else {
                double ratio = static_cast<double>(targetTime - t1) / static_cast<double>(t2 - t1);
                result.values.push_back(v1 + ratio * (v2 - v1));
            }
        } else {
            result.values.push_back(input.valueAt(input.count - 1));
        }
    }

    return seastar::make_ready_future<FunctionResult<double>>(std::move(result));
}

// Legacy function for backward compatibility
}  // namespace timestar::functions