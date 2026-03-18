#pragma once

#include "../anomaly/anomaly_result.hpp"  // Reuse Seasonality enum

#include <cmath>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar {
namespace forecast {

// Reuse Seasonality from anomaly namespace
using anomaly::parseSeasonality;
using anomaly::Seasonality;
using anomaly::seasonalityToPeriod;

// Extended seasonality modes for forecasting
enum class ForecastSeasonality {
    NONE,
    HOURLY,
    DAILY,
    WEEKLY,
    AUTO,  // Auto-detect single best period
    MULTI  // Auto-detect and combine multiple periods
};

// Convert string to ForecastSeasonality
inline ForecastSeasonality parseForecastSeasonality(const std::string& str) {
    if (str == "none")
        return ForecastSeasonality::NONE;
    if (str == "hourly")
        return ForecastSeasonality::HOURLY;
    if (str == "daily")
        return ForecastSeasonality::DAILY;
    if (str == "weekly")
        return ForecastSeasonality::WEEKLY;
    if (str == "auto")
        return ForecastSeasonality::AUTO;
    if (str == "multi")
        return ForecastSeasonality::MULTI;
    throw std::invalid_argument("Unknown seasonality: " + str);
}

inline std::string forecastSeasonalityToString(ForecastSeasonality s) {
    switch (s) {
        case ForecastSeasonality::NONE:
            return "none";
        case ForecastSeasonality::HOURLY:
            return "hourly";
        case ForecastSeasonality::DAILY:
            return "daily";
        case ForecastSeasonality::WEEKLY:
            return "weekly";
        case ForecastSeasonality::AUTO:
            return "auto";
        case ForecastSeasonality::MULTI:
            return "multi";
    }
    return "unknown";
}

// Convert ForecastSeasonality to period in data points
inline size_t forecastSeasonalityToPeriod(ForecastSeasonality s, uint64_t dataIntervalNs) {
    // For AUTO/MULTI, return 0 (will be detected)
    if (s == ForecastSeasonality::AUTO || s == ForecastSeasonality::MULTI || s == ForecastSeasonality::NONE) {
        return 0;
    }

    // Convert to anomaly::Seasonality for period calculation
    anomaly::Seasonality as;
    switch (s) {
        case ForecastSeasonality::HOURLY:
            as = anomaly::Seasonality::HOURLY;
            break;
        case ForecastSeasonality::DAILY:
            as = anomaly::Seasonality::DAILY;
            break;
        case ForecastSeasonality::WEEKLY:
            as = anomaly::Seasonality::WEEKLY;
            break;
        default:
            return 0;
    }
    return anomaly::seasonalityToPeriod(as, dataIntervalNs);
}

// Supported forecasting algorithms
enum class Algorithm {
    LINEAR,   // Simple linear regression extrapolation
    SEASONAL  // SARIMA-based seasonal forecasting
};

// Linear model types (for Algorithm::LINEAR only)
enum class LinearModelType {
    DEFAULT,  // Standard least-squares regression
    SIMPLE,   // Less sensitive to recent changes (uniform weighting on last half)
    REACTIVE  // More sensitive to recent changes (exponential decay weighting)
};

// Convert string to Algorithm enum
inline Algorithm parseAlgorithm(const std::string& str) {
    if (str == "linear")
        return Algorithm::LINEAR;
    if (str == "seasonal")
        return Algorithm::SEASONAL;
    throw std::invalid_argument("Unknown forecast algorithm: " + str + ". Use 'linear' or 'seasonal'");
}

// Convert Algorithm enum to string
inline std::string algorithmToString(Algorithm algo) {
    switch (algo) {
        case Algorithm::LINEAR:
            return "linear";
        case Algorithm::SEASONAL:
            return "seasonal";
    }
    return "unknown";
}

// Convert string to LinearModelType enum
inline LinearModelType parseLinearModel(const std::string& str) {
    if (str == "default")
        return LinearModelType::DEFAULT;
    if (str == "simple")
        return LinearModelType::SIMPLE;
    if (str == "reactive")
        return LinearModelType::REACTIVE;
    throw std::invalid_argument("Unknown linear model type: " + str + ". Use 'default', 'simple', or 'reactive'");
}

// Convert LinearModelType enum to string
inline std::string linearModelToString(LinearModelType model) {
    switch (model) {
        case LinearModelType::DEFAULT:
            return "default";
        case LinearModelType::SIMPLE:
            return "simple";
        case LinearModelType::REACTIVE:
            return "reactive";
    }
    return "unknown";
}

// Parse duration string like "1w", "3d", "12h", "30m" to nanoseconds
inline uint64_t parseDurationToNs(const std::string& duration) {
    if (duration.empty()) {
        throw std::invalid_argument("Empty duration string");
    }

    // Find the position where unit starts (first non-digit/non-decimal from the left)
    size_t unitPos = duration.size();
    for (size_t i = 0; i < duration.size(); ++i) {
        if (!std::isdigit(duration[i]) && duration[i] != '.') {
            unitPos = i;
            break;
        }
    }

    if (unitPos == 0) {
        throw std::invalid_argument("Duration missing numeric value: " + duration);
    }
    if (unitPos == duration.size()) {
        throw std::invalid_argument("Duration missing unit suffix: " + duration);
    }

    // Parse the numeric part
    std::string numStr = duration.substr(0, unitPos);
    std::string unit = duration.substr(unitPos);

    double value;
    try {
        value = std::stod(numStr);
    } catch (const std::exception&) {
        throw std::invalid_argument("Invalid duration value: " + duration);
    }

    if (value <= 0) {
        throw std::invalid_argument("Duration must be positive: " + duration);
    }

    // Convert to nanoseconds based on unit
    uint64_t multiplier;
    if (unit == "ns") {
        multiplier = 1;
    } else if (unit == "us" || unit == "µs") {
        multiplier = 1000;
    } else if (unit == "ms") {
        multiplier = 1000000;
    } else if (unit == "s") {
        multiplier = 1000000000;
    } else if (unit == "m") {
        multiplier = 60ULL * 1000000000;
    } else if (unit == "h") {
        multiplier = 3600ULL * 1000000000;
    } else if (unit == "d") {
        multiplier = 86400ULL * 1000000000;
    } else if (unit == "w") {
        multiplier = 604800ULL * 1000000000;  // 7 days
    } else {
        throw std::invalid_argument("Unknown duration unit '" + unit + "'. Use: ns, us, ms, s, m, h, d, w");
    }

    double result = value * static_cast<double>(multiplier);
    if (result < 1.0) {
        throw std::invalid_argument("Duration resolves to less than 1 nanosecond: " + duration);
    }
    return static_cast<uint64_t>(std::llround(result));
}

// Configuration for forecasting
struct ForecastConfig {
    Algorithm algorithm = Algorithm::LINEAR;
    double deviations = 2.0;     // Confidence interval width (1-4 std devs)
    size_t forecastHorizon = 0;  // 0 = auto (match historical period)

    // Seasonality configuration
    ForecastSeasonality forecastSeasonality = ForecastSeasonality::NONE;
    Seasonality seasonality = Seasonality::NONE;  // Keep for backwards compat

    // Auto-detection settings (for AUTO/MULTI modes)
    size_t maxSeasonalComponents = 3;  // Max periods to detect
    double seasonalThreshold = 0.2;    // Min confidence for period
    size_t minPeriod = 4;              // Minimum period to consider
    size_t maxPeriod = 0;              // Maximum period (0 = n/2)

    // SARIMA parameters for seasonal forecasting
    int arOrder = 2;          // Autoregressive order (p)
    int maOrder = 2;          // Moving average order (q)
    int seasonalArOrder = 1;  // Seasonal AR order (P)
    int seasonalMaOrder = 1;  // Seasonal MA order (Q)

    // Minimum points required for forecasting
    size_t minDataPoints = 10;

    // Linear model type
    LinearModelType linearModel = LinearModelType::DEFAULT;  // Model type for linear algorithm

    // History duration
    uint64_t historyDurationNs = 0;  // 0 = use entire query range

    // Auto-windowing: trim old data before expensive computation
    size_t maxHistoryCycles = 4;     // Keep this many cycles of the largest period
    bool disableAutoWindow = false;  // Set true to skip auto-windowing
};

// Input for forecasting
struct ForecastInput {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Optional group tags for multi-series
    std::vector<std::string> groupTags;

    bool empty() const { return timestamps.empty(); }
    size_t size() const { return timestamps.size(); }
};

// Output from a single forecaster
struct ForecastOutput {
    // Historical values (for "past" series piece)
    std::vector<double> past;

    // Predicted values (for "forecast" series piece)
    std::vector<double> forecast;

    // Confidence bounds
    std::vector<double> upper;
    std::vector<double> lower;

    // Counts
    size_t historicalCount = 0;
    size_t forecastCount = 0;

    // Model fit statistics
    double slope = 0.0;           // For linear: trend slope
    double intercept = 0.0;       // For linear: y-intercept
    double rSquared = 0.0;        // Goodness of fit
    double residualStdDev = 0.0;  // For confidence bounds

    bool empty() const { return past.empty() && forecast.empty(); }
};

// A single piece of the forecast result (past, forecast, upper, lower)
struct ForecastSeriesPiece {
    // Piece identifier: "past", "forecast", "upper", "lower"
    std::string piece;

    // Group tags for this series (e.g., ["host=server01", "region=us-west"])
    std::vector<std::string> groupTags;

    // Values for this piece
    // - past: actual values for historical period, null for forecast period
    // - forecast: null (or last point) for historical, predicted for forecast period
    // - upper/lower: null for historical, bounds for forecast period
    std::vector<std::optional<double>> values;

    // Query index (for multi-query support)
    size_t queryIndex = 0;
};

// Forecast execution statistics
struct ForecastStatistics {
    std::string algorithm;
    double deviations = 0.0;
    std::string seasonality;
    std::vector<size_t> detectedPeriods;  // Periods detected in AUTO/MULTI mode

    // Model parameters
    double slope = 0.0;
    double intercept = 0.0;
    double rSquared = 0.0;
    double residualStdDev = 0.0;

    // Counts
    size_t historicalPoints = 0;
    size_t forecastPoints = 0;
    size_t seriesCount = 0;

    // Windowing
    size_t originalPoints = 0;
    size_t windowedPoints = 0;

    // Performance
    double executionTimeMs = 0.0;
};

// Complete result of forecast query
struct ForecastQueryResult {
    // Extended timestamps covering historical + forecast period
    std::vector<uint64_t> times;

    // Index where forecast begins (times[forecastStartIndex] is first forecast point)
    size_t forecastStartIndex = 0;

    // Multiple series pieces (past, forecast, upper, lower per group)
    std::vector<ForecastSeriesPiece> series;

    // Execution statistics
    ForecastStatistics statistics;

    // Error information
    bool success = true;
    std::string errorMessage;

    bool empty() const { return times.empty(); }

    // Helper to get piece by name and group
    const ForecastSeriesPiece* getPiece(const std::string& pieceName,
                                        const std::vector<std::string>& groupTags = {}) const {
        for (const auto& piece : series) {
            if (piece.piece == pieceName && piece.groupTags == groupTags) {
                return &piece;
            }
        }
        return nullptr;
    }
};

// SARIMA model state for seasonal forecasting
struct SARIMAState {
    // Model coefficients
    std::vector<double> arCoeffs;  // AR coefficients
    std::vector<double> maCoeffs;  // MA coefficients
    std::vector<double> seasonalArCoeffs;
    std::vector<double> seasonalMaCoeffs;

    // Seasonal period
    size_t seasonalPeriod = 0;

    // Differencing state
    double lastValue = 0.0;
    double lastSeasonalValue = 0.0;

    // Residuals for MA terms
    std::vector<double> residuals;

    // Model fit
    double sigma = 0.0;  // Residual standard deviation
};

}  // namespace forecast
}  // namespace timestar
