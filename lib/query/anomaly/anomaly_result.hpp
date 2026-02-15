#ifndef ANOMALY_RESULT_H_INCLUDED
#define ANOMALY_RESULT_H_INCLUDED

#include <string>
#include <vector>
#include <optional>
#include <map>
#include <cstdint>
#include <stdexcept>

namespace tsdb {
namespace anomaly {

// Supported anomaly detection algorithms
enum class Algorithm {
    BASIC,   // Rolling quantile/stddev - no seasonality awareness
    AGILE,   // SARIMA-based - adapts to level shifts
    ROBUST   // STL decomposition - stable predictions
};

// Seasonality options for Agile and Robust algorithms
enum class Seasonality {
    NONE,
    HOURLY,  // 60-minute cycle
    DAILY,   // 24-hour cycle
    WEEKLY   // 7-day cycle
};

// Convert string to Algorithm enum
inline Algorithm parseAlgorithm(const std::string& str) {
    if (str == "basic") return Algorithm::BASIC;
    if (str == "agile") return Algorithm::AGILE;
    if (str == "robust") return Algorithm::ROBUST;
    throw std::invalid_argument("Unknown algorithm: " + str + ". Use 'basic', 'agile', or 'robust'");
}

// Convert Algorithm enum to string
inline std::string algorithmToString(Algorithm algo) {
    switch (algo) {
        case Algorithm::BASIC: return "basic";
        case Algorithm::AGILE: return "agile";
        case Algorithm::ROBUST: return "robust";
    }
    return "unknown";
}

// Convert string to Seasonality enum
inline Seasonality parseSeasonality(const std::string& str) {
    if (str.empty() || str == "none") return Seasonality::NONE;
    if (str == "hourly") return Seasonality::HOURLY;
    if (str == "daily") return Seasonality::DAILY;
    if (str == "weekly") return Seasonality::WEEKLY;
    throw std::invalid_argument("Unknown seasonality: " + str + ". Use 'hourly', 'daily', or 'weekly'");
}

// Convert Seasonality to period in data points
// Assumes 1-minute data intervals by default; caller should adjust for actual interval
inline size_t seasonalityToPeriod(Seasonality seasonality, uint64_t dataIntervalNs = 60000000000ULL) {
    uint64_t periodNs = 0;
    switch (seasonality) {
        case Seasonality::NONE:
            return 0;
        case Seasonality::HOURLY:
            periodNs = 3600ULL * 1000000000ULL;  // 1 hour
            break;
        case Seasonality::DAILY:
            periodNs = 86400ULL * 1000000000ULL;  // 24 hours
            break;
        case Seasonality::WEEKLY:
            periodNs = 604800ULL * 1000000000ULL;  // 7 days
            break;
    }
    return static_cast<size_t>(periodNs / dataIntervalNs);
}

// Configuration for anomaly detection
struct AnomalyConfig {
    Algorithm algorithm = Algorithm::BASIC;
    double bounds = 2.0;  // Standard deviations (1-4 typical)
    Seasonality seasonality = Seasonality::NONE;

    // Rolling window size for Basic algorithm
    size_t windowSize = 60;

    // Minimum data points required before producing bounds
    size_t minDataPoints = 10;

    // For STL decomposition (Robust algorithm)
    size_t stlSeasonalWindow = 7;  // Must be odd
    bool stlRobust = true;         // Robust to outliers
};

// A single piece of the anomaly result (raw, upper, lower, scores, etc.)
struct AnomalySeriesPiece {
    // Piece identifier: "raw", "upper", "lower", "scores", "ratings"
    std::string piece;

    // Group tags for this series (e.g., ["host=server01", "region=us-west"])
    std::vector<std::string> groupTags;

    // Values for this piece (same length as times array)
    std::vector<double> values;

    // Alert value (only for "scores" piece) - maximum anomaly score
    std::optional<double> alertValue;

    // Alert window end timestamp (matches Datadog format)
    std::optional<uint64_t> alertWindowEnd;

    // Query index (for multi-query support)
    size_t queryIndex = 0;
};

// Complete result of anomaly detection
struct AnomalyQueryResult {
    // Shared timestamps for all pieces
    std::vector<uint64_t> times;

    // Multiple series pieces (raw, upper, lower, scores, etc.)
    std::vector<AnomalySeriesPiece> series;

    // Execution statistics
    struct Statistics {
        std::string algorithm;
        double bounds = 0.0;
        std::string seasonality;
        size_t anomalyCount = 0;
        size_t totalPoints = 0;
        double executionTimeMs = 0.0;
    } statistics;

    // Error information (if detection failed)
    bool success = true;
    std::string errorMessage;

    // Helper to check if result is empty
    bool empty() const { return times.empty(); }

    // Helper to get piece by name
    const AnomalySeriesPiece* getPiece(const std::string& pieceName) const {
        for (const auto& piece : series) {
            if (piece.piece == pieceName) {
                return &piece;
            }
        }
        return nullptr;
    }
};

// STL decomposition components (used by Robust algorithm)
struct STLComponents {
    std::vector<double> trend;
    std::vector<double> seasonal;
    std::vector<double> residual;

    bool empty() const { return trend.empty(); }
    size_t size() const { return trend.size(); }
};

// SARIMA model parameters (used by Agile algorithm)
struct SARIMAParams {
    // Non-seasonal parameters
    int p = 1;  // Autoregressive order
    int d = 1;  // Differencing order
    int q = 1;  // Moving average order

    // Seasonal parameters
    int P = 1;  // Seasonal AR order
    int D = 1;  // Seasonal differencing order
    int Q = 1;  // Seasonal MA order
    int s = 0;  // Seasonal period (0 = no seasonality)
};

} // namespace anomaly
} // namespace tsdb

#endif // ANOMALY_RESULT_H_INCLUDED
