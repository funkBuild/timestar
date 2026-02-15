#ifndef DERIVED_QUERY_EXECUTOR_H_INCLUDED
#define DERIVED_QUERY_EXECUTOR_H_INCLUDED

#include "derived_query.hpp"
#include "expression_parser.hpp"
#include "expression_evaluator.hpp"
#include "series_aligner.hpp"
#include "query_parser.hpp"
#include "http_query_handler.hpp"
#include "anomaly/anomaly_result.hpp"
#include "anomaly/anomaly_executor.hpp"
#include "forecast/forecast_result.hpp"
#include "forecast/forecast_executor.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <map>
#include <string>
#include <memory>
#include <chrono>
#include <variant>

// Forward declarations
class Engine;
class LevelDBIndex;

namespace tsdb {

// Configuration for derived query execution
struct DerivedQueryConfig {
    // Alignment strategy for combining series
    AlignmentStrategy alignmentStrategy = AlignmentStrategy::INNER;

    // Interpolation method for missing values
    InterpolationMethod interpolationMethod = InterpolationMethod::LINEAR;

    // Maximum number of sub-queries allowed
    size_t maxSubQueries = 10;

    // Maximum total points across all sub-queries
    size_t maxTotalPoints = 10000000;

    // Timeout for the entire derived query (milliseconds)
    uint64_t timeoutMs = 30000;
};

// Result type that can be regular, anomaly, or forecast result
using DerivedQueryResultVariant = std::variant<
    DerivedQueryResult,
    anomaly::AnomalyQueryResult,
    forecast::ForecastQueryResult
>;

// Executes derived metric queries
class DerivedQueryExecutor {
public:
    DerivedQueryExecutor(seastar::sharded<Engine>* engine,
                        seastar::sharded<LevelDBIndex>* index = nullptr,
                        DerivedQueryConfig config = {});

    // Execute a derived query
    seastar::future<DerivedQueryResult> execute(const DerivedQueryRequest& request);

    // Execute from JSON request body
    seastar::future<DerivedQueryResult> executeFromJson(const std::string& jsonBody);

    // Execute with support for anomaly detection
    seastar::future<DerivedQueryResultVariant> executeWithAnomaly(
        const DerivedQueryRequest& request);

    // Execute from JSON with anomaly support
    seastar::future<DerivedQueryResultVariant> executeFromJsonWithAnomaly(
        const std::string& jsonBody);

    // Format result as JSON response
    std::string formatResponse(const DerivedQueryResult& result);

    // Format anomaly result as JSON response
    std::string formatAnomalyResponse(const anomaly::AnomalyQueryResult& result);

    // Format forecast result as JSON response
    std::string formatForecastResponse(const forecast::ForecastQueryResult& result);

    // Format variant result as JSON response
    std::string formatResponseVariant(const DerivedQueryResultVariant& result);

    // Create error response JSON
    static std::string createErrorResponse(const std::string& code, const std::string& message);

    // Check if a formula is an anomaly function
    static bool isAnomalyFormula(const std::string& formula);

    // Check if a formula is a forecast function
    static bool isForecastFormula(const std::string& formula);

private:
    seastar::sharded<Engine>* engine_;
    seastar::sharded<LevelDBIndex>* index_;
    DerivedQueryConfig config_;

    // Execute a single sub-query
    seastar::future<SubQueryResult> executeSubQuery(
        const std::string& name,
        const QueryRequest& query);

    // Execute all sub-queries in parallel
    seastar::future<std::map<std::string, SubQueryResult>> executeAllSubQueries(
        const DerivedQueryRequest& request);

    // Convert HTTP query handler results to SubQueryResult
    SubQueryResult convertQueryResponse(
        const std::string& name,
        const QueryRequest& query,
        const std::vector<SeriesResult>& results);

    // Validate request before execution
    void validateRequest(const DerivedQueryRequest& request);

    // Execute anomaly detection on query results
    seastar::future<anomaly::AnomalyQueryResult> executeAnomalyDetection(
        const DerivedQueryRequest& request,
        const ExpressionNode& anomalyNode);

    // Execute forecast on query results
    seastar::future<forecast::ForecastQueryResult> executeForecast(
        const DerivedQueryRequest& request,
        const ExpressionNode& forecastNode);
};

// JSON structures for derived query API (using Glaze)
struct GlazeDerivedQueryRequest {
    std::map<std::string, std::string> queries;  // name -> query string
    std::string formula;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    std::string aggregationInterval;  // e.g., "5m", "1h"
};

struct GlazeDerivedQueryResponse {
    std::string status;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::string formula;

    struct Statistics {
        size_t pointCount = 0;
        double executionTimeMs = 0.0;
        size_t subQueriesExecuted = 0;
        size_t pointsDroppedDueToAlignment = 0;
    } statistics;

    struct Error {
        std::string code;
        std::string message;
    } error;
};

// Glaze-serializable struct for anomaly response
struct GlazeAnomalySeriesPiece {
    std::string piece;
    std::vector<std::string> group_tags;
    std::vector<double> values;  // NaN/Inf will be handled during serialization
    std::optional<double> alert_value;
};

struct GlazeAnomalyStatistics {
    std::string algorithm;
    double bounds = 0.0;
    std::string seasonality;
    size_t anomaly_count = 0;
    size_t total_points = 0;
    double execution_time_ms = 0.0;
};

struct GlazeAnomalyResponse {
    std::string status;
    std::vector<uint64_t> times;
    std::vector<GlazeAnomalySeriesPiece> series;
    GlazeAnomalyStatistics statistics;
    struct Error {
        std::string message;
    } error;
};

// Glaze-serializable struct for forecast response
struct GlazeForecastSeriesPiece {
    std::string piece;
    std::vector<std::string> group_tags;
    std::vector<std::optional<double>> values;
};

struct GlazeForecastStatistics {
    std::string algorithm;
    double deviations = 0.0;
    std::string seasonality;
    double slope = 0.0;
    double intercept = 0.0;
    double r_squared = 0.0;
    double residual_std_dev = 0.0;
    size_t historical_points = 0;
    size_t forecast_points = 0;
    size_t series_count = 0;
    double execution_time_ms = 0.0;
};

struct GlazeForecastResponse {
    std::string status;
    std::vector<uint64_t> times;
    size_t forecast_start_index = 0;
    std::vector<GlazeForecastSeriesPiece> series;
    GlazeForecastStatistics statistics;
    struct Error {
        std::string message;
    } error;
};

} // namespace tsdb

#endif // DERIVED_QUERY_EXECUTOR_H_INCLUDED
