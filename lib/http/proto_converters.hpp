#pragma once

// Bidirectional converters between Protobuf wire format and internal C++ types.
//
// All functions are pure (no Seastar dependency) and can be unit-tested standalone.
// Parse functions take raw bytes (const void*, size_t) and return internal types.
// Format functions take internal types and return serialized protobuf bytes.
//
// IMPORTANT: The proto package is "timestar", generating classes in ::timestar::,
// which collides with the project's own ::timestar:: namespace.  To avoid ODR
// violations, this header does NOT include timestar.pb.h and does NOT include
// any internal headers that define conflicting types (e.g. QueryResponse,
// StreamingBatch, SubscriptionStats).  Instead, all converter parameters use
// lightweight intermediate structs defined here that can be trivially constructed
// from either side.

#include <climits>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

// Forward declarations for types that don't conflict (in global namespace)
struct DownsamplePolicy;
struct RetentionPolicy;

namespace timestar::proto {

// ============================================================================
// Intermediate types for converters (no proto/internal header dependency)
// ============================================================================

// Mirrors HttpWriteHandler::FieldArrays
struct FieldArrays {
    std::vector<double> doubles;
    std::vector<uint8_t> bools;
    std::vector<std::string> strings;
    std::vector<int64_t> integers;
    enum Type { DOUBLE, BOOL, STRING, INTEGER } type;
};

// Mirrors HttpWriteHandler::MultiWritePoint
struct MultiWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::map<std::string, FieldArrays> fields;
    std::vector<uint64_t> timestamps;
};

// Variant for per-field typed values
using FieldValues =
    std::variant<std::vector<double>, std::vector<bool>, std::vector<std::string>, std::vector<int64_t>>;

// Mirrors timestar::SeriesResult
struct SeriesResultData {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::map<std::string, std::pair<std::vector<uint64_t>, FieldValues>> fields;
};

// Mirrors timestar::QueryStatistics
struct QueryStatisticsData {
    size_t seriesCount = 0;
    size_t pointCount = 0;
    size_t failedSeriesCount = 0;
    double executionTimeMs = 0.0;
    std::vector<int> shardsQueried;
    bool truncated = false;
    std::string truncationReason;
};

// Mirrors timestar::QueryResponse
struct QueryResponseData {
    bool success = false;
    std::vector<SeriesResultData> series;
    QueryStatisticsData statistics;
    std::string errorMessage;
    std::string errorCode;
};

// Mirrors timestar::StreamingDataPoint
struct StreamingDataPointData {
    std::string measurement;
    std::string field;
    std::map<std::string, std::string> tags;
    uint64_t timestamp = 0;
    std::variant<double, bool, std::string, int64_t> value;
};

// Mirrors timestar::StreamingBatch
struct StreamingBatchData {
    std::vector<StreamingDataPointData> points;
    uint64_t sequenceId = 0;
    std::string label;
    bool isDrop = false;
    uint64_t droppedCount = 0;
};

// Mirrors timestar::SubscriptionStats
struct SubscriptionStatsData {
    uint64_t id = 0;
    std::string measurement;
    std::map<std::string, std::string> scopes;
    std::vector<std::string> fields;
    std::string label;
    unsigned handlerShard = 0;
    uint64_t queueDepth = 0;
    uint64_t queueCapacity = 0;
    uint64_t droppedPoints = 0;
    uint64_t eventsSent = 0;
};

// Mirrors timestar::DerivedQueryResult
struct DerivedQueryResultData {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::string formula;
    struct Stats {
        size_t pointCount = 0;
        double executionTimeMs = 0.0;
        size_t subQueriesExecuted = 0;
        size_t pointsDroppedDueToAlignment = 0;
    } stats;
};

// Mirrors timestar::anomaly::AnomalySeriesPiece
struct AnomalySeriesPieceData {
    std::string piece;
    std::vector<std::string> groupTags;
    std::vector<double> values;
    std::optional<double> alertValue;
};

// Mirrors timestar::anomaly::AnomalyQueryResult
struct AnomalyQueryResultData {
    bool success = true;
    std::vector<uint64_t> times;
    std::vector<AnomalySeriesPieceData> series;
    struct Statistics {
        std::string algorithm;
        double bounds = 0.0;
        std::string seasonality;
        size_t anomalyCount = 0;
        size_t totalPoints = 0;
        double executionTimeMs = 0.0;
    } statistics;
    std::string errorMessage;
};

// Mirrors timestar::forecast::ForecastSeriesPiece
struct ForecastSeriesPieceData {
    std::string piece;
    std::vector<std::string> groupTags;
    std::vector<std::optional<double>> values;
};

// Mirrors timestar::forecast::ForecastStatistics
struct ForecastStatisticsData {
    std::string algorithm;
    double deviations = 0.0;
    std::string seasonality;
    double slope = 0.0;
    double intercept = 0.0;
    double rSquared = 0.0;
    double residualStdDev = 0.0;
    size_t historicalPoints = 0;
    size_t forecastPoints = 0;
    size_t seriesCount = 0;
    double executionTimeMs = 0.0;
};

// Mirrors timestar::forecast::ForecastQueryResult
struct ForecastQueryResultData {
    bool success = true;
    std::vector<uint64_t> times;
    size_t forecastStartIndex = 0;
    std::vector<ForecastSeriesPieceData> series;
    ForecastStatisticsData statistics;
    std::string errorMessage;
};

// ============================================================================
// Write converters
// ============================================================================

// Parse a WriteRequest proto from raw bytes into a vector of MultiWritePoints.
// Throws std::runtime_error on parse failure.
std::vector<MultiWritePoint> parseWriteRequest(const void* data, size_t size);

// Format a write success response as serialized WriteResponse proto bytes.
std::string formatWriteResponse(const std::string& status, int64_t pointsWritten, int64_t failedWrites = 0,
                                const std::vector<std::string>& errors = {});

// ============================================================================
// Query converters
// ============================================================================

// Parsed query request fields (mirroring GlazeQueryRequest)
struct ParsedQueryRequest {
    std::string query;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    std::string aggregationInterval;
};

// Parse a QueryRequest proto from raw bytes.
ParsedQueryRequest parseQueryRequest(const void* data, size_t size);

// Format a QueryResponseData to serialized QueryResponse proto bytes.
std::string formatQueryResponse(QueryResponseData& response);

// Format a query error response as serialized QueryResponse proto bytes.
std::string formatQueryError(const std::string& code, const std::string& message);

// ============================================================================
// Delete converters
// ============================================================================

struct ParsedDeleteRequest {
    std::string seriesKey;
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;
    std::vector<std::string> fields;
    uint64_t startTime = 0;
    uint64_t endTime = UINT64_MAX;
    bool isStructured = false;
    bool isPattern = false;
};

// Parse a BatchDeleteRequest proto from raw bytes into a vector of delete requests.
std::vector<ParsedDeleteRequest> parseBatchDeleteRequest(const void* data, size_t size);

// Parse a single DeleteRequest proto from raw bytes.
ParsedDeleteRequest parseSingleDeleteRequest(const void* data, size_t size);

// Format a delete response as serialized DeleteResponse proto bytes.
std::string formatDeleteResponse(const std::string& status, uint64_t deletedCount, uint64_t totalRequests,
                                 const std::string& errorMessage = "");

// ============================================================================
// Metadata converters (response-only since GET endpoints use query params)
// ============================================================================

std::string formatMeasurementsResponse(const std::vector<std::string>& measurements, size_t total);

std::string formatTagsResponse(const std::string& measurement,
                               const std::unordered_map<std::string, std::vector<std::string>>& tags);

std::string formatFieldsResponse(const std::string& measurement,
                                 const std::unordered_map<std::string, std::string>& fields);

std::string formatCardinalityResponse(const std::string& measurement, double estimatedSeriesCount,
                                      const std::unordered_map<std::string, double>& tagCardinalities);

// ============================================================================
// Retention converters
// ============================================================================

struct ParsedRetentionPutRequest {
    std::string measurement;
    std::string ttl;
    struct DownsampleData {
        std::string after;
        uint64_t afterNanos = 0;
        std::string interval;
        uint64_t intervalNanos = 0;
        std::string method;
    };
    std::optional<DownsampleData> downsample;
};

// Parse a RetentionPutRequest proto from raw bytes.
ParsedRetentionPutRequest parseRetentionPutRequest(const void* data, size_t size);

// Retention policy data for formatting (mirrors RetentionPolicy)
struct RetentionPolicyData {
    std::string measurement;
    std::string ttl;
    uint64_t ttlNanos = 0;
    std::optional<ParsedRetentionPutRequest::DownsampleData> downsample;
};

// Format retention get response as serialized RetentionGetResponse proto bytes.
std::string formatRetentionGetResponse(const RetentionPolicyData& policy);

// Format a generic status response as serialized StatusResponse proto bytes.
std::string formatStatusResponse(const std::string& status, const std::string& message = "",
                                 const std::string& code = "");

// ============================================================================
// Streaming converters
// ============================================================================

struct ParsedSubscribeRequest {
    std::string query;
    struct QueryEntry {
        std::string query;
        std::string label;
    };
    std::vector<QueryEntry> queries;
    std::string formula;
    uint64_t startTime = 0;
    bool backfill = false;
    std::string aggregationInterval;
};

// Parse a SubscribeRequest proto from raw bytes.
ParsedSubscribeRequest parseSubscribeRequest(const void* data, size_t size);

// Format a StreamingBatchData to serialized StreamingBatch proto bytes.
std::string formatStreamingBatch(const StreamingBatchData& batch);

// Format subscriptions list as serialized SubscriptionsResponse proto bytes.
std::string formatSubscriptionsResponse(const std::vector<SubscriptionStatsData>& subscriptions);

// ============================================================================
// Derived query converters
// ============================================================================

struct ParsedDerivedQueryRequest {
    std::map<std::string, std::string> queries;  // name -> query string
    std::string formula;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    std::string aggregationInterval;
};

// Parse a DerivedQueryRequest proto from raw bytes.
ParsedDerivedQueryRequest parseDerivedQueryRequest(const void* data, size_t size);

// Format a DerivedQueryResultData as serialized DerivedQueryResponse proto bytes.
std::string formatDerivedQueryResponse(const DerivedQueryResultData& result);

// Format an anomaly result as serialized AnomalyResponse proto bytes.
std::string formatAnomalyResponse(const AnomalyQueryResultData& result);

// Format a forecast result as serialized ForecastResponse proto bytes.
std::string formatForecastResponse(const ForecastQueryResultData& result);

// Format a derived query error as serialized DerivedQueryResponse proto bytes.
std::string formatDerivedQueryError(const std::string& code, const std::string& message);

// ============================================================================
// Health / generic error
// ============================================================================

std::string formatHealthResponse(const std::string& status);

std::string formatErrorResponse(const std::string& message, const std::string& code = "");

}  // namespace timestar::proto
