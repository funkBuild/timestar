#pragma once

#include "native_index.hpp"
#include "query_parser.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"

#include <glaze/glaze.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include "field_values.hpp"

#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>

// Forward declaration
class Engine;

// Glaze struct forward declaration
struct GlazeQueryRequest;

namespace timestar {

// Forward declaration for finalizeSingleShardPartials
struct PartialAggregationResult;

// Forward declarations for executeQuery phase types (defined in http_query_handler.cpp)
struct SeriesQueryContext;
struct SeriesDiscoveryResult;
struct ShardQueryResult;
struct ShardFanOutResult;
struct QueryTimingInfo;

struct SeriesResult {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::map<std::string, std::pair<std::vector<uint64_t>, FieldValues>> fields;  // fieldName -> (timestamps, values)
};

struct QueryStatistics {
    size_t seriesCount = 0;
    size_t pointCount = 0;
    size_t failedSeriesCount = 0;  // Number of series that failed to query
    double executionTimeMs = 0.0;
    std::vector<int> shardsQueried;
    bool truncated = false;        // True if results were truncated due to limits
    std::string truncationReason;  // Reason for truncation if truncated
};

struct QueryResponse {
    bool success = false;
    std::vector<SeriesResult> series;
    QueryStatistics statistics;
    std::string errorMessage;
    std::string errorCode;
};

class HttpQueryHandler {
private:
    seastar::sharded<Engine>* engineSharded;

public:
    // Security limits to prevent DoS attacks
    static size_t maxQueryBodySize() { return timestar::config().http.max_query_body_size; }

    // Query result limits to prevent OOM and excessive response sizes
    static size_t maxSeriesCount() { return timestar::config().http.max_series_count; }
    static size_t maxTotalPoints() { return timestar::config().http.max_total_points; }

    // Query timeout to prevent indefinite hangs from stuck shards
    static std::chrono::seconds defaultQueryTimeout() {
        return std::chrono::seconds(timestar::config().http.query_timeout_seconds);
    }

    explicit HttpQueryHandler(seastar::sharded<Engine>* engine) : engineSharded(engine) {}

    // Validate request body size and content type (public for testing).
    // Returns a reply with an error if validation fails, or nullptr if valid.
    std::unique_ptr<seastar::http::reply> validateRequest(const seastar::http::request& req) const;

    // Main query handler
    seastar::future<std::unique_ptr<seastar::http::reply>> handleQuery(std::unique_ptr<seastar::http::request> req);

    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& r, std::string_view authToken = "");

    // Execute query across shards
    seastar::future<QueryResponse> executeQuery(const QueryRequest& request);

    // Parse JSON request body (public for testing)
    QueryRequest parseQueryRequest(const GlazeQueryRequest& glazeReq);

    // Parse interval string like "5m", "1h", "30s" to nanoseconds
    static uint64_t parseInterval(const std::string& interval);

    // Finalize partial aggregation results from a single shard into the query response.
    // Each partial must have a unique groupKey; callers with duplicate groupKeys must use
    // the merge fallback path (mergePartialAggregationsGrouped) instead.
    // Public static for testability.
    static void finalizeSingleShardPartials(std::vector<PartialAggregationResult>& partials, AggregationMethod method,
                                            QueryResponse& response);

    // Format response as JSON (public for testing)
    // Note: field filtering is performed in executeQuery(), so by the time this
    // function is called, response.series already contains only the requested fields.
    std::string formatQueryResponse(QueryResponse& response);

    // Create error response (public for testing)
    std::string createErrorResponse(const std::string& code, const std::string& message);

    // Test-only utility: returns all shard IDs 0..smp::count.
    // Production query path iterates shards inline in executeQuery().
    std::vector<unsigned> determineTargetShards(const QueryRequest& request);

    // Test-only utility: flat concatenation of shard result vectors.
    // Production query path uses Aggregator::mergePartialAggregationsGrouped()
    // which performs full two-phase aggregation, not a simple concat.
    std::vector<SeriesResult> mergeResults(std::vector<std::vector<SeriesResult>> shardResults);

private:
    // Query all relevant shards
    seastar::future<std::vector<SeriesResult>> queryAllShards(const QueryRequest& request);

    // ---- executeQuery phases (see http_query_handler.cpp for contracts) ----

    // Phase 1: scatter-gather series discovery across all shards.
    seastar::future<SeriesDiscoveryResult> discoverSeriesAcrossShards(const QueryRequest& request);

    // Phase 2: fan out query execution to shards that own series
    // (consumes the per-shard context vectors in seriesByShard).
    seastar::future<ShardFanOutResult> fanOutShardQueries(const QueryRequest& request,
                                                          std::vector<std::vector<SeriesQueryContext>>& seriesByShard);

    // Phase 3a/3b: partial merge + aggregation finalize into `response`.
    // Return std::nullopt on success; a complete error QueryResponse on early exit.
    std::optional<QueryResponse> finalizeSingleShardResponse(const QueryRequest& request,
                                                             std::pair<unsigned, ShardQueryResult>& shardResult,
                                                             QueryTimingInfo& timing, QueryResponse& response);
    std::optional<QueryResponse> finalizeMultiShardResponse(
        const QueryRequest& request, std::vector<std::pair<unsigned, ShardQueryResult>>& shardResults,
        QueryTimingInfo& timing, QueryResponse& response);

    // Phase 4: timing breakdown + slow-query logging.
    void logQueryCompletion(const QueryRequest& request, QueryTimingInfo& timing);
};

}  // namespace timestar
