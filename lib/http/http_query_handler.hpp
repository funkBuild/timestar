#pragma once

#include "field_values.hpp"
#include "native_index.hpp"
#include "query_parser.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"

#include <glaze/json.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <utility>
#include <vector>

// Forward declaration
class Engine;

// Glaze struct forward declaration
struct GlazeQueryRequest;

namespace timestar {

// Forward declaration for finalizeSingleShardPartials (defined in lib/query/aggregator.hpp)
struct PartialAggregationResult;

}  // namespace timestar

namespace timestar::http {

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
    // Takes the request BY VALUE, not by reference: this is a coroutine, so a
    // reference parameter is read after every suspension point while callers do
    // not all outlive us — the SSE backfill loop passes a loop-body local
    // through seastar::with_timeout, which by design does NOT cancel the inner
    // future, so on a backfill timeout that local dies mid-flight.  By-value
    // puts the copy in the parameter, before the coroutine body can suspend.
    seastar::future<QueryResponse> executeQuery(QueryRequest request);

    // Parse JSON request body (public for testing)
    QueryRequest parseQueryRequest(const GlazeQueryRequest& glazeReq);

    // Parse interval string like "5m", "1h", "30s" to nanoseconds
    static uint64_t parseInterval(const std::string& interval);

    // Finalize partial aggregation results from a single shard into the query response.
    // Each partial must have a unique groupKey; callers with duplicate groupKeys must use
    // the merge fallback path (mergePartialAggregationsGrouped) instead.
    // Coroutine: yields every ~64k points (reactor-stall prevention).
    // Public static for testability.
    static seastar::future<> finalizeSingleShardPartials(std::vector<PartialAggregationResult>& partials,
                                                         AggregationMethod method, QueryResponse& response);

    // Format response as JSON (public for testing).
    // Coroutine — see ResponseFormatter::format for the yielding policy.
    // Note: field filtering is performed in executeQuery(), so by the time this
    // function is called, response.series already contains only the requested fields.
    seastar::future<std::string> formatQueryResponse(QueryResponse& response);

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
    // Coroutines: the merge and per-point loops yield periodically so a large
    // result set cannot monopolize the coordinator's reactor.
    seastar::future<std::optional<QueryResponse>> finalizeSingleShardResponse(
        const QueryRequest& request, std::pair<unsigned, ShardQueryResult>& shardResult, QueryTimingInfo& timing,
        QueryResponse& response);
    seastar::future<std::optional<QueryResponse>> finalizeMultiShardResponse(
        const QueryRequest& request, std::vector<std::pair<unsigned, ShardQueryResult>>& shardResults,
        QueryTimingInfo& timing, QueryResponse& response);

    // Merge series sharing measurement+tags into one multi-field SeriesResult.
    // Both finalize paths must call this so the response shape does not depend
    // on shard placement (each aggregation path emits one series per field).
    static void consolidateSeriesFields(std::vector<SeriesResult>& series);

    // Phase 4: timing breakdown + slow-query logging.
    void logQueryCompletion(const QueryRequest& request, QueryTimingInfo& timing);
};

// Internals exposed for testing.  Not part of the handler's public surface.
namespace detail {

// Bounded re-read of one non-numeric series, reducing to LATEST-per-bucket as it
// goes so peak memory is O(points in one chunk) rather than O(points in range).
// Production reaches this only as recovery after a single-shot read threw, but
// its bucket-merge behaviour is worth testing directly -- see
// nonnumeric_chunked_recovery_test.cpp.
//
// `initialChunkWidth` is the starting chunk width in nanoseconds; 0 selects the
// default heuristic. Tests pass a small value to force many chunk boundaries,
// which is what exercises the merge of a bucket split across chunks.
//
// Returns nullopt when the series is numeric (the caller then reports the drop).
//
// Parameters are BY VALUE, not by reference: this is a coroutine, so every
// parameter is read after each co_await. A caller passing a temporary (or any
// object that does not outlive the whole chunk loop) would leave the coroutine
// reading freed memory. Same rule as HttpQueryHandler::executeQuery.
seastar::future<std::optional<SeriesResult>> queryNonNumericBucketedChunked(Engine& engine, std::string seriesKey,
                                                                            SeriesId128 seriesId, std::string field,
                                                                            std::map<std::string, std::string> tags,
                                                                            std::string measurement, uint64_t startTime,
                                                                            uint64_t endTime, uint64_t interval,
                                                                            uint64_t initialChunkWidth = 0);

}  // namespace detail

}  // namespace timestar::http

// Backward-compatibility aliases: these types historically lived directly in
// namespace timestar. New code should use timestar::http:: directly.
namespace timestar {
using http::HttpQueryHandler;
using http::QueryResponse;
using http::QueryStatistics;
using http::SeriesResult;
}  // namespace timestar
