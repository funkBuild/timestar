#ifndef HTTP_QUERY_HANDLER_H_INCLUDED
#define HTTP_QUERY_HANDLER_H_INCLUDED

#include "query_parser.hpp"
#include "query_planner.hpp"
#include "leveldb_index.hpp"
#include "series_id.hpp"
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <glaze/glaze.hpp>
#include <memory>
#include <chrono>
#include <variant>

// Forward declaration
class Engine;

// Glaze struct forward declaration
struct GlazeQueryRequest;

namespace tsdb {

// Variant type for field values - can be double, bool, or string
using FieldValues = std::variant<
    std::vector<double>,
    std::vector<bool>,
    std::vector<std::string>
>;

struct SeriesResult {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::map<std::string, std::pair<std::vector<uint64_t>, FieldValues>> fields;  // fieldName -> (timestamps, values)
};

struct QueryStatistics {
    size_t seriesCount = 0;
    size_t pointCount = 0;
    size_t failedSeriesCount = 0;    // Number of series that failed to query
    double executionTimeMs = 0.0;
    std::vector<int> shardsQueried;
    bool truncated = false;          // True if results were truncated due to limits
    std::string truncationReason;    // Reason for truncation if truncated
};

struct QueryResponse {
    bool success = false;
    std::vector<SeriesResult> series;
    QueryStatistics statistics;
    std::string errorMessage;
    std::string errorCode;
    // Store scopes from the original request for response formatting
    std::map<std::string, std::string> scopes;
};

class HttpQueryHandler {
private:
    seastar::sharded<Engine>* engineSharded;
    seastar::sharded<LevelDBIndex>* indexSharded;
    std::unique_ptr<QueryPlanner> planner;

public:
    // Security limits to prevent DoS attacks
    static constexpr size_t MAX_QUERY_BODY_SIZE = 1 * 1024 * 1024; // 1MB - queries should be small

    // Query result limits to prevent OOM and excessive response sizes
    static constexpr size_t MAX_SERIES_COUNT = 10'000;       // Max series in response
    static constexpr size_t MAX_TOTAL_POINTS = 10'000'000;   // Max total points across all series

    explicit HttpQueryHandler(seastar::sharded<Engine>* engine,
                            seastar::sharded<LevelDBIndex>* index = nullptr)
        : engineSharded(engine), indexSharded(index), planner(std::make_unique<QueryPlanner>()) {}

    // Validate request body size and content type (public for testing).
    // Returns a reply with an error if validation fails, or nullptr if valid.
    std::unique_ptr<seastar::http::reply> validateRequest(
        const seastar::http::request& req) const;

    // Main query handler
    seastar::future<std::unique_ptr<seastar::http::reply>>
    handleQuery(std::unique_ptr<seastar::http::request> req);

    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& r);

    // Execute query across shards
    seastar::future<QueryResponse> executeQuery(const QueryRequest& request);

    // Parse JSON request body (public for testing)
    QueryRequest parseQueryRequest(const GlazeQueryRequest& glazeReq);

    // Parse interval string like "5m", "1h", "30s" to nanoseconds
    static uint64_t parseInterval(const std::string& interval);

    // Format response as JSON (public for testing)
    std::string formatQueryResponse(const QueryResponse& response, const std::vector<std::string>& requestedFields = {});

    // Create error response (public for testing)
    std::string createErrorResponse(const std::string& code, const std::string& message);

    // Determine which shards to query based on request (public for testing)
    std::vector<unsigned> determineTargetShards(const QueryRequest& request);

    // Merge results from multiple shards (public for testing)
    std::vector<SeriesResult> mergeResults(std::vector<std::vector<SeriesResult>> shardResults);

private:

    // Query all relevant shards
    seastar::future<std::vector<SeriesResult>> queryAllShards(const QueryRequest& request);
};

} // namespace tsdb

#endif // HTTP_QUERY_HANDLER_H_INCLUDED
