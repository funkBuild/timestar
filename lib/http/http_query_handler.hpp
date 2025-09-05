#ifndef __HTTP_QUERY_HANDLER_H_INCLUDED__
#define __HTTP_QUERY_HANDLER_H_INCLUDED__

#include "query_parser.hpp"
#include "query_planner.hpp"
#include "leveldb_index.hpp"
#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <rapidjson/document.h>
#include <memory>
#include <chrono>
#include <variant>

// Forward declaration
class Engine;

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
    double executionTimeMs = 0.0;
    std::vector<int> shardsQueried;
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
    explicit HttpQueryHandler(seastar::sharded<Engine>* engine,
                            seastar::sharded<LevelDBIndex>* index = nullptr) 
        : engineSharded(engine), indexSharded(index), planner(std::make_unique<QueryPlanner>()) {}
    
    // Main query handler
    seastar::future<std::unique_ptr<seastar::httpd::reply>>
    handleQuery(std::unique_ptr<seastar::httpd::request> req);
    
    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& r);
    
    // Execute query across shards
    seastar::future<QueryResponse> executeQuery(const QueryRequest& request);
    
    // Parse JSON request body (public for testing)
    QueryRequest parseQueryRequest(const rapidjson::Document& doc);
    
    // Parse interval string like "5m", "1h", "30s" to nanoseconds
    static uint64_t parseInterval(const std::string& interval);
    
    // Format response as JSON (public for testing)
    std::string formatQueryResponse(const QueryResponse& response);
    
    // Create error response (public for testing)
    std::string createErrorResponse(const std::string& code, const std::string& message);
    
    // Determine which shards to query based on request (public for testing)
    std::vector<unsigned> determineTargetShards(const QueryRequest& request);
    
    // Merge results from multiple shards (public for testing)
    std::vector<SeriesResult> mergeResults(std::vector<std::vector<SeriesResult>> shardResults);
    
    // Apply aggregation to results (public for testing)
    void applyAggregation(std::vector<SeriesResult>& results, 
                         const QueryRequest& request);
    
    // Apply grouping to results (public for testing)
    std::vector<SeriesResult> applyGroupBy(const std::vector<SeriesResult>& results,
                                          const QueryRequest& request);
    
private:
    
    // Query all relevant shards
    seastar::future<std::vector<SeriesResult>> queryAllShards(const QueryRequest& request);
};

} // namespace tsdb

#endif // __HTTP_QUERY_HANDLER_H_INCLUDED__