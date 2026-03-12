#ifndef FUNCTION_HTTP_HANDLER_H_INCLUDED
#define FUNCTION_HTTP_HANDLER_H_INCLUDED

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations
class Engine;

namespace timestar::functions {

// Simple request/response structures
struct FunctionQueryRequest {
    std::string query;
    uint64_t startTime;
    uint64_t endTime;
    std::string aggregationInterval;
    bool enableFunctions = true;
    size_t maxPoints = 100000;
    bool enableOptimizations = true;
    bool enableCache = true;
};

struct FunctionQueryResponse {
    bool success = false;
    std::string errorMessage;
    std::string errorCode;
    std::string originalQuery;
    std::string parsedQuery;

    struct FunctionMetadata {
        std::vector<std::string> functionsExecuted;
        size_t totalFunctionExecutions = 0;
        double totalFunctionTimeMs = 0.0;
        std::map<std::string, std::string> performanceMetrics;
    } functionMetadata;
};

struct FunctionRegistryResponse {
    std::string status = "success";
};

struct FunctionPerformanceResponse {
    std::string status = "success";
};

struct QueryParseResponse {
    std::string status = "success";
    bool valid = true;
    std::string error;
};

// Performance tracking structures
// All access is serialized by PerformanceTracker::mutex_, so plain types suffice.
struct FunctionStats {
    uint64_t executions{0};
    uint64_t totalTimeNs{0};
    uint64_t cacheHits{0};
    uint64_t cacheMisses{0};

    double getAverageTime() const { return executions > 0 ? (totalTimeNs / 1000000.0) / executions : 0.0; }

    double getCacheHitRate() const {
        uint64_t total = cacheHits + cacheMisses;
        return total > 0 ? static_cast<double>(cacheHits) / total : 0.0;
    }
};

class PerformanceTracker {
private:
    // No mutex needed: this is used as a static thread_local (one instance per
    // Seastar shard), so only a single thread ever accesses it.
    std::unordered_map<std::string, FunctionStats> functionStats_;
    std::chrono::steady_clock::time_point startTime_;

public:
    PerformanceTracker() : startTime_(std::chrono::steady_clock::now()) {}

    void recordExecution(const std::string& functionName, std::chrono::nanoseconds duration);
    void recordCacheHit(const std::string& functionName);
    void recordCacheMiss(const std::string& functionName);
    std::string getPerformanceStats() const;

    uint64_t getTotalExecutions() const;
    double getAverageExecutionTime() const;
};

class FunctionHttpHandler {
private:
    seastar::sharded<Engine>& engine_;
    // Removed baseQueryHandler_ to fix unique_ptr<void> compilation issue
    static thread_local PerformanceTracker performanceTracker_;

public:
    explicit FunctionHttpHandler(seastar::sharded<Engine>& engine);

    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& routes);

    // Synchronous function endpoints
    std::string handleFunctionListSync();
    std::string handleFunctionInfoSync(const seastar::http::request& req);
    std::string handleFunctionValidationSync(const seastar::http::request& req);
    std::string handleQueryParsingSync(const seastar::http::request& req);
    std::string handleFunctionQuerySync(const seastar::http::request& req);
    std::string handlePerformanceStatsSync();
    std::string handleCacheStatsSync();

    // Multi-series operation handler
    std::string handleMultiSeriesOperation(const std::string& functionQuery, uint64_t startTimeVal, uint64_t endTimeVal,
                                           const std::chrono::high_resolution_clock::time_point& startTime);

private:
    // Helper methods
    seastar::future<std::unique_ptr<seastar::http::reply>> createJsonReply(const std::string& json);

    seastar::future<std::unique_ptr<seastar::http::reply>> createErrorReply(
        const std::string& error,
        seastar::http::reply::status_type status = seastar::http::reply::status_type::bad_request);

    // Stub methods for header compatibility
    seastar::future<FunctionQueryResponse> executeFunctionQuery(const FunctionQueryRequest& request);
    FunctionRegistryResponse buildFunctionRegistryResponse() const;
    FunctionPerformanceResponse buildPerformanceResponse() const;
    QueryParseResponse parseAndValidateQuery(const std::string& query) const;
    void updatePerformanceMetrics(const std::string& functionName, double executionTimeMs) const;
    std::unique_ptr<seastar::http::reply> createErrorResponse(
        const std::string& error,
        seastar::http::reply::status_type status = seastar::http::reply::status_type::bad_request) const;
};

}  // namespace timestar::functions

#endif  // FUNCTION_HTTP_HANDLER_H_INCLUDED