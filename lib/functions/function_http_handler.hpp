#ifndef __FUNCTION_HTTP_HANDLER_H_INCLUDED__
#define __FUNCTION_HTTP_HANDLER_H_INCLUDED__

#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <string>
#include <memory>
#include <vector>
#include <map>
#include <algorithm>
#include <chrono>
#include <atomic>
#include <unordered_map>
#include <mutex>

// Forward declarations
class Engine;

namespace tsdb::functions {

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
struct FunctionStats {
    std::atomic<uint64_t> executions{0};
    std::atomic<uint64_t> totalTimeNs{0};
    std::atomic<uint64_t> cacheHits{0};
    std::atomic<uint64_t> cacheMisses{0};
    
    double getAverageTime() const {
        uint64_t execs = executions.load();
        return execs > 0 ? (totalTimeNs.load() / 1000000.0) / execs : 0.0;
    }
    
    double getCacheHitRate() const {
        uint64_t hits = cacheHits.load();
        uint64_t misses = cacheMisses.load();
        uint64_t total = hits + misses;
        return total > 0 ? static_cast<double>(hits) / total : 0.0;
    }
};

class PerformanceTracker {
private:
    mutable std::mutex mutex_;
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
    static PerformanceTracker performanceTracker_;

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
    std::string handleMultiSeriesOperation(const std::string& functionQuery, 
                                          uint64_t startTimeVal, uint64_t endTimeVal, 
                                          const std::chrono::high_resolution_clock::time_point& startTime);

private:
    // Helper methods
    seastar::future<std::unique_ptr<seastar::http::reply>>
    createJsonReply(const std::string& json);
    
    seastar::future<std::unique_ptr<seastar::http::reply>>
    createErrorReply(const std::string& error, seastar::http::reply::status_type status = seastar::http::reply::status_type::bad_request);

    // Stub methods for header compatibility
    seastar::future<FunctionQueryResponse> executeFunctionQuery(const FunctionQueryRequest& request);
    FunctionRegistryResponse buildFunctionRegistryResponse() const;
    FunctionPerformanceResponse buildPerformanceResponse() const;
    QueryParseResponse parseAndValidateQuery(const std::string& query) const;
    void updatePerformanceMetrics(const std::string& functionName, double executionTimeMs) const;
    std::unique_ptr<seastar::http::reply> createErrorResponse(const std::string& error, 
                                                             seastar::http::reply::status_type status = seastar::http::reply::status_type::bad_request) const;
};

} // namespace tsdb::functions

#endif // __FUNCTION_HTTP_HANDLER_H_INCLUDED__