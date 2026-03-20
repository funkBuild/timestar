#pragma once

#include <chrono>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <string>
#include <vector>

// Forward declarations
class Engine;

namespace timestar::functions {

class FunctionHttpHandler {
private:
    seastar::sharded<Engine>& engine_;

public:
    explicit FunctionHttpHandler(seastar::sharded<Engine>& engine);

    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& routes);

    // Synchronous function endpoints
    std::string handleFunctionListSync();
    std::string handleFunctionInfoSync(const seastar::http::request& req);
    std::string handleFunctionValidationSync(const seastar::http::request& req);
    std::string handleQueryParsingSync(const seastar::http::request& req);
    std::pair<bool, std::string> handleFunctionQuerySync(const seastar::http::request& req);

    // Multi-series operation handler
    std::pair<bool, std::string> handleMultiSeriesOperation(
        const std::string& functionQuery, uint64_t startTimeVal, uint64_t endTimeVal,
        const std::chrono::high_resolution_clock::time_point& startTime);

private:
    // Helper methods
    seastar::future<std::unique_ptr<seastar::http::reply>> createErrorReply(
        const std::string& error,
        seastar::http::reply::status_type status = seastar::http::reply::status_type::bad_request);

    std::unique_ptr<seastar::http::reply> createErrorResponse(
        const std::string& error,
        seastar::http::reply::status_type status = seastar::http::reply::status_type::bad_request) const;
};

}  // namespace timestar::functions
