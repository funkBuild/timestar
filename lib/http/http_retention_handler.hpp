#pragma once

#include "engine.hpp"
#include "retention_policy.hpp"

#include <glaze/json.hpp>

#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <string>

namespace timestar::http {

class HttpRetentionHandler : public std::enable_shared_from_this<HttpRetentionHandler> {
private:
    seastar::sharded<Engine>* engineSharded;

    // Parse and validate a duration string, returning nanoseconds.
    // Throws std::runtime_error on invalid format.
    static uint64_t parseDuration(const std::string& duration);

public:
    HttpRetentionHandler(seastar::sharded<Engine>* _engineSharded) : engineSharded(_engineSharded) {}

    // Validate an aggregation method string.
    // Exposed publicly to allow direct unit testing.
    static bool isValidMethod(const std::string& method);

    std::string createErrorResponse(const std::string& error);

    seastar::future<std::unique_ptr<seastar::http::reply>> handlePut(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleGet(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleDelete(std::unique_ptr<seastar::http::request> req);

    void registerRoutes(seastar::httpd::routes& r, std::string_view authToken = "");
};

}  // namespace timestar::http

// Backward-compatibility aliases: HttpRetentionHandler historically lived in
// the global namespace. New code should use timestar::http:: directly.
using timestar::http::HttpRetentionHandler;  // NOLINT(misc-unused-using-decls)
namespace timestar {
using http::HttpRetentionHandler;
}  // namespace timestar
