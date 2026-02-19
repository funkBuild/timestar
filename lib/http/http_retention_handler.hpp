#ifndef HTTP_RETENTION_HANDLER_H_INCLUDED
#define HTTP_RETENTION_HANDLER_H_INCLUDED

#include <string>
#include <memory>
#include <glaze/glaze.hpp>

#include "engine.hpp"
#include "retention_policy.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>

class HttpRetentionHandler {
private:
    seastar::sharded<Engine>* engineSharded;

    // Parse and validate a duration string, returning nanoseconds.
    // Throws std::runtime_error on invalid format.
    static uint64_t parseDuration(const std::string& duration);

    // Validate an aggregation method string.
    static bool isValidMethod(const std::string& method);

    std::string createErrorResponse(const std::string& error);

public:
    HttpRetentionHandler(seastar::sharded<Engine>* _engineSharded)
        : engineSharded(_engineSharded) {}

    seastar::future<std::unique_ptr<seastar::http::reply>>
    handlePut(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>>
    handleGet(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>>
    handleDelete(std::unique_ptr<seastar::http::request> req);

    void registerRoutes(seastar::httpd::routes& r);
};

#endif // HTTP_RETENTION_HANDLER_H_INCLUDED
