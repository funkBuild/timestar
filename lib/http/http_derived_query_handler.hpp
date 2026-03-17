#pragma once

#include "derived_query_executor.hpp"

#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>

// Forward declarations
class Engine;

namespace timestar {

// HTTP handler for derived metric queries
// Endpoint: POST /derived
class HttpDerivedQueryHandler {
public:
    // Security limits to prevent DoS attacks
    static constexpr size_t MAX_DERIVED_QUERY_BODY_SIZE = 1 * 1024 * 1024;  // 1MB

    HttpDerivedQueryHandler(seastar::sharded<Engine>* engine, seastar::sharded<index::NativeIndex>* index = nullptr,
                            DerivedQueryConfig config = {});

    // Register routes with the HTTP server
    void registerRoutes(seastar::httpd::routes& r);

private:
    seastar::sharded<Engine>* engine_;
    seastar::sharded<index::NativeIndex>* index_;
    DerivedQueryConfig config_;

    // Handle POST /derived request
    seastar::future<std::unique_ptr<seastar::http::reply>> handleDerivedQuery(
        std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep);
};

}  // namespace timestar
