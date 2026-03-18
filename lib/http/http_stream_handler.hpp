#pragma once

#include "expression_ast.hpp"
#include "http_query_handler.hpp"
#include "query_parser.hpp"
#include "subscription_manager.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>

class Engine;

namespace timestar {

class HttpStreamHandler {
public:
    explicit HttpStreamHandler(seastar::sharded<Engine>* engine) : _engineSharded(engine) {}

    void registerRoutes(seastar::httpd::routes& r, std::string_view authToken = "");

    // Wait for all active streaming connections to finish (for graceful shutdown).
    seastar::future<> stop() { return _connectionGate.close(); }

    seastar::future<std::unique_ptr<seastar::http::reply>> handleSubscribe(std::unique_ptr<seastar::http::request> req);

    seastar::future<std::unique_ptr<seastar::http::reply>> handleGetSubscriptions(
        std::unique_ptr<seastar::http::request> req);

    // Format a StreamingBatch as an SSE event string
    static std::string formatSSEEvent(const StreamingBatch& batch);

    // Format a StreamingBatch as a backfill SSE event
    static std::string formatSSEBackfillEvent(const StreamingBatch& batch);

    // Convert query response series to StreamingBatch objects for SSE emission
    static std::vector<StreamingBatch> queryResponseToBatches(const std::vector<SeriesResult>& series,
                                                              const std::string& label = "");

    // Apply a formula expression to a streaming batch (per-series evaluation).
    // Each unique (measurement, tags, field) group is evaluated independently.
    static StreamingBatch applyFormulaToBatch(const StreamingBatch& batch, const ExpressionNode& formula,
                                              const std::string& queryRef = "a");

private:
    seastar::sharded<Engine>* _engineSharded;
    seastar::gate _connectionGate;
};

}  // namespace timestar
