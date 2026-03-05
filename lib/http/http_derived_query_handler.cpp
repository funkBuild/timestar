#include "http_derived_query_handler.hpp"
#include "logger.hpp"
#include "anomaly/anomaly_result.hpp"
#include "forecast/forecast_result.hpp"

#include <stdexcept>
#include <seastar/core/future.hh>
#include <seastar/http/reply.hh>

namespace tsdb {

HttpDerivedQueryHandler::HttpDerivedQueryHandler(
    seastar::sharded<Engine>* engine,
    seastar::sharded<LevelDBIndex>* index,
    DerivedQueryConfig config)
    : engine_(engine), index_(index), config_(config) {
    if (!engine_) throw std::invalid_argument("engine must not be null");
    if (!index_) throw std::invalid_argument("index must not be null");
}

void HttpDerivedQueryHandler::registerRoutes(seastar::httpd::routes& r) {
    // POST /derived - Execute derived metric query
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req,
               std::unique_ptr<seastar::http::reply> rep)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            return handleDerivedQuery(std::move(req), std::move(rep));
        }, "json");

    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/derived"), handler);

    http_log.info("Registered HTTP derived query endpoint at /derived");
}

seastar::future<std::unique_ptr<seastar::http::reply>>
HttpDerivedQueryHandler::handleDerivedQuery(
    std::unique_ptr<seastar::http::request> req,
    std::unique_ptr<seastar::http::reply> rep) {

    try {
        // Validate request body size
        if (req->content.size() > MAX_DERIVED_QUERY_BODY_SIZE) {
            rep->set_status(seastar::http::reply::status_type::payload_too_large);
            rep->_content = DerivedQueryExecutor::createErrorResponse(
                "BODY_TOO_LARGE", "Request body too large (max 1MB)");
            rep->done("application/json");
            co_return std::move(rep);
        }

        // Validate Content-Type header if explicitly set
        auto contentType = req->get_header("Content-Type");
        // Convert to std::string to avoid sstring::npos vs std::string::npos mismatch
        // (Seastar sstring uses 32-bit npos, std::string uses 64-bit npos)
        std::string contentTypeStr(contentType.data(), contentType.size());
        if (!contentTypeStr.empty() && contentTypeStr.find("application/json") == std::string::npos) {
            rep->set_status(seastar::http::reply::status_type::unsupported_media_type);
            rep->_content = DerivedQueryExecutor::createErrorResponse(
                "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");
            rep->done("application/json");
            co_return std::move(rep);
        }

        std::string body = req->content;

        if (body.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = DerivedQueryExecutor::createErrorResponse(
                "EMPTY_REQUEST", "Request body is required");
            rep->done("application/json");
            co_return std::move(rep);
        }

        // Create executor and run the query
        DerivedQueryExecutor executor(engine_, index_, config_);

        try {
            // Use anomaly-aware execution that handles both regular and anomaly formulas
            auto result = co_await executor.executeFromJsonWithAnomaly(body);

            rep->set_status(seastar::http::reply::status_type::ok);
            rep->_content = executor.formatResponseVariant(result);
            rep->done("application/json");

            // Log execution stats
            if (std::holds_alternative<DerivedQueryResult>(result)) {
                const auto& r = std::get<DerivedQueryResult>(result);
                http_log.debug("Derived query completed: {} points, {:.2f}ms",
                    r.stats.pointCount, r.stats.executionTimeMs);
            } else if (std::holds_alternative<anomaly::AnomalyQueryResult>(result)) {
                const auto& r = std::get<anomaly::AnomalyQueryResult>(result);
                http_log.debug("Anomaly query completed: {} points, {} anomalies, {:.2f}ms",
                    r.statistics.totalPoints, r.statistics.anomalyCount,
                    r.statistics.executionTimeMs);
            } else if (std::holds_alternative<forecast::ForecastQueryResult>(result)) {
                const auto& r = std::get<forecast::ForecastQueryResult>(result);
                http_log.debug("Forecast query completed: {} historical + {} forecast points, {:.2f}ms",
                    r.statistics.historicalPoints, r.statistics.forecastPoints,
                    r.statistics.executionTimeMs);
            }

        } catch (const DerivedQueryException& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = executor.createErrorResponse("QUERY_ERROR", e.what());
            rep->done("application/json");

            http_log.warn("Derived query error: {}", e.what());
        }

        co_return std::move(rep);

    } catch (const std::exception& e) {
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = DerivedQueryExecutor::createErrorResponse(
            "INTERNAL_ERROR", e.what());
        rep->done("application/json");

        http_log.error("Derived query internal error: {}", e.what());

        co_return std::move(rep);
    }
}

} // namespace tsdb
