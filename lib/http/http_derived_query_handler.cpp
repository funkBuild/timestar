#include "http_derived_query_handler.hpp"

#include "anomaly/anomaly_result.hpp"
#include "content_negotiation.hpp"
#include "forecast/forecast_result.hpp"
#include "http_auth.hpp"
#include "json_escape.hpp"
#include "logger.hpp"
#include "proto_converters.hpp"
#include "timestar_config.hpp"

#include <seastar/core/future.hh>
#include <seastar/http/reply.hh>
#include <stdexcept>

namespace timestar {

HttpDerivedQueryHandler::HttpDerivedQueryHandler(seastar::sharded<Engine>* engine,
                                                 seastar::sharded<index::NativeIndex>* index, DerivedQueryConfig config)
    : engine_(engine), index_(index), config_(config) {
    if (!engine_)
        throw std::invalid_argument("engine must not be null");
}

void HttpDerivedQueryHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    auto* handler = new seastar::httpd::function_handler(
        timestar::wrapWithAuth(
            authToken,
            [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep)
                -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                return handleDerivedQuery(std::move(req), std::move(rep));
            }),
        "json");

    r.add(seastar::httpd::operation_type::POST, seastar::httpd::url("/derived"), handler);

    http_log.info("Registered HTTP derived query endpoint at /derived{}", authToken.empty() ? "" : " (auth required)");
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpDerivedQueryHandler::handleDerivedQuery(
    std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) {
    auto reqFmt = timestar::http::requestFormat(*req);
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        // Validate request body size
        if (req->content.size() > MAX_DERIVED_QUERY_BODY_SIZE) {
            rep->set_status(seastar::http::reply::status_type::payload_too_large);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content =
                    timestar::proto::formatDerivedQueryError("BODY_TOO_LARGE", "Request body too large (max 1MB)");
            } else {
                rep->_content =
                    DerivedQueryExecutor::createErrorResponse("BODY_TOO_LARGE", "Request body too large (max 1MB)");
            }
            timestar::http::setContentType(*rep, resFmt);
            rep->done();
            co_return std::move(rep);
        }

        std::string body = std::move(req->content);

        if (body.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content = timestar::proto::formatDerivedQueryError("EMPTY_REQUEST", "Request body is required");
            } else {
                rep->_content = DerivedQueryExecutor::createErrorResponse("EMPTY_REQUEST", "Request body is required");
            }
            timestar::http::setContentType(*rep, resFmt);
            rep->done();
            co_return std::move(rep);
        }

        // Create executor and run the query
        DerivedQueryExecutor executor(engine_, index_, config_);

        // For protobuf input, convert to JSON for the executor
        // (the executor has its own JSON parser — reuse it rather than duplicating logic)
        if (timestar::http::isProtobuf(reqFmt)) {
            auto parsed = timestar::proto::parseDerivedQueryRequest(body.data(), body.size());
            // Build a JSON body that the executor's parseFromJson can consume
            // Build JSON with proper escaping to prevent injection from
            // untrusted protobuf fields (formula, query names, query strings).
            std::string jsonBody = "{\"formula\":\"";
            timestar::jsonEscapeAppend(parsed.formula, jsonBody);
            jsonBody += "\",\"queries\":{";
            bool first = true;
            for (const auto& [name, queryStr] : parsed.queries) {
                if (!first)
                    jsonBody += ",";
                first = false;
                jsonBody += "\"";
                timestar::jsonEscapeAppend(name, jsonBody);
                jsonBody += "\":\"";
                timestar::jsonEscapeAppend(queryStr, jsonBody);
                jsonBody += "\"";
            }
            jsonBody += "},\"startTime\":" + std::to_string(parsed.startTime) +
                        ",\"endTime\":" + std::to_string(parsed.endTime);
            if (!parsed.aggregationInterval.empty()) {
                jsonBody += ",\"aggregationInterval\":\"";
                timestar::jsonEscapeAppend(parsed.aggregationInterval, jsonBody);
                jsonBody += "\"";
            }
            jsonBody += "}";
            body = std::move(jsonBody);
        }

        try {
            // Use anomaly-aware execution that handles both regular and anomaly formulas
            auto result = co_await executor.executeFromJsonWithAnomaly(body);

            rep->set_status(seastar::http::reply::status_type::ok);

            if (timestar::http::isProtobuf(resFmt)) {
                if (std::holds_alternative<DerivedQueryResult>(result)) {
                    const auto& r = std::get<DerivedQueryResult>(result);
                    timestar::proto::DerivedQueryResultData data;
                    data.timestamps = r.timestamps;
                    data.values = r.values;
                    data.formula = r.formula;
                    data.stats.pointCount = r.stats.pointCount;
                    data.stats.executionTimeMs = r.stats.executionTimeMs;
                    data.stats.subQueriesExecuted = r.stats.subQueriesExecuted;
                    data.stats.pointsDroppedDueToAlignment = r.stats.pointsDroppedDueToAlignment;
                    rep->_content = timestar::proto::formatDerivedQueryResponse(data);
                } else if (std::holds_alternative<anomaly::AnomalyQueryResult>(result)) {
                    const auto& r = std::get<anomaly::AnomalyQueryResult>(result);
                    timestar::proto::AnomalyQueryResultData data;
                    data.success = r.success;
                    data.times = r.times;
                    data.errorMessage = r.errorMessage;
                    data.statistics.algorithm = r.statistics.algorithm;
                    data.statistics.bounds = r.statistics.bounds;
                    data.statistics.seasonality = r.statistics.seasonality;
                    data.statistics.anomalyCount = r.statistics.anomalyCount;
                    data.statistics.totalPoints = r.statistics.totalPoints;
                    data.statistics.executionTimeMs = r.statistics.executionTimeMs;
                    for (const auto& sp : r.series) {
                        timestar::proto::AnomalySeriesPieceData piece;
                        piece.piece = sp.piece;
                        piece.groupTags = sp.groupTags;
                        piece.values = sp.values;
                        piece.alertValue = sp.alertValue;
                        data.series.push_back(std::move(piece));
                    }
                    rep->_content = timestar::proto::formatAnomalyResponse(data);
                } else if (std::holds_alternative<forecast::ForecastQueryResult>(result)) {
                    const auto& r = std::get<forecast::ForecastQueryResult>(result);
                    timestar::proto::ForecastQueryResultData data;
                    data.success = r.success;
                    data.times = r.times;
                    data.forecastStartIndex = r.forecastStartIndex;
                    data.errorMessage = r.errorMessage;
                    data.statistics.algorithm = r.statistics.algorithm;
                    data.statistics.deviations = r.statistics.deviations;
                    data.statistics.seasonality = r.statistics.seasonality;
                    data.statistics.slope = r.statistics.slope;
                    data.statistics.intercept = r.statistics.intercept;
                    data.statistics.rSquared = r.statistics.rSquared;
                    data.statistics.residualStdDev = r.statistics.residualStdDev;
                    data.statistics.historicalPoints = r.statistics.historicalPoints;
                    data.statistics.forecastPoints = r.statistics.forecastPoints;
                    data.statistics.seriesCount = r.statistics.seriesCount;
                    data.statistics.executionTimeMs = r.statistics.executionTimeMs;
                    for (const auto& sp : r.series) {
                        timestar::proto::ForecastSeriesPieceData piece;
                        piece.piece = sp.piece;
                        piece.groupTags = sp.groupTags;
                        piece.values = sp.values;
                        data.series.push_back(std::move(piece));
                    }
                    rep->_content = timestar::proto::formatForecastResponse(data);
                }
            } else {
                rep->_content = executor.formatResponseVariant(result);
            }

            timestar::http::setContentType(*rep, resFmt);
            rep->done();

            // Log execution stats + slow query detection
            const auto slowMs = timestar::config().http.slow_query_threshold_ms;
            if (std::holds_alternative<DerivedQueryResult>(result)) {
                const auto& r = std::get<DerivedQueryResult>(result);
                http_log.debug("Derived query completed: {} points, {:.2f}ms", r.stats.pointCount,
                               r.stats.executionTimeMs);
                if (slowMs > 0 && r.stats.executionTimeMs > static_cast<double>(slowMs)) {
                    query_log.warn("[SLOW_QUERY] derived {:.1f}ms (threshold {}ms) points={}", r.stats.executionTimeMs,
                                   slowMs, r.stats.pointCount);
                }
            } else if (std::holds_alternative<anomaly::AnomalyQueryResult>(result)) {
                const auto& r = std::get<anomaly::AnomalyQueryResult>(result);
                http_log.debug("Anomaly query completed: {} points, {} anomalies, {:.2f}ms", r.statistics.totalPoints,
                               r.statistics.anomalyCount, r.statistics.executionTimeMs);
                if (slowMs > 0 && r.statistics.executionTimeMs > static_cast<double>(slowMs)) {
                    query_log.warn("[SLOW_QUERY] anomaly {:.1f}ms (threshold {}ms) points={} anomalies={}",
                                   r.statistics.executionTimeMs, slowMs, r.statistics.totalPoints,
                                   r.statistics.anomalyCount);
                }
            } else if (std::holds_alternative<forecast::ForecastQueryResult>(result)) {
                const auto& r = std::get<forecast::ForecastQueryResult>(result);
                http_log.debug("Forecast query completed: {} historical + {} forecast points, {:.2f}ms",
                               r.statistics.historicalPoints, r.statistics.forecastPoints,
                               r.statistics.executionTimeMs);
                if (slowMs > 0 && r.statistics.executionTimeMs > static_cast<double>(slowMs)) {
                    query_log.warn("[SLOW_QUERY] forecast {:.1f}ms (threshold {}ms) points={}+{}",
                                   r.statistics.executionTimeMs, slowMs, r.statistics.historicalPoints,
                                   r.statistics.forecastPoints);
                }
            }

        } catch (const DerivedQueryException& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content = timestar::proto::formatDerivedQueryError("QUERY_ERROR", e.what());
            } else {
                rep->_content = executor.createErrorResponse("QUERY_ERROR", e.what());
            }
            timestar::http::setContentType(*rep, resFmt);
            rep->done();

            http_log.warn("Derived query error: {}", e.what());
        }

        co_return std::move(rep);

    } catch (const std::exception& e) {
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatDerivedQueryError("INTERNAL_ERROR", "Internal server error");
        } else {
            rep->_content = DerivedQueryExecutor::createErrorResponse("INTERNAL_ERROR", "Internal server error");
        }
        timestar::http::setContentType(*rep, resFmt);
        rep->done();

        http_log.error("Derived query internal error: {}", e.what());

        co_return std::move(rep);
    }
}

}  // namespace timestar
