#include "http_retention_handler.hpp"
#include "http_auth.hpp"

#include "http_query_handler.hpp"
#include "logger.hpp"

#include <seastar/core/smp.hh>

using namespace seastar;
using namespace httpd;

uint64_t HttpRetentionHandler::parseDuration(const std::string& duration) {
    // Reuse the existing parseInterval logic from HttpQueryHandler
    return timestar::HttpQueryHandler::parseInterval(duration);
}

bool HttpRetentionHandler::isValidMethod(const std::string& method) {
    return method == "avg" || method == "min" || method == "max" || method == "sum" || method == "latest";
}

std::string HttpRetentionHandler::createErrorResponse(const std::string& error) {
    auto response = glz::obj{"status", "error", "error", error};
    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","error":"Failed to serialize error response"})";
    }
    return buffer;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpRetentionHandler::handlePut(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    reply->add_header("Content-Type", "application/json");

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Retention handler not initialized"})";
        co_return reply;
    }

    // Body size limit to prevent DoS via large payloads
    if (req->content.size() > timestar::config().http.max_query_body_size) {
        reply->set_status(seastar::http::reply::status_type::payload_too_large);
        reply->_content = R"({"status":"error","error":"Request body too large"})";
        co_return reply;
    }

    // Validate Content-Type if explicitly set
    {
        auto ct = req->get_header("Content-Type");
        std::string ctStr(ct.data(), ct.size());
        if (!ctStr.empty() && !ctStr.starts_with("application/json")) {
            reply->set_status(seastar::http::reply::status_type::unsupported_media_type);
            reply->_content = R"({"status":"error","error":"Content-Type must be application/json"})";
            co_return reply;
        }
    }

    try {
        RetentionPolicyRequest policyReq;
        auto err = glz::read_json(policyReq, req->content);
        if (err) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(err)));
            co_return reply;
        }

        if (policyReq.measurement.empty()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("'measurement' is required");
            co_return reply;
        }

        // Validate measurement name (no control characters or index key separators)
        for (char c : policyReq.measurement) {
            if (c < 0x20 || c == '\x7f') {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Measurement name contains control characters");
                co_return reply;
            }
        }

        if (!policyReq.ttl.has_value() && !policyReq.downsample.has_value()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("At least one of 'ttl' or 'downsample' is required");
            co_return reply;
        }

        // Build the RetentionPolicy
        RetentionPolicy policy;
        policy.measurement = policyReq.measurement;

        if (policyReq.ttl.has_value()) {
            policy.ttl = *policyReq.ttl;
            try {
                policy.ttlNanos = parseDuration(policy.ttl);
            } catch (const std::exception& e) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Invalid ttl: " + std::string(e.what()));
                co_return reply;
            }
        }

        if (policyReq.downsample.has_value()) {
            DownsamplePolicy ds = *policyReq.downsample;

            if (ds.after.empty()) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("downsample.after is required");
                co_return reply;
            }
            if (ds.interval.empty()) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("downsample.interval is required");
                co_return reply;
            }
            if (ds.method.empty()) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("downsample.method is required");
                co_return reply;
            }
            if (!isValidMethod(ds.method)) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content =
                    createErrorResponse("Invalid downsample.method: must be one of avg, min, max, sum, latest");
                co_return reply;
            }

            try {
                ds.afterNanos = parseDuration(ds.after);
            } catch (const std::exception& e) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Invalid downsample.after: " + std::string(e.what()));
                co_return reply;
            }
            try {
                ds.intervalNanos = parseDuration(ds.interval);
            } catch (const std::exception& e) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Invalid downsample.interval: " + std::string(e.what()));
                co_return reply;
            }

            policy.downsample = ds;
        }

        // Validate: if both TTL and downsample are set, TTL must be > downsample.after
        if (policy.ttlNanos > 0 && policy.downsample.has_value()) {
            if (policy.ttlNanos <= policy.downsample->afterNanos) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("ttl must be greater than downsample.after");
                co_return reply;
            }
        }

        // Write to NativeIndex on shard 0
        co_await engineSharded->invoke_on(0, [policy](Engine& engine) -> seastar::future<> {
            co_await engine.getIndex().setRetentionPolicy(policy);
        });

        // Broadcast to all shards' caches
        co_await engineSharded->invoke_on_all([policy](Engine& engine) {
            engine.updateRetentionPolicyCache(policy);
            return seastar::make_ready_future<>();
        });

        // Build response
        auto responseObj = glz::obj{"status", "success", "policy", policy};
        reply->set_status(seastar::http::reply::status_type::ok);
        reply->_content = glz::write_json(responseObj).value_or("{}");

    } catch (const std::exception& e) {
        timestar::http_log.error("Retention PUT handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = createErrorResponse("Internal server error");
    }

    co_return reply;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpRetentionHandler::handleGet(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    reply->add_header("Content-Type", "application/json");

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Retention handler not initialized"})";
        co_return reply;
    }

    try {
        // Check for ?measurement= query parameter
        std::string measurement = req->get_query_param("measurement");

        // Validate measurement name if provided
        for (char c : measurement) {
            if (c < 0x20 || c == '\x7f') {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Measurement name contains control characters");
                co_return reply;
            }
        }

        if (!measurement.empty()) {
            // Get single policy
            auto policyOpt = co_await engineSharded->invoke_on(
                0, [measurement](Engine& engine) { return engine.getIndex().getRetentionPolicy(measurement); });

            if (policyOpt.has_value()) {
                auto responseObj = glz::obj{"status", "success", "policy", *policyOpt};
                reply->set_status(seastar::http::reply::status_type::ok);
                reply->_content = glz::write_json(responseObj).value_or("{}");
            } else {
                reply->set_status(seastar::http::reply::status_type::not_found);
                reply->_content = createErrorResponse("No retention policy found for measurement: " + measurement);
            }
        } else {
            // Get all policies
            auto policies = co_await engineSharded->invoke_on(
                0, [](Engine& engine) { return engine.getIndex().getAllRetentionPolicies(); });

            auto responseObj = glz::obj{"status", "success", "policies", policies};
            reply->set_status(seastar::http::reply::status_type::ok);
            reply->_content = glz::write_json(responseObj).value_or("{}");
        }

    } catch (const std::exception& e) {
        timestar::http_log.error("Retention GET handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = createErrorResponse("Internal server error");
    }

    co_return reply;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpRetentionHandler::handleDelete(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    reply->add_header("Content-Type", "application/json");

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Retention handler not initialized"})";
        co_return reply;
    }

    try {
        std::string measurement = req->get_query_param("measurement");

        if (measurement.empty()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("'measurement' query parameter is required");
            co_return reply;
        }

        // Validate measurement name
        for (char c : measurement) {
            if (c < 0x20 || c == '\x7f') {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Measurement name contains control characters");
                co_return reply;
            }
        }

        bool deleted = co_await engineSharded->invoke_on(
            0, [measurement](Engine& engine) { return engine.getIndex().deleteRetentionPolicy(measurement); });

        if (deleted) {
            // Remove from all shards' caches
            co_await engineSharded->invoke_on_all([measurement](Engine& engine) {
                engine.removeRetentionPolicyCache(measurement);
                return seastar::make_ready_future<>();
            });

            auto responseObj =
                glz::obj{"status", "success", "message", "Retention policy deleted for measurement: " + measurement};
            reply->set_status(seastar::http::reply::status_type::ok);
            reply->_content = glz::write_json(responseObj).value_or("{}");
        } else {
            reply->set_status(seastar::http::reply::status_type::not_found);
            reply->_content = createErrorResponse("No retention policy found for measurement: " + measurement);
        }

    } catch (const std::exception& e) {
        timestar::http_log.error("Retention DELETE handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = createErrorResponse("Internal server error");
    }

    co_return reply;
}

void HttpRetentionHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    auto self = shared_from_this();
    auto wrap = [&](auto fn) { return timestar::wrapWithAuth(authToken, std::move(fn)); };

    r.add(seastar::httpd::operation_type::PUT, seastar::httpd::url("/retention"),
          new seastar::httpd::function_handler(
              wrap([self](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
                       -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  return self->handlePut(std::move(req));
              }),
              "json"));

    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/retention"),
          new seastar::httpd::function_handler(
              wrap([self](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
                       -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  return self->handleGet(std::move(req));
              }),
              "json"));

    r.add(seastar::httpd::operation_type::DELETE, seastar::httpd::url("/retention"),
          new seastar::httpd::function_handler(
              wrap([self](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
                       -> seastar::future<std::unique_ptr<seastar::http::reply>> {
                  return self->handleDelete(std::move(req));
              }),
              "json"));

    timestar::http_log.info("Registered retention endpoints at /retention (PUT/GET/DELETE){}",
                            authToken.empty() ? "" : " (auth required)");
}
