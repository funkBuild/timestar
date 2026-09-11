#include "http_retention_handler.hpp"

#include "content_negotiation.hpp"
#include "http_auth.hpp"
#include "http_error.hpp"
#include "http_query_handler.hpp"
#include "http_routes.hpp"
#include "logger.hpp"
#include "proto_converters.hpp"
#include "value_type_dispatch.hpp"  // valueTypeFromName -> isNonNumericValueType

#include <map>
#include <seastar/core/smp.hh>
#include <set>

using namespace seastar;
using namespace httpd;

namespace timestar::http {

uint64_t HttpRetentionHandler::parseDuration(const std::string& duration) {
    // Reuse the existing parseInterval logic from HttpQueryHandler
    return HttpQueryHandler::parseInterval(duration);
}

bool HttpRetentionHandler::isValidMethod(const std::string& method) {
    return timestar::retention::isValidDownsampleMethod(method);
}

std::string HttpRetentionHandler::createErrorResponse(const std::string& error) {
    return timestar::http::jsonError(error);
}

namespace {

// Protobuf projection of a normalized policy: the full cascade plus the legacy
// single-tier mirror, so a client that predates the cascade still reads the
// finest tier out of `downsample`.
timestar::proto::RetentionPolicyData toProtoPolicyData(const RetentionPolicy& policy) {
    timestar::proto::RetentionPolicyData data;
    data.measurement = policy.measurement;
    data.ttl = policy.ttl;
    data.ttlNanos = policy.ttlNanos;

    auto convert = [](const DownsamplePolicy& src) {
        timestar::proto::ParsedRetentionPutRequest::DownsampleData ds;
        ds.after = src.after;
        ds.afterNanos = src.afterNanos;
        ds.interval = src.interval;
        ds.intervalNanos = src.intervalNanos;
        ds.method = src.method;
        ds.fieldMethods = timestar::retention::fieldMethodsOf(src);
        return ds;
    };

    if (policy.downsample.has_value()) {
        data.downsample = convert(*policy.downsample);
    }
    data.downsampleTiers.reserve(policy.downsampleTiers.size());
    for (const auto& tier : policy.downsampleTiers) {
        data.downsampleTiers.push_back(convert(tier));
    }
    return data;
}

// "downsample" for a single tier (preserving the error text single-tier callers
// have always seen), "downsample[k]" once the body is a list.
std::string tierLabel(size_t index, size_t total) {
    if (total <= 1) {
        return "downsample";
    }
    return "downsample[" + std::to_string(index) + "]";
}

}  // namespace

seastar::future<std::unique_ptr<seastar::http::reply>> HttpRetentionHandler::handlePut(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    auto reqFmt = timestar::http::requestFormat(*req);
    auto resFmt = timestar::http::responseFormat(*req);

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Retention handler not initialized"})";
        timestar::http::setContentType(*reply, resFmt);
        co_return reply;
    }

    // Body size limit to prevent DoS via large payloads
    if (req->content.size() > timestar::config().http.max_query_body_size) {
        reply->set_status(seastar::http::reply::status_type::payload_too_large);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatErrorResponse("Request body too large");
        } else {
            reply->_content = R"({"status":"error","error":"Request body too large"})";
        }
        timestar::http::setContentType(*reply, resFmt);
        co_return reply;
    }

    try {
        RetentionPolicyRequest policyReq;
        // The requested cascade, finest first. A legacy single object (JSON or
        // proto) lands here as one tier, so everything downstream sees exactly
        // one shape.
        std::vector<DownsamplePolicy> requestedTiers;
        bool downsampleFieldPresent = false;

        if (timestar::http::isProtobuf(reqFmt)) {
            // Parse protobuf request
            auto parsed = timestar::proto::parseRetentionPutRequest(req->content.data(), req->content.size());
            policyReq.measurement = std::move(parsed.measurement);
            policyReq.ttl = std::move(parsed.ttl);

            auto convert = [](timestar::proto::ParsedRetentionPutRequest::DownsampleData& src) {
                DownsamplePolicy ds;
                ds.after = std::move(src.after);
                ds.afterNanos = src.afterNanos;
                ds.interval = std::move(src.interval);
                ds.intervalNanos = src.intervalNanos;
                ds.method = std::move(src.method);
                if (!src.fieldMethods.empty()) {
                    ds.fieldMethods = std::move(src.fieldMethods);
                }
                return ds;
            };
            // downsample_tiers WINS over the legacy singular field; absent
            // (proto3 repeated-absent = empty) falls back to it.
            if (!parsed.downsampleTiers.empty()) {
                for (auto& tier : parsed.downsampleTiers) {
                    requestedTiers.push_back(convert(tier));
                }
                downsampleFieldPresent = true;
            } else if (parsed.downsample.has_value()) {
                requestedTiers.push_back(convert(*parsed.downsample));
                downsampleFieldPresent = true;
            }
        } else {
            auto err = glz::read_json(policyReq, req->content);
            if (err) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    reply->_content =
                        timestar::proto::formatErrorResponse("Invalid JSON: " + std::string(glz::format_error(err)));
                } else {
                    reply->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(err)));
                }
                timestar::http::setContentType(*reply, resFmt);
                co_return reply;
            }
            if (policyReq.downsample.has_value()) {
                downsampleFieldPresent = true;
                // `downsample` is accepted as an OBJECT (legacy, one tier) or
                // an ARRAY of objects (the cascade).
                if (auto why = timestar::retention::parseDownsampleRequestField(*policyReq.downsample, requestedTiers);
                    why.has_value()) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    if (timestar::http::isProtobuf(resFmt)) {
                        reply->_content = timestar::proto::formatErrorResponse(*why);
                    } else {
                        reply->_content = createErrorResponse(*why);
                    }
                    timestar::http::setContentType(*reply, resFmt);
                    co_return reply;
                }
                if (requestedTiers.empty()) {
                    downsampleFieldPresent = false;  // explicit JSON null
                }
            }
        }

        if (policyReq.measurement.empty()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("'measurement' is required");
            timestar::http::setContentType(*reply, resFmt);
            co_return reply;
        }

        // Validate measurement name (no control characters or index key separators)
        for (char c : policyReq.measurement) {
            if (static_cast<unsigned char>(c) < 0x20 || c == '\x7f') {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Measurement name contains control characters");
                timestar::http::setContentType(*reply, resFmt);
                co_return reply;
            }
        }

        if (!policyReq.ttl.has_value() && !downsampleFieldPresent) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("At least one of 'ttl' or 'downsample' is required");
            timestar::http::setContentType(*reply, resFmt);
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
                timestar::http::setContentType(*reply, resFmt);
                co_return reply;
            }
        }

        // Presence + duration parsing per tier. Everything beyond this (method
        // set, ordering, divisibility, ttl relation) is delegated to the shared
        // validateRetentionPolicy() so no other caller can diverge from it.
        std::optional<std::string> fieldError;
        for (size_t k = 0; k < requestedTiers.size() && !fieldError.has_value(); ++k) {
            DownsamplePolicy& ds = requestedTiers[k];
            const std::string label = tierLabel(k, requestedTiers.size());

            if (ds.after.empty()) {
                fieldError = label + ".after is required";
                break;
            }
            if (ds.interval.empty()) {
                fieldError = label + ".interval is required";
                break;
            }
            if (ds.method.empty()) {
                fieldError = label + ".method is required";
                break;
            }
            try {
                ds.afterNanos = parseDuration(ds.after);
            } catch (const std::exception& e) {
                fieldError = "Invalid " + label + ".after: " + std::string(e.what());
                break;
            }
            try {
                ds.intervalNanos = parseDuration(ds.interval);
            } catch (const std::exception& e) {
                fieldError = "Invalid " + label + ".interval: " + std::string(e.what());
                break;
            }
        }
        if (fieldError.has_value()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            if (timestar::http::isProtobuf(resFmt)) {
                reply->_content = timestar::proto::formatErrorResponse(*fieldError);
            } else {
                reply->_content = createErrorResponse(*fieldError);
            }
            timestar::http::setContentType(*reply, resFmt);
            co_return reply;
        }

        policy.downsampleTiers = std::move(requestedTiers);
        timestar::retention::normalizeRetentionTiers(policy);

        // PER-FIELD METHODS ON NON-NUMERIC FIELDS.
        //
        // Only `latest` may downsample a Boolean/String field, and deciding
        // that needs the field's stored type — which lives in the index, not in
        // the request. Resolve the named fields' types HERE, at the one
        // boundary that can, and hand the answer to the shared validator as a
        // probe; the compactor enforces the same rule at fold time from the
        // series' real TSMValueType, where no lookup is needed.
        //
        // A field the index has never seen resolves to nullopt, which ACCEPTS:
        // a policy may legitimately be installed before the first write, and
        // the fold-time gate still refuses a wrong method later.
        std::map<std::string, std::optional<bool>> nonNumericByField;
        {
            std::set<std::string> namedFields;
            for (const auto& tier : policy.downsampleTiers) {
                for (const auto& [field, method] : timestar::retention::fieldMethodsOf(tier)) {
                    (void)method;
                    namedFields.insert(field);
                }
            }
            for (const auto& field : namedFields) {
                const std::string measurement = policy.measurement;
                auto typeName = co_await engineSharded->invoke_on(0, [measurement, field](Engine& engine) {
                    return engine.getIndex().getFieldType(measurement, field);
                });
                auto type = timestar::valueTypeFromName(typeName);
                nonNumericByField[field] =
                    type.has_value() ? std::optional<bool>(isNonNumericValueType(*type)) : std::nullopt;
            }
        }
        timestar::retention::FieldTypeProbe probe;
        if (!nonNumericByField.empty()) {
            probe = [&nonNumericByField](const std::string& field) -> std::optional<bool> {
                auto it = nonNumericByField.find(field);
                return it == nonNumericByField.end() ? std::nullopt : it->second;
            };
        }

        if (auto why = timestar::retention::validateRetentionPolicy(policy, probe); why.has_value()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            if (timestar::http::isProtobuf(resFmt)) {
                reply->_content = timestar::proto::formatErrorResponse(*why);
            } else {
                reply->_content = createErrorResponse(*why);
            }
            timestar::http::setContentType(*reply, resFmt);
            co_return reply;
        }

        // Write to NativeIndex on shard 0
        // NOTE: Retention policies are stored on shard 0 only. In a future multi-server
        // deployment, this should be replicated to all shards or moved to cluster-wide config.
        co_await engineSharded->invoke_on(0, [policy](Engine& engine) -> seastar::future<> {
            co_await engine.getIndex().setRetentionPolicy(policy);
        });

        // Broadcast to all shards' caches
        co_await engineSharded->invoke_on_all([policy](Engine& engine) {
            engine.updateRetentionPolicyCache(policy);
            return seastar::make_ready_future<>();
        });

        // Build response
        reply->set_status(seastar::http::reply::status_type::ok);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatRetentionGetResponse(toProtoPolicyData(policy));
        } else {
            auto responseObj = glz::obj{"status", "success", "policy", policy};
            reply->_content = glz::write_json(responseObj).value_or("{}");
        }

    } catch (const std::runtime_error& e) {
        // Protobuf parse errors come as runtime_error
        reply->set_status(seastar::http::reply::status_type::bad_request);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatErrorResponse(e.what());
        } else {
            reply->_content = createErrorResponse(e.what());
        }
    } catch (const std::exception& e) {
        timestar::http_log.error("Retention PUT handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatErrorResponse("Internal server error");
        } else {
            reply->_content = createErrorResponse("Internal server error");
        }
    }

    timestar::http::setContentType(*reply, resFmt);
    co_return reply;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpRetentionHandler::handleGet(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Retention handler not initialized"})";
        timestar::http::setContentType(*reply, resFmt);
        co_return reply;
    }

    try {
        // Check for ?measurement= query parameter
        std::string measurement = req->get_query_param("measurement");

        // Validate measurement name if provided
        for (char c : measurement) {
            if (static_cast<unsigned char>(c) < 0x20 || c == '\x7f') {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Measurement name contains control characters");
                timestar::http::setContentType(*reply, resFmt);
                co_return reply;
            }
        }

        if (!measurement.empty()) {
            // Get single policy
            auto policyOpt = co_await engineSharded->invoke_on(
                0, [measurement](Engine& engine) { return engine.getIndex().getRetentionPolicy(measurement); });

            if (policyOpt.has_value()) {
                reply->set_status(seastar::http::reply::status_type::ok);
                if (timestar::http::isProtobuf(resFmt)) {
                    reply->_content = timestar::proto::formatRetentionGetResponse(toProtoPolicyData(*policyOpt));
                } else {
                    auto responseObj = glz::obj{"status", "success", "policy", *policyOpt};
                    reply->_content = glz::write_json(responseObj).value_or("{}");
                }
            } else {
                reply->set_status(seastar::http::reply::status_type::not_found);
                if (timestar::http::isProtobuf(resFmt)) {
                    reply->_content = timestar::proto::formatErrorResponse(
                        "No retention policy found for measurement: " + measurement);
                } else {
                    reply->_content = createErrorResponse("No retention policy found for measurement: " + measurement);
                }
            }
        } else {
            // Get all policies — for protobuf, use status response since there's no
            // dedicated "list all policies" proto message
            auto policies = co_await engineSharded->invoke_on(
                0, [](Engine& engine) { return engine.getIndex().getAllRetentionPolicies(); });

            reply->set_status(seastar::http::reply::status_type::ok);
            if (timestar::http::isProtobuf(resFmt)) {
                reply->_content = timestar::proto::formatStatusResponse(
                    "success", std::to_string(policies.size()) + " retention policies");
            } else {
                auto responseObj = glz::obj{"status", "success", "policies", policies};
                reply->_content = glz::write_json(responseObj).value_or("{}");
            }
        }

    } catch (const std::exception& e) {
        timestar::http_log.error("Retention GET handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatErrorResponse("Internal server error");
        } else {
            reply->_content = createErrorResponse("Internal server error");
        }
    }

    timestar::http::setContentType(*reply, resFmt);
    co_return reply;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpRetentionHandler::handleDelete(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Retention handler not initialized"})";
        timestar::http::setContentType(*reply, resFmt);
        co_return reply;
    }

    try {
        std::string measurement = req->get_query_param("measurement");

        if (measurement.empty()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("'measurement' query parameter is required");
            timestar::http::setContentType(*reply, resFmt);
            co_return reply;
        }

        // Validate measurement name
        for (char c : measurement) {
            if (static_cast<unsigned char>(c) < 0x20 || c == '\x7f') {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Measurement name contains control characters");
                timestar::http::setContentType(*reply, resFmt);
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

            reply->set_status(seastar::http::reply::status_type::ok);
            if (timestar::http::isProtobuf(resFmt)) {
                reply->_content = timestar::proto::formatStatusResponse(
                    "success", "Retention policy deleted for measurement: " + measurement);
            } else {
                // Materialize before glz::obj — it stores references, and a temporary
                // string dies before write_json runs on the next line.
                std::string deletedMsg = "Retention policy deleted for measurement: " + measurement;
                auto responseObj = glz::obj{"status", "success", "message", deletedMsg};
                reply->_content = glz::write_json(responseObj).value_or("{}");
            }
        } else {
            reply->set_status(seastar::http::reply::status_type::not_found);
            if (timestar::http::isProtobuf(resFmt)) {
                reply->_content =
                    timestar::proto::formatErrorResponse("No retention policy found for measurement: " + measurement);
            } else {
                reply->_content = createErrorResponse("No retention policy found for measurement: " + measurement);
            }
        }

    } catch (const std::exception& e) {
        timestar::http_log.error("Retention DELETE handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatErrorResponse("Internal server error");
        } else {
            reply->_content = createErrorResponse("Internal server error");
        }
    }

    timestar::http::setContentType(*reply, resFmt);
    co_return reply;
}

void HttpRetentionHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    auto self = shared_from_this();
    // addJsonRoute applies timestar::http::wrapWithAuth per route.
    using op = seastar::httpd::operation_type;

    timestar::http::addJsonRoute(
        r, op::PUT, "/retention", authToken,
        [self](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return self->handlePut(std::move(req)); });

    timestar::http::addJsonRoute(
        r, op::GET, "/retention", authToken,
        [self](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return self->handleGet(std::move(req)); });

    timestar::http::addJsonRoute(
        r, op::DELETE, "/retention", authToken,
        [self](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return self->handleDelete(std::move(req)); });

    timestar::http_log.info("Registered retention endpoints at /retention (PUT/GET/DELETE){}",
                            authToken.empty() ? "" : " (auth required)");
}

}  // namespace timestar::http
