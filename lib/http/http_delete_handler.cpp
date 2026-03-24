#include "http_delete_handler.hpp"

#include "content_negotiation.hpp"
#include "http_auth.hpp"
#include "logger.hpp"
#include "placement_table.hpp"
#include "proto_converters.hpp"
#include "series_key.hpp"

#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

using namespace seastar;
using namespace httpd;
using timestar::buildSeriesKey;

// GlazeDeleteRequest is defined in http_delete_handler.hpp

struct GlazeBatchDelete {
    std::vector<GlazeDeleteRequest> deletes;
};

template <>
struct glz::meta<GlazeBatchDelete> {
    using T = GlazeBatchDelete;
    static constexpr auto value = object("deletes", &T::deletes);
};

// Forward declaration for shared validation used by both JSON and protobuf paths.
static void validateDeleteRequest(const HttpDeleteHandler::DeleteRequest& req);

// Response structures for Glaze serialization - must be at namespace scope
struct DeleteDetailedResponse {
    std::string status = "success";
    uint64_t seriesDeleted;
    uint64_t totalRequests;
    std::optional<std::vector<std::string>> deletedSeries;
    std::optional<uint64_t> deletedSeriesCount;
    std::optional<std::string> note;
};

template <>
struct glz::meta<DeleteDetailedResponse> {
    using T = DeleteDetailedResponse;
    static constexpr auto value =
        object("status", &T::status, "seriesDeleted", &T::seriesDeleted, "totalRequests", &T::totalRequests,
               "deletedSeries", &T::deletedSeries, "deletedSeriesCount", &T::deletedSeriesCount, "note", &T::note);
};

HttpDeleteHandler::DeleteRequest HttpDeleteHandler::parseDeleteRequest(const GlazeDeleteRequest& glazeReq) {
    DeleteRequest req;
    req.isPattern = false;

    // Check for series key format
    if (glazeReq.series) {
        if (glazeReq.series->empty()) {
            throw std::invalid_argument("'series' must not be empty");
        }
        req.seriesKey = *glazeReq.series;
        req.isStructured = false;
    }
    // Check for structured/pattern format
    else if (glazeReq.measurement) {
        if (glazeReq.measurement->empty()) {
            throw std::invalid_argument("'measurement' must not be empty");
        }
        req.measurement = *glazeReq.measurement;
        req.isStructured = true;

        // Parse tags if present
        if (glazeReq.tags) {
            req.tags = *glazeReq.tags;
        }

        // Check for single field (backward compatibility)
        if (glazeReq.field) {
            req.field = *glazeReq.field;
            req.fields.push_back(req.field);
        }
        // Check for multiple fields (pattern-based)
        else if (glazeReq.fields) {
            req.isPattern = true;
            req.fields = *glazeReq.fields;
            if (!req.fields.empty()) {
                req.field = req.fields[0];  // Set first field for compatibility
            }
        }
        // If neither field nor fields is specified, it's a pattern delete for all fields
        else {
            req.isPattern = true;
            // Empty fields vector means match all fields
        }
    } else {
        throw std::runtime_error("Either 'series' or 'measurement' is required");
    }

    // Parse time range with defaults
    req.startTime = glazeReq.startTime.value_or(0);       // Start of time
    req.endTime = glazeReq.endTime.value_or(UINT64_MAX);  // End of time

    validateDeleteRequest(req);
    return req;
}

// Shared validation for both JSON and protobuf delete requests.
static void validateDeleteRequest(const HttpDeleteHandler::DeleteRequest& req) {
    if (req.seriesKey.empty() && req.measurement.empty()) {
        throw std::runtime_error("Either 'series' or 'measurement' is required");
    }
    if (req.isStructured && req.field.empty() && !req.isPattern) {
        throw std::invalid_argument("Structured delete requires a non-empty 'field'");
    }

    // Reject characters that are used as key separators in series keys.
    auto hasReservedChar = [](const std::string& s, bool allowSpace) {
        for (char c : s) {
            if (c == '\0' || c == ',' || c == '=')
                return true;
            if (!allowSpace && c == ' ')
                return true;
        }
        return false;
    };
    if (hasReservedChar(req.measurement, false)) {
        throw std::runtime_error("Measurement name must not contain null bytes, commas, equals signs, or spaces");
    }
    if (!req.seriesKey.empty() && req.seriesKey.find('\0') != std::string::npos) {
        throw std::runtime_error("Series key must not contain null bytes");
    }
    if (hasReservedChar(req.field, false) && !req.field.empty()) {
        throw std::runtime_error("Field name must not contain null bytes, commas, equals signs, or spaces");
    }
    for (const auto& f : req.fields) {
        if (f.empty())
            throw std::invalid_argument("'fields' must not contain empty strings");
        if (hasReservedChar(f, false))
            throw std::runtime_error("Field names must not contain null bytes, commas, equals signs, or spaces");
    }
    for (const auto& [k, v] : req.tags) {
        if (k.empty())
            throw std::invalid_argument("Tag keys must not be empty");
        if (hasReservedChar(k, false)) {
            throw std::runtime_error("Tag keys must not contain null bytes, commas, equals signs, or spaces");
        }
        if (hasReservedChar(v, true)) {
            throw std::runtime_error("Tag values must not contain null bytes, commas, or equals signs");
        }
    }

    if (req.startTime > req.endTime) {
        throw std::runtime_error("startTime must be less than or equal to endTime");
    }
}

std::string HttpDeleteHandler::createErrorResponse(const std::string& error) {
    // Create JSON object directly
    auto response = glz::obj{"status", "error", "error", error};

    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","error":"Failed to serialize error response"})";
    }
    return buffer;
}

std::string HttpDeleteHandler::createSuccessResponse(uint64_t deletedCount, uint64_t totalRequests) {
    // Create JSON object directly
    auto response = glz::obj{"status", "success", "deleted", deletedCount, "total", totalRequests};

    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","error":"Failed to serialize success response"})";
    }
    return buffer;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpDeleteHandler::handleDelete(
    std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    auto reqFmt = timestar::http::requestFormat(*req);
    auto resFmt = timestar::http::responseFormat(*req);

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Delete handler not initialized"})";
        timestar::http::setContentType(*reply, resFmt);
        co_return reply;
    }

    // Body size limit to prevent DoS via large payloads
    if (req->content.size() > timestar::config().http.max_query_body_size) {
        reply->set_status(seastar::http::reply::status_type::payload_too_large);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatDeleteResponse("error", 0, 0, "Request body too large");
        } else {
            reply->_content = R"({"status":"error","error":"Request body too large"})";
        }
        timestar::http::setContentType(*reply, resFmt);
        co_return reply;
    }

    timestar::http_log.debug("[DELETE_HANDLER] Received delete request");

    try {
        // Parse request body
        std::vector<DeleteRequest> deleteRequests;

        if (timestar::http::isProtobuf(reqFmt)) {
            // Parse protobuf request — try batch first, fall back to single
            try {
                auto parsedList = timestar::proto::parseBatchDeleteRequest(req->content.data(), req->content.size());
                static constexpr size_t MAX_BATCH_DELETE_SIZE = 10'000;
                if (parsedList.size() > MAX_BATCH_DELETE_SIZE) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    if (timestar::http::isProtobuf(resFmt)) {
                        reply->_content = timestar::proto::formatDeleteResponse(
                            "error", 0, 0,
                            "Batch delete exceeds maximum size of " + std::to_string(MAX_BATCH_DELETE_SIZE));
                    } else {
                        reply->_content = createErrorResponse("Batch delete exceeds maximum size of " +
                                                              std::to_string(MAX_BATCH_DELETE_SIZE));
                    }
                    timestar::http::setContentType(*reply, resFmt);
                    co_return reply;
                }
                for (auto& pd : parsedList) {
                    DeleteRequest dr;
                    dr.seriesKey = std::move(pd.seriesKey);
                    dr.measurement = std::move(pd.measurement);
                    dr.tags = std::move(pd.tags);
                    dr.field = std::move(pd.field);
                    dr.fields = std::move(pd.fields);
                    dr.startTime = pd.startTime;
                    dr.endTime = pd.endTime;
                    dr.isStructured = pd.isStructured;
                    dr.isPattern = pd.isPattern;
                    validateDeleteRequest(dr);
                    deleteRequests.push_back(std::move(dr));
                }
            } catch (const std::exception& e) {
                // Batch parse failed, try single delete
                timestar::http_log.debug("Batch delete parse failed ({}), falling back to single delete", e.what());
                auto pd = timestar::proto::parseSingleDeleteRequest(req->content.data(), req->content.size());
                DeleteRequest dr;
                dr.seriesKey = std::move(pd.seriesKey);
                dr.measurement = std::move(pd.measurement);
                dr.tags = std::move(pd.tags);
                dr.field = std::move(pd.field);
                dr.fields = std::move(pd.fields);
                dr.startTime = pd.startTime;
                dr.endTime = pd.endTime;
                dr.isStructured = pd.isStructured;
                dr.isPattern = pd.isPattern;
                validateDeleteRequest(dr);
                deleteRequests.push_back(std::move(dr));
            }
        } else {
            // Parse JSON body using Glaze
            // Try to parse as batch delete first
            GlazeBatchDelete batchDelete;
            auto batch_error = glz::read_json(batchDelete, req->content);

            static constexpr size_t MAX_BATCH_DELETE_SIZE = 10'000;
            if (!batch_error && !batchDelete.deletes.empty()) {
                if (batchDelete.deletes.size() > MAX_BATCH_DELETE_SIZE) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    reply->_content = createErrorResponse("Batch delete exceeds maximum size of " +
                                                          std::to_string(MAX_BATCH_DELETE_SIZE));
                    timestar::http::setContentType(*reply, resFmt);
                    co_return reply;
                }
                // Batch delete request
                for (const auto& glazeReq : batchDelete.deletes) {
                    try {
                        deleteRequests.push_back(parseDeleteRequest(glazeReq));
                    } catch (const std::exception& e) {
                        reply->set_status(seastar::http::reply::status_type::bad_request);
                        reply->_content = createErrorResponse(std::string("Invalid delete request: ") + e.what());
                        timestar::http::setContentType(*reply, resFmt);
                        co_return reply;
                    }
                }
            } else {
                // Try single delete request
                GlazeDeleteRequest singleDelete;
                auto single_error = glz::read_json(singleDelete, req->content);

                if (single_error) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    reply->_content = createErrorResponse("Invalid JSON in delete request");
                    timestar::http::setContentType(*reply, resFmt);
                    co_return reply;
                }

                try {
                    deleteRequests.push_back(parseDeleteRequest(singleDelete));
                } catch (const std::exception& e) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    reply->_content = createErrorResponse(e.what());
                    timestar::http::setContentType(*reply, resFmt);
                    co_return reply;
                }
            }
        }

        // Execute deletes — parallelized by shard.
        // Targeted deletes (series-key or structured single-field) are grouped by
        // their owning shard so all shards execute in parallel.  Pattern deletes
        // (which scatter-gather to every shard) are also launched in parallel.
        uint64_t totalSeriesDeleted = 0;
        std::vector<std::string> allDeletedSeries;
        static constexpr size_t MAX_REPORTED_SERIES = 100;

        timestar::http_log.debug("[DELETE_HANDLER] Processing {} delete requests", deleteRequests.size());

        // Targeted delete work item — self-contained copy of everything needed
        // so the lambda on the target shard does not dereference cross-shard ptrs.
        struct TargetedDelete {
            std::string seriesKey;    // precomputed full key (structured) or raw key
            std::string measurement;  // only used for structured deletes
            std::map<std::string, std::string> tags;
            std::string field;
            uint64_t startTime;
            uint64_t endTime;
            bool isStructured;
        };

        // Group targeted deletes by shard; collect pattern deletes separately.
        std::vector<std::vector<TargetedDelete>> perShard(seastar::smp::count);
        std::vector<size_t> patternDeleteIndices;  // indices into deleteRequests

        for (size_t i = 0; i < deleteRequests.size(); ++i) {
            const auto& delReq = deleteRequests[i];
            timestar::http_log.info(
                "[DELETE_HANDLER] Delete request: measurement={}, tags={}, fields={}, startTime={}, endTime={}",
                delReq.measurement, delReq.tags.size(), delReq.fields.size(), delReq.startTime, delReq.endTime);

            if (delReq.isPattern) {
                patternDeleteIndices.push_back(i);
            } else if (delReq.isStructured && !delReq.field.empty()) {
                std::string fullKey = buildSeriesKey(delReq.measurement, delReq.tags, delReq.field);
                unsigned shard = timestar::routeToCore(SeriesId128::fromSeriesKey(fullKey));
                perShard[shard].push_back(TargetedDelete{std::move(fullKey), delReq.measurement, delReq.tags,
                                                         delReq.field, delReq.startTime, delReq.endTime, true});
            } else if (!delReq.isStructured) {
                unsigned shard = timestar::routeToCore(SeriesId128::fromSeriesKey(delReq.seriesKey));
                perShard[shard].push_back(
                    TargetedDelete{delReq.seriesKey, {}, {}, {}, delReq.startTime, delReq.endTime, false});
            }
        }

        // --- Launch all work in parallel ---
        // Each future returns {seriesDeleted, deletedSeriesNames}.
        using ShardResult = std::pair<uint64_t, std::vector<std::string>>;
        std::vector<seastar::future<ShardResult>> allFutures;

        // 1) Per-shard targeted deletes — one future per shard that has work.
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            if (perShard[s].empty())
                continue;

            // Move the work items into a shared_ptr so the lambda owns them
            // on the target shard.
            auto items = std::make_shared<std::vector<TargetedDelete>>(std::move(perShard[s]));

            allFutures.push_back(engineSharded->invoke_on(s, [items](Engine& engine) -> seastar::future<ShardResult> {
                uint64_t deleted = 0;
                std::vector<std::string> names;
                for (auto& item : *items) {
                    bool ok;
                    if (item.isStructured) {
                        ok = co_await engine.deleteRangeBySeries(item.measurement, item.tags, item.field,
                                                                 item.startTime, item.endTime);
                    } else {
                        ok = co_await engine.deleteRange(item.seriesKey, item.startTime, item.endTime);
                    }
                    if (ok) {
                        deleted++;
                        names.push_back(std::move(item.seriesKey));
                    }
                }
                co_return ShardResult{deleted, std::move(names)};
            }));
        }

        // 2) Pattern deletes — each one fans out to every shard.
        //    Multiple pattern deletes are independent so launch them all now.
        for (size_t idx : patternDeleteIndices) {
            const auto& patReq = deleteRequests[idx];
            Engine::DeleteRequest engineReq;
            engineReq.measurement = patReq.measurement;
            engineReq.tags = patReq.tags;
            engineReq.fields = patReq.fields;
            engineReq.startTime = patReq.startTime;
            engineReq.endTime = patReq.endTime;

            auto sharedReq = std::make_shared<const Engine::DeleteRequest>(std::move(engineReq));

            // Scatter to every shard, collect into one ShardResult.
            std::vector<seastar::future<Engine::DeleteResult>> scatterFutures;
            for (unsigned s = 0; s < seastar::smp::count; ++s) {
                scatterFutures.push_back(
                    engineSharded->invoke_on(s, [sharedReq](Engine& engine) -> seastar::future<Engine::DeleteResult> {
                        co_return co_await engine.deleteByPattern(*sharedReq);
                    }));
            }

            // Wrap the scatter-gather into a single ShardResult future.
            allFutures.push_back(seastar::when_all_succeed(scatterFutures.begin(), scatterFutures.end())
                                     .then([](std::vector<Engine::DeleteResult> results) -> ShardResult {
                                         uint64_t deleted = 0;
                                         std::vector<std::string> names;
                                         for (auto& r : results) {
                                             deleted += r.seriesDeleted;
                                             for (auto& s : r.deletedSeries) {
                                                 names.push_back(std::move(s));
                                             }
                                         }
                                         return ShardResult{deleted, std::move(names)};
                                     }));
        }

        // --- Await all parallel work and merge results ---
        auto results = co_await seastar::when_all_succeed(allFutures.begin(), allFutures.end());
        for (auto& [deleted, names] : results) {
            totalSeriesDeleted += deleted;
            if (allDeletedSeries.size() < MAX_REPORTED_SERIES) {
                for (auto& s : names) {
                    if (allDeletedSeries.size() >= MAX_REPORTED_SERIES)
                        break;
                    allDeletedSeries.push_back(std::move(s));
                }
            }
        }

        // Return success response with detailed information
        reply->set_status(seastar::http::reply::status_type::ok);

        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content =
                timestar::proto::formatDeleteResponse("success", totalSeriesDeleted, deleteRequests.size());
        } else {
            // Create detailed response using structured approach
            DeleteDetailedResponse response;
            response.seriesDeleted = totalSeriesDeleted;
            response.totalRequests = deleteRequests.size();

            // Include deleted series list if not too large
            if (!allDeletedSeries.empty() && allDeletedSeries.size() <= 100) {
                response.deletedSeries = allDeletedSeries;
            } else if (allDeletedSeries.size() > 100) {
                response.deletedSeriesCount = allDeletedSeries.size();
                response.note = "Series list omitted due to size";
            }

            reply->_content = glz::write_json(response).value_or("{}");
        }

    } catch (const std::exception& e) {
        timestar::http_log.error("Delete handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            reply->_content = timestar::proto::formatDeleteResponse("error", 0, 0, "Internal server error");
        } else {
            reply->_content = createErrorResponse("Internal server error");
        }
    }

    timestar::http::setContentType(*reply, resFmt);
    co_return reply;
}

void HttpDeleteHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    auto* handler = new seastar::httpd::function_handler(
        timestar::wrapWithAuth(
            authToken,
            [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
                -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleDelete(std::move(req)); }),
        "json");

    r.add(seastar::httpd::operation_type::POST, seastar::httpd::url("/delete"), handler);

    timestar::http_log.info("Registered DELETE endpoint at /delete{}", authToken.empty() ? "" : " (auth required)");
}