#include "http_delete_handler.hpp"

#include "content_negotiation.hpp"
#include "http_auth.hpp"
#include "http_error.hpp"
#include "http_routes.hpp"
#include "logger.hpp"
#include "placement_table.hpp"
#include "proto_converters.hpp"
#include "scatter_gather.hpp"
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

namespace timestar::http {

// Forward declaration for shared validation used by both JSON and protobuf paths.
static void validateDeleteRequest(const HttpDeleteHandler::DeleteRequest& req);

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
    return timestar::http::jsonError(error);
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
            // Wire-format ambiguity: a bare DeleteRequest that uses only the
            // structured fields (measurement=2, tags=3, field=4, fields=5,
            // start/end_time=6/7) contains no field number that
            // BatchDeleteRequest recognizes (its only field is deletes=1), so
            // protobuf "successfully" parses it as a BatchDeleteRequest with
            // ZERO entries by skipping every field as unknown.  Treating that
            // empty parse as an authoritative batch made structured single
            // deletes silently no-op (success, totalRequests=0, nothing
            // deleted).  Therefore a batch parse only wins when it yields at
            // least one entry; otherwise the body is re-parsed as a single
            // DeleteRequest.
            //
            // An empty body is fully ambiguous (an empty batch, an empty
            // single delete, and zero bytes are all wire-identical) and a
            // delete that targets nothing is almost certainly a client bug —
            // reject it instead of reporting a vacuous success.
            auto rejectBadRequest = [&](const std::string& message) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    reply->_content = timestar::proto::formatDeleteResponse("error", 0, 0, message);
                } else {
                    reply->_content = createErrorResponse(message);
                }
                timestar::http::setContentType(*reply, resFmt);
            };

            if (req->content.empty()) {
                rejectBadRequest("Empty delete request body: at least one delete must be specified");
                co_return reply;
            }

            // Converts + validates a parsed request; throws on invalid input.
            auto convertAndValidate = [](timestar::proto::ParsedDeleteRequest&& pd) {
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
                return dr;
            };

            static constexpr size_t MAX_BATCH_DELETE_SIZE = 10'000;

            // Attempt 1: BatchDeleteRequest — authoritative only if it parses,
            // yields at least one entry, AND every entry validates.  (A single
            // DeleteRequest using only the `series` string can coincidentally
            // parse as a batch whose garbage entries fail validation; such
            // bodies must fall through to the single-delete parse below, which
            // is also what the previous implementation did via its catch-all.)
            std::string batchError;
            bool parsedAsBatch = false;
            try {
                auto parsedList = timestar::proto::parseBatchDeleteRequest(req->content.data(), req->content.size());
                if (!parsedList.empty()) {
                    if (parsedList.size() > MAX_BATCH_DELETE_SIZE) {
                        rejectBadRequest("Batch delete exceeds maximum size of " +
                                         std::to_string(MAX_BATCH_DELETE_SIZE));
                        co_return reply;
                    }
                    std::vector<DeleteRequest> batchRequests;
                    batchRequests.reserve(parsedList.size());
                    for (auto& pd : parsedList) {
                        batchRequests.push_back(convertAndValidate(std::move(pd)));
                    }
                    deleteRequests = std::move(batchRequests);
                    parsedAsBatch = true;
                }
            } catch (const std::exception& e) {
                batchError = e.what();
                timestar::http_log.debug("Batch delete parse failed ({}), falling back to single delete", e.what());
            }

            // Attempt 2: single DeleteRequest (zero-entry batch, batch parse
            // failure, or batch entries that failed validation).
            if (!parsedAsBatch) {
                try {
                    deleteRequests.push_back(convertAndValidate(
                        timestar::proto::parseSingleDeleteRequest(req->content.data(), req->content.size())));
                } catch (const std::exception& e) {
                    timestar::http_log.debug("Single delete parse failed: {}", e.what());
                    // Prefer the batch-side error when there was one — it came
                    // from a body that structurally looked like a batch.
                    rejectBadRequest(std::string("Invalid delete request: ") +
                                     (batchError.empty() ? e.what() : batchError.c_str()));
                    co_return reply;
                }
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

            // Scatter to every shard, fold per-shard DeleteResults into one ShardResult.
            allFutures.push_back(
                timestar::cluster::scatterAll(*engineSharded,
                                              [sharedReq](Engine& engine) -> seastar::future<Engine::DeleteResult> {
                                                  co_return co_await engine.deleteByPattern(*sharedReq);
                                              })
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
    // addJsonRoute applies timestar::http::wrapWithAuth per route.
    timestar::http::addJsonRoute(
        r, seastar::httpd::operation_type::POST, "/delete", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleDelete(std::move(req)); });

    timestar::http_log.info("Registered DELETE endpoint at /delete{}", authToken.empty() ? "" : " (auth required)");
}

}  // namespace timestar::http
