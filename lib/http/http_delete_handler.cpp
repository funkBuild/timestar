#include "http_delete_handler.hpp"

#include "logger.hpp"
#include "series_key.hpp"

#include <chrono>
#include <seastar/core/smp.hh>

using namespace seastar;
using namespace httpd;
using timestar::buildSeriesKey;

// Glaze-compatible structures for JSON parsing
struct GlazeDeleteRequest {
    // For series key format
    std::optional<std::string> series;

    // For structured format
    std::optional<std::string> measurement;
    std::optional<std::map<std::string, std::string>> tags;
    std::optional<std::string> field;
    std::optional<std::vector<std::string>> fields;

    // Time range (optional - defaults to all time)
    std::optional<uint64_t> startTime;
    std::optional<uint64_t> endTime;
};

template <>
struct glz::meta<GlazeDeleteRequest> {
    using T = GlazeDeleteRequest;
    static constexpr auto value =
        object("series", &T::series, "measurement", &T::measurement, "tags", &T::tags, "field", &T::field, "fields",
               &T::fields, "startTime", &T::startTime, "endTime", &T::endTime);
};

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

HttpDeleteHandler::DeleteRequest HttpDeleteHandler::parseDeleteRequest(const GlazeDeleteRequest& glazeReq) {
    DeleteRequest req;
    req.isPattern = false;

    // Check for series key format
    if (glazeReq.series) {
        req.seriesKey = *glazeReq.series;
        req.isStructured = false;
    }
    // Check for structured/pattern format
    else if (glazeReq.measurement) {
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

    // Validate strings for null bytes (would corrupt LevelDB keys)
    auto hasNullByte = [](const std::string& s) { return s.find('\0') != std::string::npos; };
    if (hasNullByte(req.measurement) || hasNullByte(req.seriesKey) || hasNullByte(req.field)) {
        throw std::runtime_error("Measurement, series, and field names must not contain null bytes");
    }
    for (const auto& f : req.fields) {
        if (hasNullByte(f))
            throw std::runtime_error("Field names must not contain null bytes");
    }
    for (const auto& [k, v] : req.tags) {
        if (hasNullByte(k) || hasNullByte(v)) {
            throw std::runtime_error("Tag keys and values must not contain null bytes");
        }
    }

    // Parse time range with defaults
    req.startTime = glazeReq.startTime.value_or(0);       // Start of time
    req.endTime = glazeReq.endTime.value_or(UINT64_MAX);  // End of time

    // Validate time range
    if (req.startTime > req.endTime) {
        throw std::runtime_error("startTime must be less than or equal to endTime");
    }

    return req;
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
    reply->add_header("Content-Type", "application/json");

    if (!engineSharded) {
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = R"({"status":"error","error":"Delete handler not initialized"})";
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

    timestar::http_log.debug("[DELETE_HANDLER] Received delete request");

    try {
        // Parse JSON body using Glaze
        std::vector<DeleteRequest> deleteRequests;

        // Try to parse as batch delete first
        GlazeBatchDelete batchDelete;
        auto batch_error = glz::read_json(batchDelete, req->content);

        static constexpr size_t MAX_BATCH_DELETE_SIZE = 10'000;
        if (!batch_error && !batchDelete.deletes.empty()) {
            if (batchDelete.deletes.size() > MAX_BATCH_DELETE_SIZE) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse("Batch delete exceeds maximum size of " +
                                                      std::to_string(MAX_BATCH_DELETE_SIZE));
                co_return reply;
            }
            // Batch delete request
            for (const auto& glazeReq : batchDelete.deletes) {
                try {
                    deleteRequests.push_back(parseDeleteRequest(glazeReq));
                } catch (const std::exception& e) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    reply->_content = createErrorResponse(std::string("Invalid delete request: ") + e.what());
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
                co_return reply;
            }

            try {
                deleteRequests.push_back(parseDeleteRequest(singleDelete));
            } catch (const std::exception& e) {
                reply->set_status(seastar::http::reply::status_type::bad_request);
                reply->_content = createErrorResponse(e.what());
                co_return reply;
            }
        }

        // Execute deletes
        uint64_t totalSeriesDeleted = 0;
        uint64_t totalPointsDeleted = 0;
        std::vector<std::string> allDeletedSeries;

        timestar::http_log.info("[DELETE_HANDLER] Processing {} delete requests", deleteRequests.size());

        for (const auto& delReq : deleteRequests) {
            timestar::http_log.info(
                "[DELETE_HANDLER] Delete request: measurement={}, tags={}, fields={}, startTime={}, endTime={}",
                delReq.measurement, delReq.tags.size(), delReq.fields.size(), delReq.startTime, delReq.endTime);
            if (delReq.isPattern) {
                // Pattern-based deletion
                std::vector<Engine::DeleteResult> shardResults;

                // Always query shard 0 first for metadata to find matching series,
                // regardless of whether we have specific tags/fields.
                // This ensures we use the centralized metadata.
                {
                    auto matchingSeries = co_await engineSharded->invoke_on(
                        0,
                        [measurement = delReq.measurement, tags = delReq.tags, fields = delReq.fields](
                            Engine& engine) -> seastar::future<std::vector<std::pair<std::string, size_t>>> {
                            auto& index = engine.getIndex();
                            std::vector<std::pair<std::string, size_t>> seriesWithShards;

                            // Find all series IDs that match the pattern
                            // Delete operations don't enforce a series limit (0 = unlimited)
                            std::vector<SeriesId128> seriesIds;
                            if (tags.empty()) {
                                auto findResult = co_await index.getAllSeriesForMeasurement(measurement);
                                if (findResult.has_value()) {
                                    seriesIds = std::move(findResult.value());
                                }
                            } else {
                                auto findResult = co_await index.findSeries(measurement, tags);
                                if (findResult.has_value()) {
                                    seriesIds = std::move(findResult.value());
                                }
                            }

                            // Guard against unbounded series expansion
                            static constexpr size_t MAX_DELETE_SERIES = 100000;
                            if (seriesIds.size() > MAX_DELETE_SERIES) {
                                throw std::runtime_error(
                                    "Delete matches too many series (" + std::to_string(seriesIds.size()) +
                                    "). Narrow with tag filters (limit: " + std::to_string(MAX_DELETE_SERIES) + ").");
                            }

                            // Get metadata for each series to check field filters and determine shard
                            for (const SeriesId128& seriesId : seriesIds) {
                                auto metadata = co_await index.getSeriesMetadata(seriesId);
                                if (!metadata.has_value()) {
                                    continue;
                                }

                                // Check if field matches (if field filter is specified)
                                if (!fields.empty()) {
                                    bool fieldMatches = false;
                                    for (const auto& field : fields) {
                                        if (metadata->field == field) {
                                            fieldMatches = true;
                                            break;
                                        }
                                    }
                                    if (!fieldMatches) {
                                        continue;
                                    }
                                }

                                // Build series key directly without constructing a temporary
                                // TimeStarInsert<double> object
                                std::string seriesKey =
                                    buildSeriesKey(metadata->measurement, metadata->tags, metadata->field);

                                // Calculate target shard for this series using SeriesId128
                                SeriesId128 seriesIdForSharding = SeriesId128::fromSeriesKey(seriesKey);
                                size_t shard = SeriesId128::Hash{}(seriesIdForSharding) % seastar::smp::count;
                                seriesWithShards.push_back({seriesKey, shard});
                            }

                            co_return seriesWithShards;
                        });

                    // Group series by shard
                    std::map<size_t, std::vector<std::string>> seriesByShard;
                    for (const auto& [seriesKey, shard] : matchingSeries) {
                        seriesByShard[shard].push_back(seriesKey);
                    }

                    // Execute targeted deletes on each shard that has matching series
                    for (const auto& [shard, seriesKeys] : seriesByShard) {
                        auto result = co_await engineSharded->invoke_on(
                            shard,
                            [seriesKeys, startTime = delReq.startTime,
                             endTime = delReq.endTime](Engine& engine) -> seastar::future<Engine::DeleteResult> {
                                Engine::DeleteResult result;

                                for (const auto& seriesKey : seriesKeys) {
                                    bool deleted = co_await engine.deleteRange(seriesKey, startTime, endTime);
                                    if (deleted) {
                                        result.seriesDeleted++;
                                        result.deletedSeries.push_back(seriesKey);
                                        result.pointsDeleted++;  // Placeholder
                                    }
                                }

                                co_return result;
                            });

                        if (result.seriesDeleted > 0) {
                            shardResults.push_back(result);
                        }
                    }
                }

                // Aggregate results from all shards
                for (const auto& result : shardResults) {
                    totalSeriesDeleted += result.seriesDeleted;
                    totalPointsDeleted += result.pointsDeleted;
                    allDeletedSeries.insert(allDeletedSeries.end(), result.deletedSeries.begin(),
                                            result.deletedSeries.end());
                }
            } else if (delReq.isStructured && !delReq.field.empty()) {
                // Single series structured delete - determine shard based on series
                std::string fullSeriesKey = buildSeriesKey(delReq.measurement, delReq.tags, delReq.field);

                // Hash the full series key to determine shard using SeriesId128
                SeriesId128 seriesIdForSharding = SeriesId128::fromSeriesKey(fullSeriesKey);
                unsigned targetShard = SeriesId128::Hash{}(seriesIdForSharding) % seastar::smp::count;

                // Execute delete on the target shard
                bool deleted = co_await engineSharded->invoke_on(targetShard, [delReq](Engine& engine) {
                    return engine.deleteRangeBySeries(delReq.measurement, delReq.tags, delReq.field, delReq.startTime,
                                                      delReq.endTime);
                });

                if (deleted) {
                    totalSeriesDeleted++;
                    allDeletedSeries.push_back(fullSeriesKey);
                }
            } else if (!delReq.isStructured) {
                // Series key delete - determine shard using SeriesId128
                SeriesId128 seriesIdForSharding = SeriesId128::fromSeriesKey(delReq.seriesKey);
                unsigned targetShard = SeriesId128::Hash{}(seriesIdForSharding) % seastar::smp::count;

                // Execute delete on the target shard
                bool deleted = co_await engineSharded->invoke_on(targetShard, [delReq](Engine& engine) {
                    return engine.deleteRange(delReq.seriesKey, delReq.startTime, delReq.endTime);
                });

                if (deleted) {
                    totalSeriesDeleted++;
                    allDeletedSeries.push_back(delReq.seriesKey);
                }
            }
        }

        // Return success response with detailed information
        reply->set_status(seastar::http::reply::status_type::ok);

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

    } catch (const std::exception& e) {
        timestar::http_log.error("Delete handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = createErrorResponse("Internal server error");
    }

    co_return reply;
}

void HttpDeleteHandler::registerRoutes(seastar::httpd::routes& r) {
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req,
               std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            // We don't use the provided reply, create our own
            return handleDelete(std::move(req));
        },
        "json");

    r.add(seastar::httpd::operation_type::POST, seastar::httpd::url("/delete"), handler);

    timestar::http_log.info("Registered DELETE endpoint at /delete");
}