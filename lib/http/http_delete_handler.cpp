#include "http_delete_handler.hpp"
#include "logger.hpp"
#include <chrono>
#include <seastar/core/smp.hh>

using namespace seastar;
using namespace httpd;

// Glaze-compatible structures for JSON parsing
struct GlazeDeleteRequest {
    // For series key format
    std::optional<std::string> series;
    
    // For structured format
    std::optional<std::string> measurement;
    std::optional<std::map<std::string, std::string>> tags;
    std::optional<std::string> field;
    std::optional<std::vector<std::string>> fields;
    
    // Time range (required)
    uint64_t startTime;
    uint64_t endTime;
};

template <>
struct glz::meta<GlazeDeleteRequest> {
    using T = GlazeDeleteRequest;
    static constexpr auto value = object(
        "series", &T::series,
        "measurement", &T::measurement,
        "tags", &T::tags,
        "field", &T::field,
        "fields", &T::fields,
        "startTime", &T::startTime,
        "endTime", &T::endTime
    );
};

struct GlazeBatchDelete {
    std::vector<GlazeDeleteRequest> deletes;
};

template <>
struct glz::meta<GlazeBatchDelete> {
    using T = GlazeBatchDelete;
    static constexpr auto value = object(
        "deletes", &T::deletes
    );
};

// Response structures for Glaze serialization - must be at namespace scope
struct DeleteDetailedResponse {
    std::string status = "success";
    int seriesDeleted;
    int totalRequests;
    std::optional<std::vector<std::string>> deletedSeries;
    std::optional<int> deletedSeriesCount;
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
                req.field = req.fields[0]; // Set first field for compatibility
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
    
    // Parse time range
    req.startTime = glazeReq.startTime;
    req.endTime = glazeReq.endTime;
    
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
    glz::write_json(response, buffer);
    return buffer;
}

std::string HttpDeleteHandler::createSuccessResponse(int deletedCount, int totalRequests) {
    // Create JSON object directly
    auto response = glz::obj{"status", "success", "deleted", deletedCount, "total", totalRequests};
    
    std::string buffer;
    glz::write_json(response, buffer);
    return buffer;
}

seastar::future<std::unique_ptr<seastar::http::reply>>
HttpDeleteHandler::handleDelete(std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    reply->add_header("Content-Type", "application/json");
    
    try {
        // Parse JSON body using Glaze
        std::vector<DeleteRequest> deleteRequests;
        
        // Try to parse as batch delete first
        GlazeBatchDelete batchDelete;
        auto batch_error = glz::read_json(batchDelete, req->content);
        
        if (!batch_error && !batchDelete.deletes.empty()) {
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
                reply->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(single_error)));
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
        int totalSeriesDeleted = 0;
        int totalPointsDeleted = 0;
        std::vector<std::string> allDeletedSeries;
        
        for (const auto& delReq : deleteRequests) {
            if (delReq.isPattern) {
                // Pattern-based deletion - needs to run on all shards
                // since we don't know which shards have matching series
                std::vector<Engine::DeleteResult> shardResults;
                
                // Create delete request for Engine
                Engine::DeleteRequest engineReq;
                engineReq.measurement = delReq.measurement;
                engineReq.tags = delReq.tags;
                engineReq.fields = delReq.fields;
                engineReq.startTime = delReq.startTime;
                engineReq.endTime = delReq.endTime;
                
                // Execute on all shards and collect results
                co_await engineSharded->invoke_on_all([engineReq, &shardResults](Engine& engine) -> seastar::future<> {
                    auto result = co_await engine.deleteByPattern(engineReq);
                    if (result.seriesDeleted > 0) {
                        shardResults.push_back(result);
                    }
                });
                
                // Aggregate results from all shards
                for (const auto& result : shardResults) {
                    totalSeriesDeleted += result.seriesDeleted;
                    totalPointsDeleted += result.pointsDeleted;
                    allDeletedSeries.insert(allDeletedSeries.end(), 
                                           result.deletedSeries.begin(), 
                                           result.deletedSeries.end());
                }
            }
            else if (delReq.isStructured && !delReq.field.empty()) {
                // Single series structured delete - determine shard based on series
                std::string fullSeriesKey = delReq.measurement;
                for (const auto& [tagKey, tagValue] : delReq.tags) {
                    fullSeriesKey += "," + tagKey + "=" + tagValue;
                }
                fullSeriesKey += "." + delReq.field;
                
                // Hash the full series key to determine shard
                std::hash<std::string> hasher;
                size_t hash = hasher(fullSeriesKey);
                unsigned targetShard = hash % seastar::smp::count;
                
                // Execute delete on the target shard
                bool deleted = co_await engineSharded->invoke_on(targetShard,
                    [delReq](Engine& engine) {
                        return engine.deleteRangeBySeries(
                            delReq.measurement,
                            delReq.tags,
                            delReq.field,
                            delReq.startTime,
                            delReq.endTime
                        );
                    }
                );
                
                if (deleted) {
                    totalSeriesDeleted++;
                    allDeletedSeries.push_back(fullSeriesKey);
                }
            } else if (!delReq.isStructured) {
                // Series key delete - determine shard
                std::hash<std::string> hasher;
                size_t hash = hasher(delReq.seriesKey);
                unsigned targetShard = hash % seastar::smp::count;
                
                // Execute delete on the target shard
                bool deleted = co_await engineSharded->invoke_on(targetShard,
                    [delReq](Engine& engine) {
                        return engine.deleteRange(
                            delReq.seriesKey,
                            delReq.startTime,
                            delReq.endTime
                        );
                    }
                );
                
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
        response.totalRequests = static_cast<int>(deleteRequests.size());
        
        // Include deleted series list if not too large
        if (!allDeletedSeries.empty() && allDeletedSeries.size() <= 100) {
            response.deletedSeries = allDeletedSeries;
        } else if (allDeletedSeries.size() > 100) {
            response.deletedSeriesCount = static_cast<int>(allDeletedSeries.size());
            response.note = "Series list omitted due to size";
        }
        
        reply->_content = glz::write_json(response).value_or("{}");
        
    } catch (const std::exception& e) {
        tsdb::http_log.error("Delete handler error: {}", e.what());
        reply->set_status(seastar::http::reply::status_type::internal_server_error);
        reply->_content = createErrorResponse(std::string("Internal error: ") + e.what());
    }
    
    co_return reply;
}

void HttpDeleteHandler::registerRoutes(seastar::httpd::routes& r) {
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::httpd::request> req, std::unique_ptr<seastar::httpd::reply> rep) -> seastar::future<std::unique_ptr<seastar::httpd::reply>> {
            // We don't use the provided reply, create our own
            return handleDelete(std::move(req));
        },
        "json"
    );
    
    r.add(seastar::httpd::operation_type::POST, 
          seastar::httpd::url("/delete"), 
          handler);
    
    tsdb::http_log.info("Registered DELETE endpoint at /delete");
}