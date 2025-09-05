#include "http_delete_handler.hpp"
#include "logger.hpp"
#include <chrono>
#include <seastar/core/smp.hh>

using namespace seastar;
using namespace httpd;

HttpDeleteHandler::DeleteRequest HttpDeleteHandler::parseDeleteRequest(const rapidjson::Value& doc) {
    DeleteRequest req;
    req.isPattern = false;
    
    // Check for series key format
    if (doc.HasMember("series") && doc["series"].IsString()) {
        req.seriesKey = doc["series"].GetString();
        req.isStructured = false;
    }
    // Check for structured/pattern format
    else if (doc.HasMember("measurement") && doc["measurement"].IsString()) {
        req.measurement = doc["measurement"].GetString();
        req.isStructured = true;
        
        // Parse tags if present
        if (doc.HasMember("tags") && doc["tags"].IsObject()) {
            for (auto& tag : doc["tags"].GetObject()) {
                if (tag.value.IsString()) {
                    req.tags[tag.name.GetString()] = tag.value.GetString();
                }
            }
        }
        
        // Check for single field (backward compatibility)
        if (doc.HasMember("field") && doc["field"].IsString()) {
            req.field = doc["field"].GetString();
            req.fields.push_back(req.field);
        }
        // Check for multiple fields (pattern-based)
        else if (doc.HasMember("fields") && doc["fields"].IsArray()) {
            req.isPattern = true;
            for (auto& field : doc["fields"].GetArray()) {
                if (field.IsString()) {
                    req.fields.push_back(field.GetString());
                }
            }
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
    if (!doc.HasMember("startTime") || !doc["startTime"].IsUint64()) {
        throw std::runtime_error("startTime is required and must be uint64");
    }
    req.startTime = doc["startTime"].GetUint64();
    
    if (!doc.HasMember("endTime") || !doc["endTime"].IsUint64()) {
        throw std::runtime_error("endTime is required and must be uint64");
    }
    req.endTime = doc["endTime"].GetUint64();
    
    // Validate time range
    if (req.startTime > req.endTime) {
        throw std::runtime_error("startTime must be less than or equal to endTime");
    }
    
    return req;
}

std::string HttpDeleteHandler::createErrorResponse(const std::string& error) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    
    writer.StartObject();
    writer.Key("status");
    writer.String("error");
    writer.Key("error");
    writer.String(error.c_str());
    writer.EndObject();
    
    return buffer.GetString();
}

std::string HttpDeleteHandler::createSuccessResponse(int deletedCount, int totalRequests) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    
    writer.StartObject();
    writer.Key("status");
    writer.String("success");
    writer.Key("deleted");
    writer.Int(deletedCount);
    writer.Key("total");
    writer.Int(totalRequests);
    writer.EndObject();
    
    return buffer.GetString();
}

seastar::future<std::unique_ptr<seastar::http::reply>>
HttpDeleteHandler::handleDelete(std::unique_ptr<seastar::http::request> req) {
    auto reply = std::make_unique<seastar::http::reply>();
    reply->add_header("Content-Type", "application/json");
    
    try {
        // Parse JSON body
        rapidjson::Document doc;
        doc.Parse(req->content.c_str());
        
        if (doc.HasParseError()) {
            reply->set_status(seastar::http::reply::status_type::bad_request);
            reply->_content = createErrorResponse("Invalid JSON");
            co_return reply;
        }
        
        std::vector<DeleteRequest> deleteRequests;
        
        // Check for batch deletes
        if (doc.HasMember("deletes") && doc["deletes"].IsArray()) {
            for (auto& deleteDoc : doc["deletes"].GetArray()) {
                try {
                    deleteRequests.push_back(parseDeleteRequest(deleteDoc));
                } catch (const std::exception& e) {
                    reply->set_status(seastar::http::reply::status_type::bad_request);
                    reply->_content = createErrorResponse(std::string("Invalid delete request: ") + e.what());
                    co_return reply;
                }
            }
        } else {
            // Single delete request
            try {
                deleteRequests.push_back(parseDeleteRequest(doc));
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
        
        // Create detailed response
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        
        writer.StartObject();
        writer.Key("status");
        writer.String("success");
        writer.Key("seriesDeleted");
        writer.Int(totalSeriesDeleted);
        writer.Key("totalRequests");
        writer.Int(deleteRequests.size());
        
        // Include deleted series list if not too large
        if (!allDeletedSeries.empty() && allDeletedSeries.size() <= 100) {
            writer.Key("deletedSeries");
            writer.StartArray();
            for (const auto& series : allDeletedSeries) {
                writer.String(series.c_str());
            }
            writer.EndArray();
        } else if (allDeletedSeries.size() > 100) {
            writer.Key("deletedSeriesCount");
            writer.Int(allDeletedSeries.size());
            writer.Key("note");
            writer.String("Series list omitted due to size");
        }
        
        writer.EndObject();
        reply->_content = buffer.GetString();
        
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