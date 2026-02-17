#include "http_metadata_handler.hpp"
#include <glaze/glaze.hpp>
#include <seastar/core/smp.hh>
#include <unordered_map>
#include <algorithm>
#include <optional>

// Response struct definitions for Glaze serialization
struct ErrorInfo {
    std::string code;
    std::string message;
};

struct ErrorResponse {
    std::string status = "error";
    ErrorInfo error;
};

struct MeasurementsResponse {
    std::vector<std::string> measurements;
    size_t total;
};

struct TagValuesResponse {
    std::string measurement;
    std::string tag;
    std::vector<std::string> values;
};

struct TagsResponse {
    std::string measurement;
    std::unordered_map<std::string, std::vector<std::string>> tags;
};

struct FieldInfo {
    std::string name;
    std::string type;
};

struct FieldsResponse {
    std::string measurement;
    std::unordered_map<std::string, FieldInfo> fields;
    std::optional<std::unordered_map<std::string, std::string>> filtered_by;
};

HttpMetadataHandler::HttpMetadataHandler(seastar::sharded<Engine>* _engineSharded)
    : engineSharded(_engineSharded) {
    tsdb::http_log.info("HttpMetadataHandler initialized");
}

void HttpMetadataHandler::registerRoutes(seastar::httpd::routes& r) {
    // /measurements endpoint
    auto measurementsHandler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) 
            -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            return handleMeasurements(std::move(req));
        }, "json"
    );
    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/measurements"), measurementsHandler);
    
    // /tags endpoint
    auto tagsHandler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) 
            -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            return handleTags(std::move(req));
        }, "json"
    );
    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/tags"), tagsHandler);
    
    // /fields endpoint
    auto fieldsHandler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) 
            -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            return handleFields(std::move(req));
        }, "json"
    );
    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/fields"), fieldsHandler);
    
    tsdb::http_log.info("Registered metadata endpoints: /measurements, /tags, /fields");
}

seastar::future<std::unique_ptr<seastar::http::reply>> 
HttpMetadataHandler::handleMeasurements(std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    
    try {
        tsdb::http_log.debug("Processing /measurements request");
        
        // Metadata is centralized on shard 0, so query only shard 0
        auto measurements = co_await engineSharded->invoke_on(0, [](Engine& engine) -> seastar::future<std::vector<std::string>> {
            co_return co_await engine.getAllMeasurements();
        });
        
        // Apply filters if specified
        std::string prefix = req->get_query_param("prefix");
        if (!prefix.empty()) {
            auto it = std::remove_if(measurements.begin(), measurements.end(), 
                [&prefix](const std::string& m) {
                    return !m.starts_with(prefix);
                });
            measurements.erase(it, measurements.end());
        }
        
        // Apply pagination
        size_t offset = 0;
        size_t limit = std::numeric_limits<size_t>::max();
        
        std::string offsetParam = req->get_query_param("offset");
        if (!offsetParam.empty()) {
            offset = std::stoul(offsetParam);
        }
        
        std::string limitParam = req->get_query_param("limit");
        if (!limitParam.empty()) {
            limit = std::stoul(limitParam);
        }
        
        size_t totalCount = measurements.size();
        
        if (offset < measurements.size()) {
            auto startIt = measurements.begin() + offset;
            auto endIt = measurements.begin() + std::min(offset + limit, measurements.size());
            measurements = std::vector<std::string>(startIt, endIt);
        } else {
            measurements.clear();
        }
        
        // Format response
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = formatMeasurementsResponse(measurements, totalCount);
        rep->add_header("Content-Type", "application/json");
        
        tsdb::http_log.debug("Returning {} measurements", measurements.size());
        
    } catch (const std::exception& e) {
        tsdb::http_log.error("Error processing /measurements: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> 
HttpMetadataHandler::handleTags(std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    
    try {
        std::string measurement = req->get_query_param("measurement");
        if (measurement.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("MISSING_PARAMETER", "measurement parameter is required");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        std::string specificTag = req->get_query_param("tag");
        
        tsdb::http_log.debug("Processing /tags request for measurement: {}, tag: {}", 
                           measurement, specificTag.empty() ? "all" : specificTag);
        
        // Metadata is centralized on shard 0, so query only shard 0
        std::unordered_map<std::string, std::set<std::string>> allTagsResults = co_await engineSharded->invoke_on(0, [measurement, specificTag](Engine& engine) -> seastar::future<std::unordered_map<std::string, std::set<std::string>>> {
            std::unordered_map<std::string, std::set<std::string>> tagsResults;
            
            if (specificTag.empty()) {
                // Get all tags for measurement
                auto tagKeys = co_await engine.getMeasurementTags(measurement);
                for (const auto& tagKey : tagKeys) {
                    auto tagValues = co_await engine.getTagValues(measurement, tagKey);
                    tagsResults[tagKey] = tagValues;
                }
            } else {
                // Get values for specific tag
                auto tagValues = co_await engine.getTagValues(measurement, specificTag);
                tagsResults[specificTag] = tagValues;
            }
            
            co_return tagsResults;
        });
        
        // Convert sets to vectors for response formatting
        std::unordered_map<std::string, std::vector<std::string>> tagsResult;
        for (const auto& [tagKey, tagValues] : allTagsResults) {
            tagsResult[tagKey] = std::vector<std::string>(tagValues.begin(), tagValues.end());
            std::sort(tagsResult[tagKey].begin(), tagsResult[tagKey].end());
        }
        
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = formatTagsResponse(measurement, tagsResult, specificTag);
        rep->add_header("Content-Type", "application/json");
        
        tsdb::http_log.debug("Returning tags for measurement: {}", measurement);
        
    } catch (const std::exception& e) {
        tsdb::http_log.error("Error processing /tags: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> 
HttpMetadataHandler::handleFields(std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    
    try {
        std::string measurement = req->get_query_param("measurement");
        if (measurement.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("MISSING_PARAMETER", "measurement parameter is required");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        tsdb::http_log.debug("Processing /fields request for measurement: {}", measurement);
        
        // Parse optional tags parameter for filtering
        std::string tagsParam = req->get_query_param("tags");
        std::unordered_map<std::string, std::string> tagFilters;
        if (!tagsParam.empty()) {
            // Parse tags in format "key1:value1,key2:value2"
            size_t pos = 0;
            while (pos < tagsParam.length()) {
                size_t colonPos = tagsParam.find(':', pos);
                if (colonPos == std::string::npos) break;
                
                size_t commaPos = tagsParam.find(',', colonPos);
                if (commaPos == std::string::npos) commaPos = tagsParam.length();
                
                std::string key = tagsParam.substr(pos, colonPos - pos);
                std::string value = tagsParam.substr(colonPos + 1, commaPos - colonPos - 1);
                tagFilters[key] = value;
                
                pos = commaPos + 1;
            }
        }
        
        // Metadata is centralized on shard 0, so query only shard 0
        auto allFields = co_await engineSharded->invoke_on(0, [measurement](Engine& engine) -> seastar::future<std::set<std::string>> {
            co_return co_await engine.getMeasurementFields(measurement);
        });
        
        // Look up actual field types from the LevelDB index on shard 0
        std::unordered_map<std::string, std::string> fieldsWithTypes;
        for (const auto& field : allFields) {
            auto fieldType = co_await engineSharded->invoke_on(0, [measurement, field](Engine& engine) -> seastar::future<std::string> {
                co_return co_await engine.getIndex().getFieldType(measurement, field);
            });
            // Default to "float" if type was never recorded (e.g., legacy data)
            fieldsWithTypes[field] = fieldType.empty() ? "float" : fieldType;
        }
        
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = formatFieldsResponse(measurement, fieldsWithTypes, tagFilters);
        rep->add_header("Content-Type", "application/json");
        
        tsdb::http_log.debug("Returning {} fields for measurement: {}", allFields.size(), measurement);
        
    } catch (const std::exception& e) {
        tsdb::http_log.error("Error processing /fields: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

std::string HttpMetadataHandler::createErrorResponse(const std::string& code, const std::string& message) {
    ErrorResponse response;
    response.error.code = code;
    response.error.message = message;
    
    std::string buffer;
    (void)glz::write_json(response, buffer);
    return buffer;
}

std::string HttpMetadataHandler::formatMeasurementsResponse(const std::vector<std::string>& measurements, size_t total) {
    MeasurementsResponse response;
    response.measurements = measurements;
    response.total = total;
    
    std::string buffer;
    (void)glz::write_json(response, buffer);
    return buffer;
}

std::string HttpMetadataHandler::formatTagsResponse(const std::string& measurement, 
                                                   const std::unordered_map<std::string, std::vector<std::string>>& tags,
                                                   const std::string& specificTag) {
    if (!specificTag.empty()) {
        // Return specific tag values
        TagValuesResponse response;
        response.measurement = measurement;
        response.tag = specificTag;
        if (tags.find(specificTag) != tags.end()) {
            response.values = tags.at(specificTag);
        }
        
        std::string buffer;
        (void)glz::write_json(response, buffer);
        return buffer;
    } else {
        // Return all tags
        TagsResponse response;
        response.measurement = measurement;
        response.tags = tags;
        
        std::string buffer;
        (void)glz::write_json(response, buffer);
        return buffer;
    }
}

std::string HttpMetadataHandler::formatFieldsResponse(const std::string& measurement,
                                                     const std::unordered_map<std::string, std::string>& fields,
                                                     const std::unordered_map<std::string, std::string>& tagFilters) {
    FieldsResponse response;
    response.measurement = measurement;
    
    for (const auto& [fieldName, fieldType] : fields) {
        response.fields[fieldName].name = fieldName;
        response.fields[fieldName].type = fieldType;
    }
    
    if (!tagFilters.empty()) {
        response.filtered_by = tagFilters;
    }
    
    std::string buffer;
    (void)glz::write_json(response, buffer);
    return buffer;
}