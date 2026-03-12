#include "http_metadata_handler.hpp"
#include <glaze/glaze.hpp>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <unordered_map>
#include <algorithm>
#include <optional>
#include <stdexcept>

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

// static
size_t HttpMetadataHandler::parsePaginationParam(const std::string& str,
                                                  const std::string& paramName,
                                                  size_t defaultValue) {
    if (str.empty()) {
        return defaultValue;
    }
    // Reject strings that start with '-' since stoul would throw std::invalid_argument,
    // but we want a clear bad-request message.
    if (str[0] == '-') {
        throw BadRequestException("Invalid " + paramName + " parameter: must be a non-negative integer");
    }
    std::size_t pos = 0;
    unsigned long result = 0;
    try {
        result = std::stoul(str, &pos);
    } catch (const std::invalid_argument&) {
        throw BadRequestException("Invalid " + paramName + " parameter: not a valid integer");
    } catch (const std::out_of_range&) {
        throw BadRequestException("Invalid " + paramName + " parameter: value out of range");
    }
    // Reject trailing non-numeric characters (e.g. "3.14", "10abc")
    if (pos != str.size()) {
        throw BadRequestException("Invalid " + paramName + " parameter: not a valid integer");
    }
    return static_cast<size_t>(result);
}

// static
std::string HttpMetadataHandler::validateQueryParam(const std::string& name,
                                                     const std::string& context) {
    if (name.empty()) {
        return context + " must not be empty";
    }
    for (char c : name) {
        if (c == '\0') return context + " must not contain null bytes";
        if (static_cast<unsigned char>(c) < 32) return context + " must not contain control characters";
    }
    return ""; // Valid
}

HttpMetadataHandler::HttpMetadataHandler(seastar::sharded<Engine>* _engineSharded)
    : engineSharded(_engineSharded) {
    timestar::http_log.info("HttpMetadataHandler initialized");
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
    
    timestar::http_log.info("Registered metadata endpoints: /measurements, /tags, /fields");
}

seastar::future<std::unique_ptr<seastar::http::reply>> 
HttpMetadataHandler::handleMeasurements(std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    
    try {
        timestar::http_log.debug("Processing /measurements request");
        
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
        
        // Apply pagination - validate parameters and return 400 on invalid input
        size_t offset = 0;
        size_t limit = std::numeric_limits<size_t>::max();

        try {
            offset = parsePaginationParam(req->get_query_param("offset"), "offset", 0);
            limit  = parsePaginationParam(req->get_query_param("limit"),  "limit",  std::numeric_limits<size_t>::max());
        } catch (const BadRequestException& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("INVALID_PARAMETER", e.what());
            rep->add_header("Content-Type", "application/json");
            co_return rep;
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
        
        timestar::http_log.debug("Returning {} measurements", measurements.size());
        
    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /measurements: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", "Internal server error");
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

        {
            auto err = validateQueryParam(measurement, "Measurement name");
            if (!err.empty()) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content = createErrorResponse("INVALID_PARAMETER", err);
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }
        }

        std::string specificTag = req->get_query_param("tag");
        if (!specificTag.empty()) {
            auto err = validateQueryParam(specificTag, "Tag name");
            if (!err.empty()) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content = createErrorResponse("INVALID_PARAMETER", err);
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }
        }

        timestar::http_log.debug("Processing /tags request for measurement: {}, tag: {}",
                           measurement, specificTag.empty() ? "all" : specificTag);
        
        // Metadata is centralized on shard 0, so query only shard 0
        std::unordered_map<std::string, std::set<std::string>> allTagsResults = co_await engineSharded->invoke_on(0, [measurement, specificTag](Engine& engine) -> seastar::future<std::unordered_map<std::string, std::set<std::string>>> {
            std::unordered_map<std::string, std::set<std::string>> tagsResults;

            if (specificTag.empty()) {
                // Get all tags for measurement, then fetch values in parallel
                auto tagKeys = co_await engine.getMeasurementTags(measurement);
                std::vector<seastar::future<std::set<std::string>>> futures;
                futures.reserve(tagKeys.size());
                std::vector<std::string> keyVec(tagKeys.begin(), tagKeys.end());
                for (const auto& tagKey : keyVec) {
                    futures.push_back(engine.getTagValues(measurement, tagKey));
                }
                auto allValues = co_await seastar::when_all_succeed(futures.begin(), futures.end());
                for (size_t i = 0; i < keyVec.size(); ++i) {
                    tagsResults[keyVec[i]] = std::move(allValues[i]);
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
        
        timestar::http_log.debug("Returning tags for measurement: {}", measurement);
        
    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /tags: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", "Internal server error");
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

        {
            auto err = validateQueryParam(measurement, "Measurement name");
            if (!err.empty()) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content = createErrorResponse("INVALID_PARAMETER", err);
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }
        }

        timestar::http_log.debug("Processing /fields request for measurement: {}", measurement);
        
        // Parse optional tags parameter for filtering
        std::string tagsParam = req->get_query_param("tags");
        std::unordered_map<std::string, std::string> tagFilters;
        if (!tagsParam.empty()) {
            // Parse tags in format "key1:value1,key2:value2"
            // Use only the first colon as delimiter so values can contain colons
            // (e.g., "host:port:8080" -> key="host", value="port:8080")
            size_t pos = 0;
            while (pos < tagsParam.length()) {
                size_t colonPos = tagsParam.find(':', pos);
                if (colonPos == std::string::npos) break;

                std::string key = tagsParam.substr(pos, colonPos - pos);

                // Find the next comma after the key:value pair.
                // We need to find the next comma that is NOT part of the value.
                // Since we split on the first colon, the value runs until the next
                // top-level comma or end of string.
                size_t commaPos = tagsParam.find(',', colonPos + 1);
                if (commaPos == std::string::npos) commaPos = tagsParam.length();

                std::string value = tagsParam.substr(colonPos + 1, commaPos - colonPos - 1);
                tagFilters[key] = value;

                pos = commaPos + 1;
            }
        }
        
        // Metadata is centralized on shard 0, so query only shard 0
        auto allFields = co_await engineSharded->invoke_on(0, [measurement](Engine& engine) -> seastar::future<std::set<std::string>> {
            co_return co_await engine.getMeasurementFields(measurement);
        });
        
        // Look up all field types in a single RPC to shard 0
        auto fieldsWithTypes = co_await engineSharded->invoke_on(0,
            [measurement, fields = std::vector<std::string>(allFields.begin(), allFields.end())]
            (Engine& engine) -> seastar::future<std::unordered_map<std::string, std::string>> {
                std::unordered_map<std::string, std::string> result;
                for (const auto& field : fields) {
                    auto fieldType = co_await engine.getIndex().getFieldType(measurement, field);
                    result[field] = fieldType.empty() ? "float" : fieldType;
                }
                co_return result;
            });
        
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = formatFieldsResponse(measurement, fieldsWithTypes, tagFilters);
        rep->add_header("Content-Type", "application/json");
        
        timestar::http_log.debug("Returning {} fields for measurement: {}", allFields.size(), measurement);
        
    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /fields: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", "Internal server error");
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