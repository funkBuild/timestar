#include "http_query_handler.hpp"
#include "engine.hpp"
#include "query_parser.hpp"
#include "query_planner.hpp"
#include "aggregator.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <algorithm>
#include <numeric>
#include <seastar/core/when_all.hh>

namespace tsdb {

seastar::future<std::unique_ptr<seastar::httpd::reply>>
HttpQueryHandler::handleQuery(std::unique_ptr<seastar::httpd::request> req) {
    auto rep = std::make_unique<seastar::httpd::reply>();
    
    try {
        // Parse JSON request body
        rapidjson::Document doc;
        doc.Parse(req->content.c_str());
        
        if (doc.HasParseError()) {
            rep->set_status(seastar::httpd::reply::status_type::bad_request);
            rep->_content = createErrorResponse("INVALID_JSON", "Failed to parse JSON request");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        // Parse query request
        QueryRequest queryRequest;
        try {
            queryRequest = parseQueryRequest(doc);
        } catch (const QueryParseException& e) {
            rep->set_status(seastar::httpd::reply::status_type::bad_request);
            rep->_content = createErrorResponse("INVALID_QUERY", e.what());
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        // Execute query
        auto startTime = std::chrono::high_resolution_clock::now();
        QueryResponse response = co_await executeQuery(queryRequest);
        auto endTime = std::chrono::high_resolution_clock::now();
        
        // Calculate execution time
        response.statistics.executionTimeMs = 
            std::chrono::duration<double, std::milli>(endTime - startTime).count();
        
        // Format response
        if (response.success) {
            rep->set_status(seastar::httpd::reply::status_type::ok);
            rep->_content = formatQueryResponse(response);
        } else {
            rep->set_status(seastar::httpd::reply::status_type::internal_server_error);
            rep->_content = createErrorResponse(response.errorCode, response.errorMessage);
        }
        
        rep->add_header("Content-Type", "application/json");
        
    } catch (const std::exception& e) {
        std::cerr << "Error handling query request: " << e.what() << std::endl;
        rep->set_status(seastar::httpd::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

void HttpQueryHandler::registerRoutes(seastar::httpd::routes& r) {
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::httpd::request> req, 
               std::unique_ptr<seastar::httpd::reply> rep) 
            -> seastar::future<std::unique_ptr<seastar::httpd::reply>> {
            return handleQuery(std::move(req));
        }, "json");
    
    r.add(seastar::httpd::operation_type::POST, 
          seastar::httpd::url("/query"), handler);
    
    tsdb::http_log.info("Registered HTTP query endpoint at /query");
}

QueryRequest HttpQueryHandler::parseQueryRequest(const rapidjson::Document& doc) {
    // Validate required fields
    if (!doc.HasMember("query") || !doc["query"].IsString()) {
        throw QueryParseException("Missing or invalid 'query' field");
    }
    
    // Handle both numeric and string timestamps for backward compatibility
    uint64_t startTime, endTime;
    
    // Parse startTime - can be number or string
    if (!doc.HasMember("startTime")) {
        throw QueryParseException("Missing 'startTime' field");
    }
    
    if (doc["startTime"].IsNumber()) {
        // Direct numeric timestamp (assumed to be nanoseconds)
        // Support both integer and floating point numbers
        if (doc["startTime"].IsUint64()) {
            startTime = doc["startTime"].GetUint64();
        } else if (doc["startTime"].IsInt64()) {
            startTime = static_cast<uint64_t>(doc["startTime"].GetInt64());
        } else if (doc["startTime"].IsDouble()) {
            startTime = static_cast<uint64_t>(doc["startTime"].GetDouble());
        } else {
            startTime = static_cast<uint64_t>(doc["startTime"].GetInt());
        }
    } else if (doc["startTime"].IsString()) {
        // Try to parse as numeric string first
        std::string timeStr = doc["startTime"].GetString();
        try {
            startTime = std::stoull(timeStr);
        } catch (...) {
            // Fall back to date string parsing (legacy support)
            startTime = QueryParser::parseTime(timeStr);
        }
    } else {
        throw QueryParseException("Invalid 'startTime' field type");
    }
    
    // Parse endTime - can be number or string
    if (!doc.HasMember("endTime")) {
        throw QueryParseException("Missing 'endTime' field");
    }
    
    if (doc["endTime"].IsNumber()) {
        // Direct numeric timestamp (assumed to be nanoseconds)
        if (doc["endTime"].IsUint64()) {
            endTime = doc["endTime"].GetUint64();
        } else if (doc["endTime"].IsInt64()) {
            endTime = static_cast<uint64_t>(doc["endTime"].GetInt64());
        } else if (doc["endTime"].IsDouble()) {
            endTime = static_cast<uint64_t>(doc["endTime"].GetDouble());
        } else {
            endTime = static_cast<uint64_t>(doc["endTime"].GetInt());
        }
    } else if (doc["endTime"].IsString()) {
        // Try to parse as numeric string first
        std::string timeStr = doc["endTime"].GetString();
        try {
            endTime = std::stoull(timeStr);
        } catch (...) {
            // Fall back to date string parsing (legacy support)
            endTime = QueryParser::parseTime(timeStr);
        }
    } else {
        throw QueryParseException("Invalid 'endTime' field type");
    }
    
    // Parse query string and create request with timestamps
    QueryRequest request = QueryParser::parseQueryString(doc["query"].GetString());
    request.startTime = startTime;
    request.endTime = endTime;
    
    // Parse optional aggregation interval 
    // Support both: numeric values (nanoseconds) and string intervals (e.g., "5m", "1h", "30s")
    if (doc.HasMember("aggregationInterval")) {
        if (doc["aggregationInterval"].IsNumber()) {
            // Direct numeric value in nanoseconds
            if (doc["aggregationInterval"].IsUint64()) {
                request.aggregationInterval = doc["aggregationInterval"].GetUint64();
            } else if (doc["aggregationInterval"].IsInt64()) {
                request.aggregationInterval = static_cast<uint64_t>(doc["aggregationInterval"].GetInt64());
            } else if (doc["aggregationInterval"].IsDouble()) {
                request.aggregationInterval = static_cast<uint64_t>(doc["aggregationInterval"].GetDouble());
            } else {
                request.aggregationInterval = static_cast<uint64_t>(doc["aggregationInterval"].GetInt());
            }
        } else if (doc["aggregationInterval"].IsString()) {
            // String interval like "5m", "1h", etc.
            request.aggregationInterval = parseInterval(doc["aggregationInterval"].GetString());
        } else {
            throw QueryParseException("Invalid 'aggregationInterval' field type");
        }
    } else if (doc.HasMember("interval") && doc["interval"].IsString()) {
        // Backward compatibility: support "interval" field
        request.aggregationInterval = parseInterval(doc["interval"].GetString());
    }
    
    return request;
}

// Helper to parse interval strings like "5m", "1h", "30s", "100ms", "500us", "1000ns"
uint64_t HttpQueryHandler::parseInterval(const std::string& interval) {
    if (interval.empty()) {
        throw QueryParseException("Empty interval string");
    }
    
    // Extract number and unit
    size_t pos = 0;
    while (pos < interval.size() && (std::isdigit(interval[pos]) || interval[pos] == '.')) {
        pos++;
    }
    
    if (pos == 0 || pos == interval.size()) {
        throw QueryParseException("Invalid interval format: " + interval);
    }
    
    double value = std::stod(interval.substr(0, pos));
    std::string unit = interval.substr(pos);
    
    // Convert to nanoseconds
    if (unit == "ns") {
        return static_cast<uint64_t>(value);  // nanoseconds
    } else if (unit == "us" || unit == "µs") {
        return static_cast<uint64_t>(value * 1000ULL);  // microseconds
    } else if (unit == "ms") {
        return static_cast<uint64_t>(value * 1000000ULL);  // milliseconds
    } else if (unit == "s") {
        return static_cast<uint64_t>(value * 1000000000ULL);  // seconds
    } else if (unit == "m") {
        return static_cast<uint64_t>(value * 60 * 1000000000ULL);  // minutes
    } else if (unit == "h") {
        return static_cast<uint64_t>(value * 3600 * 1000000000ULL);  // hours
    } else if (unit == "d") {
        return static_cast<uint64_t>(value * 86400 * 1000000000ULL);  // days
    } else {
        throw QueryParseException("Unknown interval unit: " + unit + 
                                 ". Supported units: ns, us, ms, s, m, h, d");
    }
}

std::string HttpQueryHandler::formatQueryResponse(const QueryResponse& response) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("status", "success", allocator);
    
    // Add series array
    rapidjson::Value seriesArray(rapidjson::kArrayType);
    for (const auto& series : response.series) {
        rapidjson::Value seriesObj(rapidjson::kObjectType);
        
        // Add measurement
        rapidjson::Value measurement;
        measurement.SetString(series.measurement.c_str(), allocator);
        seriesObj.AddMember("measurement", measurement, allocator);
        
        // Add tags
        rapidjson::Value tagsObj(rapidjson::kObjectType);
        for (const auto& [key, value] : series.tags) {
            rapidjson::Value k, v;
            k.SetString(key.c_str(), allocator);
            v.SetString(value.c_str(), allocator);
            tagsObj.AddMember(k, v, allocator);
        }
        seriesObj.AddMember("tags", tagsObj, allocator);
        
        // Add fields
        rapidjson::Value fieldsObj(rapidjson::kObjectType);
        for (const auto& [fieldName, data] : series.fields) {
            rapidjson::Value fieldObj(rapidjson::kObjectType);
            
            // Add timestamps array
            rapidjson::Value timestampsArray(rapidjson::kArrayType);
            for (uint64_t ts : data.first) {
                timestampsArray.PushBack(rapidjson::Value(ts), allocator);
            }
            fieldObj.AddMember("timestamps", timestampsArray, allocator);
            
            // Add values array - handle different types
            rapidjson::Value valuesArray(rapidjson::kArrayType);
            std::visit([&valuesArray, &allocator](auto&& values) {
                using T = std::decay_t<decltype(values)>;
                if constexpr (std::is_same_v<T, std::vector<double>>) {
                    for (double val : values) {
                        valuesArray.PushBack(rapidjson::Value(val), allocator);
                    }
                } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
                    for (bool val : values) {
                        valuesArray.PushBack(rapidjson::Value(val), allocator);
                    }
                } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                    for (const auto& val : values) {
                        rapidjson::Value strVal;
                        strVal.SetString(val.c_str(), allocator);
                        valuesArray.PushBack(strVal, allocator);
                    }
                }
            }, data.second);
            fieldObj.AddMember("values", valuesArray, allocator);
            
            rapidjson::Value fieldKey;
            fieldKey.SetString(fieldName.c_str(), allocator);
            fieldsObj.AddMember(fieldKey, fieldObj, allocator);
        }
        seriesObj.AddMember("fields", fieldsObj, allocator);
        
        seriesArray.PushBack(seriesObj, allocator);
    }
    doc.AddMember("series", seriesArray, allocator);
    
    // Add scopes if they exist
    if (!response.scopes.empty()) {
        rapidjson::Value scopesArray(rapidjson::kArrayType);
        
        for (const auto& [name, value] : response.scopes) {
            rapidjson::Value scopeObj(rapidjson::kObjectType);
            rapidjson::Value nameVal, valueVal;
            nameVal.SetString(name.c_str(), allocator);
            valueVal.SetString(value.c_str(), allocator);
            scopeObj.AddMember("name", nameVal, allocator);
            scopeObj.AddMember("value", valueVal, allocator);
            scopesArray.PushBack(scopeObj, allocator);
        }
        
        doc.AddMember("scopes", scopesArray, allocator);
    }
    
    // Add statistics
    rapidjson::Value statsObj(rapidjson::kObjectType);
    statsObj.AddMember("series_count", static_cast<uint64_t>(response.statistics.seriesCount), allocator);
    statsObj.AddMember("point_count", static_cast<uint64_t>(response.statistics.pointCount), allocator);
    statsObj.AddMember("execution_time_ms", response.statistics.executionTimeMs, allocator);
    doc.AddMember("statistics", statsObj, allocator);
    
    // Convert to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
}

std::string HttpQueryHandler::createErrorResponse(const std::string& code, const std::string& message) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("status", "error", allocator);
    
    // Put message directly in "error" field for backward compatibility with tests
    rapidjson::Value msgVal;
    msgVal.SetString(message.c_str(), allocator);
    doc.AddMember("error", msgVal, allocator);
    
    // Keep detailed error info in separate "details" field
    rapidjson::Value errorDetails(rapidjson::kObjectType);
    rapidjson::Value codeVal, msgVal2;
    codeVal.SetString(code.c_str(), allocator);
    msgVal2.SetString(message.c_str(), allocator);
    errorDetails.AddMember("code", codeVal, allocator);
    errorDetails.AddMember("message", msgVal2, allocator);
    doc.AddMember("details", errorDetails, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
}

seastar::future<QueryResponse> HttpQueryHandler::executeQuery(const QueryRequest& request) {
    QueryResponse response;
    
    try {
        // For now, directly query without the planner
        // In the future, we can create a sharded index service properly
        
        // Query all shards directly
        auto results = co_await queryAllShards(request);
        
        // Apply group-by first if specified, then aggregation
        if (request.hasGroupBy()) {
            results = applyGroupBy(results, request);
        }
        
        // Always apply aggregation (will handle cross-series if no group-by)
        applyAggregation(results, request);
        
        response.series = results;
        response.success = true;
        response.statistics.seriesCount = results.size();
        response.scopes = request.scopes;  // Copy scopes from request for response formatting
        
        // Calculate total points
        for (const auto& series : results) {
            for (const auto& [field, data] : series.fields) {
                response.statistics.pointCount += data.first.size();
            }
        }
        
        // OLD CODE - keeping for reference:
        // Use query planner to create execution plan if index is available
        if (false && indexSharded && planner) {
            // Create query plan
            QueryPlan plan = co_await planner->createPlan(request, indexSharded);
            
            response.statistics.seriesCount = plan.estimatedSeriesCount;
            
            // If we have shard queries, execute them
            if (!plan.shardQueries.empty()) {
                // Execute queries across shards
                auto results = co_await queryAllShards(request);
                
                // Apply group-by first if specified, then aggregation
                if (request.hasGroupBy()) {
                    results = applyGroupBy(results, request);
                }
                
                // Always apply aggregation (will handle cross-series if no group-by)
                applyAggregation(results, request);
                
                response.series = results;
                response.success = true;
                
                // Calculate total points
                for (const auto& series : results) {
                    for (const auto& [field, data] : series.fields) {
                        response.statistics.pointCount += data.first.size();
                    }
                }
            } else {
                // No matching series found
                response.success = true;
                response.series.clear();
                response.statistics.seriesCount = 0;
                response.statistics.pointCount = 0;
            }
        } else if (false) {
            // Fallback to mock response when no index available
            response.success = true;
            
            SeriesResult series;
            series.measurement = request.measurement;
            series.tags = request.scopes;
            
            // Add mock data points
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            
            uint64_t timeStep = (request.endTime - request.startTime) / 10;
            for (int i = 0; i < 10; i++) {
                timestamps.push_back(request.startTime + i * timeStep);
                values.push_back(20.0 + i * 0.5);
            }
            
            if (request.requestsAllFields()) {
                series.fields["value"] = std::make_pair(timestamps, values);
            } else {
                for (const auto& field : request.fields) {
                    series.fields[field] = std::make_pair(timestamps, values);
                }
            }
            
            response.series.push_back(series);
            response.statistics.seriesCount = 1;
            response.statistics.pointCount = timestamps.size();
        }
        
    } catch (const std::exception& e) {
        response.success = false;
        response.errorCode = "QUERY_EXECUTION_ERROR";
        response.errorMessage = e.what();
    }
    
    co_return response;
}

seastar::future<std::vector<SeriesResult>> 
HttpQueryHandler::queryAllShards(const QueryRequest& request) {
    // For now, query all shards directly without the planner
    // Each shard will use its local index to find matching series
    
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment
    
    std::vector<seastar::future<std::vector<SeriesResult>>> shardFutures;
    
    for (unsigned shardId = 0; shardId < shardCount; ++shardId) {
        auto fut = engineSharded->invoke_on(shardId, 
            [request](Engine& engine) -> seastar::future<std::vector<SeriesResult>> {
                std::vector<SeriesResult> results;
                
                // Get the local index for this shard
                auto& index = engine.getIndex();
                
                // Find all series matching the measurement and tags
                LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} finding series for measurement: '{}' with {} tag filters", 
                               seastar::this_shard_id(), request.measurement, request.scopes.size());
                auto seriesIds = co_await index.findSeries(request.measurement, request.scopes);
                LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} found {} series IDs for measurement '{}'", 
                               seastar::this_shard_id(), seriesIds.size(), request.measurement);
                
                // For each series, query the data
                for (uint64_t seriesId : seriesIds) {
                    // Get series metadata
                    auto metadataOpt = co_await index.getSeriesMetadata(seriesId);
                    if (!metadataOpt.has_value()) {
                        LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} - no metadata found for series ID {}", 
                                       seastar::this_shard_id(), seriesId);
                        continue;
                    }
                    
                    const auto& metadata = metadataOpt.value();
                    LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} - processing series {} (measurement: '{}', field: '{}')", 
                                   seastar::this_shard_id(), seriesId, metadata.measurement, metadata.field);
                    
                    // Check if this field is requested
                    if (!request.requestsAllFields()) {
                        if (std::find(request.fields.begin(), request.fields.end(), 
                                     metadata.field) == request.fields.end()) {
                            LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} - skipping series {} field '{}' not in requested fields", 
                                           seastar::this_shard_id(), seriesId, metadata.field);
                            continue;  // Skip this series, field not requested
                        }
                    }
                    
                    // Create a ShardQuery for this specific series
                    tsdb::ShardQuery shardQuery;
                    shardQuery.shardId = seastar::this_shard_id();
                    shardQuery.seriesIds = {seriesId};
                    shardQuery.startTime = request.startTime;
                    shardQuery.endTime = request.endTime;
                    
                    // Execute the query for this series
                    LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} executing query for series {} (time range: {} - {})", 
                                   seastar::this_shard_id(), seriesId, shardQuery.startTime, shardQuery.endTime);
                    auto seriesResults = co_await engine.executeLocalQuery(shardQuery);
                    LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} query for series {} returned {} results", 
                                   seastar::this_shard_id(), seriesId, seriesResults.size());
                    
                    // Add results
                    for (const auto& result : seriesResults) {
                        results.push_back(result);
                        // Log field data found
                        for (const auto& [field, fieldData] : result.fields) {
                            LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Shard {} - series {} field '{}' has {} data points", 
                                           seastar::this_shard_id(), seriesId, field, std::get<0>(fieldData).size());
                        }
                    }
                }
                
                co_return results;
            });
        shardFutures.push_back(std::move(fut));
    }
    
    // Wait for all shards to complete
    auto shardResults = co_await seastar::when_all_succeed(shardFutures.begin(), shardFutures.end());
    
    // Merge results from all shards
    std::vector<SeriesResult> mergedResults;
    for (const auto& shardResult : shardResults) {
        for (const auto& series : shardResult) {
            mergedResults.push_back(series);
        }
    }
    
    co_return mergedResults;
}

std::vector<unsigned> HttpQueryHandler::determineTargetShards(const QueryRequest& request) {
    std::vector<unsigned> shards;
    
    // If we have specific tags, we can calculate the exact shard
    // Otherwise, we need to query all shards
    if (!request.hasNoFilters()) {
        // TODO: Calculate specific shards based on series key hash
        // For now, query all shards
    }
    
    // Default: query all shards
    // Handle case where Seastar isn't initialized (in unit tests)
    unsigned shard_count = seastar::smp::count;
    if (shard_count == 0) {
        shard_count = 1;  // Default to 1 shard for testing
    }
    
    for (unsigned i = 0; i < shard_count; ++i) {
        shards.push_back(i);
    }
    
    return shards;
}

std::vector<SeriesResult> HttpQueryHandler::mergeResults(
    std::vector<std::vector<SeriesResult>> shardResults) {
    
    std::vector<SeriesResult> merged;
    
    // Simple concatenation for now
    // TODO: Implement proper merging based on series keys
    for (const auto& shardResult : shardResults) {
        for (const auto& series : shardResult) {
            merged.push_back(series);
        }
    }
    
    return merged;
}

void HttpQueryHandler::applyAggregation(std::vector<SeriesResult>& results,
                                       const QueryRequest& request) {
    
    // Special case: Single series with no interval should preserve all values
    // (no cross-series aggregation needed)
    if (results.size() == 1 && request.aggregationInterval == 0) {
        // Single series and no interval - pass through unchanged
        return;
    }
    
    // Check if we should aggregate across all series (no group-by)
    if (!request.hasGroupBy() && results.size() > 1) {
        // Group all series by field name for cross-series aggregation
        std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, tsdb::FieldValues>>> fieldGroups;
        std::string commonMeasurement = results.empty() ? "" : results[0].measurement;
        
        // Collect all data grouped by field
        for (const auto& series : results) {
            for (const auto& [fieldName, fieldData] : series.fields) {
                fieldGroups[fieldName].push_back(fieldData);
            }
        }
        
        // Create a single aggregated result
        SeriesResult aggregatedResult;
        aggregatedResult.measurement = commonMeasurement;
        // Empty tags when aggregating across all series
        aggregatedResult.tags.clear();
        
        // Aggregate each field across all series
        for (const auto& [fieldName, fieldSeries] : fieldGroups) {
            // Check if all values are numeric (double) - only aggregate those
            bool allNumeric = true;
            for (const auto& fieldData : fieldSeries) {
                const auto& values = fieldData.second;
                if (!std::holds_alternative<std::vector<double>>(values)) {
                    allNumeric = false;
                    break;
                }
            }
            
            if (!allNumeric) {
                // Can't aggregate non-numeric types across series
                // Just take the first series' data
                if (!fieldSeries.empty()) {
                    aggregatedResult.fields[fieldName] = fieldSeries[0];
                }
                continue;
            }
            
            // For MIN/MAX/AVG/SUM, we need to aggregate at each unique timestamp
            // First, collect all unique timestamps
            std::map<uint64_t, std::vector<double>> timeMap;
            
            for (const auto& fieldData : fieldSeries) {
                const auto& timestamps = fieldData.first;
                const auto& doubleValues = std::get<std::vector<double>>(fieldData.second);
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    timeMap[timestamps[i]].push_back(doubleValues[i]);
                }
            }
            
            // Now apply aggregation at each timestamp
            std::vector<uint64_t> aggregatedTimestamps;
            std::vector<double> aggregatedValues;
            
            for (const auto& [timestamp, vals] : timeMap) {
                aggregatedTimestamps.push_back(timestamp);
                
                double aggregatedValue;
                switch (request.aggregation) {
                    case AggregationMethod::MIN:
                        aggregatedValue = *std::min_element(vals.begin(), vals.end());
                        break;
                    case AggregationMethod::MAX:
                        aggregatedValue = *std::max_element(vals.begin(), vals.end());
                        break;
                    case AggregationMethod::AVG:
                        aggregatedValue = std::accumulate(vals.begin(), vals.end(), 0.0) / vals.size();
                        break;
                    case AggregationMethod::SUM:
                        aggregatedValue = std::accumulate(vals.begin(), vals.end(), 0.0);
                        break;
                    case AggregationMethod::LATEST:
                        // For latest, just take the last value (they're all at same timestamp)
                        aggregatedValue = vals.back();
                        break;
                    default:
                        aggregatedValue = vals.front();
                        break;
                }
                
                aggregatedValues.push_back(aggregatedValue);
            }
            
            // Apply time interval aggregation if specified
            if (request.aggregationInterval > 0) {
                auto aggregated = Aggregator::aggregate(
                    aggregatedTimestamps,
                    aggregatedValues,
                    request.aggregation,
                    request.aggregationInterval
                );
                
                aggregatedTimestamps.clear();
                aggregatedValues.clear();
                
                for (const auto& point : aggregated) {
                    aggregatedTimestamps.push_back(point.timestamp);
                    aggregatedValues.push_back(point.value);
                }
            }
            
            aggregatedResult.fields[fieldName] = std::make_pair(aggregatedTimestamps, 
                tsdb::FieldValues(aggregatedValues));
        }
        
        // Replace results with single aggregated series
        results.clear();
        results.push_back(aggregatedResult);
        
    } else if (request.aggregationInterval > 0) {
        // Apply aggregation to each series independently only if interval is specified
        // For group-by or when we have time bucketing
        for (auto& series : results) {
            for (auto& [fieldName, fieldData] : series.fields) {
                auto& timestamps = fieldData.first;
                auto& values = fieldData.second;
                
                // Only aggregate numeric values
                if (std::holds_alternative<std::vector<double>>(values)) {
                    auto& doubleValues = std::get<std::vector<double>>(values);
                    
                    auto aggregated = Aggregator::aggregate(
                        timestamps, 
                        doubleValues, 
                        request.aggregation,
                        request.aggregationInterval
                    );
                    
                    timestamps.clear();
                    doubleValues.clear();
                    
                    for (const auto& point : aggregated) {
                        timestamps.push_back(point.timestamp);
                        doubleValues.push_back(point.value);
                    }
                    
                    values = doubleValues; // Update the variant
                }
                // Non-numeric values pass through unchanged
            }
        }
    }
    // else: No aggregation needed - data passes through as-is
}

std::vector<SeriesResult> HttpQueryHandler::applyGroupBy(
    const std::vector<SeriesResult>& results,
    const QueryRequest& request) {
    
    if (!request.hasGroupBy()) {
        return results;
    }
    
    // Group series by specified tags
    std::map<std::string, std::vector<const SeriesResult*>> groups;
    
    for (const auto& series : results) {
        // Build group key from specified tags
        std::string groupKey;
        for (const auto& tagName : request.groupByTags) {
            auto it = series.tags.find(tagName);
            if (it != series.tags.end()) {
                if (!groupKey.empty()) groupKey += ",";
                groupKey += tagName + "=" + it->second;
            }
        }
        
        groups[groupKey].push_back(&series);
    }
    
    // Aggregate within each group
    std::vector<SeriesResult> groupedResults;
    
    for (const auto& [groupKey, groupSeries] : groups) {
        if (groupSeries.empty()) continue;
        
        SeriesResult grouped;
        grouped.measurement = groupSeries[0]->measurement;
        
        // Extract tags for this group
        for (const auto& tagName : request.groupByTags) {
            auto it = groupSeries[0]->tags.find(tagName);
            if (it != groupSeries[0]->tags.end()) {
                grouped.tags[tagName] = it->second;
            }
        }
        
        // Merge all series in this group by field
        std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, tsdb::FieldValues>>> fieldGroups;
        
        for (const auto* series : groupSeries) {
            for (const auto& [fieldName, fieldData] : series->fields) {
                fieldGroups[fieldName].push_back(fieldData);
            }
        }
        
        // Aggregate each field
        for (const auto& [fieldName, fieldSeries] : fieldGroups) {
            // If only one series in the group and no interval, preserve it as-is
            if (fieldSeries.size() == 1 && request.aggregationInterval == 0) {
                grouped.fields[fieldName] = fieldSeries[0];
            } else if (request.aggregationInterval == 0) {
                // Check if all values are numeric  
                bool allNumeric = true;
                for (const auto& fieldData : fieldSeries) {
                    const auto& values = fieldData.second;
                    if (!std::holds_alternative<std::vector<double>>(values)) {
                        allNumeric = false;
                        break;
                    }
                }
                
                if (!allNumeric) {
                    // Can't aggregate non-numeric types
                    if (!fieldSeries.empty()) {
                        grouped.fields[fieldName] = fieldSeries[0];
                    }
                    continue;
                }
                
                // Merge series at same timestamps for the group
                std::map<uint64_t, std::vector<double>> timeMap;
                
                for (const auto& fieldData : fieldSeries) {
                    const auto& timestamps = fieldData.first;
                    const auto& doubleValues = std::get<std::vector<double>>(fieldData.second);
                    for (size_t i = 0; i < timestamps.size(); ++i) {
                        timeMap[timestamps[i]].push_back(doubleValues[i]);
                    }
                }
                
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                
                // Apply aggregation method at each timestamp
                for (const auto& [timestamp, vals] : timeMap) {
                    timestamps.push_back(timestamp);
                    double aggregatedValue;
                    switch (request.aggregation) {
                        case AggregationMethod::MIN:
                            aggregatedValue = *std::min_element(vals.begin(), vals.end());
                            break;
                        case AggregationMethod::MAX:
                            aggregatedValue = *std::max_element(vals.begin(), vals.end());
                            break;
                        case AggregationMethod::AVG:
                            aggregatedValue = std::accumulate(vals.begin(), vals.end(), 0.0) / vals.size();
                            break;
                        case AggregationMethod::SUM:
                            aggregatedValue = std::accumulate(vals.begin(), vals.end(), 0.0);
                            break;
                        case AggregationMethod::LATEST:
                            // Take the last value for this timestamp
                            aggregatedValue = vals.back();
                            break;
                        default:
                            aggregatedValue = vals.front();
                    }
                    values.push_back(aggregatedValue);
                }
                
                grouped.fields[fieldName] = std::make_pair(timestamps, 
                    tsdb::FieldValues(values));
            } else {
                // With interval, check if all values are numeric
                bool allNumeric = true;
                std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>> numericSeries;
                
                for (const auto& fieldData : fieldSeries) {
                    const auto& timestamps = fieldData.first;
                    if (!std::holds_alternative<std::vector<double>>(fieldData.second)) {
                        allNumeric = false;
                        break;
                    }
                    const auto& doubleValues = std::get<std::vector<double>>(fieldData.second);
                    numericSeries.push_back(std::make_pair(timestamps, doubleValues));
                }
                
                if (!allNumeric) {
                    // Can't aggregate non-numeric types with intervals
                    if (!fieldSeries.empty()) {
                        grouped.fields[fieldName] = fieldSeries[0];
                    }
                    continue;
                }
                
                // With interval, use existing aggregation
                auto aggregated = Aggregator::aggregateMultiple(
                    numericSeries,
                    request.aggregation,
                    request.aggregationInterval
                );
                
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                
                for (const auto& point : aggregated) {
                    timestamps.push_back(point.timestamp);
                    values.push_back(point.value);
                }
                
                grouped.fields[fieldName] = std::make_pair(timestamps, 
                    tsdb::FieldValues(values));
            }
        }
        
        groupedResults.push_back(grouped);
    }
    
    return groupedResults;
}

} // namespace tsdb