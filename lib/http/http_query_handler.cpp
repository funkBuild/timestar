#include "http_query_handler.hpp"
#include "engine.hpp"
#include "query_parser.hpp"
#include "query_planner.hpp"
#include "aggregator.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include <algorithm>
#include <numeric>
#include <seastar/core/when_all.hh>
#include <seastar/core/smp.hh>

// Glaze-compatible structures for JSON parsing  
struct GlazeQueryRequest {
    std::string query;
    std::variant<uint64_t, std::string> startTime;
    std::variant<uint64_t, std::string> endTime;
    std::optional<std::variant<uint64_t, std::string>> aggregationInterval;
};

template <>
struct glz::meta<GlazeQueryRequest> {
    using T = GlazeQueryRequest;
    static constexpr auto value = object(
        "query", &T::query,
        "startTime", &T::startTime,
        "endTime", &T::endTime,
        "aggregationInterval", &T::aggregationInterval
    );
};

namespace tsdb {

// Response structures for Glaze serialization - must be at namespace scope
struct QueryFieldData {
    std::vector<uint64_t> timestamps;
    std::variant<std::vector<double>, std::vector<bool>, std::vector<std::string>> values;
};

struct QuerySeriesData {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::map<std::string, QueryFieldData> fields;
    std::optional<std::vector<std::pair<std::string, std::string>>> scopes;
};

struct QueryStatisticsData {
    int64_t series_count;
    int64_t point_count;
    double execution_time_ms;
};

struct ScopeData {
    std::string name;
    std::string value;
};

struct QueryFormattedResponse {
    std::string status = "success";
    std::vector<QuerySeriesData> series;
    std::optional<std::vector<ScopeData>> scopes;
    QueryStatisticsData statistics;
};

struct QueryErrorResponse {
    std::string status = "error";
    std::string message;
    std::string error;  // Changed to string to match test expectations
};

seastar::future<std::unique_ptr<seastar::httpd::reply>>
HttpQueryHandler::handleQuery(std::unique_ptr<seastar::httpd::request> req) {
    auto rep = std::make_unique<seastar::httpd::reply>();
    
    try {
        // Parse JSON request body using Glaze
        GlazeQueryRequest glazeRequest;
        auto parse_error = glz::read_json(glazeRequest, req->content);
        
        if (parse_error) {
            rep->set_status(seastar::httpd::reply::status_type::bad_request);
            rep->_content = createErrorResponse("INVALID_JSON", "Failed to parse JSON request: " + std::string(glz::format_error(parse_error)));
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        // Convert to QueryRequest
        QueryRequest queryRequest;
        try {
            queryRequest = parseQueryRequest(glazeRequest);
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

QueryRequest HttpQueryHandler::parseQueryRequest(const GlazeQueryRequest& glazeReq) {
    // Parse the query string
    std::string queryStr = glazeReq.query;
    
    // Handle both numeric and string timestamps for backward compatibility
    uint64_t startTime, endTime;
    
    // Parse startTime - can be number or string
    if (std::holds_alternative<uint64_t>(glazeReq.startTime)) {
        startTime = std::get<uint64_t>(glazeReq.startTime);
    } else {
        std::string timeStr = std::get<std::string>(glazeReq.startTime);
        try {
            startTime = std::stoull(timeStr);
        } catch (...) {
            // Fall back to date string parsing (legacy support)
            startTime = QueryParser::parseTime(timeStr);
        }
    }
    
    // Parse endTime - can be number or string
    if (std::holds_alternative<uint64_t>(glazeReq.endTime)) {
        endTime = std::get<uint64_t>(glazeReq.endTime);
    } else {
        std::string timeStr = std::get<std::string>(glazeReq.endTime);
        try {
            endTime = std::stoull(timeStr);
        } catch (...) {
            // Fall back to date string parsing (legacy support)
            endTime = QueryParser::parseTime(timeStr);
        }
    }
    
    // Parse aggregation interval if provided
    uint64_t aggregationInterval = 0;
    if (glazeReq.aggregationInterval) {
        const auto& interval = *glazeReq.aggregationInterval;
        if (std::holds_alternative<uint64_t>(interval)) {
            aggregationInterval = std::get<uint64_t>(interval);
        } else {
            std::string intervalStr = std::get<std::string>(interval);
            // TODO: implement parseIntervalString 
            // For now, parse simple patterns like "5m" -> 5 * 60 * 1e9
            if (intervalStr.back() == 's') {
                aggregationInterval = std::stoull(intervalStr.substr(0, intervalStr.size()-1)) * 1000000000ULL;
            } else if (intervalStr.back() == 'm') {
                aggregationInterval = std::stoull(intervalStr.substr(0, intervalStr.size()-1)) * 60 * 1000000000ULL;
            } else if (intervalStr.back() == 'h') {
                aggregationInterval = std::stoull(intervalStr.substr(0, intervalStr.size()-1)) * 3600 * 1000000000ULL;
            } else {
                aggregationInterval = std::stoull(intervalStr);
            }
        }
    }
    
    LOG_QUERY_PATH(tsdb::http_log, debug, "[QUERY] Parsed request - Query: '{}', Start: {}, End: {}, Interval: {}",
                   queryStr, startTime, endTime, 
                   aggregationInterval ? std::to_string(aggregationInterval) : "none");
    
    // Parse the query string to extract components  
    QueryRequest request = QueryParser::parseQueryString(queryStr);
    request.startTime = startTime;
    request.endTime = endTime;
    request.aggregationInterval = aggregationInterval;
    
    return request;
}

seastar::future<QueryResponse> HttpQueryHandler::executeQuery(const QueryRequest& request) {
    LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Executing query - Measurement: {}, Fields: {}, Scopes: {}, Start: {}, End: {}",
                   request.measurement,
                   request.fields.size(),
                   request.scopes.size(),
                   request.startTime,
                   request.endTime);
    
    QueryResponse response;
    response.success = true;
    response.scopes = request.scopes;
    
    try {
        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Checking pointers - engineSharded: {}, indexSharded: {}", 
                       (void*)engineSharded, (void*)indexSharded);
        
        if (!engineSharded) {
            LOG_QUERY_PATH(tsdb::http_log, error, "[QUERY] engineSharded is NULL!");
            response.success = false;
            response.errorCode = "NULL_ENGINE";
            response.errorMessage = "Engine pointer is null";
            co_return response;
        }
        
        // For now, we'll create a simple plan without using the index
        // The Engine's executeLocalQuery will handle the actual querying
        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Creating simple query plan without index lookup");
        
        QueryPlan plan;
        plan.aggregation = request.aggregation;
        plan.aggregationInterval = request.aggregationInterval;
        plan.groupByTags = request.groupByTags;
        
        // For now, query all shards since we don't have the index to determine which shards have the data
        unsigned shardCount = seastar::smp::count;
        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Creating queries for {} shards", shardCount);
        
        for (unsigned shardId = 0; shardId < shardCount; ++shardId) {
            ShardQuery sq;
            sq.shardId = shardId;
            sq.startTime = request.startTime;
            sq.endTime = request.endTime;
            sq.requiresAllSeries = true;  // We'll need to find series on each shard
            
            // Set fields to query
            if (request.requestsAllFields()) {
                sq.fields.clear();  // Empty means all fields
            } else {
                sq.fields.insert(request.fields.begin(), request.fields.end());
            }
            
            plan.shardQueries.push_back(sq);
        }
        
        plan.requiresMerging = shardCount > 1;
        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Query plan created with {} shard queries", 
                       plan.shardQueries.size());
        
        // Execute queries on each shard
        std::vector<seastar::future<std::vector<tsdb::SeriesResult>>> futures;
        for (const auto& shardQuery : plan.shardQueries) {
            auto f = engineSharded->invoke_on(shardQuery.shardId, 
                [sq = shardQuery, measurement = request.measurement, scopes = request.scopes, 
                 fields = request.fields](Engine& engine) -> seastar::future<std::vector<tsdb::SeriesResult>> {
                    // Look up series IDs on this shard based on the query parameters
                    auto& index = engine.getIndex();
                    
                    // Find series matching the measurement and tag filters (scopes)
                    auto seriesIds = co_await index.findSeries(measurement, scopes);
                    
                    LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard {} found {} series IDs for measurement '{}' with {} scopes",
                                   sq.shardId, seriesIds.size(), measurement, scopes.size());
                    
                    // Update the shard query with the found series IDs
                    tsdb::ShardQuery localQuery = sq;
                    localQuery.seriesIds = seriesIds;
                    localQuery.requiresAllSeries = false;  // We have specific series IDs now
                    
                    // If specific fields were requested, we need to filter the series IDs
                    // to only those that have the requested fields
                    if (!fields.empty() && !seriesIds.empty()) {
                        std::vector<uint64_t> filteredSeriesIds;
                        for (uint64_t seriesId : seriesIds) {
                            auto seriesMetaOpt = co_await index.getSeriesMetadata(seriesId);
                            if (seriesMetaOpt.has_value()) {
                                const auto& meta = seriesMetaOpt.value();
                                // Check if this series has one of the requested fields
                                if (std::find(fields.begin(), fields.end(), meta.field) != fields.end()) {
                                    filteredSeriesIds.push_back(seriesId);
                                }
                            }
                        }
                        localQuery.seriesIds = filteredSeriesIds;
                        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Filtered to {} series IDs with requested fields",
                                       filteredSeriesIds.size());
                    }
                    
                    // Now execute the query with the populated series IDs
                    co_return co_await engine.executeLocalQuery(localQuery);
                });
            futures.push_back(std::move(f));
        }
        
        // Wait for all shards to complete
        auto shardResults = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        
        // Merge results from all shards
        for (const auto& shardSeriesVec : shardResults) {
            for (const auto& series : shardSeriesVec) {
                // Copy the series directly - it's already in the right format
                response.series.push_back(series);
                
                // Update statistics
                for (const auto& [fieldName, fieldData] : series.fields) {
                    // Count points based on timestamps
                    response.statistics.pointCount += fieldData.first.size();
                }
            }
        }
        
        // Apply aggregation and grouping
        if (!response.series.empty()) {
            std::vector<SeriesResult> aggregatedSeries;
            
            // Check if we need to group series
            if (request.groupByTags.empty()) {
                // No group by - merge all series into one aggregated result
                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] No group-by specified, merging {} series into one", response.series.size());
                
                // Create a single merged series
                SeriesResult mergedSeries;
                mergedSeries.measurement = response.series[0].measurement;
                // Clear tags since we're aggregating across all series
                mergedSeries.tags.clear();
                
                // Collect all series data by field name for aggregation
                std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>> numericFieldData;
                std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, FieldValues>>> nonNumericFieldData;
                
                for (const auto& series : response.series) {
                    for (const auto& [fieldName, data] : series.fields) {
                        const auto& timestamps = data.first;
                        const auto& values = data.second;
                        
                        if (std::holds_alternative<std::vector<double>>(values)) {
                            // Numeric values - can be aggregated
                            const auto& doubleValues = std::get<std::vector<double>>(values);
                            numericFieldData[fieldName].push_back(std::make_pair(timestamps, doubleValues));
                        } else {
                            // Non-numeric values (bool, string) - keep for LATEST aggregation
                            nonNumericFieldData[fieldName].push_back(std::make_pair(timestamps, values));
                        }
                    }
                }
                
                // Aggregate numeric fields using time-aligned aggregation
                for (const auto& [fieldName, seriesData] : numericFieldData) {
                    if (!seriesData.empty()) {
                        auto aggregated = Aggregator::aggregateMultiple(
                            seriesData, request.aggregation, request.aggregationInterval);
                        
                        std::vector<uint64_t> newTimestamps;
                        std::vector<double> newValues;
                        
                        for (const auto& point : aggregated) {
                            newTimestamps.push_back(point.timestamp);
                            newValues.push_back(point.value);
                        }
                        
                        mergedSeries.fields[fieldName] = std::make_pair(newTimestamps, FieldValues(newValues));
                    }
                }
                
                // Handle non-numeric fields (for LATEST aggregation only)
                if (request.aggregation == AggregationMethod::LATEST) {
                    for (const auto& [fieldName, seriesData] : nonNumericFieldData) {
                        if (!seriesData.empty()) {
                            if (request.aggregationInterval == 0) {
                                // No interval - return all values
                                std::vector<std::pair<uint64_t, FieldValues>> allData;
                                
                                for (const auto& [timestamps, values] : seriesData) {
                                    for (size_t i = 0; i < timestamps.size(); ++i) {
                                        if (std::holds_alternative<std::vector<bool>>(values)) {
                                            const auto& boolVals = std::get<std::vector<bool>>(values);
                                            allData.push_back({timestamps[i], std::vector<bool>{boolVals[i]}});
                                        } else if (std::holds_alternative<std::vector<std::string>>(values)) {
                                            const auto& strVals = std::get<std::vector<std::string>>(values);
                                            allData.push_back({timestamps[i], std::vector<std::string>{strVals[i]}});
                                        }
                                    }
                                }
                                
                                // Sort by timestamp
                                std::sort(allData.begin(), allData.end(),
                                    [](const auto& a, const auto& b) { return a.first < b.first; });
                                
                                // Combine into result vectors
                                std::vector<uint64_t> resultTimestamps;
                                FieldValues resultValues;
                                
                                if (!allData.empty()) {
                                    // Determine the type from first element
                                    if (std::holds_alternative<std::vector<bool>>(allData[0].second)) {
                                        std::vector<bool> boolResults;
                                        for (const auto& [ts, val] : allData) {
                                            resultTimestamps.push_back(ts);
                                            const auto& boolVec = std::get<std::vector<bool>>(val);
                                            boolResults.push_back(boolVec[0]);
                                        }
                                        resultValues = boolResults;
                                    } else if (std::holds_alternative<std::vector<std::string>>(allData[0].second)) {
                                        std::vector<std::string> strResults;
                                        for (const auto& [ts, val] : allData) {
                                            resultTimestamps.push_back(ts);
                                            const auto& strVec = std::get<std::vector<std::string>>(val);
                                            strResults.push_back(strVec[0]);
                                        }
                                        resultValues = strResults;
                                    }
                                    
                                    mergedSeries.fields[fieldName] = std::make_pair(resultTimestamps, resultValues);
                                }
                            } else {
                                // With interval - return latest from each bucket (not implemented for non-numeric yet)
                                // For now, just return the single latest value
                                uint64_t latestTime = 0;
                                FieldValues latestValue;
                                
                                for (const auto& [timestamps, values] : seriesData) {
                                    if (!timestamps.empty() && timestamps.back() > latestTime) {
                                        latestTime = timestamps.back();
                                        if (std::holds_alternative<std::vector<bool>>(values)) {
                                            const auto& boolVals = std::get<std::vector<bool>>(values);
                                            latestValue = std::vector<bool>{boolVals.back()};
                                        } else if (std::holds_alternative<std::vector<std::string>>(values)) {
                                            const auto& strVals = std::get<std::vector<std::string>>(values);
                                            latestValue = std::vector<std::string>{strVals.back()};
                                        }
                                    }
                                }
                                
                                if (latestTime > 0) {
                                    mergedSeries.fields[fieldName] = std::make_pair(
                                        std::vector<uint64_t>{latestTime}, latestValue);
                                }
                            }
                        }
                    }
                }
                
                if (!mergedSeries.fields.empty()) {
                    aggregatedSeries.push_back(mergedSeries);
                }
                
            } else {
                // Group by specified tags
                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Group-by tags: {}", fmt::join(request.groupByTags, ", "));
                
                // Group series by the specified tags
                std::map<std::map<std::string, std::string>, std::vector<SeriesResult*>> groups;
                
                for (auto& series : response.series) {
                    // Extract group-by tag values
                    std::map<std::string, std::string> groupKey;
                    for (const auto& tagName : request.groupByTags) {
                        auto it = series.tags.find(tagName);
                        if (it != series.tags.end()) {
                            groupKey[tagName] = it->second;
                        }
                    }
                    groups[groupKey].push_back(&series);
                }
                
                // Aggregate each group
                for (const auto& [groupTags, seriesGroup] : groups) {
                    SeriesResult groupedSeries;
                    groupedSeries.measurement = seriesGroup[0]->measurement;
                    groupedSeries.tags = groupTags; // Only keep group-by tags
                    
                    // Collect all series data in this group by field name for time-aligned aggregation
                    std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>> fieldSeriesData;
                    
                    for (const auto* series : seriesGroup) {
                        for (const auto& [fieldName, data] : series->fields) {
                            const auto& timestamps = data.first;
                            const auto& values = data.second;
                            
                            if (std::holds_alternative<std::vector<double>>(values)) {
                                const auto& doubleValues = std::get<std::vector<double>>(values);
                                
                                // Add this series to the field's collection
                                fieldSeriesData[fieldName].push_back(std::make_pair(timestamps, doubleValues));
                            }
                        }
                    }
                    
                    // Aggregate the grouped data using time-aligned aggregation
                    for (const auto& [fieldName, seriesData] : fieldSeriesData) {
                        if (!seriesData.empty()) {
                            // Use aggregateMultiple for time-aligned aggregation
                            auto aggregated = Aggregator::aggregateMultiple(
                                seriesData, request.aggregation, request.aggregationInterval);
                            
                            std::vector<uint64_t> newTimestamps;
                            std::vector<double> newValues;
                            
                            for (const auto& point : aggregated) {
                                newTimestamps.push_back(point.timestamp);
                                newValues.push_back(point.value);
                            }
                            
                            groupedSeries.fields[fieldName] = std::make_pair(newTimestamps, FieldValues(newValues));
                        }
                    }
                    
                    if (!groupedSeries.fields.empty()) {
                        aggregatedSeries.push_back(groupedSeries);
                    }
                }
            }
            
            // Replace response series with aggregated results
            response.series = std::move(aggregatedSeries);
            
            // Update statistics
            response.statistics.pointCount = 0;
            for (const auto& series : response.series) {
                for (const auto& [fieldName, fieldData] : series.fields) {
                    response.statistics.pointCount += fieldData.first.size();
                }
            }
        }
        
        response.statistics.seriesCount = response.series.size();
        
    } catch (const std::exception& e) {
        response.success = false;
        response.errorCode = "QUERY_EXECUTION_ERROR";
        response.errorMessage = e.what();
        LOG_QUERY_PATH(tsdb::http_log, error, "[QUERY] Query execution failed: {}", e.what());
    }
    
    co_return response;
}

std::string HttpQueryHandler::formatQueryResponse(const QueryResponse& response) {
    // Build response JSON using structured approach
    QueryFormattedResponse formattedResponse;
    
    // Convert series
    for (const auto& series : response.series) {
        QuerySeriesData sd;
        sd.measurement = series.measurement;
        sd.tags = series.tags;
        
        // Convert fields
        for (const auto& [fieldName, fieldData] : series.fields) {
            QueryFieldData fd;
            fd.timestamps = fieldData.first;
            fd.values = fieldData.second;
            sd.fields[fieldName] = fd;
        }
        
        // Add scopes if present
        if (!response.scopes.empty()) {
            std::vector<std::pair<std::string, std::string>> scopeVec;
            for (const auto& [name, value] : response.scopes) {
                scopeVec.emplace_back(name, value);
            }
            sd.scopes = scopeVec;
        }
        
        formattedResponse.series.push_back(std::move(sd));
    }
    
    // Set scopes at top level if present
    if (!response.scopes.empty()) {
        std::vector<ScopeData> scopeVec;
        for (const auto& [name, value] : response.scopes) {
            scopeVec.push_back(ScopeData{name, value});
        }
        formattedResponse.scopes = scopeVec;
    }
    
    // Set statistics
    formattedResponse.statistics.series_count = response.statistics.seriesCount;
    formattedResponse.statistics.point_count = response.statistics.pointCount;
    formattedResponse.statistics.execution_time_ms = response.statistics.executionTimeMs;
    
    return glz::write_json(formattedResponse).value_or("{}");
}

std::string HttpQueryHandler::createErrorResponse(const std::string& code, const std::string& message) {
    QueryErrorResponse response;
    response.message = message;
    response.error = message;  // Set error as string to match test expectations
    
    return glz::write_json(response).value_or("{}");
}

std::vector<unsigned> HttpQueryHandler::determineTargetShards(const QueryRequest& request) {
    // For now, query all shards
    std::vector<unsigned> shards;
    for (unsigned i = 0; i < seastar::smp::count; ++i) {
        shards.push_back(i);
    }
    return shards;
}

std::vector<SeriesResult> HttpQueryHandler::mergeResults(std::vector<std::vector<SeriesResult>> shardResults) {
    std::vector<SeriesResult> merged;
    for (const auto& shardResult : shardResults) {
        merged.insert(merged.end(), shardResult.begin(), shardResult.end());
    }
    return merged;
}

void HttpQueryHandler::applyAggregation(std::vector<SeriesResult>& results, const QueryRequest& request) {
    // This is a stub - actual aggregation logic would be more complex
    // For testing purposes, we'll do basic aggregation
    if (request.aggregationInterval == 0 && request.aggregation == AggregationMethod::AVG) {
        // Simple average aggregation for testing
        for (auto& series : results) {
            for (auto& [fieldName, fieldData] : series.fields) {
                if (std::holds_alternative<std::vector<double>>(fieldData.second)) {
                    auto& values = std::get<std::vector<double>>(fieldData.second);
                    if (!values.empty()) {
                        double sum = std::accumulate(values.begin(), values.end(), 0.0);
                        double avg = sum / values.size();
                        values = {avg};
                        fieldData.first = {fieldData.first.front()};  // Keep first timestamp
                    }
                }
            }
        }
    }
}

std::vector<SeriesResult> HttpQueryHandler::applyGroupBy(const std::vector<SeriesResult>& results, const QueryRequest& request) {
    // For now, return results unchanged
    // Actual group-by logic would be more complex
    return results;
}

uint64_t HttpQueryHandler::parseInterval(const std::string& interval) {
    if (interval.empty()) {
        throw QueryParseException("Interval string cannot be empty");
    }
    
    // Find where the numeric part ends
    size_t unitPos = 0;
    bool hasDecimal = false;
    for (size_t i = 0; i < interval.length(); ++i) {
        if (std::isdigit(interval[i]) || (interval[i] == '.' && !hasDecimal)) {
            if (interval[i] == '.') hasDecimal = true;
            unitPos = i + 1;
        } else {
            break;
        }
    }
    
    if (unitPos == 0) {
        throw QueryParseException("Invalid interval format: no numeric value");
    }
    
    double value = std::stod(interval.substr(0, unitPos));
    std::string unit = interval.substr(unitPos);
    
    // Convert to nanoseconds based on unit
    uint64_t multiplier = 0;
    if (unit == "ns") {
        multiplier = 1;
    } else if (unit == "us" || unit == "µs") {
        multiplier = 1000;
    } else if (unit == "ms") {
        multiplier = 1000000;
    } else if (unit == "s") {
        multiplier = 1000000000;
    } else if (unit == "m") {
        multiplier = 60ULL * 1000000000;
    } else if (unit == "h") {
        multiplier = 3600ULL * 1000000000;
    } else if (unit == "d") {
        multiplier = 86400ULL * 1000000000;
    } else {
        throw QueryParseException("Unknown time unit: " + unit);
    }
    
    return static_cast<uint64_t>(value * multiplier);
}

} // namespace tsdb