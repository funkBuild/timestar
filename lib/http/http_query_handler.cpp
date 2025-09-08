#include "http_query_handler.hpp"
#include "engine.hpp"
#include "query_parser.hpp"
#include "query_planner.hpp"
#include "query_runner.hpp"
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
        
        // Log query summary after response is ready (controlled by TSDB_LOG_QUERY_PATH)
        LOG_QUERY_PATH(tsdb::http_log, info, 
            "[QUERY_SUMMARY] Query: '{}' | StartTime: {} | EndTime: {} | AggregationInterval: {} | ExecutionTime: {:.2f}ms",
            glazeRequest.query,
            queryRequest.startTime,
            queryRequest.endTime,
            queryRequest.aggregationInterval ? std::to_string(queryRequest.aggregationInterval) : "none",
            response.statistics.executionTimeMs);
        
    } catch (const std::exception& e) {
        LOG_QUERY_PATH(tsdb::http_log, error, "[QUERY] Error handling query request: {}", e.what());
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
        
        // First, query shard 0 for all series metadata (centralized metadata)
        // Get full metadata with series keys and determine which shard owns each series
        struct SeriesMetadataWithShard {
            uint64_t seriesId;
            std::string seriesKey;
            std::string measurement;
            std::map<std::string, std::string> tags;
            std::string field;
            unsigned shardId;
        };
        
        auto seriesWithShards = co_await engineSharded->invoke_on(0, 
            [measurement = request.measurement, scopes = request.scopes, fields = request.fields](Engine& engine) 
                -> seastar::future<std::vector<SeriesMetadataWithShard>> {
                auto& index = engine.getIndex();
                auto seriesIds = co_await index.findSeries(measurement, scopes);
                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard 0 (metadata) found {} series IDs for measurement '{}' with {} scopes",
                               seriesIds.size(), measurement, scopes.size());
                
                std::vector<SeriesMetadataWithShard> result;
                for (uint64_t seriesId : seriesIds) {
                    auto seriesMetaOpt = co_await index.getSeriesMetadata(seriesId);
                    if (!seriesMetaOpt.has_value()) {
                        continue;
                    }
                    
                    const auto& meta = seriesMetaOpt.value();
                    
                    // Filter by fields if specified
                    if (!fields.empty() && std::find(fields.begin(), fields.end(), meta.field) == fields.end()) {
                        continue;
                    }
                    
                    // Build series key for sharding
                    TSDBInsert<double> temp(meta.measurement, meta.field);
                    temp.tags = meta.tags;
                    std::string seriesKey = temp.seriesKey();
                    
                    // Calculate which shard owns this series
                    unsigned shardId = std::hash<std::string>{}(seriesKey) % seastar::smp::count;
                    
                    SeriesMetadataWithShard smws;
                    smws.seriesId = seriesId;
                    smws.seriesKey = seriesKey;
                    smws.measurement = meta.measurement;
                    smws.tags = meta.tags;
                    smws.field = meta.field;
                    smws.shardId = shardId;
                    
                    result.push_back(smws);
                }
                
                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Filtered to {} series with metadata",
                               result.size());
                co_return result;
            });
        
        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Total series from centralized metadata: {}", seriesWithShards.size());
        
        // Debug: Print series details
        for (const auto& sm : seriesWithShards) {
            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Series: key='{}', shard={}, measurement='{}', field='{}'",
                           sm.seriesKey, sm.shardId, sm.measurement, sm.field);
        }
        
        // Group series by shard
        std::map<unsigned, std::vector<std::string>> seriesByShard;
        for (const auto& sm : seriesWithShards) {
            seriesByShard[sm.shardId].push_back(sm.seriesKey);
        }
        
        // Execute queries on each shard that has series
        std::vector<seastar::future<std::vector<tsdb::SeriesResult>>> futures;
        for (const auto& [shardId, seriesKeys] : seriesByShard) {
            auto f = engineSharded->invoke_on(shardId, 
                [shardId, seriesKeys, startTime = request.startTime, endTime = request.endTime, 
                 measurement = request.measurement](Engine& engine) -> seastar::future<std::vector<tsdb::SeriesResult>> {
                    LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard {} querying {} series keys",
                                   shardId, seriesKeys.size());
                    
                    std::vector<tsdb::SeriesResult> results;
                    
                    // Query each series directly using series key
                    for (const auto& seriesKey : seriesKeys) {
                        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard {} querying series key: '{}'",
                                       shardId, seriesKey);
                        // Use engine's query method directly
                        auto variantResult = co_await engine.query(seriesKey, startTime, endTime);
                        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Got variant result for series '{}'", seriesKey);
                        
                        // Parse series key to extract measurement, tags, and field
                        // Format: measurement,tag1=value1,tag2=value2 field
                        // Note: space separator before field, not comma!
                        
                        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Parsing series key: '{}'", seriesKey);
                        
                        // Find the space that separates field from the rest
                        size_t spacePos = seriesKey.find(' ');
                        std::string field;
                        std::string measurementAndTags;
                        
                        if (spacePos != std::string::npos) {
                            measurementAndTags = seriesKey.substr(0, spacePos);
                            field = seriesKey.substr(spacePos + 1);
                            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Found field '{}' at position {}", field, spacePos);
                        } else {
                            // No field separator found - treat entire key as measurement
                            measurementAndTags = seriesKey;
                            LOG_QUERY_PATH(tsdb::http_log, warn, "[QUERY] No space separator found in series key");
                        }
                        
                        // Parse measurement and tags from the first part
                        size_t pos = measurementAndTags.find(',');
                        std::string meas = (pos != std::string::npos) ? measurementAndTags.substr(0, pos) : measurementAndTags;
                        
                        // Extract tags from remainder
                        std::map<std::string, std::string> tags;
                        if (pos != std::string::npos) {
                            std::string remainder = measurementAndTags.substr(pos + 1);
                            std::vector<std::string> parts;
                            std::string current;
                            for (char c : remainder) {
                                if (c == ',') {
                                    parts.push_back(current);
                                    current.clear();
                                } else {
                                    current += c;
                                }
                            }
                            if (!current.empty()) {
                                parts.push_back(current);
                            }
                            
                            // All parts are tags
                            for (const auto& part : parts) {
                                size_t eqPos = part.find('=');
                                if (eqPos != std::string::npos) {
                                    tags[part.substr(0, eqPos)] = part.substr(eqPos + 1);
                                }
                            }
                        }
                        
                        // Convert to SeriesResult format
                        tsdb::SeriesResult seriesResult;
                        seriesResult.measurement = measurement; // Use request measurement
                        seriesResult.tags = tags;
                        
                        // Handle different result types
                        std::visit([&seriesResult, &field, &seriesKey](auto&& result) {
                            using T = std::decay_t<decltype(result)>;
                            if constexpr (std::is_same_v<T, QueryResult<double>>) {
                                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Processing double result: {} timestamps, {} values for field '{}'",
                                             result.timestamps.size(), result.values.size(), field);
                                if (!result.timestamps.empty()) {
                                    std::vector<double> values(result.values.begin(), result.values.end());
                                    seriesResult.fields[field] = std::make_pair(result.timestamps, FieldValues(values));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<bool>>) {
                                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Processing bool result: {} timestamps, {} values for field '{}'",
                                             result.timestamps.size(), result.values.size(), field);
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[field] = std::make_pair(result.timestamps, FieldValues(result.values));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<std::string>>) {
                                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Processing string result: {} timestamps, {} values for field '{}'",
                                             result.timestamps.size(), result.values.size(), field);
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[field] = std::make_pair(result.timestamps, FieldValues(result.values));
                                }
                            }
                        }, variantResult);
                        
                        if (!seriesResult.fields.empty()) {
                            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Adding series result with {} fields", seriesResult.fields.size());
                            results.push_back(seriesResult);
                        } else {
                            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Series result has no fields, skipping");
                        }
                    }
                    
                    co_return results;
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
                mergedSeries.measurement = request.measurement; // Use request measurement instead of series[0]
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
                    if (seriesGroup.empty()) {
                        continue; // Skip empty groups
                    }
                    SeriesResult groupedSeries;
                    groupedSeries.measurement = request.measurement; // Use request measurement
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