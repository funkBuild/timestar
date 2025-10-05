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
#include <unordered_map>
#include <seastar/core/when_all.hh>
#include <seastar/core/smp.hh>
#include <iomanip>
#include <sstream>

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

// Timing structure to track query execution steps
struct QueryTimingInfo {
    std::chrono::high_resolution_clock::time_point startTime;
    std::chrono::high_resolution_clock::time_point endTime;
    
    // Step timings
    double parseRequestMs = 0.0;
    double findSeriesMs = 0.0;
    double shardDistributionMs = 0.0;
    double shardQueriesMs = 0.0;
    std::vector<std::pair<unsigned, double>> perShardQueryMs;
    double resultMergingMs = 0.0;
    double aggregationMs = 0.0;
    double responseFormattingMs = 0.0;
    double totalMs = 0.0;
    
    // Statistics
    size_t seriesFound = 0;
    size_t shardsQueried = 0;
    size_t totalPointsRetrieved = 0;
    size_t finalPointsReturned = 0;
    
    std::string toString() const {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2);
        ss << "\n==================== Query Timing Breakdown ===================\n";
        ss << "Parse Request:        " << std::setw(8) << parseRequestMs << " ms\n";
        ss << "Find Series:          " << std::setw(8) << findSeriesMs << " ms\n";
        ss << "Shard Distribution:   " << std::setw(8) << shardDistributionMs << " ms\n";
        ss << "Shard Queries:        " << std::setw(8) << shardQueriesMs << " ms\n";
        if (!perShardQueryMs.empty()) {
            ss << "  Per-shard breakdown:\n";
            for (const auto& [shardId, ms] : perShardQueryMs) {
                ss << "    Shard " << shardId << ":         " << std::setw(8) << ms << " ms\n";
            }
        }
        ss << "Result Merging:       " << std::setw(8) << resultMergingMs << " ms\n";
        ss << "Aggregation:          " << std::setw(8) << aggregationMs << " ms\n";
        ss << "Response Formatting:  " << std::setw(8) << responseFormattingMs << " ms\n";
        ss << "----------------------------------------------------------------\n";
        ss << "Total Execution:      " << std::setw(8) << totalMs << " ms\n";
        ss << "\n";
        ss << "Series Found:         " << seriesFound << "\n";
        ss << "Shards Queried:       " << shardsQueried << "\n";
        ss << "Points Retrieved:     " << totalPointsRetrieved << "\n";
        ss << "Points Returned:      " << finalPointsReturned << "\n";
        ss << "================================================================\n";
        return ss.str();
    }
};

// Response structures for Glaze serialization - must be at namespace scope
struct QueryFieldData {
    std::vector<uint64_t> timestamps;
    std::variant<std::vector<double>, std::vector<bool>, std::vector<std::string>> values;
};

struct QuerySeriesData {
    std::string measurement;
    std::vector<std::string> groupTags;  // Changed from tags map to groupTags array
    std::map<std::string, QueryFieldData> fields;
    // Removed scopes - filter scopes shouldn't be in series
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
    // Removed top-level scopes as per requirement
    QueryStatisticsData statistics;
};

struct QueryErrorResponse {
    std::string status = "error";
    std::string message;
    std::string error;  // Changed to string to match test expectations
};

seastar::future<std::unique_ptr<seastar::http::reply>>
HttpQueryHandler::handleQuery(std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    
    try {
        // Parse JSON request body using Glaze
        GlazeQueryRequest glazeRequest;
        auto parse_error = glz::read_json(glazeRequest, req->content);
        
        if (parse_error) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("INVALID_JSON", "Failed to parse JSON request: " + std::string(glz::format_error(parse_error)));
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        // Convert to QueryRequest
        QueryRequest queryRequest;
        auto parseStart = std::chrono::high_resolution_clock::now();
        try {
            queryRequest = parseQueryRequest(glazeRequest);
        } catch (const QueryParseException& e) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("INVALID_QUERY", e.what());
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        auto parseEnd = std::chrono::high_resolution_clock::now();
        double parseMs = std::chrono::duration<double, std::milli>(parseEnd - parseStart).count();
        
        // Execute query
        auto startTime = std::chrono::high_resolution_clock::now();
        QueryResponse response = co_await executeQuery(queryRequest);
        auto endTime = std::chrono::high_resolution_clock::now();
        
        // Calculate execution time
        response.statistics.executionTimeMs = 
            std::chrono::duration<double, std::milli>(endTime - startTime).count();
        
        // Format response
        auto formatStart = std::chrono::high_resolution_clock::now();
        if (response.success) {
            rep->set_status(seastar::http::reply::status_type::ok);
            rep->_content = formatQueryResponse(response, queryRequest.fields);
        } else {
            rep->set_status(seastar::http::reply::status_type::internal_server_error);
            rep->_content = createErrorResponse(response.errorCode, response.errorMessage);
        }
        auto formatEnd = std::chrono::high_resolution_clock::now();
        double formatMs = std::chrono::duration<double, std::milli>(formatEnd - formatStart).count();
        
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
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("INTERNAL_ERROR", e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

void HttpQueryHandler::registerRoutes(seastar::httpd::routes& r) {
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req, 
               std::unique_ptr<seastar::http::reply> rep) 
            -> seastar::future<std::unique_ptr<seastar::http::reply>> {
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
            // Use the proper parseInterval function that supports decimals and all units
            aggregationInterval = parseInterval(intervalStr);
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
    
    // Initialize timing tracker
    QueryTimingInfo timing;
    timing.startTime = std::chrono::high_resolution_clock::now();
    
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
            SeriesId128 seriesId;
            std::string seriesKey;
            std::string measurement;
            std::map<std::string, std::string> tags;
            std::string field;
            unsigned shardId;
        };
        
        auto findSeriesStart = std::chrono::high_resolution_clock::now();
        auto seriesWithShards = co_await engineSharded->invoke_on(0, 
            [measurement = request.measurement, scopes = request.scopes, fields = request.fields](Engine& engine) 
                -> seastar::future<std::vector<SeriesMetadataWithShard>> {
                auto& index = engine.getIndex();
                auto seriesIds = co_await index.findSeries(measurement, scopes);
                LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard 0 (metadata) found {} series IDs for measurement '{}' with {} scopes",
                               seriesIds.size(), measurement, scopes.size());
                
                std::vector<SeriesMetadataWithShard> result;
                for (const SeriesId128& seriesId : seriesIds) {
                    auto seriesMetaOpt = co_await index.getSeriesMetadata(seriesId);
                    if (!seriesMetaOpt.has_value()) {
                        continue;
                    }
                    
                    const auto& meta = seriesMetaOpt.value();
                    
                    // NOTE: We no longer filter by fields at the metadata level
                    // This allows us to return existing fields even when some requested fields are missing
                    // Field filtering will be handled gracefully during result processing
                    
                    // Build series key for sharding
                    TSDBInsert<double> temp(meta.measurement, meta.field);
                    temp.tags = meta.tags;
                    std::string seriesKey = temp.seriesKey();
                    
                    // Calculate which shard owns this series using SeriesId128 for consistency
                    SeriesId128 seriesIdForSharding = SeriesId128::fromSeriesKey(seriesKey);
                    unsigned shardId = SeriesId128::Hash{}(seriesIdForSharding) % seastar::smp::count;
                    
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
        
        auto findSeriesEnd = std::chrono::high_resolution_clock::now();
        timing.findSeriesMs = std::chrono::duration<double, std::milli>(findSeriesEnd - findSeriesStart).count();
        timing.seriesFound = seriesWithShards.size();
        
        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Total series from centralized metadata: {} (took {:.2f} ms)", 
                       seriesWithShards.size(), timing.findSeriesMs);
        
        // Debug: Print series details
        for (const auto& sm : seriesWithShards) {
            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Series: key='{}', shard={}, measurement='{}', field='{}'",
                           sm.seriesKey, sm.shardId, sm.measurement, sm.field);
        }
        
        // Group series by shard and create field name lookup map
        auto shardDistStart = std::chrono::high_resolution_clock::now();
        std::unordered_map<unsigned, std::vector<std::string>> seriesByShard;
        std::unordered_map<std::string, std::string> seriesKeyToField;  // Pre-parsed field names
        seriesByShard.reserve(seastar::smp::count);  // Pre-allocate for number of shards
        seriesKeyToField.reserve(seriesWithShards.size());  // Pre-allocate for all series

        for (const auto& sm : seriesWithShards) {
            seriesByShard[sm.shardId].push_back(sm.seriesKey);
            seriesKeyToField[sm.seriesKey] = sm.field;  // Store pre-parsed field
        }
        auto shardDistEnd = std::chrono::high_resolution_clock::now();
        timing.shardDistributionMs = std::chrono::duration<double, std::milli>(shardDistEnd - shardDistStart).count();
        timing.shardsQueried = seriesByShard.size();
        
        // Execute queries on each shard that has series
        auto shardQueriesStart = std::chrono::high_resolution_clock::now();
        std::vector<seastar::future<std::pair<unsigned, std::pair<std::vector<PartialAggregationResult>, double>>>> futures;
        for (const auto& [shardId, seriesKeys] : seriesByShard) {
            // Create field lookup map for this shard's series
            std::unordered_map<std::string, std::string> shardSeriesKeyToField;
            shardSeriesKeyToField.reserve(seriesKeys.size());
            for (const auto& sk : seriesKeys) {
                auto it = seriesKeyToField.find(sk);
                if (it != seriesKeyToField.end()) {
                    shardSeriesKeyToField[sk] = it->second;
                }
            }

            auto f = engineSharded->invoke_on(shardId,
                [shardId, seriesKeys, shardSeriesKeyToField = std::move(shardSeriesKeyToField),
                 startTime = request.startTime, endTime = request.endTime,
                 measurement = request.measurement,
                 aggregation = request.aggregation,
                 aggregationInterval = request.aggregationInterval,
                 groupByTags = request.groupByTags](Engine& engine) -> seastar::future<std::pair<std::vector<PartialAggregationResult>, double>> {
                    auto shardStart = std::chrono::high_resolution_clock::now();
                    LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard {} querying {} series keys in parallel",
                                   shardId, seriesKeys.size());

                    // OPTIMIZATION: Pre-allocate results container
                    std::vector<tsdb::SeriesResult> results;
                    results.reserve(seriesKeys.size());

                    // OPTIMIZATION: Query all series in parallel instead of serially
                    struct SeriesQueryContext {
                        std::string seriesKey;
                        std::string field;
                        std::map<std::string, std::string> tags;
                    };

                    // Pre-parse all series metadata before querying
                    std::vector<SeriesQueryContext> contexts;
                    contexts.reserve(seriesKeys.size());

                    for (const auto& seriesKey : seriesKeys) {
                        SeriesQueryContext ctx;
                        ctx.seriesKey = seriesKey;

                        // Use pre-parsed field name from metadata
                        auto fieldIt = shardSeriesKeyToField.find(seriesKey);
                        if (fieldIt != shardSeriesKeyToField.end()) {
                            ctx.field = fieldIt->second;
                        } else {
                            // Fallback: Parse series key if not found in map
                            size_t spacePos = seriesKey.find(' ');
                            if (spacePos != std::string::npos) {
                                ctx.field = seriesKey.substr(spacePos + 1);
                            }
                        }

                        // Parse measurement and tags from series key
                        size_t spacePos = seriesKey.find(' ');
                        std::string measurementAndTags = (spacePos != std::string::npos) ? seriesKey.substr(0, spacePos) : seriesKey;

                        size_t pos = measurementAndTags.find(',');

                        // Extract tags from remainder
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
                                    ctx.tags[part.substr(0, eqPos)] = part.substr(eqPos + 1);
                                }
                            }
                        }

                        contexts.push_back(std::move(ctx));
                    }

                    // OPTIMIZATION: Launch all queries in parallel
                    std::vector<seastar::future<std::optional<VariantQueryResult>>> queryFutures;
                    queryFutures.reserve(contexts.size());

                    for (const auto& ctx : contexts) {
                        auto queryFuture = engine.query(ctx.seriesKey, startTime, endTime)
                            .then([](VariantQueryResult variantResult) {
                                return std::optional<VariantQueryResult>(std::move(variantResult));
                            })
                            .handle_exception([seriesKey = ctx.seriesKey, shardId](std::exception_ptr eptr) -> std::optional<VariantQueryResult> {
                                try {
                                    std::rethrow_exception(eptr);
                                } catch (const std::runtime_error& e) {
                                    if (std::string(e.what()).find("Series not found") != std::string::npos) {
                                        LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Series '{}' not found on shard {} - skipping",
                                                       seriesKey, shardId);
                                    } else {
                                        LOG_QUERY_PATH(tsdb::http_log, warn, "[QUERY] Error querying series '{}' on shard {}: {} - skipping",
                                                       seriesKey, shardId, e.what());
                                    }
                                } catch (...) {
                                    LOG_QUERY_PATH(tsdb::http_log, warn, "[QUERY] Unknown error querying series '{}' on shard {} - skipping",
                                                   seriesKey, shardId);
                                }
                                return std::nullopt;
                            });
                        queryFutures.push_back(std::move(queryFuture));
                    }

                    // Wait for all queries to complete
                    auto queryResults = co_await seastar::when_all_succeed(queryFutures.begin(), queryFutures.end());

                    // Process results
                    for (size_t i = 0; i < queryResults.size(); ++i) {
                        const auto& optResult = queryResults[i];
                        if (!optResult.has_value()) {
                            continue; // Skip failed queries
                        }

                        const auto& variantResult = optResult.value();
                        const auto& ctx = contexts[i];

                        // Convert to SeriesResult format
                        tsdb::SeriesResult seriesResult;
                        seriesResult.measurement = measurement;
                        seriesResult.tags = ctx.tags;

                        // Handle different result types
                        std::visit([&seriesResult, &ctx](auto&& result) {
                            using T = std::decay_t<decltype(result)>;
                            if constexpr (std::is_same_v<T, QueryResult<double>>) {
                                if (!result.timestamps.empty()) {
                                    std::vector<double> values(result.values.begin(), result.values.end());
                                    seriesResult.fields[ctx.field] = std::make_pair(result.timestamps, FieldValues(values));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<bool>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(result.timestamps, FieldValues(result.values));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<std::string>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(result.timestamps, FieldValues(result.values));
                                }
                            }
                        }, variantResult);

                        if (!seriesResult.fields.empty()) {
                            results.push_back(std::move(seriesResult));
                        }
                    }

                    // OPTIMIZATION: Perform partial aggregation on this shard before returning
                    auto partialAggStart = std::chrono::high_resolution_clock::now();
                    auto partialResults = Aggregator::createPartialAggregations(
                        results, aggregation, aggregationInterval, groupByTags);
                    auto partialAggEnd = std::chrono::high_resolution_clock::now();
                    double partialAggMs = std::chrono::duration<double, std::milli>(partialAggEnd - partialAggStart).count();

                    auto shardEnd = std::chrono::high_resolution_clock::now();
                    double shardMs = std::chrono::duration<double, std::milli>(shardEnd - shardStart).count();
                    LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Shard {} completed {} parallel queries + partial aggregation in {:.2f} ms (aggregation: {:.2f} ms)",
                                   shardId, seriesKeys.size(), shardMs, partialAggMs);
                    co_return std::make_pair(partialResults, shardMs);
                }).then([shardId](std::pair<std::vector<PartialAggregationResult>, double> result) {
                    return std::make_pair(shardId, result);
                });
            futures.push_back(std::move(f));
        }
        
        // Wait for all shards to complete
        auto shardResults = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        auto shardQueriesEnd = std::chrono::high_resolution_clock::now();
        timing.shardQueriesMs = std::chrono::duration<double, std::milli>(shardQueriesEnd - shardQueriesStart).count();
        
        // Collect partial aggregations from all shards
        auto mergeStart = std::chrono::high_resolution_clock::now();

        std::vector<PartialAggregationResult> allPartialResults;
        size_t totalPartialResults = 0;
        for (const auto& sr : shardResults) {
            totalPartialResults += sr.second.first.size();
        }
        allPartialResults.reserve(totalPartialResults);

        for (auto& shardResult : shardResults) {
            unsigned shardId = shardResult.first;
            auto& [partialResults, shardMs] = shardResult.second;
            timing.perShardQueryMs.push_back({shardId, shardMs});

            // Count points from partial results
            for (const auto& partial : partialResults) {
                timing.totalPointsRetrieved += partial.totalPoints;
            }

            // Collect all partial results
            allPartialResults.insert(allPartialResults.end(),
                std::make_move_iterator(partialResults.begin()),
                std::make_move_iterator(partialResults.end()));
        }

        auto mergeEnd = std::chrono::high_resolution_clock::now();
        timing.resultMergingMs = std::chrono::duration<double, std::milli>(mergeEnd - mergeStart).count();

        // Merge partial aggregations from all shards into final aggregated points
        auto aggregationStart = std::chrono::high_resolution_clock::now();
        if (!allPartialResults.empty()) {
            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Merging {} partial aggregations from {} shards",
                           allPartialResults.size(), timing.shardsQueried);

            // OPTIMIZATION & FIX: Use grouped merge to preserve metadata associations
            auto groupedResults = Aggregator::mergePartialAggregationsGrouped(
                allPartialResults, request.aggregation);

            LOG_QUERY_PATH(tsdb::http_log, info, "[QUERY] Merged into {} grouped results",
                           groupedResults.size());

            // OPTIMIZATION: Pre-allocate response series
            response.series.reserve(groupedResults.size());

            // Convert GroupedAggregationResult to SeriesResult (metadata already preserved)
            for (auto& groupedResult : groupedResults) {
                SeriesResult series;
                series.measurement = std::move(groupedResult.measurement);
                series.tags = std::move(groupedResult.tags);

                // OPTIMIZATION: Pre-allocate and move data (no copying)
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                timestamps.reserve(groupedResult.points.size());
                values.reserve(groupedResult.points.size());

                for (const auto& point : groupedResult.points) {
                    timestamps.push_back(point.timestamp);
                    values.push_back(point.value);
                }

                series.fields[std::move(groupedResult.fieldName)] =
                    std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

                response.series.push_back(std::move(series));
            }

            // Update statistics
            response.statistics.pointCount = 0;
            for (const auto& series : response.series) {
                for (const auto& [fieldName, fieldData] : series.fields) {
                    size_t points = fieldData.first.size();
                    response.statistics.pointCount += points;
                    timing.finalPointsReturned += points;
                }
            }
        }
        auto aggregationEnd = std::chrono::high_resolution_clock::now();
        timing.aggregationMs = std::chrono::duration<double, std::milli>(aggregationEnd - aggregationStart).count();
        
        response.statistics.seriesCount = response.series.size();
        
        // Calculate total timing
        timing.endTime = std::chrono::high_resolution_clock::now();
        timing.totalMs = std::chrono::duration<double, std::milli>(timing.endTime - timing.startTime).count();
        
        // Print timing information
        tsdb::http_log.info("{}", timing.toString());
        
    } catch (const std::exception& e) {
        response.success = false;
        response.errorCode = "QUERY_EXECUTION_ERROR";
        response.errorMessage = e.what();
        LOG_QUERY_PATH(tsdb::http_log, error, "[QUERY] Query execution failed: {}", e.what());
    }
    
    co_return response;
}

std::string HttpQueryHandler::formatQueryResponse(const QueryResponse& response, const std::vector<std::string>& requestedFields) {
    // Build response JSON using structured approach
    QueryFormattedResponse formattedResponse;
    
    // Convert series
    for (const auto& series : response.series) {
        QuerySeriesData sd;
        sd.measurement = series.measurement;
        
        // Convert tags map to groupTags array format
        for (const auto& [key, value] : series.tags) {
            sd.groupTags.push_back(key + "=" + value);
        }
        std::sort(sd.groupTags.begin(), sd.groupTags.end());  // Sort for consistent ordering
        
        // Convert fields (filter by requested fields if specified)
        for (const auto& [fieldName, fieldData] : series.fields) {
            // If specific fields were requested, only include those fields
            if (!requestedFields.empty() && 
                std::find(requestedFields.begin(), requestedFields.end(), fieldName) == requestedFields.end()) {
                continue;
            }
            
            QueryFieldData fd;
            fd.timestamps = fieldData.first;
            fd.values = fieldData.second;
            sd.fields[fieldName] = fd;
        }
        
        // Only include series that have at least one field after filtering
        if (!sd.fields.empty()) {
            formattedResponse.series.push_back(std::move(sd));
        }
    }
    // Removed top-level scopes as per requirement
    
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
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) {
        shardCount = 1; // Default for test environment
    }
    for (unsigned i = 0; i < shardCount; ++i) {
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