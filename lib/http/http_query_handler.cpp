#include "http_query_handler.hpp"

#include "aggregator.hpp"
#include "block_aggregator.hpp"
#include "content_negotiation.hpp"
#include "engine.hpp"
#include "group_key.hpp"
#include "http_auth.hpp"
#include "http_routes.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "proto_converters.hpp"
#include "query_parser.hpp"
#include "query_runner.hpp"
#include "response_formatter.hpp"
#include "series_key.hpp"
#include "series_matcher.hpp"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <numeric>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

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
    static constexpr auto value = object("query", &T::query, "startTime", &T::startTime, "endTime", &T::endTime,
                                         "aggregationInterval", &T::aggregationInterval);
};

namespace timestar {

// Struct holding pre-parsed series metadata for per-shard query execution.
// Includes pre-computed SeriesId128 to eliminate redundant hash computations.
// Defined at namespace level so streaming group-by helpers can reference it.
struct SeriesQueryContext {
    std::string seriesKey;
    SeriesId128 seriesId;  // Pre-computed from seriesKey — avoids rehashing in QueryRunner
    std::string field;
    std::map<std::string, std::string> tags;
};

// ---------------------------------------------------------------------------
// Streaming group-by aggregation
// ---------------------------------------------------------------------------
// For group-by queries with streamable methods (avg, sum, min, max, count,
// spread, stddev, stdvar, latest, first), fold TSM blocks one-at-a-time into
// per-group accumulators instead of materializing all raw data.  Memory goes
// from O(points) to O(groups x buckets).

// Returns true when a group-by query can use the streaming path.
// MEDIAN and EXACT_MEDIAN are NOT streamable (both need raw values).
// Returns true when the query can use streaming aggregation (fold per-block
// into accumulators instead of materializing all raw data).
//
// Requires EITHER group-by tags OR an aggregation interval — without both,
// the old path returns raw timestamp/value pairs which internal callers
// (DerivedQueryExecutor, ForecastExecutor) depend on for formula evaluation.
static bool canStreamAggregation(AggregationMethod method, const std::vector<std::string>& groupByTags,
                                 uint64_t aggregationInterval) {
    if (!isStreamableMethod(method))
        return false;
    // Need at least one of: group-by (fold into groups) or bucketing (fold into buckets)
    return !groupByTags.empty() || aggregationInterval > 0;
}

// Streaming group-by coroutine.  Iterates each series context, folds the
// per-series PushdownResult into a per-group accumulator, and returns
// PartialAggregationResults in the same format the rest of the pipeline
// expects.
//
// Uses direct AggregationState accumulators (not BlockAggregator) so that
// PushdownResult states can be merged in without converting back to raw
// points.
//
// Key invariant: Seastar is single-threaded per shard, so the group
// accumulators are never accessed concurrently.  After each co_await the
// accumulators are safe to mutate.
static seastar::future<std::vector<PartialAggregationResult>> streamingGroupByAggregation(
    Engine& engine, std::vector<SeriesQueryContext>& contexts, const std::string& measurement, uint64_t startTime,
    uint64_t endTime, AggregationMethod aggregation, uint64_t aggregationInterval,
    const std::vector<std::string>& groupByTags) {
    // ---- Phase 1: Pre-group series by groupKey ----
    struct GroupAccumulator {
        // For bucketed queries (interval > 0): one AggregationState per bucket
        std::unordered_map<uint64_t, AggregationState> bucketStates;
        // For non-bucketed queries (interval == 0): single collapsed state
        AggregationState collapsedState;
        // Metadata
        std::string groupKey;
        size_t groupKeyHash = 0;
        std::map<std::string, std::string> cachedTags;
        std::string fieldName;
        size_t totalPoints = 0;
    };

    // Use PrehashedString for O(1) lookups
    std::unordered_map<PrehashedString, std::unique_ptr<GroupAccumulator>, PrehashedStringHash, PrehashedStringEqual>
        groupMap;
    groupMap.reserve(contexts.size());  // upper-bound

    // Map each context to its group accumulator
    std::vector<GroupAccumulator*> contextGroupPtrs;
    contextGroupPtrs.reserve(contexts.size());

    for (auto& ctx : contexts) {
        auto gkr = buildGroupKeyDirect(measurement, ctx.field, ctx.tags, groupByTags);
        PrehashedString pkey(gkr.key, gkr.hash);

        auto [it, inserted] = groupMap.try_emplace(pkey, nullptr);
        if (inserted) {
            it->second = std::make_unique<GroupAccumulator>();
            it->second->groupKey = std::move(gkr.key);
            it->second->groupKeyHash = gkr.hash;
            it->second->cachedTags = std::move(gkr.tags);
            it->second->fieldName = ctx.field;
            // Pre-reserve bucket map for bucketed queries
            if (aggregationInterval > 0 && endTime > startTime) {
                uint64_t range = endTime - startTime;
                uint64_t bucketCount = (range + aggregationInterval - 1) / aggregationInterval;
                if (bucketCount > BlockAggregator::MAX_PREALLOCATED_BUCKETS) {
                    bucketCount = BlockAggregator::MAX_PREALLOCATED_BUCKETS;
                }
                it->second->bucketStates.reserve(static_cast<size_t>(bucketCount));
            }
        }
        contextGroupPtrs.push_back(it->second.get());
    }

    // ---- Phase 2: Prefetch TSM indices ----
    {
        std::vector<SeriesId128> prefetchIds;
        prefetchIds.reserve(contexts.size());
        for (const auto& ctx : contexts) {
            prefetchIds.push_back(ctx.seriesId);
        }
        co_await engine.prefetchSeriesIndices(prefetchIds);
    }

    // ---- Phase 3: Helper lambdas for folding values into groups ----

    // Fold a single (timestamp, value) pair into a group accumulator.
    auto foldPoint = [&](GroupAccumulator& group, uint64_t ts, double val) {
        if (aggregationInterval > 0) {
            uint64_t bucket = (ts / aggregationInterval) * aggregationInterval;
            group.bucketStates[bucket].addValueForMethod(val, ts, aggregation);
        } else {
            group.collapsedState.addValueForMethod(val, ts, aggregation);
        }
    };

    // Merge a PushdownResult into a group accumulator.
    auto mergePushdownIntoGroup = [&](GroupAccumulator& group, PushdownResult& pr) {
        group.totalPoints += pr.totalPoints;

        if (aggregationInterval > 0 && !pr.bucketStates.empty()) {
            // Bucketed: merge each bucket state
            for (auto& [bucketTs, state] : pr.bucketStates) {
                group.bucketStates[bucketTs].mergeForMethod(state, aggregation);
            }
        } else if (pr.aggregatedState.has_value()) {
            // Non-bucketed collapsed state: merge directly
            group.collapsedState.mergeForMethod(*pr.aggregatedState, aggregation);
        } else if (!pr.sortedTimestamps.empty()) {
            // Raw values from pushdown: fold each value
            for (size_t i = 0; i < pr.sortedTimestamps.size(); ++i) {
                foldPoint(group, pr.sortedTimestamps[i], pr.sortedValues[i]);
            }
        }
    };

    // ---- Phase 4: Iterate series with bounded concurrency ----
    // Build (context*, group*) pairs for index-free iteration.
    struct ContextGroupPair {
        SeriesQueryContext* ctx;
        GroupAccumulator* group;
    };
    std::vector<ContextGroupPair> pairs;
    pairs.reserve(contexts.size());
    for (size_t i = 0; i < contexts.size(); ++i) {
        pairs.push_back({&contexts[i], contextGroupPtrs[i]});
    }

    static constexpr size_t MAX_CONCURRENT_SERIES_QUERIES = 64;

    co_await seastar::max_concurrent_for_each(
        pairs, MAX_CONCURRENT_SERIES_QUERIES, [&](ContextGroupPair& pair) -> seastar::future<> {
            auto& ctx = *pair.ctx;
            auto* group = pair.group;

            // --- Try pushdown path ---
            auto pushdownResult = co_await engine.queryAggregated(ctx.seriesKey, ctx.seriesId, startTime, endTime,
                                                                  aggregationInterval, aggregation);

            if (pushdownResult.has_value()) {
                mergePushdownIntoGroup(*group, *pushdownResult);
                co_return;
            }

            // --- Fallback path ---
            // Pushdown not applicable (non-float, memory data, overlap).
            // Query the full data for this single series and fold.
            // Memory store data is bounded in size so materializing one
            // series at a time is fine.
            std::optional<VariantQueryResult> optResult;
            try {
                optResult = co_await engine.query(ctx.seriesKey, ctx.seriesId, startTime, endTime);
            } catch (const std::exception& e) {
                timestar::http_log.debug("Streaming fallback: skipping series {} due to error: {}", ctx.seriesKey,
                                         e.what());
                co_return;
            }

            if (!optResult.has_value()) {
                co_return;
            }

            std::visit(
                [&](auto&& result) {
                    using T = std::decay_t<decltype(result)>;
                    if constexpr (std::is_same_v<T, QueryResult<double>>) {
                        group->totalPoints += result.timestamps.size();
                        // Batch fold through BlockAggregator: sorted-run bucket
                        // batching + SIMD reductions instead of per-point
                        // division + hash lookup + method switch. Only streamable
                        // methods reach this function (gated by the caller), so
                        // BlockAggregator's fold semantics match foldPoint's.
                        if (aggregationInterval > 0) {
                            BlockAggregator agg(aggregationInterval, startTime, endTime, aggregation, true);
                            agg.addPoints(result.timestamps, result.values);
                            for (auto& [bucketTs, state] : agg.takeBucketStates()) {
                                group->bucketStates[bucketTs].mergeForMethod(std::move(state), aggregation);
                            }
                        } else {
                            BlockAggregator agg(0, aggregation);
                            agg.enableFoldToSingleState();
                            agg.addPoints(result.timestamps, result.values);
                            group->collapsedState.mergeForMethod(agg.takeSingleState(), aggregation);
                        }
                    } else if constexpr (std::is_same_v<T, QueryResult<int64_t>>) {
                        group->totalPoints += result.timestamps.size();
                        for (size_t i = 0; i < result.timestamps.size(); ++i) {
                            foldPoint(*group, result.timestamps[i], static_cast<double>(result.values[i]));
                        }
                    } else if constexpr (std::is_same_v<T, QueryResult<bool>>) {
                        group->totalPoints += result.timestamps.size();
                        for (size_t i = 0; i < result.timestamps.size(); ++i) {
                            foldPoint(*group, result.timestamps[i], result.values[i] ? 1.0 : 0.0);
                        }
                    }
                    // QueryResult<std::string> is skipped — can't numerically aggregate
                },
                *optResult);
        });

    // ---- Phase 5: Convert group accumulators to PartialAggregationResults ----
    std::vector<PartialAggregationResult> results;
    results.reserve(groupMap.size());

    for (auto& [pkey, groupPtr] : groupMap) {
        PartialAggregationResult partial;
        partial.measurement = measurement;
        partial.fieldName = groupPtr->fieldName;
        partial.groupKey = groupPtr->groupKey;
        partial.groupKeyHash = groupPtr->groupKeyHash;
        partial.cachedTags = std::move(groupPtr->cachedTags);
        partial.totalPoints = groupPtr->totalPoints;

        if (aggregationInterval > 0) {
            if (groupPtr->bucketStates.empty())
                continue;  // No data for this group
            partial.bucketStates = std::move(groupPtr->bucketStates);
        } else {
            if (groupPtr->collapsedState.count == 0)
                continue;  // No data for this group
            partial.collapsedState = std::move(groupPtr->collapsedState);
        }

        results.push_back(std::move(partial));
    }

    co_return results;
}

// Finalize partial aggregation results from a single shard directly into the query response,
// bypassing the mergePartialAggregationsGrouped() → GroupedAggregationResult pipeline.
// Precondition: each partial must have a unique groupKey (i.e. no two partials share the
// same measurement+field+tags combination). When multiple series produce partials with the
// same groupKey (e.g. non-group-by queries with multiple matching series), callers must use
// the merge fallback path instead.
void HttpQueryHandler::finalizeSingleShardPartials(std::vector<PartialAggregationResult>& partials,
                                                   AggregationMethod method, QueryResponse& response) {
    response.series.reserve(partials.size());

    for (auto& partial : partials) {
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        if (!partial.bucketStates.empty()) {
            // Bucketed: extract values directly into a sortable structure.
            // Use vector<pair<ts,value>> — avoids the indirection through state pointers
            // and getValue() is called during extraction, not during the sort.
            size_t n = partial.bucketStates.size();
            std::vector<std::pair<uint64_t, double>> tvPairs;
            tvPairs.reserve(n);
            for (auto& [ts, state] : partial.bucketStates) {
                tvPairs.emplace_back(ts, state.getValue(method));
            }
            std::sort(tvPairs.begin(), tvPairs.end());
            timestamps.reserve(n);
            values.reserve(n);
            for (auto& [ts, val] : tvPairs) {
                timestamps.push_back(ts);
                values.push_back(val);
            }
        } else if (partial.collapsedState.has_value()) {
            // Non-bucketed streaming pushdown — single collapsed AggregationState
            auto& state = *partial.collapsedState;
            if (state.count > 0) {
                timestamps.push_back(state.getTimestamp(method));
                values.push_back(state.getValue(method));
            }
        } else if (!partial.sortedValues.empty()) {
            // Non-bucketed raw values from pushdown — zero-copy move
            if (method == AggregationMethod::COUNT) {
                timestamps = std::move(partial.sortedTimestamps);
                values.assign(timestamps.size(), 1.0);
            } else {
                timestamps = std::move(partial.sortedTimestamps);
                values = std::move(partial.sortedValues);
            }
        } else if (!partial.sortedStates.empty()) {
            // Non-bucketed states from fallback aggregation
            timestamps.reserve(partial.sortedStates.size());
            values.reserve(partial.sortedStates.size());
            for (size_t i = 0; i < partial.sortedStates.size(); ++i) {
                timestamps.push_back(partial.sortedTimestamps[i]);
                values.push_back(partial.sortedStates[i].getValue(method));
            }
        } else {
            continue;
        }

        if (timestamps.empty())
            continue;

        // Both pushdown and fallback paths now populate cachedTags via buildGroupKeyDirect()
        auto tags = std::move(partial.cachedTags);

        // Each partial produces its own series entry (one field per series).
        // The API returns each field as a separate series — clients merge if needed.
        SeriesResult series;
        series.measurement = std::move(partial.measurement);
        series.tags = std::move(tags);
        series.fields[std::move(partial.fieldName)] =
            std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

        response.series.push_back(std::move(series));
    }
}

// Timing structure to track query execution steps
struct QueryTimingInfo {
    std::chrono::high_resolution_clock::time_point startTime;
    std::chrono::high_resolution_clock::time_point endTime;

    // Step timings
    double findSeriesMs = 0.0;
    double shardQueriesMs = 0.0;
    std::vector<std::pair<unsigned, double>> perShardQueryMs;
    double resultCollectionMs = 0.0;  // time to move shard results into local vectors
    double aggregationMs = 0.0;       // time for merge + reduce + response building
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
        ss << "Find Series:          " << std::setw(8) << findSeriesMs << " ms\n";
        ss << "Shard Queries:        " << std::setw(8) << shardQueriesMs << " ms\n";
        if (!perShardQueryMs.empty()) {
            ss << "  Per-shard breakdown:\n";
            for (const auto& [shardId, ms] : perShardQueryMs) {
                ss << "    Shard " << shardId << ":         " << std::setw(8) << ms << " ms\n";
            }
        }
        ss << "Result Collection:    " << std::setw(8) << resultCollectionMs << " ms\n";
        ss << "Aggregation:          " << std::setw(8) << aggregationMs << " ms\n";
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

std::unique_ptr<seastar::http::reply> HttpQueryHandler::validateRequest(const seastar::http::request& req) const {
    auto resFmt = timestar::http::responseFormat(req);

    // Check body size limit
    if (req.content.size() > maxQueryBodySize()) {
        auto rep = std::make_unique<seastar::http::reply>();
        rep->set_status(seastar::http::reply::status_type::payload_too_large);
        auto maxBytes = maxQueryBodySize();
        auto msg = "Request body too large (max " + std::to_string(maxBytes / 1024) + "KB)";
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatQueryError("PAYLOAD_TOO_LARGE", msg);
        } else {
            auto errObj = glz::obj{"status", "error", "message", msg, "error", msg};
            rep->_content = glz::write_json(errObj).value_or("{\"status\":\"error\"}");
        }
        timestar::http::setContentType(*rep, resFmt);
        return rep;
    }

    // Content-Type validation is handled by the content negotiation layer
    // (timestar::http::requestFormat), which parses the header case-insensitively
    // and defaults unknown types to JSON for backward compatibility.

    return nullptr;  // Validation passed
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpQueryHandler::handleQuery(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto reqFmt = timestar::http::requestFormat(*req);
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        // Validate request body size
        auto validationError = validateRequest(*req);
        if (validationError) {
            co_return validationError;
        }

        // Parse request body
        QueryRequest queryRequest;
        std::string queryStr;  // For logging

        if (timestar::http::isProtobuf(reqFmt)) {
            // Parse protobuf request
            auto parsed = timestar::proto::parseQueryRequest(req->content.data(), req->content.size());
            queryStr = parsed.query;

            // Build a GlazeQueryRequest-compatible struct to reuse parseQueryRequest()
            GlazeQueryRequest glazeRequest;
            glazeRequest.query = std::move(parsed.query);
            glazeRequest.startTime = parsed.startTime;
            glazeRequest.endTime = parsed.endTime;
            if (!parsed.aggregationInterval.empty()) {
                glazeRequest.aggregationInterval = parsed.aggregationInterval;
            }

            try {
                queryRequest = parseQueryRequest(glazeRequest);
            } catch (const QueryParseException& e) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatQueryError("INVALID_QUERY", e.what());
                } else {
                    rep->_content = createErrorResponse("INVALID_QUERY", e.what());
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }
        } else {
            // Parse JSON request body using Glaze
            GlazeQueryRequest glazeRequest;
            auto parse_error = glz::read_json(glazeRequest, req->content);

            if (parse_error) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatQueryError(
                        "INVALID_JSON", "Failed to parse JSON request: " + std::string(glz::format_error(parse_error)));
                } else {
                    rep->_content = createErrorResponse(
                        "INVALID_JSON", "Failed to parse JSON request: " + std::string(glz::format_error(parse_error)));
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }

            queryStr = glazeRequest.query;

            // Convert to QueryRequest
            try {
                queryRequest = parseQueryRequest(glazeRequest);
            } catch (const QueryParseException& e) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatQueryError("INVALID_QUERY", e.what());
                } else {
                    rep->_content = createErrorResponse("INVALID_QUERY", e.what());
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }
        }

        // Execute query
        auto startTime = std::chrono::high_resolution_clock::now();
        QueryResponse response = co_await executeQuery(queryRequest);
        auto endTime = std::chrono::high_resolution_clock::now();

        // Calculate execution time
        response.statistics.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

        // Format response
        if (response.success) {
            rep->set_status(seastar::http::reply::status_type::ok);
            if (timestar::http::isProtobuf(resFmt)) {
                // Build QueryResponseData from internal QueryResponse
                timestar::proto::QueryResponseData protoResp;
                protoResp.success = true;
                protoResp.statistics.seriesCount = response.statistics.seriesCount;
                protoResp.statistics.pointCount = response.statistics.pointCount;
                protoResp.statistics.failedSeriesCount = response.statistics.failedSeriesCount;
                protoResp.statistics.executionTimeMs = response.statistics.executionTimeMs;
                protoResp.statistics.shardsQueried = response.statistics.shardsQueried;
                protoResp.statistics.truncated = response.statistics.truncated;
                protoResp.statistics.truncationReason = response.statistics.truncationReason;

                for (auto& sr : response.series) {
                    timestar::proto::SeriesResultData srd;
                    srd.measurement = std::move(sr.measurement);
                    srd.tags = std::move(sr.tags);
                    for (auto& [fieldName, fieldData] : sr.fields) {
                        auto& [timestamps, values] = fieldData;
                        timestar::proto::FieldValues protoValues;
                        std::visit([&protoValues](auto& vec) { protoValues = std::move(vec); }, values);
                        srd.fields[std::move(fieldName)] =
                            std::make_pair(std::move(timestamps), std::move(protoValues));
                    }
                    protoResp.series.push_back(std::move(srd));
                }

                rep->_content = timestar::proto::formatQueryResponse(protoResp);
            } else {
                rep->_content = formatQueryResponse(response);
            }
        } else {
            rep->set_status(seastar::http::reply::status_type::internal_server_error);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content = timestar::proto::formatQueryError(response.errorCode, response.errorMessage);
            } else {
                rep->_content = createErrorResponse(response.errorCode, response.errorMessage);
            }
        }

        timestar::http::setContentType(*rep, resFmt);

        // Log query summary after response is ready (controlled by TIMESTAR_LOG_QUERY_PATH)
        LOG_QUERY_PATH(timestar::http_log, info,
                       "[QUERY_SUMMARY] Query: '{}' | StartTime: {} | EndTime: {} | AggregationInterval: {} | "
                       "ExecutionTime: {:.2f}ms",
                       queryStr, queryRequest.startTime, queryRequest.endTime,
                       queryRequest.aggregationInterval ? std::to_string(queryRequest.aggregationInterval) : "none",
                       response.statistics.executionTimeMs);

    } catch (const std::exception& e) {
        if (engineSharded) {
            ++engineSharded->local().metrics().query_errors_total;
        }
        timestar::http_log.error("[QUERY] Error handling query request: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatQueryError("INTERNAL_ERROR", "Internal query error");
        } else {
            rep->_content = createErrorResponse("INTERNAL_ERROR", "Internal query error");
        }
        timestar::http::setContentType(*rep, resFmt);
    }

    co_return rep;
}

void HttpQueryHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    // addJsonRoute applies timestar::wrapWithAuth per route.
    timestar::http::addJsonRoute(
        r, seastar::httpd::operation_type::POST, "/query", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleQuery(std::move(req)); });

    timestar::http_log.info("Registered HTTP query endpoint at /query{}", authToken.empty() ? "" : " (auth required)");
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
            if (!timeStr.empty() && timeStr[0] == '-')
                throw std::invalid_argument("Negative timestamps are not supported");
            startTime = std::stoull(timeStr);
        } catch (const std::invalid_argument&) {
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
            if (!timeStr.empty() && timeStr[0] == '-')
                throw std::invalid_argument("Negative timestamps are not supported");
            endTime = std::stoull(timeStr);
        } catch (const std::invalid_argument&) {
            // Fall back to date string parsing (legacy support)
            endTime = QueryParser::parseTime(timeStr);
        }
    }

    // Validate time range
    if (startTime >= endTime) {
        throw QueryParseException("startTime must be less than endTime");
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

    LOG_QUERY_PATH(timestar::http_log, debug, "[QUERY] Parsed request - Query: '{}', Start: {}, End: {}, Interval: {}",
                   queryStr, startTime, endTime, aggregationInterval ? std::to_string(aggregationInterval) : "none");

    // Parse the query string to extract components
    QueryRequest request = QueryParser::parseQueryString(queryStr);
    request.startTime = startTime;
    request.endTime = endTime;
    request.aggregationInterval = aggregationInterval;

    return request;
}

// ---------------------------------------------------------------------------
// executeQuery phase types
// ---------------------------------------------------------------------------
// executeQuery() is decomposed into phase methods (following the extraction
// pattern used by streamingGroupByAggregation() and finalizeSingleShardPartials()
// above):
//
//   1. discoverSeriesAcrossShards()  — scatter-gather series discovery
//   2. fanOutShardQueries()          — per-shard query execution via
//      executeShardQuery(), which runs on the owning shard
//   3. finalizeSingleShardResponse() / finalizeMultiShardResponse() — partial
//      merge + aggregation finalize into the QueryResponse
//   4. logQueryCompletion()          — timing breakdown + slow-query logging

// Per-shard discovery result
struct PerShardDiscovery {
    std::vector<SeriesQueryContext> contexts;
    bool limitExceeded = false;
    size_t discovered = 0;
    size_t limit = 0;
};

// Combined result of the discovery phase across all shards.
struct SeriesDiscoveryResult {
    std::vector<std::vector<SeriesQueryContext>> seriesByShard;
    bool limitExceeded = false;
    size_t discovered = 0;
    size_t limit = 0;
    bool timedOut = false;  // discovery exceeded defaultQueryTimeout()
};

// Struct to hold shard query results including both aggregatable and non-aggregatable data
struct ShardQueryResult {
    std::vector<PartialAggregationResult> partialResults;
    std::vector<SeriesResult> stringResults;  // String fields bypass aggregation
    double shardMs = 0.0;
};

// Result of the shard query fan-out phase.
struct ShardFanOutResult {
    std::vector<std::pair<unsigned, ShardQueryResult>> shardResults;
    bool timedOut = false;  // shard queries exceeded defaultQueryTimeout()
};

// Execute the query for every series context owned by one shard.  Runs on the
// owning shard (dispatched via invoke_on from fanOutShardQueries()).  Parameters
// are taken by value: they are moved into the coroutine frame at invocation, so
// they outlive every suspension point regardless of the caller's closure lifetime.
static seastar::future<ShardQueryResult> executeShardQuery(Engine& engine, unsigned shardId,
                                                           std::vector<SeriesQueryContext> contexts,
                                                           std::string measurement, uint64_t startTime,
                                                           uint64_t endTime, AggregationMethod aggregation,
                                                           uint64_t aggregationInterval,
                                                           std::vector<std::string> groupByTags) {
    auto shardStart = std::chrono::high_resolution_clock::now();
    LOG_QUERY_PATH(timestar::http_log, info,
                   "[QUERY] Shard {} querying {} series keys in parallel", shardId,
                   contexts.size());

    // Containers for pushdown results and fallback results
    std::vector<PartialAggregationResult> pushdownPartials;
    std::vector<timestar::SeriesResult> fallbackResults;
    fallbackResults.reserve(contexts.size());

    const bool isLatestOrFirst =
        (aggregation == AggregationMethod::LATEST || aggregation == AggregationMethod::FIRST);

    // ---- BATCH LATEST/FIRST FAST PATH ----
    // For non-bucketed LATEST/FIRST, resolve all series in a single
    // pass over TSM sparse indices and memory stores.  This avoids
    // per-series: file snapshot, sort, coroutine overhead, and
    // intermediate PushdownResult/BlockAggregator allocations.
    if (isLatestOrFirst && aggregationInterval == 0) {
        const bool wantFirst = (aggregation == AggregationMethod::FIRST);

        // Build batch entries
        std::vector<Engine::BatchLatestEntry> batchEntries;
        batchEntries.resize(contexts.size());
        for (size_t i = 0; i < contexts.size(); ++i) {
            batchEntries[i].seriesId = contexts[i].seriesId;
        }

        co_await engine.batchLatest(batchEntries, startTime, endTime, wantFirst);

        // Convert resolved entries to PartialAggregationResults
        pushdownPartials.reserve(batchEntries.size());
        for (size_t i = 0; i < batchEntries.size(); ++i) {
            if (!batchEntries[i].resolved)
                continue;

            PartialAggregationResult partial;
            partial.measurement = measurement;
            partial.fieldName = contexts[i].field;
            partial.totalPoints = 1;

            auto gkr = timestar::buildGroupKeyDirect(measurement, contexts[i].field,
                                                     contexts[i].tags, groupByTags);
            partial.groupKey = std::move(gkr.key);
            partial.groupKeyHash = gkr.hash;
            partial.cachedTags = std::move(gkr.tags);

            AggregationState state;
            state.addValue(batchEntries[i].value, batchEntries[i].timestamp);
            partial.collapsedState = std::move(state);

            pushdownPartials.push_back(std::move(partial));
        }

        // Collect unresolved series (e.g. string types) for
        // standard per-series fallback below.
        std::vector<SeriesQueryContext> unresolvedContexts;
        for (size_t i = 0; i < batchEntries.size(); ++i) {
            if (!batchEntries[i].resolved) {
                unresolvedContexts.push_back(std::move(contexts[i]));
            }
        }
        contexts = std::move(unresolvedContexts);
    }

    if (contexts.empty()) {
        // All series resolved by batch or streaming path — skip standard path.
    } else if (canStreamAggregation(aggregation, groupByTags, aggregationInterval)) {
        // ---- STREAMING AGGREGATION PATH ----
        // Fold per-series results into accumulators without
        // materializing all raw data.  O(groups x buckets)
        // memory instead of O(points).  Works for both
        // group-by and non-group-by queries.
        pushdownPartials = co_await streamingGroupByAggregation(
            engine, contexts, measurement, startTime, endTime, aggregation, aggregationInterval,
            groupByTags);

    } else {
        // ---- STANDARD PER-SERIES PATH ----

        // Prefetch TSM index entries for all series on this shard.
        // Warms the full-index cache in parallel so that per-series
        // queries hit cache instead of issuing individual DMA reads.
        {
            std::vector<SeriesId128> prefetchIds;
            prefetchIds.reserve(contexts.size());
            for (const auto& ctx : contexts) {
                prefetchIds.push_back(ctx.seriesId);
            }
            co_await engine.prefetchSeriesIndices(prefetchIds);
        }

        // Use max_concurrent_for_each to avoid creating thousands of
        // concurrent futures at once. Bounded concurrency reduces memory
        // pressure and context thrashing.
        {
            static constexpr size_t MAX_CONCURRENT_SERIES_QUERIES = 64;

            co_await seastar::max_concurrent_for_each(
                contexts, MAX_CONCURRENT_SERIES_QUERIES,
                [&engine, &pushdownPartials, &fallbackResults, &measurement, startTime, endTime,
                 shardId, aggregationInterval, aggregation,
                 &groupByTags](SeriesQueryContext& ctx) -> seastar::future<> {
                    // ---- PUSHDOWN PATH ----
                    // Try aggregating directly from TSM blocks, skipping the
                    // full TSMResult → QueryResult → SeriesResult pipeline.
                    auto pushdownResult = co_await engine.queryAggregated(
                        ctx.seriesKey, ctx.seriesId, startTime, endTime, aggregationInterval,
                        aggregation);

                    if (pushdownResult.has_value()) {
                        // Build PartialAggregationResult directly from PushdownResult
                        PartialAggregationResult partial;
                        partial.measurement = measurement;
                        partial.fieldName = ctx.field;
                        partial.totalPoints = pushdownResult->totalPoints;

                        // Build composite groupKey (same format as createPartialAggregations)
                        auto gkr = timestar::buildGroupKeyDirect(measurement, ctx.field,
                                                                 ctx.tags, groupByTags);
                        partial.groupKey = std::move(gkr.key);
                        partial.groupKeyHash = gkr.hash;
                        partial.cachedTags = std::move(gkr.tags);

                        if (aggregationInterval > 0) {
                            partial.bucketStates = std::move(pushdownResult->bucketStates);
                        } else if (pushdownResult->aggregatedState.has_value()) {
                            partial.collapsedState = std::move(pushdownResult->aggregatedState);
                        } else {
                            partial.sortedTimestamps =
                                std::move(pushdownResult->sortedTimestamps);
                            partial.sortedValues = std::move(pushdownResult->sortedValues);
                        }

                        pushdownPartials.push_back(std::move(partial));
                        co_return;
                    }

                    // ---- FALLBACK PATH ----
                    // Pushdown not applicable (non-float, memory data, overlap).
                    std::optional<VariantQueryResult> optResult;
                    try {
                        optResult = co_await engine.query(ctx.seriesKey, ctx.seriesId,
                                                          startTime, endTime);
                    } catch (const SeriesNotFoundException&) {
                        LOG_QUERY_PATH(timestar::http_log, info,
                                       "[QUERY] Series '{}' not found on shard {} - skipping",
                                       ctx.seriesKey, shardId);
                        co_return;
                    } catch (const std::exception& e) {
                        LOG_QUERY_PATH(timestar::http_log, warn,
                                       "[QUERY] Unexpected error querying series '{}' on shard "
                                       "{}: {} - skipping",
                                       ctx.seriesKey, shardId, e.what());
                        co_return;
                    }

                    if (!optResult.has_value()) {
                        co_return;
                    }

                    auto variantResult = std::move(optResult.value());

                    timestar::SeriesResult seriesResult;
                    seriesResult.measurement = measurement;
                    seriesResult.tags = std::move(ctx.tags);

                    std::visit(
                        [&seriesResult, &ctx](auto&& result) {
                            using T = std::decay_t<decltype(result)>;
                            if constexpr (std::is_same_v<T, QueryResult<double>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] =
                                        std::make_pair(std::move(result.timestamps),
                                                       FieldValues(std::move(result.values)));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<bool>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(
                                        std::move(result.timestamps),
                                        FieldValues(std::move(result.values)));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<std::string>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] =
                                        std::make_pair(std::move(result.timestamps),
                                                       FieldValues(std::move(result.values)));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<int64_t>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] =
                                        std::make_pair(std::move(result.timestamps),
                                                       FieldValues(std::move(result.values)));
                                }
                            }
                        },
                        variantResult);

                    if (!seriesResult.fields.empty()) {
                        fallbackResults.push_back(std::move(seriesResult));
                    }
                });
        }  // end of non-batch per-series loop
    }  // end of batch-vs-standard if/else

    // Separate non-numeric (string) fallback results from numeric.
    // String data bypasses aggregation and is returned as-is.  Boolean fields
    // are converted to numeric 0/1 here so the fallback path agrees with the
    // pushdown path, which already aggregates booleans numerically — without
    // this, the same query returned true/false or 0/1 depending on where the
    // data happened to sit (memory vs TSM).
    std::vector<timestar::SeriesResult> stringResults;
    std::vector<timestar::SeriesResult> numericResults;
    for (auto& sr : fallbackResults) {
        bool hasStringField = false;
        for (auto& [fn, fd] : sr.fields) {
            if (std::holds_alternative<std::vector<std::string>>(fd.second)) {
                hasStringField = true;
            } else if (auto* boolVals = std::get_if<std::vector<bool>>(&fd.second)) {
                std::vector<double> numeric;
                numeric.reserve(boolVals->size());
                for (bool b : *boolVals) {
                    numeric.push_back(b ? 1.0 : 0.0);
                }
                fd.second = std::move(numeric);
            }
        }
        if (hasStringField) {
            stringResults.push_back(std::move(sr));
        } else {
            numericResults.push_back(std::move(sr));
        }
    }

    // Run partial aggregation only on fallback numeric results
    auto partialAggStart = std::chrono::high_resolution_clock::now();
    auto partialResults = Aggregator::createPartialAggregations(
        numericResults, aggregation, aggregationInterval, groupByTags);
    // Combine pushdown partials with fallback partials
    partialResults.insert(partialResults.end(),
                          std::make_move_iterator(pushdownPartials.begin()),
                          std::make_move_iterator(pushdownPartials.end()));
    auto partialAggEnd = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] double partialAggMs =
        std::chrono::duration<double, std::milli>(partialAggEnd - partialAggStart).count();  // used by LOG_QUERY_PATH

    auto shardEnd = std::chrono::high_resolution_clock::now();
    double shardMs = std::chrono::duration<double, std::milli>(shardEnd - shardStart).count();
    LOG_QUERY_PATH(timestar::http_log, info,
                   "[QUERY] Shard {} completed {} parallel queries + partial aggregation in "
                   "{:.2f} ms (aggregation: {:.2f} ms)",
                   shardId, contexts.size(), shardMs, partialAggMs);
    ShardQueryResult sqr;
    sqr.partialResults = std::move(partialResults);
    sqr.stringResults = std::move(stringResults);
    sqr.shardMs = shardMs;
    co_return sqr;
}

// Phase 1: scatter-gather series discovery.  Each shard discovers its own
// series from its local index; results are combined per shard.  No centralized
// shard-0 bottleneck.  Sets timedOut when the discovery phase exceeds the
// query timeout, and limitExceeded when a shard hit the series limit.
seastar::future<SeriesDiscoveryResult> HttpQueryHandler::discoverSeriesAcrossShards(const QueryRequest& request) {
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0)
        shardCount = 1;

    // Split scopes into exact-match (drives the postings-bitmap intersect in
    // the index) and pattern (wildcard / ~regex / /regex/) scopes. Pattern
    // scopes cannot be looked up as literal bitmap keys — doing so returns
    // zero series — so they are applied as a post-filter on resolved series
    // metadata via SeriesMatcher. The index discovery only sees exactScopes.
    std::map<std::string, std::string> exactScopes;
    std::map<std::string, std::string> patternScopes;
    for (const auto& [k, v] : request.scopes) {
        if (SeriesMatcher::classifyScope(v) == ScopeMatchType::EXACT)
            exactScopes.emplace(k, v);
        else
            patternScopes.emplace(k, v);
    }

    // Fan out discovery to all shards in parallel
    std::vector<seastar::future<std::pair<unsigned, PerShardDiscovery>>> discoveryFutures;
    discoveryFutures.reserve(shardCount);
    for (unsigned s = 0; s < shardCount; ++s) {
        auto f =
            engineSharded
                ->invoke_on(s,
                            [measurement = request.measurement, scopes = exactScopes,
                             patternScopes = patternScopes, fields = request.fields,
                             startTime = request.startTime, endTime = request.endTime,
                             maxSeries = maxSeriesCount()](Engine& engine) -> seastar::future<PerShardDiscovery> {
                                auto& index = engine.getIndex();
                                std::unordered_set<std::string> fieldFilter(fields.begin(), fields.end());
                                // Wide-range optimisation: use discovery cache for ranges > 365 days.
                                static constexpr uint64_t NS_PER_DAY = 86400ULL * 1'000'000'000ULL;
                                static constexpr uint64_t WIDE_RANGE_THRESHOLD = 365ULL * NS_PER_DAY;
                                const bool wideRange =
                                    (endTime > startTime) && (endTime - startTime) > WIDE_RANGE_THRESHOLD;

                                const std::vector<IndexBackend::SeriesWithMetadata>* swmPtr = nullptr;
                                std::shared_ptr<const std::vector<IndexBackend::SeriesWithMetadata>> cachedPtr;

                                PerShardDiscovery result;

                                if (wideRange) {
                                    auto cr = co_await index.findSeriesWithMetadataCached(measurement, scopes,
                                                                                          fieldFilter, maxSeries);
                                    if (!cr.has_value()) {
                                        result.limitExceeded = true;
                                        result.discovered = cr.error().discovered;
                                        result.limit = cr.error().limit;
                                        co_return result;
                                    }
                                    cachedPtr = std::move(*cr);
                                    swmPtr = cachedPtr.get();
                                } else {
                                    // Day-scoped discovery is cached too (shared
                                    // immutable vector — no per-query metadata copies).
                                    auto findResult = co_await index.findSeriesWithMetadataTimeScopedCached(
                                        measurement, scopes, fieldFilter, startTime, endTime, maxSeries);
                                    if (!findResult.has_value()) {
                                        result.limitExceeded = true;
                                        result.discovered = findResult.error().discovered;
                                        result.limit = findResult.error().limit;
                                        co_return result;
                                    }
                                    cachedPtr = std::move(*findResult);
                                    swmPtr = cachedPtr.get();
                                }

                                result.contexts.reserve(swmPtr->size());

                                for (const auto& swm : *swmPtr) {
                                    // Apply wildcard/regex scopes that the bitmap intersect could not.
                                    if (!patternScopes.empty() &&
                                        !SeriesMatcher::matches(swm.metadata.tags, patternScopes)) {
                                        continue;
                                    }
                                    SeriesQueryContext ctx;
                                    ctx.seriesKey = buildSeriesKey(swm.metadata.measurement, swm.metadata.tags,
                                                                   swm.metadata.field);
                                    ctx.seriesId = swm.seriesId;
                                    ctx.field = swm.metadata.field;
                                    ctx.tags = swm.metadata.tags;
                                    result.contexts.push_back(std::move(ctx));
                                }

                                co_return result;
                            })
                .then([s](PerShardDiscovery result) { return std::make_pair(s, std::move(result)); });
        discoveryFutures.push_back(std::move(f));
    }

    // Wait for all shards to complete discovery, with timeout to prevent indefinite hangs
    auto discoveryDeadline = seastar::lowres_clock::now() + defaultQueryTimeout();
    std::vector<std::pair<unsigned, PerShardDiscovery>> discoveryResults;
    SeriesDiscoveryResult discoveryResult;
    try {
        discoveryResults = co_await seastar::with_timeout(
            discoveryDeadline, seastar::when_all_succeed(discoveryFutures.begin(), discoveryFutures.end()));
    } catch (seastar::timed_out_error&) {
        discoveryResult.timedOut = true;
        co_return discoveryResult;
    }

    // Combine discovery results into seriesByShard
    discoveryResult.seriesByShard.resize(shardCount);

    for (auto& [s, perShard] : discoveryResults) {
        if (perShard.limitExceeded) {
            discoveryResult.limitExceeded = true;
            discoveryResult.discovered = perShard.discovered;
            discoveryResult.limit = perShard.limit;
            break;
        }
        discoveryResult.seriesByShard[s] = std::move(perShard.contexts);
    }

    co_return discoveryResult;
}

// Phase 2: fan out query execution to every shard that owns series.
// Consumes (moves from) the per-shard context vectors in seriesByShard.
// Sets timedOut when the shard query phase exceeds the query timeout.
seastar::future<ShardFanOutResult> HttpQueryHandler::fanOutShardQueries(
    const QueryRequest& request, std::vector<std::vector<SeriesQueryContext>>& seriesByShard) {
    std::vector<seastar::future<std::pair<unsigned, ShardQueryResult>>> futures;
    for (unsigned shardId = 0; shardId < seriesByShard.size(); ++shardId) {
        if (seriesByShard[shardId].empty())
            continue;
        auto& shardContexts = seriesByShard[shardId];
        auto f =
            engineSharded
                ->invoke_on(shardId,
                            [shardId, contexts = std::move(shardContexts), startTime = request.startTime,
                             endTime = request.endTime, measurement = request.measurement,
                             aggregation = request.aggregation, aggregationInterval = request.aggregationInterval,
                             groupByTags = request.groupByTags](Engine& engine) mutable {
                                // executeShardQuery takes its parameters by value: they are moved
                                // into the coroutine frame at invocation, so the closure (and its
                                // captures) need not outlive the returned future.
                                return executeShardQuery(engine, shardId, std::move(contexts), std::move(measurement),
                                                         startTime, endTime, aggregation, aggregationInterval,
                                                         std::move(groupByTags));
                            })
                .then([shardId](ShardQueryResult result) { return std::make_pair(shardId, std::move(result)); });
        futures.push_back(std::move(f));
    }

    // Wait for all shards to complete, with timeout to prevent indefinite hangs
    auto deadline = seastar::lowres_clock::now() + defaultQueryTimeout();
    ShardFanOutResult fanOut;
    try {
        fanOut.shardResults =
            co_await seastar::with_timeout(deadline, seastar::when_all_succeed(futures.begin(), futures.end()));
    } catch (seastar::timed_out_error&) {
        fanOut.timedOut = true;
    }
    co_return fanOut;
}

// Phase 3a: single-shard fast path finalize.
// All matching series live on one shard — skip the cross-shard merge pipeline.
// finalizeSingleShardPartials() produces SeriesResult entries directly from
// PartialAggregationResult, bypassing mergePartialAggregationsGrouped() and
// the GroupedAggregationResult intermediary.
//
// Fills `response` in place and returns std::nullopt on success; returns a
// complete error QueryResponse when a limit was exceeded (the caller returns
// it as-is).
// Consolidate series that share the same measurement+tags into a single
// SeriesResult with multiple fields.  The aggregation paths emit one series
// per field; the HTTP response format groups fields under a single series
// entry.  Called by BOTH finalize paths so the response shape does not depend
// on which shards the series landed on.
// Uses hash-indexed lookup for O(N) instead of O(N²) flat scan.
void HttpQueryHandler::consolidateSeriesFields(std::vector<SeriesResult>& seriesList) {
    // Multimap from (measurement+tags) hash to candidate indices in `merged`.
    // Same hash with different tag-sets must coexist; without this, a hash
    // collision overwrote the bucket and subsequent merges for the first
    // tag-set would miss its existing entry.
    std::unordered_multimap<size_t, size_t> tagHashToIdx;
    tagHashToIdx.reserve(seriesList.size());
    std::vector<SeriesResult> merged;
    merged.reserve(seriesList.size());

    for (auto& s : seriesList) {
        size_t h = std::hash<std::string>{}(s.measurement);
        for (const auto& [k, v] : s.tags) {
            h ^= std::hash<std::string>{}(k) * 31 + std::hash<std::string>{}(v);
        }

        bool mergedIn = false;
        auto [lo, hi] = tagHashToIdx.equal_range(h);
        for (auto it = lo; it != hi; ++it) {
            auto& existing = merged[it->second];
            if (existing.measurement == s.measurement && existing.tags == s.tags) {
                for (auto& [fname, fdata] : s.fields) {
                    existing.fields[std::move(fname)] = std::move(fdata);
                }
                mergedIn = true;
                break;
            }
        }
        if (!mergedIn) {
            tagHashToIdx.emplace(h, merged.size());
            merged.push_back(std::move(s));
        }
    }
    seriesList = std::move(merged);
}

std::optional<QueryResponse> HttpQueryHandler::finalizeSingleShardResponse(
    const QueryRequest& request, std::pair<unsigned, ShardQueryResult>& shardResult, QueryTimingInfo& timing,
    QueryResponse& response) {
    auto& [singleShardId, sqr] = shardResult;
    timing.perShardQueryMs.push_back({singleShardId, sqr.shardMs});

    for (const auto& partial : sqr.partialResults) {
        timing.totalPointsRetrieved += partial.totalPoints;
    }
    timing.resultCollectionMs = 0.0;

    bool aggregationReducesOutput = request.aggregationInterval > 0 || !request.groupByTags.empty();
    if (!aggregationReducesOutput) {
        for (const auto& p : sqr.partialResults) {
            if (p.collapsedState.has_value()) {
                aggregationReducesOutput = true;
                break;
            }
        }
    }
    if (!aggregationReducesOutput && timing.totalPointsRetrieved > maxTotalPoints()) {
        QueryResponse limitResponse;
        limitResponse.success = false;
        limitResponse.errorCode = "TOO_MANY_POINTS";
        limitResponse.errorMessage = "Total points " + std::to_string(timing.totalPointsRetrieved) +
                                     " exceeds limit of " + std::to_string(maxTotalPoints());
        limitResponse.statistics.truncated = true;
        limitResponse.statistics.truncationReason = limitResponse.errorMessage;
        return limitResponse;
    }

    auto aggregationStart = std::chrono::high_resolution_clock::now();
    std::unordered_set<std::string> requestedFieldSet(request.fields.begin(), request.fields.end());
    if (!sqr.partialResults.empty()) {
        // Check for duplicate groupKeys: non-group-by queries with multiple matching
        // series produce partials that share the same groupKey. finalizeSingleShardPartials
        // would overwrite earlier data, so we fall back to the merge path for correctness.
        bool hasDuplicateGroupKeys = false;
        if (sqr.partialResults.size() > 1) {
            std::unordered_set<std::string_view> seen;
            seen.reserve(sqr.partialResults.size());
            for (const auto& p : sqr.partialResults) {
                if (!seen.insert(p.groupKey).second) {
                    hasDuplicateGroupKeys = true;
                    break;
                }
            }
        }

        if (!hasDuplicateGroupKeys) {
            // All groupKeys unique — direct finalization (no merge needed)
            finalizeSingleShardPartials(sqr.partialResults, request.aggregation, response);
        } else {
            // Duplicate groupKeys — must merge partials before finalizing
            auto groupedResults =
                Aggregator::mergePartialAggregationsGrouped(sqr.partialResults, request.aggregation);

            response.series.reserve(groupedResults.size());
            for (auto& groupedResult : groupedResults) {
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                if (!groupedResult.rawTimestamps.empty()) {
                    timestamps = std::move(groupedResult.rawTimestamps);
                    values = std::move(groupedResult.rawValues);
                } else {
                    timestamps.reserve(groupedResult.points.size());
                    values.reserve(groupedResult.points.size());
                    for (const auto& point : groupedResult.points) {
                        timestamps.push_back(point.timestamp);
                        values.push_back(point.value);
                    }
                }

                // Each grouped result produces its own series (one field per series)
                SeriesResult series;
                series.measurement = std::move(groupedResult.measurement);
                series.tags = std::move(groupedResult.tags);
                std::string fieldName = std::move(groupedResult.fieldName);
                series.fields[fieldName] =
                    std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

                response.series.push_back(std::move(series));
            }
        }

        // Same consolidation as the multi-shard path: without it, the response
        // shape depended on shard placement (multi-field series consolidated
        // only when the fields happened to land on different shards).
        consolidateSeriesFields(response.series);

        if (!requestedFieldSet.empty()) {
            for (auto& s : response.series) {
                std::erase_if(s.fields, [&](const auto& kv) { return !requestedFieldSet.contains(kv.first); });
            }
            std::erase_if(response.series, [](const SeriesResult& s) { return s.fields.empty(); });
        }
    }

    if (!sqr.stringResults.empty()) {
        if (!requestedFieldSet.empty()) {
            for (auto& sr : sqr.stringResults) {
                std::erase_if(sr.fields,
                              [&](const auto& item) { return !requestedFieldSet.contains(item.first); });
            }
            std::erase_if(sqr.stringResults, [](const SeriesResult& s) { return s.fields.empty(); });
        }
        // Each string/bool result is its own series entry (one field per series)
        for (auto& strResult : sqr.stringResults) {
            response.series.push_back(std::move(strResult));
        }
    }

    response.statistics.pointCount = 0;
    timing.finalPointsReturned = 0;
    for (const auto& series : response.series) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            size_t points = fieldData.first.size();
            response.statistics.pointCount += points;
            timing.finalPointsReturned += points;
        }
    }
    if (response.statistics.pointCount > maxTotalPoints()) {
        response.statistics.truncated = true;
        response.statistics.truncationReason = "Total points " +
                                               std::to_string(response.statistics.pointCount) +
                                               " exceeds limit of " + std::to_string(maxTotalPoints());
        response.success = false;
        response.errorCode = "TOO_MANY_POINTS";
        response.errorMessage = response.statistics.truncationReason;
        return std::move(response);
    }

    auto aggregationEnd = std::chrono::high_resolution_clock::now();
    timing.aggregationMs = std::chrono::duration<double, std::milli>(aggregationEnd - aggregationStart).count();

    return std::nullopt;
}

// Phase 3b: multi-shard merge + aggregation finalize.
// Collects partial aggregations from all shards, merges them grouped, and
// builds the response series.  Fills `response` in place and returns
// std::nullopt on success; returns a complete error QueryResponse when a
// limit was exceeded (the caller returns it as-is).
std::optional<QueryResponse> HttpQueryHandler::finalizeMultiShardResponse(
    const QueryRequest& request, std::vector<std::pair<unsigned, ShardQueryResult>>& shardResults,
    QueryTimingInfo& timing, QueryResponse& response) {
    auto mergeStart = std::chrono::high_resolution_clock::now();

    std::vector<PartialAggregationResult> allPartialResults;
    std::vector<SeriesResult> allStringResults;
    size_t totalPartialResults = 0;
    for (const auto& sr : shardResults) {
        totalPartialResults += sr.second.partialResults.size();
    }
    allPartialResults.reserve(totalPartialResults);

    for (auto& shardResult : shardResults) {
        unsigned shardId = shardResult.first;
        auto& sqr = shardResult.second;
        timing.perShardQueryMs.push_back({shardId, sqr.shardMs});

        // Count points from partial results
        for (const auto& partial : sqr.partialResults) {
            timing.totalPointsRetrieved += partial.totalPoints;
        }

        // Collect all partial results
        allPartialResults.insert(allPartialResults.end(), std::make_move_iterator(sqr.partialResults.begin()),
                                 std::make_move_iterator(sqr.partialResults.end()));

        // Collect string results (these bypass aggregation)
        allStringResults.insert(allStringResults.end(), std::make_move_iterator(sqr.stringResults.begin()),
                                std::make_move_iterator(sqr.stringResults.end()));
    }

    auto mergeEnd = std::chrono::high_resolution_clock::now();
    timing.resultCollectionMs = std::chrono::duration<double, std::milli>(mergeEnd - mergeStart).count();

    // Early point-count check: totalPointsRetrieved is an upper bound on
    // the final output (merging can only reduce counts via timestamp dedup).
    // Fail fast before the expensive merge + JSON serialization phase.
    //
    // Skip when aggregation or group-by is active: the aggregation pipeline
    // (including pushdown) reduces output far below the raw point count, so
    // totalPointsRetrieved would be a massive overcount.  The final output
    // limit is still enforced after aggregation (line ~884).
    bool aggregationReducesOutput = request.aggregationInterval > 0 || !request.groupByTags.empty();
    if (!aggregationReducesOutput) {
        for (const auto& p : allPartialResults) {
            if (p.collapsedState.has_value()) {
                aggregationReducesOutput = true;
                break;
            }
        }
    }
    if (!aggregationReducesOutput && timing.totalPointsRetrieved > maxTotalPoints()) {
        QueryResponse limitResponse;
        limitResponse.success = false;
        limitResponse.errorCode = "TOO_MANY_POINTS";
        limitResponse.errorMessage = "Total points " + std::to_string(timing.totalPointsRetrieved) +
                                     " exceeds limit of " + std::to_string(maxTotalPoints());
        limitResponse.statistics.truncated = true;
        limitResponse.statistics.truncationReason = limitResponse.errorMessage;
        return limitResponse;
    }

    // Merge partial aggregations from all shards into final aggregated points
    auto aggregationStart = std::chrono::high_resolution_clock::now();

    // Build field filter set once, shared by both numeric and string result paths
    std::unordered_set<std::string> requestedFieldSet(request.fields.begin(), request.fields.end());

    if (!allPartialResults.empty()) {
        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Merging {} partial aggregations from {} shards",
                       allPartialResults.size(), timing.shardsQueried);

        // OPTIMIZATION & FIX: Use grouped merge to preserve metadata associations
        auto groupedResults =
            Aggregator::mergePartialAggregationsGrouped(allPartialResults, request.aggregation);

        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Merged into {} grouped results",
                       groupedResults.size());

        // Each grouped result produces its own series (one field per series).
        response.series.reserve(groupedResults.size());

        for (auto& groupedResult : groupedResults) {
            // Build timestamps and values for this field.
            // Fast path: if the merge returned raw vectors (single-partial
            // pushdown), move them directly without the AggregatedPoint split.
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            if (!groupedResult.rawTimestamps.empty()) {
                timestamps = std::move(groupedResult.rawTimestamps);
                values = std::move(groupedResult.rawValues);
            } else {
                timestamps.reserve(groupedResult.points.size());
                values.reserve(groupedResult.points.size());
                for (const auto& point : groupedResult.points) {
                    timestamps.push_back(point.timestamp);
                    values.push_back(point.value);
                }
            }

            // Each grouped result produces its own series (one field per series)
            SeriesResult series;
            series.measurement = std::move(groupedResult.measurement);
            series.tags = std::move(groupedResult.tags);
            series.fields[std::move(groupedResult.fieldName)] =
                std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

            response.series.push_back(std::move(series));
        }

        // Consolidate series that share the same measurement+tags into
        // a single SeriesResult with multiple fields.  The aggregation
        // loop above emits one SeriesResult per field; the HTTP response
        // format groups fields under a single series entry.
        consolidateSeriesFields(response.series);

        // Filter fields: remove non-requested fields from each series, then remove empty series.
        if (!requestedFieldSet.empty()) {
            for (auto& s : response.series) {
                std::erase_if(s.fields, [&](const auto& kv) { return !requestedFieldSet.contains(kv.first); });
            }
            std::erase_if(response.series, [](const SeriesResult& s) { return s.fields.empty(); });
        }
    }
    // Add string results that bypassed aggregation directly to response
    if (!allStringResults.empty()) {
        // Filter string results using the shared requestedFieldSet.
        // When requestedFieldSet is empty (all fields requested), skip filtering entirely.
        // When filtering is needed, erase non-matching fields in-place to avoid
        // building a temporary map and copying/moving entries.
        if (!requestedFieldSet.empty()) {
            for (auto& sr : allStringResults) {
                std::erase_if(sr.fields,
                              [&](const auto& item) { return !requestedFieldSet.contains(item.first); });
            }
            // Remove string series with no fields after filtering
            std::erase_if(allStringResults, [](const SeriesResult& s) { return s.fields.empty(); });
        }

        // Each string/bool result is its own series entry (one field per series)
        for (auto& strResult : allStringResults) {
            response.series.push_back(std::move(strResult));
        }
    }

    // Update statistics and enforce maxTotalPoints() (covers both numeric and string results)
    response.statistics.pointCount = 0;
    timing.finalPointsReturned = 0;
    for (const auto& series : response.series) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            size_t points = fieldData.first.size();
            response.statistics.pointCount += points;
            timing.finalPointsReturned += points;
        }
    }

    // Enforce maxTotalPoints() limit
    if (response.statistics.pointCount > maxTotalPoints()) {
        response.statistics.truncated = true;
        response.statistics.truncationReason = "Total points " +
                                               std::to_string(response.statistics.pointCount) +
                                               " exceeds limit of " + std::to_string(maxTotalPoints());
        response.success = false;
        response.errorCode = "TOO_MANY_POINTS";
        response.errorMessage = response.statistics.truncationReason;
        return std::move(response);
    }

    auto aggregationEnd = std::chrono::high_resolution_clock::now();
    timing.aggregationMs = std::chrono::duration<double, std::milli>(aggregationEnd - aggregationStart).count();

    return std::nullopt;
}

// Phase 4: timing breakdown + slow-query logging.
void HttpQueryHandler::logQueryCompletion(const QueryRequest& request, QueryTimingInfo& timing) {
    // Calculate total timing
    timing.endTime = std::chrono::high_resolution_clock::now();
    timing.totalMs = std::chrono::duration<double, std::milli>(timing.endTime - timing.startTime).count();

    // Print timing information
    LOG_QUERY_PATH(timestar::http_log, info, "{}", timing.toString());

    // Slow query detection (always compiled in, runtime configurable)
    const auto slowThresholdMs = timestar::config().http.slow_query_threshold_ms;
    if (slowThresholdMs > 0 && timing.totalMs > static_cast<double>(slowThresholdMs)) {
        if (engineSharded)
            ++engineSharded->local().metrics().slow_queries_total;
        timestar::query_log.warn(
            "[SLOW_QUERY] {:.1f}ms (threshold {}ms) | "
            "measurement={} series={} points={} shards={} | "
            "discovery={:.1f}ms shard_queries={:.1f}ms aggregation={:.1f}ms",
            timing.totalMs, slowThresholdMs, request.measurement, timing.seriesFound, timing.finalPointsReturned,
            timing.shardsQueried, timing.findSeriesMs, timing.shardQueriesMs, timing.aggregationMs);

        // Per-shard breakdown for slow queries (only log shards that took >50% of threshold)
        const double shardWarnMs = static_cast<double>(slowThresholdMs) / 2.0;
        for (const auto& [shardId, ms] : timing.perShardQueryMs) {
            if (ms > shardWarnMs) {
                timestar::query_log.warn("[SLOW_QUERY]   shard {} took {:.1f}ms", shardId, ms);
            }
        }
    }
}

seastar::future<QueryResponse> HttpQueryHandler::executeQuery(const QueryRequest& request_in) {
    // Copy needed: aggregationInterval may be mutated for interval collapse below.
    QueryRequest request = request_in;

    // Interval collapse for LATEST/FIRST: when the aggregation interval covers
    // the entire query range, drop it so the non-bucketed fast path is used
    // (sparse index zero-I/O lookup instead of bucketed block reads).
    if (request.aggregationInterval > 0 && request.endTime > request.startTime &&
        (request.aggregation == AggregationMethod::LATEST || request.aggregation == AggregationMethod::FIRST)) {
        uint64_t range = request.endTime - request.startTime;
        if (request.aggregationInterval >= range) {
            request.aggregationInterval = 0;
        }
    }

    LOG_QUERY_PATH(timestar::http_log, info,
                   "[QUERY] Executing query - Measurement: {}, Fields: {}, Scopes: {}, Start: {}, End: {}",
                   request.measurement, request.fields.size(), request.scopes.size(), request.startTime,
                   request.endTime);

    QueryResponse response;
    response.success = true;

    // Initialize timing tracker
    QueryTimingInfo timing;
    timing.startTime = std::chrono::high_resolution_clock::now();

    // Increment query counter on the local shard
    if (engineSharded) {
        ++engineSharded->local().metrics().queries_total;
    }

    try {
        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Checking pointer - engineSharded: {}",
                       static_cast<const void*>(engineSharded));

        if (!engineSharded) {
            LOG_QUERY_PATH(timestar::http_log, error, "[QUERY] engineSharded is NULL!");
            response.success = false;
            response.errorCode = "NULL_ENGINE";
            response.errorMessage = "Engine pointer is null";
            co_return response;
        }

        // Scatter-gather: each shard discovers its own series from its local index,
        // then we combine the results. No centralized shard-0 bottleneck.
        auto findSeriesStart = std::chrono::high_resolution_clock::now();

        SeriesDiscoveryResult discoveryResult = co_await discoverSeriesAcrossShards(request);

        if (discoveryResult.timedOut) {
            QueryResponse timeoutResponse;
            timeoutResponse.success = false;
            timeoutResponse.errorCode = "QUERY_TIMEOUT";
            timeoutResponse.errorMessage =
                "Discovery phase timed out after " + std::to_string(defaultQueryTimeout().count()) + " seconds";
            co_return timeoutResponse;
        }

        auto findSeriesEnd = std::chrono::high_resolution_clock::now();
        timing.findSeriesMs = std::chrono::duration<double, std::milli>(findSeriesEnd - findSeriesStart).count();

        // Check if the series limit was exceeded in the index layer (early bailout)
        if (discoveryResult.limitExceeded) {
            response.success = false;
            response.errorCode = "TOO_MANY_SERIES";
            response.errorMessage = "Too many series: " + std::to_string(discoveryResult.discovered) +
                                    " exceeds limit of " + std::to_string(discoveryResult.limit);
            co_return response;
        }

        auto& seriesByShard = discoveryResult.seriesByShard;

        // Compute total series count from the grouped buckets
        size_t totalSeriesFound = 0;
        size_t shardsWithData = 0;
        for (const auto& contexts : seriesByShard) {
            if (!contexts.empty()) {
                totalSeriesFound += contexts.size();
                ++shardsWithData;
            }
        }
        timing.seriesFound = totalSeriesFound;
        timing.shardsQueried = shardsWithData;

        LOG_QUERY_PATH(timestar::http_log, info,
                       "[QUERY] Total series from centralized metadata: {} across {} shards (took {:.2f} ms)",
                       totalSeriesFound, shardsWithData, timing.findSeriesMs);

        // Safety net: enforce maxSeriesCount() limit (should rarely trigger now
        // that the index layer checks early, but kept for defense-in-depth)
        if (totalSeriesFound > maxSeriesCount()) {
            response.success = false;
            response.errorCode = "TOO_MANY_SERIES";
            response.errorMessage = "Too many series: " + std::to_string(totalSeriesFound) + " exceeds limit of " +
                                    std::to_string(maxSeriesCount());
            co_return response;
        }

        // Execute queries on each shard that has series
        auto shardQueriesStart = std::chrono::high_resolution_clock::now();
        ShardFanOutResult fanOut = co_await fanOutShardQueries(request, seriesByShard);
        if (fanOut.timedOut) {
            QueryResponse timeoutResponse;
            timeoutResponse.success = false;
            timeoutResponse.errorCode = "QUERY_TIMEOUT";
            timeoutResponse.errorMessage =
                "Query timed out after " + std::to_string(defaultQueryTimeout().count()) + " seconds";
            co_return timeoutResponse;
        }
        auto shardQueriesEnd = std::chrono::high_resolution_clock::now();
        timing.shardQueriesMs = std::chrono::duration<double, std::milli>(shardQueriesEnd - shardQueriesStart).count();

        auto& shardResults = fanOut.shardResults;
        if (shardResults.size() == 1) {
            // === SINGLE-SHARD FAST PATH ===
            if (auto earlyReturn = finalizeSingleShardResponse(request, shardResults[0], timing, response)) {
                co_return std::move(*earlyReturn);
            }
        } else {
            // === MULTI-SHARD GENERAL PATH ===
            if (auto earlyReturn = finalizeMultiShardResponse(request, shardResults, timing, response)) {
                co_return std::move(*earlyReturn);
            }
        }

        response.statistics.seriesCount = response.series.size();

        logQueryCompletion(request, timing);

    } catch (const std::exception& e) {
        response.success = false;
        response.errorCode = "QUERY_EXECUTION_ERROR";
        response.errorMessage = "Query execution failed";
        timestar::http_log.error("Query execution failed: {}", e.what());
    }

    co_return response;
}

std::string HttpQueryHandler::formatQueryResponse(QueryResponse& response) {
    return ResponseFormatter::format(response);
}

std::string HttpQueryHandler::createErrorResponse(const std::string& code, const std::string& message) {
    return ResponseFormatter::formatError(message, code);
}

// Test-only utility: production query path iterates shards inline in executeQuery().
std::vector<unsigned> HttpQueryHandler::determineTargetShards(const QueryRequest& /*request*/) {
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0)
        shardCount = 1;  // test environment default
    std::vector<unsigned> shards(shardCount);
    std::iota(shards.begin(), shards.end(), 0u);
    return shards;
}

// Test-only utility: flat concat. Production uses Aggregator::mergePartialAggregationsGrouped().
std::vector<SeriesResult> HttpQueryHandler::mergeResults(std::vector<std::vector<SeriesResult>> shardResults) {
    std::vector<SeriesResult> merged;
    for (auto& shardResult : shardResults) {
        merged.insert(merged.end(), std::make_move_iterator(shardResult.begin()),
                      std::make_move_iterator(shardResult.end()));
    }
    return merged;
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
            if (interval[i] == '.')
                hasDecimal = true;
            unitPos = i + 1;
        } else {
            break;
        }
    }

    if (unitPos == 0) {
        throw QueryParseException("Invalid interval format: no numeric value");
    }

    std::string valueStr = interval.substr(0, unitPos);
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
    } else if (unit.empty()) {
        throw QueryParseException("Interval '" + interval +
                                  "' has no unit suffix. Please specify a unit (ns, us, ms, s, m, h, d)");
    } else {
        throw QueryParseException("Unknown time unit: '" + unit + "'. Supported units: ns, us, ms, s, m, h, d");
    }

    // Use integer arithmetic when possible to avoid floating-point precision loss
    if (hasDecimal) {
        double value;
        try {
            value = std::stod(valueStr);
        } catch (const std::invalid_argument&) {
            throw QueryParseException("Invalid interval format: '" + valueStr + "' is not a valid number");
        } catch (const std::out_of_range&) {
            throw QueryParseException("Interval value overflow: " + interval +
                                      " exceeds maximum representable nanoseconds");
        }
        // Guard against NaN or Inf before any arithmetic: casting a non-finite
        // double to uint64_t is undefined behavior in C++.
        if (!std::isfinite(value) || value < 0) {
            throw QueryParseException("Invalid interval: '" + interval + "' must be a finite positive number");
        }
        double result = value * static_cast<double>(multiplier);
        // Clamp to UINT64_MAX on overflow — any interval larger than the query
        // range simply produces a single bucket, so saturation is correct.
        if (!std::isfinite(result) || result > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
            return std::numeric_limits<uint64_t>::max();
        }
        return static_cast<uint64_t>(result);
    } else {
        uint64_t value;
        try {
            value = std::stoull(valueStr);
        } catch (const std::out_of_range&) {
            throw QueryParseException("Interval value overflow: " + interval);
        }
        // Clamp to UINT64_MAX on overflow (same rationale as decimal path).
        if (multiplier > 1 && value > std::numeric_limits<uint64_t>::max() / multiplier) {
            return std::numeric_limits<uint64_t>::max();
        }
        return value * multiplier;
    }
}

}  // namespace timestar