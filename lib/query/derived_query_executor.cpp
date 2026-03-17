#include "derived_query_executor.hpp"

#include "anomaly/anomaly_executor.hpp"
#include "anomaly/anomaly_result.hpp"
#include "engine.hpp"
#include "http_query_handler.hpp"
#include "logger.hpp"
#include "native_index.hpp"

#include <glaze/glaze.hpp>

#include <chrono>
#include <regex>
#include <seastar/core/when_all.hh>

// Glaze reflection for JSON parsing - must be outside namespace
template <>
struct glz::meta<timestar::GlazeDerivedQueryRequest> {
    using T = timestar::GlazeDerivedQueryRequest;
    static constexpr auto value = object("queries", &T::queries, "formula", &T::formula, "startTime", &T::startTime,
                                         "endTime", &T::endTime, "aggregationInterval", &T::aggregationInterval);
};

template <>
struct glz::meta<timestar::GlazeDerivedQueryResponse::Statistics> {
    using T = timestar::GlazeDerivedQueryResponse::Statistics;
    static constexpr auto value =
        object("point_count", &T::pointCount, "execution_time_ms", &T::executionTimeMs, "sub_queries_executed",
               &T::subQueriesExecuted, "points_dropped_due_to_alignment", &T::pointsDroppedDueToAlignment);
};

template <>
struct glz::meta<timestar::GlazeDerivedQueryResponse::Error> {
    using T = timestar::GlazeDerivedQueryResponse::Error;
    static constexpr auto value = object("code", &T::code, "message", &T::message);
};

template <>
struct glz::meta<timestar::GlazeDerivedQueryResponse> {
    using T = timestar::GlazeDerivedQueryResponse;
    static constexpr auto value = object("status", &T::status, "timestamps", &T::timestamps, "values", &T::values,
                                         "formula", &T::formula, "statistics", &T::statistics, "error", &T::error);
};

// Anomaly response Glaze meta templates
template <>
struct glz::meta<timestar::GlazeAnomalySeriesPiece> {
    using T = timestar::GlazeAnomalySeriesPiece;
    static constexpr auto value =
        object("piece", &T::piece, "group_tags", &T::group_tags, "values", &T::values, "alert_value", &T::alert_value);
};

template <>
struct glz::meta<timestar::GlazeAnomalyStatistics> {
    using T = timestar::GlazeAnomalyStatistics;
    static constexpr auto value =
        object("algorithm", &T::algorithm, "bounds", &T::bounds, "seasonality", &T::seasonality, "anomaly_count",
               &T::anomaly_count, "total_points", &T::total_points, "execution_time_ms", &T::execution_time_ms);
};

template <>
struct glz::meta<timestar::GlazeAnomalyResponse::Error> {
    using T = timestar::GlazeAnomalyResponse::Error;
    static constexpr auto value = object("message", &T::message);
};

template <>
struct glz::meta<timestar::GlazeAnomalyResponse> {
    using T = timestar::GlazeAnomalyResponse;
    static constexpr auto value = object("status", &T::status, "times", &T::times, "series", &T::series, "statistics",
                                         &T::statistics, "error", &T::error);
};

// Forecast response Glaze meta templates
template <>
struct glz::meta<timestar::GlazeForecastSeriesPiece> {
    using T = timestar::GlazeForecastSeriesPiece;
    static constexpr auto value = object("piece", &T::piece, "group_tags", &T::group_tags, "values", &T::values);
};

template <>
struct glz::meta<timestar::GlazeForecastStatistics> {
    using T = timestar::GlazeForecastStatistics;
    static constexpr auto value =
        object("algorithm", &T::algorithm, "deviations", &T::deviations, "seasonality", &T::seasonality, "slope",
               &T::slope, "intercept", &T::intercept, "r_squared", &T::r_squared, "residual_std_dev",
               &T::residual_std_dev, "historical_points", &T::historical_points, "forecast_points", &T::forecast_points,
               "series_count", &T::series_count, "execution_time_ms", &T::execution_time_ms);
};

template <>
struct glz::meta<timestar::GlazeForecastResponse::Error> {
    using T = timestar::GlazeForecastResponse::Error;
    static constexpr auto value = object("message", &T::message);
};

template <>
struct glz::meta<timestar::GlazeForecastResponse> {
    using T = timestar::GlazeForecastResponse;
    static constexpr auto value =
        object("status", &T::status, "times", &T::times, "forecast_start_index", &T::forecast_start_index, "series",
               &T::series, "statistics", &T::statistics, "error", &T::error);
};

namespace timestar {

DerivedQueryExecutor::DerivedQueryExecutor(seastar::sharded<Engine>* engine,
                                           seastar::sharded<index::NativeIndex>* index, DerivedQueryConfig config)
    : engine_(engine), index_(index), config_(config) {}

seastar::future<DerivedQueryResult> DerivedQueryExecutor::execute(const DerivedQueryRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    DerivedQueryResult result;
    result.formula = request.formula;

    try {
        // Validate request
        validateRequest(request);

        // Execute all sub-queries in parallel
        auto subResults = co_await executeAllSubQueries(request);

        result.stats.subQueriesExecuted = subResults.size();

        // Check if we got any results
        if (subResults.empty()) {
            co_return result;
        }

        // Align all series to common timestamps
        SeriesAligner aligner(config_.alignmentStrategy, config_.interpolationMethod);
        if (request.aggregationInterval > 0) {
            aligner.setTargetInterval(request.aggregationInterval);
        }

        auto alignedSeries = aligner.align(subResults);
        auto alignStats = aligner.getStats();
        result.stats.pointsDroppedDueToAlignment = alignStats.pointsDropped;

        if (alignedSeries.empty()) {
            // No common timestamps found
            co_return result;
        }

        // Parse the formula (uses cached AST from executeWithAnomaly if available)
        std::unique_ptr<ExpressionNode> localAst;
        if (!cachedAst_) {
            ExpressionParser parser(request.formula);
            localAst = parser.parse();
        }
        const auto& ast = cachedAst_ ? *cachedAst_ : *localAst;

        // Convert to evaluator format
        ExpressionEvaluator::QueryResultMap evalInput;
        for (auto& [name, series] : alignedSeries) {
            evalInput[name] = std::move(series);
        }

        // Evaluate the expression
        ExpressionEvaluator evaluator;
        auto computed = evaluator.evaluate(ast, evalInput);

        // Store results (copy out of shared_ptr into DerivedQueryResult's own vector)
        result.timestamps = *computed.timestamps;
        result.values = std::move(computed.values);
        result.stats.pointCount = result.timestamps.size();

        // Record execution time
        auto endTime = std::chrono::high_resolution_clock::now();
        result.stats.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

        co_return result;

    } catch (const DerivedQueryException& e) {
        throw;
    } catch (const ExpressionParseException& e) {
        throw DerivedQueryException("Formula error: " + std::string(e.what()));
    } catch (const EvaluationException& e) {
        throw DerivedQueryException("Evaluation error: " + std::string(e.what()));
    } catch (const std::exception& e) {
        throw DerivedQueryException("Execution error: " + std::string(e.what()));
    }
}

seastar::future<DerivedQueryResult> DerivedQueryExecutor::executeFromJson(const std::string& jsonBody) {
    // Parse JSON request
    GlazeDerivedQueryRequest glazeReq;
    auto parseResult = glz::read_json(glazeReq, jsonBody);
    if (parseResult) {
        throw DerivedQueryException("Invalid JSON: " + glz::format_error(parseResult, jsonBody));
    }

    // Convert to DerivedQueryRequest
    DerivedQueryRequest request;
    request.formula = glazeReq.formula;
    request.startTime = glazeReq.startTime;
    request.endTime = glazeReq.endTime;

    // Parse aggregation interval if provided
    if (!glazeReq.aggregationInterval.empty()) {
        request.aggregationInterval = HttpQueryHandler::parseInterval(glazeReq.aggregationInterval);
    }

    // Parse each query string
    QueryParser queryParser;
    for (const auto& [name, queryStr] : glazeReq.queries) {
        try {
            auto queryReq = queryParser.parseQueryString(queryStr);
            queryReq.startTime = request.startTime;
            queryReq.endTime = request.endTime;
            request.queries[name] = queryReq;
        } catch (const QueryParseException& e) {
            throw DerivedQueryException("Error parsing query '" + name + "': " + e.what());
        }
    }

    // Execute
    co_return co_await execute(request);
}

std::string DerivedQueryExecutor::formatResponse(const DerivedQueryResult& result) {
    GlazeDerivedQueryResponse response;
    response.status = "success";
    response.timestamps = result.timestamps;
    response.values = result.values;
    response.formula = result.formula;
    response.statistics.pointCount = result.stats.pointCount;
    response.statistics.executionTimeMs = result.stats.executionTimeMs;
    response.statistics.subQueriesExecuted = result.stats.subQueriesExecuted;
    response.statistics.pointsDroppedDueToAlignment = result.stats.pointsDroppedDueToAlignment;

    return glz::write_json(response).value_or("{}");
}

std::string DerivedQueryExecutor::createErrorResponse(const std::string& code, const std::string& message) {
    GlazeDerivedQueryResponse response;
    response.status = "error";
    response.error.code = code;
    response.error.message = message;

    return glz::write_json(response).value_or("{}");
}

void DerivedQueryExecutor::validateRequest(const DerivedQueryRequest& request) {
    if (request.queries.size() > config_.maxSubQueries) {
        throw DerivedQueryException("Too many sub-queries: " + std::to_string(request.queries.size()) +
                                    " (max: " + std::to_string(config_.maxSubQueries) + ")");
    }

    // Additional validation handled by DerivedQueryRequest::validate()
    request.validate();
}

seastar::future<std::map<std::string, SubQueryResult>> DerivedQueryExecutor::executeAllSubQueries(
    const DerivedQueryRequest& request) {
    // Get the set of queries actually referenced in the formula
    auto referencedQueries = request.getReferencedQueries();

    // Build futures for each sub-query
    std::vector<seastar::future<std::pair<std::string, SubQueryResult>>> futures;

    for (const auto& [name, query] : request.queries) {
        // Only execute queries that are actually used in the formula
        if (referencedQueries.count(name) == 0) {
            continue;
        }

        futures.push_back(executeSubQuery(name, query).then([name](SubQueryResult result) {
            return std::make_pair(name, std::move(result));
        }));
    }

    // Execute all in parallel
    auto results = co_await seastar::when_all_succeed(futures.begin(), futures.end());

    // Collect results
    std::map<std::string, SubQueryResult> resultMap;
    for (auto& [name, result] : results) {
        resultMap[name] = std::move(result);
    }

    co_return resultMap;
}

seastar::future<SubQueryResult> DerivedQueryExecutor::executeSubQuery(const std::string& name,
                                                                      const QueryRequest& query) {
    // Create a query handler to execute the sub-query
    HttpQueryHandler handler(engine_, index_);

    auto response = co_await handler.executeQuery(query);

    if (!response.success) {
        throw DerivedQueryException("Sub-query '" + name + "' failed: " + response.errorMessage);
    }

    co_return convertQueryResponse(name, query, response.series);
}

SubQueryResult DerivedQueryExecutor::convertQueryResponse(const std::string& name, const QueryRequest& query,
                                                          const std::vector<SeriesResult>& results) {
    SubQueryResult subResult;
    subResult.queryName = name;
    subResult.measurement = query.measurement;

    if (results.empty()) {
        return subResult;
    }

    if (results.size() > 1) {
        throw DerivedQueryException("Sub-query '" + name + "' returned " + std::to_string(results.size()) +
                                    " series but derived queries require exactly one series. "
                                    "Add more specific scope filters to narrow the result.");
    }

    const auto& series = results[0];
    subResult.tags = series.tags;

    // Get the first field (or the requested field)
    std::string fieldName = query.fields.empty() ? "" : query.fields[0];

    for (const auto& [fname, fieldData] : series.fields) {
        if (!fieldName.empty() && fname != fieldName) {
            continue;
        }

        subResult.field = fname;
        subResult.timestamps = fieldData.first;

        // Extract values (must be numeric for derived metrics)
        if (std::holds_alternative<std::vector<double>>(fieldData.second)) {
            subResult.values = std::get<std::vector<double>>(fieldData.second);
        } else if (std::holds_alternative<std::vector<int64_t>>(fieldData.second)) {
            const auto& intVals = std::get<std::vector<int64_t>>(fieldData.second);
            subResult.values.reserve(intVals.size());
            for (int64_t v : intVals) {
                subResult.values.push_back(static_cast<double>(v));
            }
        } else {
            throw DerivedQueryException("Sub-query '" + name + "' returned non-numeric values");
        }

        break;  // Take first matching field
    }

    return subResult;
}

bool DerivedQueryExecutor::isAnomalyFormula(const std::string& formula) {
    // Check if formula starts with "anomalies("
    std::string trimmed = formula;
    // Trim leading whitespace
    size_t start = trimmed.find_first_not_of(" \t\n\r");
    if (start != std::string::npos) {
        trimmed = trimmed.substr(start);
    }
    return trimmed.rfind("anomalies(", 0) == 0;
}

bool DerivedQueryExecutor::isForecastFormula(const std::string& formula) {
    // Check if formula starts with "forecast("
    std::string trimmed = formula;
    // Trim leading whitespace
    size_t start = trimmed.find_first_not_of(" \t\n\r");
    if (start != std::string::npos) {
        trimmed = trimmed.substr(start);
    }
    return trimmed.rfind("forecast(", 0) == 0;
}

seastar::future<DerivedQueryResultVariant> DerivedQueryExecutor::executeWithAnomaly(
    const DerivedQueryRequest& request) {
    try {
        // Parse the formula to check if it's an anomaly or forecast function
        ExpressionParser parser(request.formula);
        auto ast = parser.parse();

        if (ast->type == ExprNodeType::ANOMALY_FUNCTION) {
            // Execute anomaly detection
            auto anomalyResult = co_await executeAnomalyDetection(request, *ast);
            co_return DerivedQueryResultVariant{std::move(anomalyResult)};
        } else if (ast->type == ExprNodeType::FORECAST_FUNCTION) {
            // Execute forecast
            auto forecastResult = co_await executeForecast(request, *ast);
            co_return DerivedQueryResultVariant{std::move(forecastResult)};
        } else {
            // Execute regular derived query, reusing the already-parsed AST
            cachedAst_ = ast.get();
            auto result = co_await execute(request);
            cachedAst_ = nullptr;
            co_return DerivedQueryResultVariant{std::move(result)};
        }
    } catch (const std::exception& e) {
        throw DerivedQueryException(e.what());
    }
}

seastar::future<DerivedQueryResultVariant> DerivedQueryExecutor::executeFromJsonWithAnomaly(
    const std::string& jsonBody) {
    // Parse JSON request
    GlazeDerivedQueryRequest glazeReq;
    auto parseResult = glz::read_json(glazeReq, jsonBody);
    if (parseResult) {
        throw DerivedQueryException("Invalid JSON: " + glz::format_error(parseResult, jsonBody));
    }

    // Convert to DerivedQueryRequest
    DerivedQueryRequest request;
    request.formula = glazeReq.formula;
    request.startTime = glazeReq.startTime;
    request.endTime = glazeReq.endTime;

    // Parse aggregation interval if provided
    if (!glazeReq.aggregationInterval.empty()) {
        request.aggregationInterval = HttpQueryHandler::parseInterval(glazeReq.aggregationInterval);
    }

    // Parse each query string
    QueryParser queryParser;
    for (const auto& [name, queryStr] : glazeReq.queries) {
        try {
            auto queryReq = queryParser.parseQueryString(queryStr);
            queryReq.startTime = request.startTime;
            queryReq.endTime = request.endTime;
            request.queries[name] = queryReq;
        } catch (const QueryParseException& e) {
            throw DerivedQueryException("Error parsing query '" + name + "': " + e.what());
        }
    }

    // Execute with anomaly support
    co_return co_await executeWithAnomaly(request);
}

seastar::future<anomaly::AnomalyQueryResult> DerivedQueryExecutor::executeAnomalyDetection(
    const DerivedQueryRequest& request, const ExpressionNode& anomalyNode) {
    auto startTime = std::chrono::high_resolution_clock::now();

    const auto& anomalyFunc = anomalyNode.asAnomalyFunction();
    const std::string& queryRef = anomalyFunc.queryRef;

    // Find the referenced query
    auto it = request.queries.find(queryRef);
    if (it == request.queries.end()) {
        throw DerivedQueryException("Query '" + queryRef + "' not found in queries map");
    }

    // Execute the sub-query
    SubQueryResult subResult = co_await executeSubQuery(queryRef, it->second);

    if (subResult.timestamps.empty()) {
        anomaly::AnomalyQueryResult emptyResult;
        emptyResult.success = true;
        co_return emptyResult;
    }

    // Configure anomaly detection
    anomaly::AnomalyConfig config;
    config.algorithm = anomaly::parseAlgorithm(anomalyFunc.algorithm);
    config.bounds = anomalyFunc.bounds;
    if (anomalyFunc.seasonality.has_value()) {
        config.seasonality = anomaly::parseSeasonality(anomalyFunc.seasonality.value());
    }

    // Build group tags from sub-query
    std::vector<std::string> groupTags;
    for (const auto& [key, value] : subResult.tags) {
        groupTags.push_back(key + "=" + value);
    }

    // Execute anomaly detection
    anomaly::AnomalyExecutor executor;
    auto result = executor.execute(subResult.timestamps, subResult.values, groupTags, config);

    // Record total execution time
    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

    co_return result;
}

std::string DerivedQueryExecutor::formatAnomalyResponse(const anomaly::AnomalyQueryResult& result) {
    GlazeAnomalyResponse response;
    response.status = result.success ? "success" : "error";

    if (!result.success) {
        response.error.message = result.errorMessage;
        return glz::write_json(response).value_or("{}");
    }

    response.times = result.times;

    // Convert series pieces
    for (const auto& piece : result.series) {
        GlazeAnomalySeriesPiece glazePiece;
        glazePiece.piece = piece.piece;
        glazePiece.group_tags = piece.groupTags;

        // Convert values, replacing NaN/Inf with nullopt for proper JSON null
        glazePiece.values.reserve(piece.values.size());
        for (double v : piece.values) {
            if (std::isnan(v) || std::isinf(v)) {
                glazePiece.values.push_back(std::nullopt);
            } else {
                glazePiece.values.push_back(v);
            }
        }

        glazePiece.alert_value = piece.alertValue;
        response.series.push_back(std::move(glazePiece));
    }

    // Statistics
    response.statistics.algorithm = result.statistics.algorithm;
    response.statistics.bounds = result.statistics.bounds;
    response.statistics.seasonality = result.statistics.seasonality;
    response.statistics.anomaly_count = result.statistics.anomalyCount;
    response.statistics.total_points = result.statistics.totalPoints;
    response.statistics.execution_time_ms = result.statistics.executionTimeMs;

    return glz::write_json(response).value_or("{}");
}

std::string DerivedQueryExecutor::formatResponseVariant(const DerivedQueryResultVariant& result) {
    if (std::holds_alternative<DerivedQueryResult>(result)) {
        return formatResponse(std::get<DerivedQueryResult>(result));
    } else if (std::holds_alternative<anomaly::AnomalyQueryResult>(result)) {
        return formatAnomalyResponse(std::get<anomaly::AnomalyQueryResult>(result));
    } else {
        return formatForecastResponse(std::get<forecast::ForecastQueryResult>(result));
    }
}

seastar::future<forecast::ForecastQueryResult> DerivedQueryExecutor::executeForecast(
    const DerivedQueryRequest& request, const ExpressionNode& forecastNode) {
    auto startTime = std::chrono::high_resolution_clock::now();

    const auto& forecastFunc = forecastNode.asForecastFunction();
    const std::string& queryRef = forecastFunc.queryRef;

    // Find the referenced query
    auto it = request.queries.find(queryRef);
    if (it == request.queries.end()) {
        throw DerivedQueryException("Query '" + queryRef + "' not found in queries map");
    }

    // Execute the sub-query
    SubQueryResult subResult = co_await executeSubQuery(queryRef, it->second);

    if (subResult.timestamps.empty()) {
        forecast::ForecastQueryResult emptyResult;
        emptyResult.success = true;
        co_return emptyResult;
    }

    // Configure forecast
    forecast::ForecastConfig config;
    config.algorithm = forecast::parseAlgorithm(forecastFunc.algorithm);
    config.deviations = forecastFunc.deviations;

    // Parse seasonality if provided (for seasonal algorithm)
    if (forecastFunc.seasonality.has_value()) {
        config.seasonality = anomaly::parseSeasonality(forecastFunc.seasonality.value());
    }

    // Parse model parameter if provided (for linear algorithm only)
    if (forecastFunc.model.has_value()) {
        try {
            config.linearModel = forecast::parseLinearModel(forecastFunc.model.value());
        } catch (const std::exception& e) {
            throw DerivedQueryException("Invalid model parameter: " + std::string(e.what()));
        }
    }

    // Parse history parameter if provided (for linear algorithm only)
    if (forecastFunc.history.has_value()) {
        try {
            config.historyDurationNs = forecast::parseDurationToNs(forecastFunc.history.value());
        } catch (const std::exception& e) {
            throw DerivedQueryException("Invalid history parameter: " + std::string(e.what()));
        }
    }

    // Filter data based on history parameter if specified
    std::vector<uint64_t> filteredTimestamps = subResult.timestamps;
    std::vector<double> filteredValues = subResult.values;

    if (config.historyDurationNs > 0 && !subResult.timestamps.empty()) {
        // Find the cutoff time (last timestamp - history duration)
        uint64_t lastTimestamp = subResult.timestamps.back();
        uint64_t cutoffTime = lastTimestamp > config.historyDurationNs ? lastTimestamp - config.historyDurationNs : 0;

        // Find the first index where timestamp >= cutoffTime
        size_t startIdx = 0;
        for (size_t i = 0; i < subResult.timestamps.size(); ++i) {
            if (subResult.timestamps[i] >= cutoffTime) {
                startIdx = i;
                break;
            }
        }

        // Filter the data
        if (startIdx > 0 && startIdx < subResult.timestamps.size()) {
            filteredTimestamps =
                std::vector<uint64_t>(subResult.timestamps.begin() + startIdx, subResult.timestamps.end());
            filteredValues = std::vector<double>(subResult.values.begin() + startIdx, subResult.values.end());
        }
    }

    // Determine forecast horizon based on query time range
    // Guard against unsigned underflow if endTime <= startTime
    uint64_t duration = (request.endTime > request.startTime) ? request.endTime - request.startTime : 0;
    if (duration > 0 && filteredTimestamps.size() >= 2) {
        uint64_t timeSpan = (filteredTimestamps.back() > filteredTimestamps.front())
                                ? filteredTimestamps.back() - filteredTimestamps.front()
                                : 0;
        uint64_t interval = timeSpan / (filteredTimestamps.size() - 1);
        if (interval > 0) {
            config.forecastHorizon = duration / interval;
        }
    }

    // Build group tags from sub-query
    std::vector<std::string> groupTags;
    for (const auto& [key, value] : subResult.tags) {
        groupTags.push_back(key + "=" + value);
    }

    // Execute forecast with filtered data
    forecast::ForecastExecutor executor;
    std::vector<std::vector<double>> seriesValues = {filteredValues};
    std::vector<std::vector<std::string>> seriesGroupTags = {groupTags};

    auto result = executor.executeMulti(filteredTimestamps, seriesValues, seriesGroupTags, config);

    // Record total execution time
    auto endTime = std::chrono::high_resolution_clock::now();
    result.statistics.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

    co_return result;
}

std::string DerivedQueryExecutor::formatForecastResponse(const forecast::ForecastQueryResult& result) {
    GlazeForecastResponse response;
    response.status = result.success ? "success" : "error";

    if (!result.success) {
        response.error.message = result.errorMessage;
        return glz::write_json(response).value_or("{}");
    }

    response.times = result.times;
    response.forecast_start_index = result.forecastStartIndex;

    // Convert series pieces
    for (const auto& piece : result.series) {
        GlazeForecastSeriesPiece glazePiece;
        glazePiece.piece = piece.piece;
        glazePiece.group_tags = piece.groupTags;

        // Convert values, replacing NaN/Inf with nullopt for proper JSON null
        glazePiece.values.reserve(piece.values.size());
        for (const auto& val : piece.values) {
            if (!val.has_value()) {
                glazePiece.values.push_back(std::nullopt);
            } else if (std::isnan(val.value()) || std::isinf(val.value())) {
                glazePiece.values.push_back(std::nullopt);
            } else {
                glazePiece.values.push_back(val.value());
            }
        }

        response.series.push_back(std::move(glazePiece));
    }

    // Statistics
    response.statistics.algorithm = result.statistics.algorithm;
    response.statistics.deviations = result.statistics.deviations;
    response.statistics.seasonality = result.statistics.seasonality;
    response.statistics.slope = result.statistics.slope;
    response.statistics.intercept = result.statistics.intercept;
    response.statistics.r_squared = result.statistics.rSquared;
    response.statistics.residual_std_dev = result.statistics.residualStdDev;
    response.statistics.historical_points = result.statistics.historicalPoints;
    response.statistics.forecast_points = result.statistics.forecastPoints;
    response.statistics.series_count = result.statistics.seriesCount;
    response.statistics.execution_time_ms = result.statistics.executionTimeMs;

    return glz::write_json(response).value_or("{}");
}

}  // namespace timestar
