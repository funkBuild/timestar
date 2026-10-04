#include "derived_query_executor.hpp"

#include "anomaly/anomaly_executor.hpp"
#include "anomaly/anomaly_result.hpp"
#include "engine.hpp"
#include "http_error.hpp"
#include "http_query_handler.hpp"
#include "logger.hpp"

#include <glaze/json.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <map>
#include <seastar/core/when_all.hh>
#include <set>
#include <string>
#include <utility>
#include <vector>

// Glaze reflection for JSON parsing - must be outside namespace
template <>
struct glz::meta<timestar::GlazeDerivedQueryRequest> {
    using T = timestar::GlazeDerivedQueryRequest;
    static constexpr auto value =
        object("queries", &T::queries, "formula", &T::formula, "startTime", &T::startTime, "endTime", &T::endTime,
               "aggregationInterval", &T::aggregationInterval, "multiSeries", &T::multiSeries);
};

template <>
struct glz::meta<timestar::GlazeDerivedQueryResponse::Statistics> {
    using T = timestar::GlazeDerivedQueryResponse::Statistics;
    static constexpr auto value =
        object("point_count", &T::pointCount, "execution_time_ms", &T::executionTimeMs, "sub_queries_executed",
               &T::subQueriesExecuted, "points_dropped_due_to_alignment", &T::pointsDroppedDueToAlignment,
               "group_count", &T::groupCount);
};

template <>
struct glz::meta<timestar::GlazeDerivedQueryResponse::Error> {
    using T = timestar::GlazeDerivedQueryResponse::Error;
    static constexpr auto value = object("code", &T::code, "message", &T::message);
};

template <>
struct glz::meta<timestar::GlazeDerivedSeries> {
    using T = timestar::GlazeDerivedSeries;
    static constexpr auto value =
        object("group_tags", &T::group_tags, "timestamps", &T::timestamps, "values", &T::values);
};

template <>
struct glz::meta<timestar::GlazeDerivedQueryResponse> {
    using T = timestar::GlazeDerivedQueryResponse;
    // "series" sits between the flat columns and the formula, but its POSITION
    // costs nothing either way: it is a std::optional and Glaze skips null
    // members, so a response that did not opt in emits exactly the keys it
    // always did, in exactly the order it always did.
    static constexpr auto value =
        object("status", &T::status, "timestamps", &T::timestamps, "values", &T::values, "series", &T::series,
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
               &T::anomaly_count, "total_points", &T::total_points, "execution_time_ms", &T::execution_time_ms,
               "declined_series_count", &T::declined_series_count);
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
               "series_count", &T::series_count, "execution_time_ms", &T::execution_time_ms, "declined_series_count",
               &T::declined_series_count);
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

namespace {

// Resolve the request-level aggregation interval to nanoseconds.  Both JSON
// spellings are accepted, matching POST /query (and CLAUDE.md's cross-transport
// parity rule): a JSON number is nanoseconds, a JSON string is parsed by the
// shared duration parser, which treats a bare numeric string as nanoseconds
// too.  Absent or empty means "no interval".
//
// A malformed interval is reported as a DerivedQueryException, i.e. a CLIENT
// error.  parseInterval() signals "1x" / " 1d" / "abc" with QueryParseException,
// which is not a DerivedQueryException -- so it escaped executeFromJson() past
// the /derived handler's 400 branch into its catch-all and the endpoint
// answered HTTP 500 INTERNAL_ERROR (with the actual reason swallowed), while
// POST /query answers 400 for the very same literal.  Translating here covers
// both the JSON and the protobuf entry point, since the protobuf path
// re-encodes its request as JSON and re-enters through this same function.
uint64_t resolveAggregationInterval(const std::optional<std::variant<uint64_t, std::string>>& interval) {
    if (!interval.has_value()) {
        return 0;
    }
    try {
        return std::visit(
            [](const auto& val) -> uint64_t {
                using VT = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<VT, uint64_t>) {
                    return val;
                } else {
                    return val.empty() ? 0 : http::HttpQueryHandler::parseInterval(val);
                }
            },
            *interval);
    } catch (const QueryParseException& e) {
        throw DerivedQueryException("Invalid aggregationInterval: " + std::string(e.what()));
    }
}

// Does this query-response column carry values a formula/forecast can compute
// over?  Exactly the two alternatives extractNumericValues() accepts, so the
// planner in convertQueryResponseMulti() can decide a field's fate BEFORE any
// column is moved out.  Booleans are deliberately absent: they are non-numeric
// in this codebase (CLAUDE.md, "Non-Numeric Fields in Queries").
bool isNumericFieldValues(const FieldValues& values) {
    return std::holds_alternative<std::vector<double>>(values) || std::holds_alternative<std::vector<int64_t>>(values);
}

// Extract the numeric column out of one query-response field, widening int64
// to double.  Shared by convertQueryResponse() and convertQueryResponseMulti()
// so the two cannot drift on the rule that booleans are NOT numeric (canonical
// rule, see CLAUDE.md "Non-Numeric Fields in Queries"): they are rejected here
// exactly as strings are.  Booleans used to be coerced to 1.0/0.0, which let a
// formula compute arithmetic over a type the query path refuses to aggregate --
// and, once an aggregationInterval was set, over LATEST-per-bucket values
// rather than the every-point series the formula author expected.
//
// Moves out of `values`; the response is locally owned and discarded by the
// caller.
std::vector<double> extractNumericValues(const std::string& name, FieldValues& values) {
    if (std::holds_alternative<std::vector<double>>(values)) {
        return std::move(std::get<std::vector<double>>(values));
    }
    if (std::holds_alternative<std::vector<int64_t>>(values)) {
        const auto& intVals = std::get<std::vector<int64_t>>(values);
        std::vector<double> widened;
        widened.reserve(intVals.size());
        for (int64_t v : intVals) {
            widened.push_back(static_cast<double>(v));
        }
        return widened;
    }
    throw DerivedQueryException("Sub-query '" + name + "' returned non-numeric values");
}

// Ceiling on how many sub-queries run at once; see the note at the push_back in
// executeAllSubQueries(). Deliberately generous -- real formulas reference a
// handful.  Namespace-scope so the single-series and multi-series fan-outs
// cannot drift apart on it.
constexpr size_t kMaxConcurrentSubQueries = 16;

// A group's identity on the multi-series paths: its tag set plus the field it
// carries.  std::map is already ordered, so the default pair/map comparison IS
// the fan-out ordering rule's first two components (tags ascending
// lexicographically over (key, value), then field name) -- which is what makes
// this usable as a std::set/std::map key without a hand-written comparator.
using GroupKey = std::pair<std::map<std::string, std::string>, std::string>;

// The group's tags flattened to the "key=value" wire form, with the synthetic
// "_field=<name>" entry APPENDED LAST when -- and only when -- the result spans
// more than one distinct field.  See alignSubQueryGroups() for the full
// rationale (leading underscore, last position, the pathological tag named
// `_field`); this is the shared implementation so the arithmetic and
// forecast/anomaly fan-outs cannot label a group differently.
std::vector<std::string> flattenGroupTags(const std::map<std::string, std::string>& tags, const std::string& field,
                                          bool labelField) {
    std::vector<std::string> out;
    out.reserve(tags.size() + (labelField ? 1 : 0));
    for (const auto& [key, value] : tags) {
        out.push_back(key + "=" + value);
    }
    if (labelField) {
        out.push_back("_field=" + field);
    }
    return out;
}

// A tag set rendered for an ERROR message.  Never empty: an untagged group
// named as "" in a list of differences would read as a missing entry.
std::string describeTags(const std::map<std::string, std::string>& tags) {
    if (tags.empty()) {
        return "(no tags)";
    }
    std::string out;
    for (const auto& [tagKey, tagValue] : tags) {
        if (!out.empty()) {
            out += ",";
        }
        out += tagKey + "=" + tagValue;
    }
    return out;
}

// One group key rendered for an ERROR message.  The field is named whenever it
// is PART of the key, unlike flattenGroupTags(): the mismatch being reported may
// BE the field, and an unlabelled key would then read as a duplicate of its
// sibling.  When the key is tags-only (every leg named exactly one field, so the
// field carries no information -- see executeMultiSeries()) the field component
// is empty and naming it would invent a distinction the pairing does not make.
std::string describeGroupKey(const GroupKey& key) {
    if (key.second.empty()) {
        return describeTags(key.first);
    }
    std::string out;
    for (const auto& [tagKey, tagValue] : key.first) {
        out += tagKey + "=" + tagValue + ",";
    }
    out += "_field=" + key.second;
    return out;
}

// Is every (key, value) of `sub` present in `super` with the same value?
// The broadcast rule: a leg resolving to ONE group is spread across the other
// legs' groups only when its own tag set says nothing that contradicts theirs.
bool tagsAreSubset(const std::map<std::string, std::string>& sub, const std::map<std::string, std::string>& super) {
    for (const auto& [key, value] : sub) {
        auto it = super.find(key);
        if (it == super.end() || it->second != value) {
            return false;
        }
    }
    return true;
}

// "3 (a; b; c)" -- or, past the listing limit, "9 (a; b; c; ... and 6 more)".
// Bounded because a leg may carry up to maxSeriesPerLeg keys and an error
// message is not a data export.
std::string describeGroupKeys(const std::vector<GroupKey>& keys) {
    constexpr size_t kMaxListed = 5;
    std::string out = std::to_string(keys.size()) + " (";
    for (size_t i = 0; i < keys.size() && i < kMaxListed; ++i) {
        if (i > 0) {
            out += "; ";
        }
        out += describeGroupKey(keys[i]);
    }
    if (keys.size() > kMaxListed) {
        out += "; ... and " + std::to_string(keys.size() - kMaxListed) + " more";
    }
    out += ")";
    return out;
}

}  // namespace

DerivedQueryExecutor::DerivedQueryExecutor(seastar::sharded<Engine>* engine, DerivedQueryConfig config)
    : engine_(engine), config_(config) {}

seastar::future<DerivedQueryResult> DerivedQueryExecutor::execute(const DerivedQueryRequest& request) {
    auto startTime = std::chrono::high_resolution_clock::now();
    DerivedQueryResult result;
    result.formula = request.formula;

    try {
        // Validate request
        validateRequest(request);

        // OPT-IN multi-series path.  Everything below this branch is the
        // pre-flag single-series path, byte for byte: a request that does not
        // set multiSeries never reaches executeMultiSeries() and a sub-query
        // resolving to several series still fails in convertQueryResponse().
        if (request.multiSeries) {
            result = co_await executeMultiSeries(request);
            result.formula = request.formula;
            auto multiEndTime = std::chrono::high_resolution_clock::now();
            result.stats.executionTimeMs = std::chrono::duration<double, std::milli>(multiEndTime - startTime).count();
            co_return result;
        }

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

    // Parse aggregation interval if provided (numeric ns or duration string)
    request.aggregationInterval = resolveAggregationInterval(glazeReq.aggregationInterval);

    // Opt-in per-group evaluation of an arithmetic formula.  Absent (the only
    // thing an older client can send) means false, i.e. the behaviour that has
    // always been there.
    request.multiSeries = glazeReq.multiSeries;

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

    // Emitted whenever the request opted in -- see GlazeDerivedQueryResponse::
    // series for why this is a std::optional and not an always-present array.
    // Keyed off the PATH and not off `series` being non-empty, so an opted-in
    // request that matched nothing gets `"series": []` rather than no key at
    // all: a client that must handle `undefined` cannot tell an empty answer
    // from a server that does not know the flag.
    if (result.multiSeries) {
        std::vector<GlazeDerivedSeries> series;
        series.reserve(result.series.size());
        for (const auto& groupResult : result.series) {
            GlazeDerivedSeries glazeSeries;
            glazeSeries.group_tags = groupResult.groupTags;
            glazeSeries.timestamps = groupResult.timestamps;
            glazeSeries.values = groupResult.values;
            series.push_back(std::move(glazeSeries));
        }
        response.series = std::move(series);
        response.statistics.groupCount = result.series.size();
    }

    return glz::write_json(response).value_or("{}");
}

std::string DerivedQueryExecutor::createErrorResponse(const std::string& code, const std::string& message) {
    // Delegate to the canonical flat error shape shared by all HTTP handlers:
    //   {"status":"error","error_code":"<code>","message":"<msg>","error":"<msg>"}
    return timestar::http::jsonError(message, code);
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

    // Build futures for each sub-query.  kMaxConcurrentSubQueries (file scope)
    // is the ceiling on how many run at once; see the note at the push_back
    // below.
    std::vector<seastar::future<std::pair<std::string, SubQueryResult>>> futures;

    for (const auto& [name, query] : request.queries) {
        // Only execute queries that are actually used in the formula
        if (referencedQueries.count(name) == 0) {
            continue;
        }

        futures.push_back(
            executeSubQuery(name, applyAggregationInterval(request, query)).then([name](SubQueryResult result) {
                return std::make_pair(name, std::move(result));
            }));

        // Bound the fan-out. Each sub-query is a full query bounded only by
        // http.max_total_points, and every result stays live until the formula
        // is evaluated -- so K referenced queries hold K x that limit
        // simultaneously. A formula referencing a dozen series could commit
        // multiple GB before any arithmetic happens.
        if (futures.size() >= kMaxConcurrentSubQueries) {
            break;
        }
    }

    if (futures.size() >= kMaxConcurrentSubQueries) {
        timestar::query_log.warn(
            "Derived query references more than {} sub-queries; only the first {} were executed. "
            "Split the formula or reduce the number of referenced queries.",
            kMaxConcurrentSubQueries, kMaxConcurrentSubQueries);
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

seastar::future<std::map<std::string, MultiSeriesSubQueryResult>> DerivedQueryExecutor::executeAllSubQueriesMulti(
    const DerivedQueryRequest& request) {
    // Deliberately a mirror of executeAllSubQueries() above rather than a
    // generalisation of it: the single-series path is the one every existing
    // client is on, and the campaign's standing guarantee is that it does not
    // move.  The only difference is which converter the leg runs through.
    auto referencedQueries = request.getReferencedQueries();

    std::vector<seastar::future<std::pair<std::string, MultiSeriesSubQueryResult>>> futures;

    for (const auto& [name, query] : request.queries) {
        if (referencedQueries.count(name) == 0) {
            continue;
        }

        futures.push_back(
            executeSubQueryMulti(name, applyAggregationInterval(request, query))
                .then([name](MultiSeriesSubQueryResult result) { return std::make_pair(name, std::move(result)); }));

        // Same fan-out ceiling, and for a stronger reason: each leg here may
        // itself hold up to maxSeriesPerLeg groups.
        if (futures.size() >= kMaxConcurrentSubQueries) {
            break;
        }
    }

    if (futures.size() >= kMaxConcurrentSubQueries) {
        timestar::query_log.warn(
            "Derived query references more than {} sub-queries; only the first {} were executed. "
            "Split the formula or reduce the number of referenced queries.",
            kMaxConcurrentSubQueries, kMaxConcurrentSubQueries);
    }

    auto results = co_await seastar::when_all_succeed(futures.begin(), futures.end());

    std::map<std::string, MultiSeriesSubQueryResult> resultMap;
    for (auto& [name, result] : results) {
        resultMap[name] = std::move(result);
    }

    co_return resultMap;
}

seastar::future<DerivedQueryResult> DerivedQueryExecutor::executeMultiSeries(const DerivedQueryRequest& request) {
    DerivedQueryResult result;
    // The response shape is the per-group ARRAY from here on, whatever the
    // outcome: set before the early returns so that an empty answer still says
    // `"series": []` rather than omitting the key and making a multiSeries
    // client distinguish "no groups" from "this server does not know the flag".
    result.multiSeries = true;

    auto legs = co_await executeAllSubQueriesMulti(request);
    result.stats.subQueriesExecuted = legs.size();

    if (legs.empty()) {
        co_return result;
    }

    // A leg that matched NOTHING makes the whole result empty, and is NOT
    // reported as a key-set mismatch.  This is the same answer the single-series
    // path gives -- an empty leg has no timestamps, so the INNER alignment's
    // intersection is empty and the formula yields nothing -- and it is the
    // honest one: "b resolved to no series" means b has no data in this range,
    // not that a and b disagree about which devices exist.
    for (const auto& [name, leg] : legs) {
        (void)name;
        if (leg.series.empty()) {
            co_return result;
        }
    }

    // ---- the reference leg: whose group order the output follows ------------
    //
    // The first leg (by sub-query name) that resolves to more than one group,
    // or the first leg at all when every leg is single-group.  Every leg's own
    // order is already the fan-out ordering rule, so the output inherits it;
    // naming ONE leg as the source is what keeps the sequence deterministic
    // when two legs disagree about field order (`a = m(x,y)` vs `b = m(y,x)`
    // rank the same two fields oppositely, so "the order of a" and "the order
    // of b" are different answers and the tie has to be broken by name).
    std::string referenceName = legs.begin()->first;
    for (const auto& [name, leg] : legs) {
        if (leg.series.size() > 1) {
            referenceName = name;
            break;
        }
    }

    // ---- does the FIELD take part in the pairing key? ----------------------
    //
    // Normally yes: `a = m(rx,tx)` against `b = n(rx,tx)` must pair rx with rx.
    // But when EVERY leg resolves to exactly one distinct field name the field
    // carries no information -- it cannot distinguish two groups of the same leg
    // -- while including it makes an unambiguous cross-measurement pairing
    // impossible: `cpu.user / mem.used by {host}` is one series per host on each
    // side, obviously paired by host, and the field-bearing key refused it with
    // advice no caller could act on (no scope filter renames a field, and
    // reducing either side to one series is the very thing this work exists to
    // stop requiring).  So in that case, and only in that case, the key is the
    // TAG SET alone.
    //
    // The "_field=" label rule needs no exception: such a result spans exactly
    // one key-field ("" for every group), so it is single-field by the ordinary
    // test and carries no label.
    bool pairOnTagsOnly = true;
    for (const auto& [name, leg] : legs) {
        (void)name;
        std::set<std::string> legFields;
        for (const auto& group : leg.series) {
            legFields.insert(group.field);
        }
        if (legFields.size() > 1) {
            pairOnTagsOnly = false;
            break;
        }
    }
    const auto keyOf = [pairOnTagsOnly](const SubQuerySeries& group) {
        return GroupKey{group.tags, pairOnTagsOnly ? std::string() : group.field};
    };

    // ---- index every leg by group key --------------------------------------
    //
    // A leg CAN carry the same (tags, field) key twice -- the fan-out ordering
    // rule's fourth tie-break exists for exactly that -- so first occurrence
    // wins, on both sides: in the index below and in the output key list.
    std::map<std::string, std::map<GroupKey, size_t>> legIndex;
    for (const auto& [name, leg] : legs) {
        auto& index = legIndex[name];
        for (size_t i = 0; i < leg.series.size(); ++i) {
            index.emplace(keyOf(leg.series[i]), i);
        }
    }

    std::vector<GroupKey> outputKeys;
    std::set<GroupKey> outputKeySet;
    for (const auto& group : legs.at(referenceName).series) {
        GroupKey key = keyOf(group);
        if (outputKeySet.insert(key).second) {
            outputKeys.push_back(std::move(key));
        }
    }

    // ---- IS THERE ANY FAN-OUT AT ALL? --------------------------------------
    //
    // When EVERY leg resolves to exactly one group there is no fan-out: one
    // group in from each leg, one group out.  Nothing stands in for anything,
    // so the subset rule below does not apply -- comparing two named hosts
    // (`avg:cpu(v){host:web1} by {host}` minus `{host:web2} by {host}`) is a
    // legitimate query and must keep answering, exactly as it does without the
    // flag.
    //
    // That is a different situation from the one the subset rule guards, which
    // is ONE group standing in for MANY it does not describe.  The distinction
    // is fan-out, not tags: with several output groups a non-matching single
    // group is silently repeated against groups it provably is not; with one
    // output group it is paired once, with itself, and the only thing at risk is
    // the LABEL -- which is handled below by dropping it.
    const bool everyLegIsSingleGroup =
        std::all_of(legs.begin(), legs.end(), [](const auto& entry) { return entry.second.series.size() == 1; });

    // Do all the legs agree on the tag set?  Only meaningful when there is no
    // fan-out; decides whether the single output group may carry tags at all.
    const bool legTagsAllIdentical =
        everyLegIsSingleGroup && std::all_of(legs.begin(), legs.end(), [&legs](const auto& entry) {
            return entry.second.series[0].tags == legs.begin()->second.series[0].tags;
        });

    // ---- WHICH ONE-GROUP LEGS MAY BROADCAST --------------------------------
    //
    // A leg with exactly one group is spread across every output group -- but
    // ONLY when its own tag set is empty, or a subset of every output group's.
    // (Vacuous when there is no fan-out at all; see above.)
    //
    // Arity alone is not enough, and treating it as enough was a silent wrong
    // answer.  `a = avg:num(v){} by {dev}` (DEV-A, DEV-B, DEV-C) over
    // `b = avg:den(v){dev:DEV-A} by {dev}` (one group, tagged dev=DEV-A) used to
    // answer HTTP 200 with three groups, the one LABELLED dev=DEV-B actually
    // holding num[DEV-B] / den[DEV-A].  b provably carries a tag set that is not
    // DEV-B's, so the label asserts a pairing that never happened -- and the
    // over-narrow scope filter that produced it is exactly the mistake the
    // mismatch 400 below exists to catch.
    //
    // The documented motivation (`per_device_bytes / fleet_total`) always
    // involves an UNTAGGED single group, and a leg scoped to a tag the output
    // groups all share (`{rack:R1} by {rack}` against `by {dev,rack}`) still
    // broadcasts, so every legitimate use survives.  A tagged single group that
    // is not a subset is refused, with a message of its own (below) rather than
    // the key-set one: its cause is an over-narrow scope filter, not two fleets
    // that disagree.
    const auto tagsBroadcastOverEveryGroup = [&outputKeys](const std::map<std::string, std::string>& tags) {
        if (tags.empty()) {
            return true;
        }
        for (const auto& key : outputKeys) {
            if (!tagsAreSubset(tags, key.first)) {
                return false;
            }
        }
        return true;
    };

    // ---- the same rule on the FIELD axis -----------------------------------
    //
    // The tag test alone left the identical hole one axis over.  `a =
    // avg:net(rx,tx){} by {host}` puts the field in the pairing key (two output
    // keys, (host=h1, rx) and (host=h1, tx)); `b = avg:net2(rx){} by {host}` is
    // one group whose TAGS match both, so it broadcast -- and answered 200 with
    // a group labelled `_field=tx` holding net.tx / net2.RX.  Same false claim,
    // same over-narrow sub-query, and the message above is its own argument
    // against allowing it.
    //
    // So the field is admitted on the analogue of the tag rule, which is a
    // three-way test, not equality:
    //   * named by NO output key -- a common scalar denominator
    //     (`net(rx,tx) / total(bytes)`).  Legitimate, and the reason strict
    //     equality is the wrong fix: it would refuse this.  It also covers the
    //     cross-measurement case, where the key holds no field at all.
    //   * named by ALL of them -- the field distinguishes nothing among the
    //     output groups, so pairing it with each of them asserts nothing extra.
    //   * named by SOME but not all -- precisely the mislabel above.
    const auto fieldBroadcastsOverEveryGroup = [&outputKeys](const std::string& field) {
        size_t matched = 0;
        for (const auto& key : outputKeys) {
            if (key.second == field) {
                ++matched;
            }
        }
        return matched == 0 || matched == outputKeys.size();
    };

    // ---- MISMATCHED KEY SETS ARE A 400, never a silent intersection ---------
    //
    // Intersecting instead would answer a fleet-wide question with a subset of
    // the fleet and report success, which is the silent-short-answer class
    // CLAUDE.md forbids; so the difference is named in BOTH directions, since
    // "a has a device b lacks" and "b has a device a lacks" call for opposite
    // fixes.
    for (const auto& [name, leg] : legs) {
        if (name == referenceName) {
            continue;
        }
        if (leg.series.size() == 1) {
            // No fan-out -> nothing to stand in for, so pair regardless of tags.
            if (everyLegIsSingleGroup) {
                continue;
            }
            const bool tagsBroadcast = tagsBroadcastOverEveryGroup(leg.series[0].tags);
            if (tagsBroadcast && fieldBroadcastsOverEveryGroup(leg.series[0].field)) {
                continue;
            }
            // One group that cannot broadcast.  Diagnosed on its own terms
            // rather than through the key-set message below: the caller's
            // mistake is a scope filter that is too narrow, not a fleet that
            // disagrees, and the remedy is different.  The two axes are named
            // separately because their remedies differ too -- a tag mismatch is
            // fixed with a scope filter, a field mismatch by naming fields.
            if (!tagsBroadcast) {
                throw DerivedQueryException(
                    "Sub-query '" + name + "' resolved to a single series tagged (" + describeTags(leg.series[0].tags) +
                    "), which does not match every series of '" + referenceName +
                    "': " + describeGroupKeys(outputKeys) +
                    ". A sub-query resolving to exactly one series is broadcast across the others only when its tags "
                    "are empty or a subset of theirs -- pairing it with a group carrying different tags would label "
                    "the result with a tag set the values did not come from. Widen this sub-query's scope so it "
                    "resolves to one series per group, drop the tags it does not share, or narrow the other "
                    "sub-queries to match it.");
            }
            throw DerivedQueryException(
                "Sub-query '" + name + "' resolved to a single series carrying field '" + leg.series[0].field +
                "', which some but not all series of '" + referenceName + "' carry: " + describeGroupKeys(outputKeys) +
                ". A sub-query resolving to exactly one series is broadcast across the others only when its field is "
                "one they all carry, or one none of them names (a shared scalar denominator) -- pairing it with a "
                "group carrying a different field would label the result with a field the values did not come from. "
                "Name the same fields on both sides so this sub-query resolves to one series per group, or reduce "
                "the other sub-queries to the field this one names.");
        }
        const auto& index = legIndex.at(name);

        std::vector<GroupKey> onlyInReference;
        for (const auto& key : outputKeySet) {
            if (index.count(key) == 0) {
                onlyInReference.push_back(key);
            }
        }
        std::vector<GroupKey> onlyInOther;
        for (const auto& [key, position] : index) {
            (void)position;
            if (outputKeySet.count(key) == 0) {
                onlyInOther.push_back(key);
            }
        }

        if (onlyInReference.empty() && onlyInOther.empty()) {
            continue;
        }

        std::string detail;
        if (!onlyInReference.empty()) {
            detail += describeGroupKeys(onlyInReference) + " in '" + referenceName + "' but not '" + name + "'";
        }
        if (!onlyInOther.empty()) {
            if (!detail.empty()) {
                detail += ", ";
            }
            detail += describeGroupKeys(onlyInOther) + " in '" + name + "' but not '" + referenceName + "'";
        }
        throw DerivedQueryException(
            "Sub-queries '" + referenceName + "' and '" + name + "' resolved to different series: " + detail +
            ". A multi-series derived query pairs series across sub-queries by tag set and field, so every "
            "sub-query resolving to more than one series must resolve to the same ones. Add scope filters so "
            "they match, or reduce one sub-query to a single series whose tags are empty or a subset of the "
            "others' -- such a sub-query is broadcast across them.");
    }

    // ---- evaluate the formula once per group -------------------------------
    std::unique_ptr<ExpressionNode> localAst;
    if (!cachedAst_) {
        ExpressionParser parser(request.formula);
        localAst = parser.parse();
    }
    const auto& ast = cachedAst_ ? *cachedAst_ : *localAst;

    // The synthetic "_field=" label is added only when the RESULT spans more
    // than one field -- same rule, and the same shared helper, as the
    // forecast/anomaly fan-out, so a single-field result's group_tags are the
    // plain tag list and nothing else.
    std::set<std::string> distinctFields;
    for (const auto& key : outputKeys) {
        distinctFields.insert(key.second);
    }
    const bool labelField = distinctFields.size() > 1;

    // The per-group input to the UNCHANGED single-series machinery: exactly the
    // map<legName, SubQueryResult> SeriesAligner already consumes.  Built once
    // and refilled per group.  A BROADCAST leg (one group) is filled here and
    // then left alone for every group -- the aligner takes it by const
    // reference, so one copy serves all of them.
    std::map<std::string, SubQueryResult> legsForGroup;
    for (auto& [name, leg] : legs) {
        SubQueryResult subResult;
        subResult.queryName = name;
        subResult.measurement = leg.measurement;
        if (leg.series.size() == 1) {
            subResult.tags = leg.series[0].tags;
            subResult.field = leg.series[0].field;
            subResult.timestamps = std::move(leg.series[0].timestamps);
            subResult.values = std::move(leg.series[0].values);
        }
        legsForGroup.emplace(name, std::move(subResult));
    }

    size_t emittedPoints = 0;
    result.series.reserve(outputKeys.size());

    for (const auto& key : outputKeys) {
        for (auto& [name, leg] : legs) {
            if (leg.series.size() == 1) {
                continue;  // broadcast: already in place, and reused as-is
            }
            auto& slot = legsForGroup.at(name);
            auto& source = leg.series[legIndex.at(name).at(key)];
            slot.tags = source.tags;
            slot.field = source.field;
            // Moved: the key set is deduplicated, so each group of a
            // multi-group leg is consumed exactly once.
            slot.timestamps = std::move(source.timestamps);
            slot.values = std::move(source.values);
        }

        SeriesAligner aligner(config_.alignmentStrategy, config_.interpolationMethod);
        if (request.aggregationInterval > 0) {
            aligner.setTargetInterval(request.aggregationInterval);
        }

        // ---- BOUND the result BEFORE materialising this group --------------
        //
        // SeriesAligner::projectedOutputSize() sizes the axis this group would
        // produce without building it, so an over-large group is refused instead
        // of being built and then regretted.  Under the default INNER strategy
        // it is EXACT -- it walks the legs' timestamps as a merge, so the number
        // it refuses on is the number align() would go on to emit.  That
        // exactness is the property that matters here: an over-statement is not
        // a safe conservatism but a false 400 on a query that costs nothing, and
        // sizing the resample grid over the interval merely CONTAINING the
        // intersection produced exactly that (two legs sharing 1,000 points
        // refused as 299,999).
        //
        // The after-the-fact check below stays as a BACKSTOP.  It was the only
        // check, and being incremental it never examined the FIRST group at all:
        // four stored points with a one-second aggregationInterval over a
        // thirty-day window materialised 2,592,001 points -- 10.4x the budget,
        // with a 33 ms reactor stall -- before anything refused them.
        //
        // A ONE-GROUP result is exempt, exactly as it is in
        // alignSubQueryGroups() and for the same reason: that is the answer this
        // endpoint gave before the flag existed, and it must not start 400ing.
        if (outputKeys.size() > 1 && config_.maxFanOutPoints > 0) {
            const size_t projected = aligner.projectedOutputSize(legsForGroup);
            if (emittedPoints + projected > config_.maxFanOutPoints) {
                throw DerivedQueryException(
                    "Derived query over " + std::to_string(outputKeys.size()) + " series would produce at least " +
                    std::to_string(emittedPoints + projected) + " points -- reached at series " +
                    std::to_string(result.series.size() + 1) + " of " + std::to_string(outputKeys.size()) +
                    " -- which is more than the " + std::to_string(config_.maxFanOutPoints) +
                    " point limit for a derived query result. Use a coarser aggregationInterval (or add one, if the "
                    "query has none), narrow the time range, or narrow the scope of the query so it matches fewer "
                    "series.");
            }
        }

        auto alignedSeries = aligner.align(legsForGroup);
        result.stats.pointsDroppedDueToAlignment += aligner.getStats().pointsDropped;

        DerivedSeriesResult groupResult;
        // With no fan-out and legs that DISAGREE about their tags, the one
        // output group is labelled with NOTHING.  The key comes from the
        // reference leg, and emitting its tags would assert the result came from
        // a series that only supplied half of it -- `cpu{host:web1} - cpu{host:web2}`
        // labelled `host=web1` is the same false claim the subset rule refuses
        // under fan-out.  Empty is honest, and it is what the no-flag flat answer
        // already says (it carries no labels at all).  Legs that agree keep their
        // shared tags.
        groupResult.groupTags = (everyLegIsSingleGroup && !legTagsAllIdentical)
                                    ? std::vector<std::string>{}
                                    : flattenGroupTags(key.first, key.second, labelField);

        if (!alignedSeries.empty()) {
            ExpressionEvaluator::QueryResultMap evalInput;
            for (auto& [name, series] : alignedSeries) {
                evalInput[name] = std::move(series);
            }

            ExpressionEvaluator evaluator;
            auto computed = evaluator.evaluate(ast, evalInput);
            groupResult.timestamps = *computed.timestamps;
            groupResult.values = std::move(computed.values);
        }

        // A group with no common timestamps is EMITTED, empty, rather than
        // dropped: the caller asked for these groups by name and an absent
        // entry would be indistinguishable from a group that does not exist.
        emittedPoints += groupResult.timestamps.size();
        result.series.push_back(std::move(groupResult));

        // ---- BACKSTOP: the same bound, after the fact ---------------------
        //
        // Kept even though the pre-check is exact for the INNER strategy this
        // path is configured with: the alignment strategy is a config knob, and
        // UNION/OUTER still over-state a count (never under-state it), so this
        // is what would hold the line if one were selected -- and what holds it
        // if the projection and align() ever drift apart.  It costs at most one
        // over-wide group.  It was once the ONLY check, and being incremental it
        // never examined the first group at all.
        if (outputKeys.size() > 1 && config_.maxFanOutPoints > 0 && emittedPoints > config_.maxFanOutPoints) {
            throw DerivedQueryException(
                "Derived query over " + std::to_string(outputKeys.size()) + " series has already produced " +
                std::to_string(emittedPoints) + " points after " + std::to_string(result.series.size()) +
                " of them, which is more than the " + std::to_string(config_.maxFanOutPoints) +
                " point limit for a derived query result. Use a coarser aggregationInterval (or add one, if the "
                "query has none), narrow the time range, or narrow the scope of the query so it matches fewer "
                "series.");
        }
    }

    result.stats.pointCount = emittedPoints;
    result.stats.groupCount = result.series.size();

    // The flat timestamps/values columns stay EMPTY under the flag, at every
    // group count.  An earlier revision also filled them when there was exactly
    // one group, so a client could adopt the flag before the array -- but that
    // made a one-group answer carry every point TWICE (a measured 56.85 MB body
    // became 113.71 MB, and in the protobuf encoding FFOR+ALP ran over the same
    // points twice), and nothing bounded it: the one-group case is exempt from
    // maxFanOutPoints precisely because it is the pre-flag shape.  Paying 2x
    // forever to make the migration a two-step is the wrong trade; with the flag
    // the migration is one step -- read `series`.
    co_return result;
}

QueryRequest DerivedQueryExecutor::applyAggregationInterval(const DerivedQueryRequest& request,
                                                            const QueryRequest& leg) {
    // Propagate the request-level aggregation interval into the sub-query so it
    // is bucketed server-side exactly like a plain /query with the same
    // interval.  Without this, sub-queries run with interval == 0: an
    // arithmetic formula then aligns raw points instead of buckets, and
    // forecast()/anomalies() fit the raw series while reporting success.
    QueryRequest subQuery = leg;
    if (subQuery.aggregationInterval == 0) {
        subQuery.aggregationInterval = request.aggregationInterval;
    }
    return subQuery;
}

seastar::future<SubQueryResult> DerivedQueryExecutor::executeSubQuery(const std::string& name, QueryRequest query) {
    // Create a query handler to execute the sub-query
    http::HttpQueryHandler handler(engine_);

    auto response = co_await handler.executeQuery(query);

    if (!response.success) {
        throw DerivedQueryException("Sub-query '" + name + "' failed: " + response.errorMessage);
    }

    co_return convertQueryResponse(name, query, std::move(response.series));
}

seastar::future<MultiSeriesSubQueryResult> DerivedQueryExecutor::executeSubQueryMulti(const std::string& name,
                                                                                      QueryRequest query) {
    http::HttpQueryHandler handler(engine_);

    auto response = co_await handler.executeQuery(query);

    if (!response.success) {
        throw DerivedQueryException("Sub-query '" + name + "' failed: " + response.errorMessage);
    }

    co_return convertQueryResponseMulti(name, query, std::move(response.series));
}

SubQueryResult DerivedQueryExecutor::convertQueryResponse(const std::string& name, const QueryRequest& query,
                                                          std::vector<SeriesResult>&& results) {
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

    auto& series = results[0];
    subResult.tags = std::move(series.tags);

    // Get the first field (or the requested field)
    std::string fieldName = query.fields.empty() ? "" : query.fields[0];

    for (auto& [fname, fieldData] : series.fields) {
        if (!fieldName.empty() && fname != fieldName) {
            continue;
        }

        subResult.field = fname;
        subResult.timestamps = std::move(fieldData.first);

        // Extract values (must be numeric for derived metrics) — moved, the
        // response is locally owned and discarded after conversion.  Shared
        // with convertQueryResponseMulti(); see extractNumericValues() for why
        // booleans are refused rather than coerced.
        subResult.values = extractNumericValues(name, fieldData.second);

        break;  // Take first matching field
    }

    if (subResult.field.empty() && !fieldName.empty() && !series.fields.empty()) {
        throw DerivedQueryException("Sub-query '" + name + "' field '" + fieldName + "' not found in results");
    }

    return subResult;
}

MultiSeriesSubQueryResult DerivedQueryExecutor::convertQueryResponseMulti(const std::string& name,
                                                                          const QueryRequest& query,
                                                                          std::vector<SeriesResult>&& results) {
    MultiSeriesSubQueryResult multi;
    multi.queryName = name;
    multi.measurement = query.measurement;

    if (results.empty()) {
        return multi;
    }

    using FieldIterator = std::map<std::string, std::pair<std::vector<uint64_t>, FieldValues>>::iterator;

    // One planned entry per (series tag set x field) pair.  Planning first is
    // deliberate: it establishes the fan-out and lets the per-leg cap reject an
    // over-wide leg BEFORE any value column is copied or widened.
    struct PlannedEntry {
        size_t seriesIndex;   // index into `results`, the final tie-break
        size_t fieldRank;     // position of the field in this series' emitted order
        FieldIterator field;  // stable: `results` is not restructured after planning
    };
    std::vector<PlannedEntry> plan;

    // DEDUPE the requested field list, keeping first-occurrence order.
    // QueryParser::parseFields() does not dedupe, so `avg:m(a,a)` arrives here
    // as {"a","a"} and series.fields.find() hands back the SAME iterator twice.
    // Two plan entries then point at one column, and the emit loop moves the
    // timestamp/value vectors out for the first and reads the moved-from
    // vectors for the second — producing a phantom 0-point entry with no
    // exception at all (a moved-from FieldValues still holds a
    // vector<double>, so the numeric extractor is perfectly happy with it).
    // Deduping is done here rather than in the parser so that /query's own
    // behaviour for a duplicated field name is left exactly as it is.
    std::vector<std::string> requestedFields;
    requestedFields.reserve(query.fields.size());
    for (const auto& requested : query.fields) {
        if (std::find(requestedFields.begin(), requestedFields.end(), requested) == requestedFields.end()) {
            requestedFields.push_back(requested);
        }
    }

    // Non-numeric field names seen and skipped on an implicit "()" leg, kept
    // only to name them if the leg turns out to have no numeric field at all.
    std::set<std::string> skippedNonNumeric;

    for (size_t i = 0; i < results.size(); ++i) {
        auto& series = results[i];

        if (requestedFields.empty()) {
            // "()" — every NUMERIC field the series carries, in the series' OWN
            // map order (std::map, so ascending by field name).
            //
            // An implicit "()" selects the numeric fields and silently passes
            // over the rest: the caller named no field, so a string label
            // sitting beside the numeric channels on one device must not kill a
            // fleet-wide forecast.  An EXPLICITLY named non-numeric field still
            // throws (below) — that is the single-series contract and
            // CLAUDE.md's numeric-only rule for formula operands.  Note this is
            // a deliberate behaviour change: previously "()" threw whenever the
            // first field in std::map order happened to be non-numeric, so
            // whether a leg worked depended on field-name alphabetics.
            size_t rank = 0;
            for (auto it = series.fields.begin(); it != series.fields.end(); ++it) {
                if (!isNumericFieldValues(it->second.second)) {
                    skippedNonNumeric.insert(it->first);
                    continue;
                }
                plan.push_back({i, rank++, it});
            }
            continue;
        }

        // Explicit field list — emit in REQUESTED order, i.e. the order the
        // query string named them, not std::map order.  The single-series
        // converter keeps only query.fields[0] and discards the rest; that is
        // the defect this function exists to avoid, so the requested order is
        // load-bearing rather than incidental.
        size_t rank = 0;
        for (const auto& requested : requestedFields) {
            auto it = series.fields.find(requested);
            if (it == series.fields.end()) {
                continue;
            }
            // A field the caller NAMED must be numeric: a formula, a forecast
            // and a detector are all arithmetic, and CLAUDE.md's "Formula
            // arithmetic is numeric-only" rule says a non-numeric operand is
            // rejected rather than coerced.  Rejected during planning so the
            // per-leg cap below cannot pre-empt this (much more actionable)
            // error.
            if (!isNumericFieldValues(it->second.second)) {
                throw DerivedQueryException("Sub-query '" + name + "' returned non-numeric values for field '" +
                                            requested + "'");
            }
            plan.push_back({i, rank++, it});
        }

        // RULE: a series must contribute at least ONE of the requested fields.
        // A series carrying data but NONE of them is a failed read and throws;
        // a series missing only SOME of them simply does not appear for those
        // fields.  That is deliberate and it is NOT the short answer CLAUDE.md
        // forbids:
        //   * it is what /query itself does — `avg:m(a,b){} by {dev}` where
        //     DEV-2 has no `a` returns DEV-1/a, DEV-1/b, DEV-2/b and no error;
        //   * under fan-out every emitted group is labelled with its own
        //     group_tags (and, on a multi-field leg, its field), so the caller
        //     can SEE which (device, field) pairs came back rather than having
        //     to infer it from a count.
        // The single-series converter cannot make that distinction — it emits
        // one unlabelled column, so a missing field there IS a silent
        // substitution — which is why this is a fan-out-only relaxation rather
        // than "parity with convertQueryResponse()".  The all-missing case
        // still throws with the single-series converter's exact message, so a
        // leg that resolves to one series reports failures identically.
        if (rank == 0 && !series.fields.empty()) {
            throw DerivedQueryException("Sub-query '" + name + "' field '" + requestedFields[0] +
                                        "' not found in results");
        }
    }

    // An implicit "()" leg with fields, none of them numeric, is a real
    // failure: there is nothing to forecast.  Name what WAS found so the
    // caller can see why an apparently populated measurement produced nothing.
    if (requestedFields.empty() && plan.empty() && !skippedNonNumeric.empty()) {
        std::string found;
        for (const auto& fieldName : skippedNonNumeric) {
            if (!found.empty()) {
                found += ", ";
            }
            found += "'" + fieldName + "'";
        }
        throw DerivedQueryException("Sub-query '" + name +
                                    "' returned non-numeric values only; no numeric field to compute over (found " +
                                    found + "). Name a numeric field explicitly.");
    }

    // The cap counts the entries that will actually be EMITTED — after
    // non-numeric fields have been dropped or rejected, never before.  Counting
    // the raw (tag set x field) product instead let the cap error pre-empt a
    // request that was genuinely under the limit: a "()" leg on a measurement
    // with 4 numeric and 3 string fields reported "resolved to 7 series ... the
    // per-leg limit is 5. Add more specific scope filters", advice that cannot
    // be acted on because the 3 string fields were never going to be forecast.
    if (plan.size() > config_.maxSeriesPerLeg) {
        throw DerivedQueryException("Sub-query '" + name + "' resolved to " + std::to_string(plan.size()) +
                                    " series (tag set x field) but the per-leg limit is " +
                                    std::to_string(config_.maxSeriesPerLeg) +
                                    ". Add more specific scope filters to narrow the scope of the query, "
                                    "or request fewer fields.");
    }

    // ORDERING RULE (total, and a pure function of the input):
    //   1. tag set ascending — std::map's lexicographic order over its
    //      (key, value) pairs, so a device's entries stay contiguous and the
    //      sequence does not depend on which shard answered first;
    //   2. then field rank — requested order for an explicit field list,
    //      ascending field name for "()";
    //   3. then field NAME, which is what actually separates two DIFFERENT
    //      fields that happen to share a rank.  Two SeriesResults carrying the
    //      same tag set each rank their own first field 0, so without this the
    //      tie fell through to arrival order and the sequence FLIPPED when the
    //      shards answered in a different order — the very thing rule 1 exists
    //      to prevent;
    //   4. then input index, so even two series carrying an identical tag set
    //      AND an identical field name (which the /query response shape should
    //      already have consolidated away) cannot swap places for a fixed
    //      input.
    // Tags first rather than field first because the consumer is per-group
    // forecasting: grouping the output by device is the useful adjacency.
    std::stable_sort(plan.begin(), plan.end(), [&results](const PlannedEntry& a, const PlannedEntry& b) {
        const auto& tagsA = results[a.seriesIndex].tags;
        const auto& tagsB = results[b.seriesIndex].tags;
        if (tagsA != tagsB) {
            return tagsA < tagsB;
        }
        if (a.fieldRank != b.fieldRank) {
            return a.fieldRank < b.fieldRank;
        }
        if (a.field->first != b.field->first) {
            return a.field->first < b.field->first;
        }
        return a.seriesIndex < b.seriesIndex;
    });

    multi.series.reserve(plan.size());
    for (const auto& entry : plan) {
        SubQuerySeries out;
        // Copied, not moved: several fields of one series share its tag map.
        out.tags = results[entry.seriesIndex].tags;
        out.field = entry.field->first;
        out.timestamps = std::move(entry.field->second.first);
        out.values = extractNumericValues(name, entry.field->second.second);
        multi.series.push_back(std::move(out));
    }

    return multi;
}

DerivedQueryExecutor::AlignedSubQueryGroups DerivedQueryExecutor::alignSubQueryGroups(MultiSeriesSubQueryResult&& multi,
                                                                                      size_t maxFanOutPoints) {
    AlignedSubQueryGroups aligned;
    if (multi.series.empty()) {
        return aligned;
    }

    // A `_field=` entry is added to a group's tags ONLY when the leg yields more
    // than one distinct field.  A single-field leg (the overwhelmingly common
    // one, and every request that worked before fan-out existed) therefore
    // produces byte-identical group_tags to before: the tag map flattened to
    // "k=v", nothing else.  Adding it unconditionally would have changed every
    // existing client's response.
    std::set<std::string> distinctFields;
    for (const auto& group : multi.series) {
        distinctFields.insert(group.field);
    }
    const bool labelField = distinctFields.size() > 1;

    // ---- the shared axis: sorted union of every group's timestamps ----------
    //
    // Fast path first: when every group already carries the identical axis --
    // which is what a bucketed leg always produces, and what a raw leg over
    // co-sampled devices usually produces -- the union is that axis verbatim
    // and no NaN is introduced.  The general path sorts and uniques.
    bool sharedAxis = true;
    for (size_t i = 1; i < multi.series.size() && sharedAxis; ++i) {
        sharedAxis = (multi.series[i].timestamps == multi.series[0].timestamps);
    }

    if (sharedAxis) {
        aligned.times = std::move(multi.series[0].timestamps);
    } else {
        size_t total = 0;
        for (const auto& group : multi.series) {
            total += group.timestamps.size();
        }
        aligned.times.reserve(total);
        for (const auto& group : multi.series) {
            aligned.times.insert(aligned.times.end(), group.timestamps.begin(), group.timestamps.end());
        }
        std::sort(aligned.times.begin(), aligned.times.end());
        aligned.times.erase(std::unique(aligned.times.begin(), aligned.times.end()), aligned.times.end());
    }

    // ---- BOUND the output before it is materialised -------------------------
    //
    // Both factors are known now and neither has been paid for yet: the axis
    // exists, the value columns are still the caller's own (one per group,
    // each only as long as that group really is).  Densifying is what turns
    // them into groups x axis, so refuse here rather than after allocating.
    //
    // A SINGLE-GROUP leg is EXEMPT.  The standing guarantee of this work is
    // that a leg resolving to one series behaves exactly as it did before the
    // fan-out existed, and a one-series response over a huge range 400ing
    // where it used to succeed breaks that guarantee for a case that has
    // nothing to do with the fan-out.  The exemption adds no new exposure
    // either: an unbounded single-series response was already fully reachable
    // before this campaign, bounded only by http.max_total_points at the query
    // layer -- preserving that is exactly the point.  (It IS an unbounded
    // response, and remains a known pre-existing issue; it is simply not this
    // change's to fix.)  And the amplification this bound exists to stop is
    // specifically the O(G^2 * N) staggered-union blow-up, which requires
    // G > 1: at one group the cost is linear in N.
    //
    // Beyond one group the check is deliberately NOT restricted to the union
    // path.  A shared axis costs nothing to build -- the columns are moved,
    // not widened -- but the RESULT is groups x axis either way: the forecast
    // path expands each group into four pieces spanning the axis plus the
    // horizon, so several groups over an enormous shared axis amplify just as
    // hard as a staggered leg does.
    //
    // maxSeriesPerLeg is a different guard and stays where it is: it bounds
    // how many groups a leg may resolve to, and it must fire on group count
    // alone even when each group is short.  This one bounds their product with
    // the axis, which is the number the response size actually tracks.
    if (maxFanOutPoints > 0 && multi.series.size() > 1 && !aligned.times.empty() &&
        multi.series.size() > maxFanOutPoints / aligned.times.size()) {
        // Divided rather than multiplied, and exactly equivalent to
        // `groups * axis > maxFanOutPoints` for the values that reach here:
        // for positive integers, groups > floor(limit / axis) iff
        // groups * axis > limit.  (An earlier version of this comment
        // justified the form as overflow avoidance.  It is not: the product is
        // bounded by maxSeriesPerLeg (200) x http.max_total_points (10M) = 2e9,
        // about 31 bits, so it cannot overflow a 64-bit size_t and the
        // multiplying form would have been correct too.  The division form is
        // kept because it is correct without needing that argument at all --
        // it does not depend on either factor's ceiling staying where it is.)
        throw DerivedQueryException(
            "Sub-query '" + multi.queryName + "' fans out to " + std::to_string(multi.series.size()) +
            " series over a shared time axis of " + std::to_string(aligned.times.size()) +
            " points, which is more than the " + std::to_string(maxFanOutPoints) +
            " point limit for a derived query result. Add an aggregationInterval (series sampled at "
            "different offsets do not share timestamps, so the combined axis grows with the number of "
            "series; bucketing collapses them onto one grid), or narrow the scope of the query so it "
            "matches fewer series.");
    }

    // ---- project each group onto the axis ----------------------------------
    const double kMissing = std::numeric_limits<double>::quiet_NaN();
    aligned.values.reserve(multi.series.size());
    aligned.groupTags.reserve(multi.series.size());

    for (auto& group : multi.series) {
        if (sharedAxis) {
            // Moved, not copied: the caller hands the groups over.
            //
            // Normalised to the axis width afterwards.  Every consumer indexes
            // a row against `times` (the forecasters, the detectors, the
            // history trim), and this is the ONE path that pushes a row it did
            // not build itself -- the union path below always sizes its row to
            // aligned.times.size().  A column shorter than its own timestamp
            // vector would make this row narrower than the axis; padding with
            // NaN says "missing", which is exactly what a slot with no value
            // is (docs/nan_policy.md).  For a well-formed group -- the only
            // kind convertQueryResponseMulti() produces, since it moves a
            // /query response's paired timestamp and value columns -- the
            // resize is a no-op.
            aligned.values.push_back(std::move(group.values));
            aligned.values.back().resize(aligned.times.size(), kMissing);
        } else {
            std::vector<double> row(aligned.times.size(), kMissing);
            // Binary search per point rather than a merge walk: the axis is
            // sorted and unique but a group's own timestamps are only as sorted
            // as the query layer made them, and a lookup cannot silently slide
            // a value into the wrong slot the way a merge walk can.
            const size_t n = std::min(group.timestamps.size(), group.values.size());
            for (size_t i = 0; i < n; ++i) {
                auto slot = std::lower_bound(aligned.times.begin(), aligned.times.end(), group.timestamps[i]);
                if (slot != aligned.times.end() && *slot == group.timestamps[i]) {
                    row[static_cast<size_t>(slot - aligned.times.begin())] = group.values[i];
                }
            }
            aligned.values.push_back(std::move(row));
        }

        // group_tags: the group's tag map flattened to "key=value" in std::map
        // order (ascending by tag key), then -- on a multi-field leg only --
        // "_field=<name>" APPENDED LAST.  Last rather than sorted in among the
        // tags because it is not a tag: keeping it at the end leaves the tag
        // sequence a client already parses untouched, and makes the synthetic
        // entry trivially separable.
        //
        // The leading underscore is InfluxDB's reserved-column convention
        // (_field / _measurement), which this project already follows on the
        // write API and the default port.  The unprefixed "field=" this
        // started as collided with a real tag KEY named `field` -- and `by
        // {field}` is in the reporter's own vocabulary, so the collision is
        // reachable, not theoretical.  It produced group_tags like
        // ["field=TAGVAL", "field=a"], which a client parsing "k=v" into a map
        // silently reduces to one entry; worse, the single-field variant of the
        // same query emitted ["field=TAGVAL"], indistinguishable from a
        // synthetic label for a field named TAGVAL.
        //
        // PATHOLOGICAL CASE (documented, not defended against): a measurement
        // carrying a tag literally named `_field` still produces two "_field="
        // entries on a multi-field leg.  The tag one comes first, in its tag
        // position; the synthetic one is always last.  Position is the only
        // discriminator, which is why the synthetic entry's position is a
        // contract and not an implementation detail.
        //
        // The flattening itself lives in flattenGroupTags() so that the
        // arithmetic multi-series path labels a group identically.
        aligned.groupTags.push_back(flattenGroupTags(group.tags, group.field, labelField));
    }

    return aligned;
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
            // Execute regular derived query, reusing the already-parsed AST.
            // RAII guard ensures cachedAst_ is reset even if execute() throws.
            struct CachedAstGuard {
                const ExpressionNode*& ref;
                ~CachedAstGuard() { ref = nullptr; }
            } guard{cachedAst_};
            cachedAst_ = ast.get();
            auto result = co_await execute(request);
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

    // Parse aggregation interval if provided (numeric ns or duration string)
    request.aggregationInterval = resolveAggregationInterval(glazeReq.aggregationInterval);

    // Opt-in per-group evaluation of an arithmetic formula.  Absent (the only
    // thing an older client can send) means false, i.e. the behaviour that has
    // always been there.
    request.multiSeries = glazeReq.multiSeries;

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

    // Execute the sub-query at the request's aggregation interval, allowing it
    // to resolve to MANY groups: anomalies() runs per group and returns one set
    // of pieces per group, each labelled with its own group_tags.  A leg that
    // resolves to exactly one group behaves exactly as it did before.
    MultiSeriesSubQueryResult multi =
        co_await executeSubQueryMulti(queryRef, applyAggregationInterval(request, it->second));

    // Every group is projected onto ONE shared axis (AnomalyQueryResult carries
    // a single `times` vector), with NaN where a group has no point.
    AlignedSubQueryGroups aligned = alignSubQueryGroups(std::move(multi), config_.maxFanOutPoints);

    if (aligned.times.empty()) {
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

    // Execute anomaly detection over every group.  executeMulti() with a
    // single group is byte-identical to the execute() call this replaced: the
    // per-piece queryIndex it varies is not part of any response encoding, and
    // the statistics it accumulates (totalPoints, anomalyCount) sum to the same
    // numbers over one group.
    anomaly::AnomalyExecutor executor;
    auto result = executor.executeMulti(aligned.times, aligned.values, aligned.groupTags, config);

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
    // Reported only when it happened -- see GlazeAnomalyStatistics.
    if (result.statistics.declinedSeriesCount > 0) {
        response.statistics.declined_series_count = result.statistics.declinedSeriesCount;
    }

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

    // Training history and prediction horizon are independent. Fetch the
    // requested history even when the visible dashboard window is shorter.
    uint64_t historyNs = 0;
    uint64_t horizonNs = 0;
    try {
        if (forecastFunc.history)
            historyNs = forecast::parseDurationToNs(*forecastFunc.history);
        if (forecastFunc.horizon)
            horizonNs = forecast::parseDurationToNs(*forecastFunc.horizon);
    } catch (const std::exception& error) {
        throw DerivedQueryException("Invalid forecast duration: " + std::string(error.what()));
    }
    auto inputQuery = applyAggregationInterval(request, it->second);
    if (historyNs > 0 && inputQuery.endTime > 0) {
        const uint64_t historyStart = inputQuery.endTime > historyNs ? inputQuery.endTime - historyNs : 0;
        // Extend when necessary, but retain groups visible in the original
        // query so history trimming can report them as declined rather than
        // making stopped devices disappear from the diagnostics.
        inputQuery.startTime = std::min(inputQuery.startTime, historyStart);
    }

    // Execute the sub-query at the request's aggregation interval, allowing it
    // to resolve to MANY groups: forecast() runs per group and returns one set
    // of pieces per group, each labelled with its own group_tags.  A leg that
    // resolves to exactly one group behaves exactly as it did before.
    MultiSeriesSubQueryResult multi = co_await executeSubQueryMulti(queryRef, inputQuery);

    // Every group is projected onto ONE shared axis (ForecastQueryResult
    // carries a single `times` vector), with NaN where a group has no point.
    AlignedSubQueryGroups aligned = alignSubQueryGroups(std::move(multi), config_.maxFanOutPoints);

    if (aligned.times.empty()) {
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

    config.historyDurationNs = historyNs;
    // An explicit training window must not be silently shortened again by
    // the horizon-dependent auto-window optimization in ForecastExecutor.
    config.disableAutoWindow = historyNs > 0;

    // Filter data based on the history parameter if specified.  The trim runs
    // ONCE on the SHARED axis and is applied to every group by the same offset,
    // so all groups are projected over the identical window — running it per
    // group would give each its own history and its own horizon, and the single
    // `times` vector the result carries could then describe only one of them.
    // The aligned vectors are dead after this block, so trim them in place; the
    // cutoff search is a binary search on sorted data.
    std::vector<uint64_t> filteredTimestamps = std::move(aligned.times);
    std::vector<std::vector<double>> seriesValues = std::move(aligned.values);

    if (config.historyDurationNs > 0 && !filteredTimestamps.empty()) {
        // Anchor explicit history to the query end. An outage must not move
        // the training window backwards to stale observations.
        const uint64_t historyEnd = inputQuery.endTime > 0 ? inputQuery.endTime : filteredTimestamps.back();
        uint64_t cutoffTime = historyEnd > config.historyDurationNs ? historyEnd - config.historyDurationNs : 0;

        // First index where timestamp >= cutoffTime (timestamps are sorted)
        auto cutIt = std::lower_bound(filteredTimestamps.begin(), filteredTimestamps.end(), cutoffTime);
        size_t startIdx = static_cast<size_t>(cutIt - filteredTimestamps.begin());

        if (startIdx > 0) {
            filteredTimestamps.erase(filteredTimestamps.begin(),
                                     filteredTimestamps.begin() + static_cast<ptrdiff_t>(startIdx));
            for (auto& values : seriesValues) {
                if (values.size() >= startIdx) {
                    values.erase(values.begin(), values.begin() + static_cast<ptrdiff_t>(startIdx));
                }
            }
        }
    }

    if (filteredTimestamps.empty()) {
        forecast::ForecastQueryResult emptyResult;
        emptyResult.success = true;
        emptyResult.statistics.algorithm = forecastFunc.algorithm;
        emptyResult.statistics.deviations = config.deviations;
        emptyResult.statistics.declinedSeriesCount = seriesValues.size();
        co_return emptyResult;
    }

    // Determine forecast horizon from the query time range and the SHARED
    // axis's sampling interval, so every group is projected the same distance
    // into the future — the result carries one `times` vector for all of them.
    // Guard against unsigned underflow if endTime <= startTime
    uint64_t duration =
        horizonNs > 0 ? horizonNs : ((request.endTime > request.startTime) ? request.endTime - request.startTime : 0);
    if (duration > 0 && filteredTimestamps.size() >= 2) {
        uint64_t timeSpan = (filteredTimestamps.back() > filteredTimestamps.front())
                                ? filteredTimestamps.back() - filteredTimestamps.front()
                                : 0;
        uint64_t interval = timeSpan / (filteredTimestamps.size() - 1);
        if (interval > 0) {
            // `duration / interval` is the number of slots the WINDOW holds at
            // the leg's own sampling rate: project to the end of what was
            // asked for.  It is NOT truncated here — see the output bound
            // below for why a ceiling on this number is the wrong instrument.
            config.forecastHorizon = static_cast<size_t>(duration / interval);
            if (horizonNs > 0 && config.forecastHorizon == 0)
                throw DerivedQueryException("Forecast horizon must be at least one sampling interval");
        }
    }

    // ---- BOUND the forecast OUTPUT, before any of it is computed -----------
    //
    // `duration / interval` is only "the axis length" while the data spans the
    // window.  When it covers a SLIVER of it the two diverge without limit:
    // 60 points at one-second spacing inside a 30-day window asks for
    // 2,592,000 forecast points and produced a MEASURED ~200 MB synchronous
    // JSON body from ~480 bytes of stored data, in 114 ms of server time, with
    // reactor stalls of 442, 233, 124, 66 and 66 ms.  (The body size is
    // FIXTURE-SPECIFIC and should be read as an order of magnitude: an
    // independent reproduction of the same 60@1s-in-30-days shape measured
    // 122.19 MB.  What varies is the DECIMAL WIDTH of the values, since each of
    // the 2.59M slots is rendered as text; the point count does not vary.)
    // maxFanOutPoints cannot
    // catch it — that bounds the INPUT matrix and this amplification is not a
    // function of the input — so a caller refused by the cell bound could reach
    // the same cost through a single-device leg.
    //
    // REFUSE, do not TRUNCATE.  Clamping the horizon (this code used to clamp
    // it to max(2000, N)) silently shortened legitimate forecasts with nothing
    // in the response to say so: 1440 one-minute points over a 30-day window
    // dropped from 43,200 forecast points to 2,000, and the same leg with
    // history='1h' to 2,000 out of 129,600 because the trim takes N down to 61
    // and the clamp collapses to its floor.  A horizon ceiling cannot tell an
    // ordinary long projection from millions of points extrapolated out of tens
    // of observations; the OUTPUT SIZE can, because that is the thing that
    // actually costs.  Answer, or refuse with a reason — never quietly answer
    // something else.
    //
    // Unlike the cell bound this applies to a SINGLE-group leg too: what it
    // counts is fabricated slots, not stored ones, so "keep returning the data
    // the user stored" does not argue for exempting it.  The historical half is
    // taken pre-auto-window (auto-windowing only ever trims), so this is an
    // upper bound on the axis the response actually carries.
    // Both arithmetic steps SATURATE rather than wrap.  `horizon` is
    // `duration / interval` and nothing upstream bounds it: two points a
    // nanosecond apart inside the widest window uint64 nanoseconds can express
    // (~584 years) gives a horizon of ~2^64.  A wrapping `historical + horizon`
    // would then come out SMALL and slip the bound entirely — the one input
    // shape most in need of refusing.
    const size_t kSizeMax = std::numeric_limits<size_t>::max();
    const size_t groupCount = seriesValues.size();
    const size_t historicalPoints = filteredTimestamps.size();
    const size_t horizon = forecast::ForecastExecutor::resolveHorizon(historicalPoints, config.forecastHorizon);
    const size_t perGroup = (horizon > kSizeMax - historicalPoints) ? kSizeMax : historicalPoints + horizon;
    if (config_.maxForecastOutputPoints > 0 && groupCount > 0 &&
        perGroup > config_.maxForecastOutputPoints / groupCount) {
        // Divided rather than multiplied for the same reason as the cell bound
        // in alignSubQueryGroups(): for positive integers a > floor(L / b) iff
        // a * b > L, and the division form stays correct without an argument
        // about how large either factor can get.
        const size_t total = (perGroup > kSizeMax / groupCount) ? kSizeMax : perGroup * groupCount;
        throw DerivedQueryException(
            "Forecast for sub-query '" + queryRef + "' would produce " + std::to_string(total) + " points (" +
            std::to_string(groupCount) + " series x (" + std::to_string(historicalPoints) + " historical + " +
            std::to_string(horizon) + " forecast)), which is more than the " +
            std::to_string(config_.maxForecastOutputPoints) +
            " point limit for a forecast result. The forecast runs to the end of the requested time range at the "
            "data's own sampling interval, so a window far wider than the data's span extrapolates a very long way: "
            "query at a coarser resolution (a larger aggregationInterval), narrow the time range, or match fewer "
            "series.");
    }

    // Execute the forecast over every group against the shared axis.
    forecast::ForecastExecutor executor;
    auto result = executor.executeMulti(filteredTimestamps, seriesValues, aligned.groupTags, config);

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
    // Reported only when it happened -- see GlazeForecastStatistics.
    if (result.statistics.declinedSeriesCount > 0) {
        response.statistics.declined_series_count = result.statistics.declinedSeriesCount;
    }

    return glz::write_json(response).value_or("{}");
}

}  // namespace timestar
