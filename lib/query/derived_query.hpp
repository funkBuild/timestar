#pragma once

#include "expression_ast.hpp"
#include "expression_parser.hpp"
#include "query_parser.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar {

// Exception for derived query errors
class DerivedQueryException : public std::runtime_error {
public:
    explicit DerivedQueryException(const std::string& message) : std::runtime_error(message) {}
};

// A derived query request containing multiple sub-queries and a formula
struct DerivedQueryRequest {
    // Named sub-queries: "a" -> QueryRequest, "b" -> QueryRequest, etc.
    std::map<std::string, QueryRequest> queries;

    // Formula expression: "(a + b) / 2", "a / (a + b) * 100", etc.
    std::string formula;

    // Time range (applies to all sub-queries if not specified per-query)
    uint64_t startTime = 0;
    uint64_t endTime = 0;

    // Aggregation interval for time bucketing (applies to final result)
    uint64_t aggregationInterval = 0;

    // OPT-IN (JSON "multiSeries", protobuf DerivedQueryRequest.multi_series):
    // let an ARITHMETIC formula's sub-queries resolve to more than one series.
    // Groups are paired across sub-queries by (tag set, field) and the formula
    // is evaluated once per group; the answer arrives under
    // DerivedQueryResult::series.
    //
    // Off by default, and deliberately so: DerivedQueryResponse is a FLAT
    // timestamps/values pair, so a client that has not been taught about
    // `series` would read a multi-group answer as an empty one -- the
    // silent-wrong-answer class this project treats as a defect.  With the flag
    // clear, a multi-series sub-query keeps returning the same 400 it always
    // has.  Same shape of opt-in as /query's bucketAlignment /
    // booleansAsNumeric.
    //
    // forecast() and anomalies() fan out over groups unconditionally (they
    // carry per-group `group_tags` in their own response shape) and ignore
    // this flag.
    bool multiSeries = false;

    // Validate the request
    void validate() const {
        if (queries.empty()) {
            throw DerivedQueryException("At least one query is required");
        }

        if (formula.empty()) {
            throw DerivedQueryException("Formula is required for derived queries");
        }

        // Validate time range
        if (startTime > 0 && endTime > 0 && startTime > endTime) {
            throw DerivedQueryException("Invalid time range: startTime > endTime");
        }

        // If one of startTime/endTime is set but not the other, reject
        if ((startTime > 0) != (endTime > 0)) {
            throw DerivedQueryException("Invalid time range: both startTime and endTime must be specified together");
        }

        // Parse formula once — cache references for later use
        parseFormulaIfNeeded();

        // Check that all referenced queries are defined
        std::set<std::string> definedQueries;
        for (const auto& [name, _] : queries) {
            definedQueries.insert(name);
        }

        for (const auto& ref : cachedQueryRefs_) {
            if (definedQueries.find(ref) == definedQueries.end()) {
                throw DerivedQueryException("Formula references undefined query: '" + ref + "'");
            }
        }
    }

    // Get all query names referenced in the formula
    std::set<std::string> getReferencedQueries() const {
        parseFormulaIfNeeded();
        return cachedQueryRefs_;
    }

    // Apply global time range to queries that don't have their own
    void applyGlobalTimeRange() {
        for (auto& [name, query] : queries) {
            if (query.startTime == 0) {
                query.startTime = startTime;
            }
            if (query.endTime == 0) {
                query.endTime = endTime;
            }
        }
    }

private:
    mutable std::set<std::string> cachedQueryRefs_;
    mutable bool formulaParsed_ = false;

    void parseFormulaIfNeeded() const {
        if (formulaParsed_)
            return;
        ExpressionParser parser(formula);
        try {
            parser.parse();
        } catch (const ExpressionParseException& e) {
            throw DerivedQueryException("Invalid formula: " + std::string(e.what()));
        }
        cachedQueryRefs_ = parser.getQueryReferences();
        formulaParsed_ = true;
    }
};

// Result of a single sub-query (aligned time series)
struct SubQueryResult {
    std::string queryName;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Metadata from the original query
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;

    bool empty() const { return timestamps.empty(); }
    size_t size() const { return timestamps.size(); }
};

// One member of a multi-series sub-query result: exactly one (tag set x field)
// pair with its own time axis.  `tags` is the series' INTERNAL tag map (the
// same std::map SeriesResult carries), not the flattened "k=v" strings the
// /query JSON serializer emits under `groupTags` -- consumers that need the
// wire shape flatten it themselves.
struct SubQuerySeries {
    std::map<std::string, std::string> tags;
    std::string field;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    bool empty() const { return timestamps.empty(); }
    size_t size() const { return timestamps.size(); }
};

// Result of a single sub-query that is allowed to resolve to MORE THAN ONE
// series -- the group model behind per-device / per-group forecast() and
// anomalies().  SubQueryResult (above) is the single-series flattening of the
// same data and stays the shape the arithmetic formula path consumes.
//
// The order of `series` is a deterministic function of the input; see
// DerivedQueryExecutor::convertQueryResponseMulti() for the exact rule.
struct MultiSeriesSubQueryResult {
    std::string queryName;
    std::string measurement;
    std::vector<SubQuerySeries> series;

    bool empty() const { return series.empty(); }
    size_t size() const { return series.size(); }
};

// One series of a MULTI-SERIES derived result: the arithmetic formula
// evaluated over a single (tag set x field) group.
struct DerivedSeriesResult {
    // The group's tags flattened to "key=value" in ascending key order, then --
    // only when the RESULT spans more than one distinct field -- the synthetic
    // "_field=<name>" entry appended LAST.  Same convention as the forecast and
    // anomaly series pieces; see DerivedQueryExecutor::alignSubQueryGroups().
    std::vector<std::string> groupTags;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    bool empty() const { return timestamps.empty(); }
    size_t size() const { return timestamps.size(); }
};

// Result of a derived query (computed from sub-queries)
struct DerivedQueryResult {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Per-group results.  EMPTY unless the request set multiSeries -- that is
    // what keeps the response byte-identical for every client that predates the
    // flag.  (Byte-identity is a claim about the RESPONSE; the REQUEST surface
    // did widen in one place -- see GlazeDerivedQueryRequest::
    // aggregationInterval, which now also accepts the JSON-numeric spelling the
    // API always documented.)  Under the flag this is the WHOLE answer at every
    // group count: the
    // flat timestamps/values above stay empty, including for a single group (an
    // earlier revision duplicated a one-group answer into both forms, which
    // doubled the body for good).
    std::vector<DerivedSeriesResult> series;

    // Was this answer produced by the per-group path?  Distinguishes "the flag
    // was set and nothing matched" (`series` empty, and the serializer emits
    // `"series": []`) from "the flag was not set" (`series` omitted entirely),
    // which a multiSeries client would otherwise have to read as `undefined`.
    bool multiSeries = false;

    // Formula that produced this result
    std::string formula;

    // Sub-query metadata for tracing
    std::map<std::string, std::string> queryMeasurements;

    // Statistics
    struct Stats {
        size_t pointCount = 0;
        double executionTimeMs = 0.0;
        size_t subQueriesExecuted = 0;
        // On the multi-series path this is the SUM across groups: each group is
        // aligned independently, so a timestamp missing from one group's legs is
        // counted once for that group.  Reported (and documented) as a total
        // rather than a per-group figure.
        size_t pointsDroppedDueToAlignment = 0;
        // Groups the formula was evaluated over.  Only meaningful -- and only
        // serialized -- on the multi-series path, where it plays the part
        // `series_count` plays for forecast().
        size_t groupCount = 0;
    } stats;

    bool empty() const { return timestamps.empty(); }
    size_t size() const { return timestamps.size(); }
};

// Builder for creating derived query requests programmatically
class DerivedQueryBuilder {
public:
    DerivedQueryBuilder& addQuery(const std::string& name, const QueryRequest& query) {
        request_.queries[name] = query;
        return *this;
    }

    DerivedQueryBuilder& addQuery(const std::string& name, const std::string& queryString) {
        QueryParser parser;
        request_.queries[name] = parser.parseQueryString(queryString);
        return *this;
    }

    DerivedQueryBuilder& setFormula(const std::string& formula) {
        request_.formula = formula;
        return *this;
    }

    DerivedQueryBuilder& setTimeRange(uint64_t start, uint64_t end) {
        request_.startTime = start;
        request_.endTime = end;
        return *this;
    }

    DerivedQueryBuilder& setAggregationInterval(uint64_t interval) {
        request_.aggregationInterval = interval;
        return *this;
    }

    // Opt in to per-group evaluation of an arithmetic formula; see
    // DerivedQueryRequest::multiSeries.
    DerivedQueryBuilder& setMultiSeries(bool multiSeries) {
        request_.multiSeries = multiSeries;
        return *this;
    }

    DerivedQueryRequest build() {
        request_.applyGlobalTimeRange();
        request_.validate();
        return request_;
    }

private:
    DerivedQueryRequest request_;
};

}  // namespace timestar
