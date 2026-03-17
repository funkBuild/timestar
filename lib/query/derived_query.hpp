#pragma once

#include "expression_ast.hpp"
#include "expression_parser.hpp"
#include "query_parser.hpp"

#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>

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

// Result of a derived query (computed from sub-queries)
struct DerivedQueryResult {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Formula that produced this result
    std::string formula;

    // Sub-query metadata for tracing
    std::map<std::string, std::string> queryMeasurements;

    // Statistics
    struct Stats {
        size_t pointCount = 0;
        double executionTimeMs = 0.0;
        size_t subQueriesExecuted = 0;
        size_t pointsDroppedDueToAlignment = 0;
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

    DerivedQueryRequest build() {
        request_.applyGlobalTimeRange();
        request_.validate();
        return request_;
    }

private:
    DerivedQueryRequest request_;
};

}  // namespace timestar
