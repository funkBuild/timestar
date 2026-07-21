#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar {

enum class AggregationMethod {
    AVG,
    MIN,
    MAX,
    SUM,
    LATEST,
    COUNT,         // number of non-NaN data points
    FIRST,         // earliest value (inverse of LATEST)
    MEDIAN,        // approximate 50th percentile via t-digest (~0.01% error)
    EXACT_MEDIAN,  // exact 50th percentile via nth_element (requires all raw values)
    STDDEV,        // population standard deviation
    STDVAR,        // population variance
    SPREAD         // max - min range
};

struct QueryRequest {
    AggregationMethod aggregation = AggregationMethod::AVG;
    std::string measurement;
    std::vector<std::string> fields;            // Empty means all fields
    std::map<std::string, std::string> scopes;  // Tag filters (AND condition)
    std::vector<std::string> groupByTags;
    uint64_t startTime = 0;            // Nanoseconds since epoch
    uint64_t endTime = 0;              // Nanoseconds since epoch
    uint64_t aggregationInterval = 0;  // Nanoseconds, 0 means no time-based aggregation

    // Anchor of the bucket grid for interval queries.  0 = epoch-aligned
    // buckets (the canonical default: bucket = floor(ts/interval)*interval,
    // boundaries never shift with the query range).  Non-zero = the grid is
    // anchored at this timestamp instead (bucket = anchor +
    // floor((ts-anchor)/interval)*interval) — requested via
    // `"bucketAlignment": "start"`, which anchors at startTime.  This exists
    // for migration compatibility with rollup.js-style readers, whose buckets
    // are start-aligned; labels remain bucket starts and empty buckets are
    // still omitted in both modes.  Anchored queries bypass the pushdown /
    // batch / streaming fast paths (whose bucket math is epoch-only) and take
    // the fallback aggregation path on every placement, so the answer cannot
    // depend on the query plan.
    uint64_t bucketAnchor = 0;

    // Migration compatibility: treat boolean fields as numeric 1.0/0.0 so they
    // participate in numeric aggregation (rollup.js readers expect
    // avg([t,t,f,t,f]) == 0.6).  Default false — the canonical rule is that
    // booleans are non-numeric and never aggregate arithmetically (see
    // CLAUDE.md "Non-Numeric Fields in Queries"); this opt-in converts them
    // shard-side BEFORE aggregation, so every method, bucket grid, and
    // placement sees plain doubles.  Strings remain non-numeric regardless.
    bool booleansAsNumeric = false;

    // Helper to check if query requests all fields
    bool requestsAllFields() const { return fields.empty(); }

    // Helper to check if query has no tag filters
    bool hasNoFilters() const { return scopes.empty(); }

    // Helper to check if query has grouping
    bool hasGroupBy() const { return !groupByTags.empty(); }

    // Helper to check if any scope uses wildcard or regex patterns
    bool hasPatternFilters() const {
        for (const auto& [key, value] : scopes) {
            if (value.find('*') != std::string::npos || value.find('?') != std::string::npos ||
                (!value.empty() && (value[0] == '~' || value[0] == '/')))
                return true;
        }
        return false;
    }
};

class QueryParser {
public:
    // Parse query string format:
    // aggregationMethod:measurement(fields){scopes} by {aggregationTagKeys}
    // Time format: dd-mm-yyyy hh:mm:ss
    [[nodiscard]] static QueryRequest parse(const std::string& queryString, const std::string& startTimeStr,
                                            const std::string& endTimeStr);

    // Parse just the query string (for testing)
    [[nodiscard]] static QueryRequest parseQueryString(const std::string& queryString);

    // Parse time string to nanoseconds since epoch
    static uint64_t parseTime(const std::string& timeStr);

private:
    // Component parsers
    static AggregationMethod parseAggregation(const std::string& method);
    static std::string parseMeasurement(const std::string& query, size_t& pos);
    static std::vector<std::string> parseFields(const std::string& query, size_t& pos);
    static std::map<std::string, std::string> parseScopes(const std::string& query, size_t& pos);
    static std::vector<std::string> parseGroupBy(const std::string& query, size_t& pos);

    // Helper functions
    static std::string_view trimView(std::string_view str);
    static void skipWhitespace(const std::string& str, size_t& pos);
};

// Exception for query parsing errors
class QueryParseException : public std::runtime_error {
public:
    explicit QueryParseException(const std::string& message) : std::runtime_error("Query parse error: " + message) {}
};

}  // namespace timestar
