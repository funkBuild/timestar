#include "query_parser.hpp"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace timestar {

QueryRequest QueryParser::parse(const std::string& queryString, const std::string& startTimeStr,
                                const std::string& endTimeStr) {
    QueryRequest request = parseQueryString(queryString);
    request.startTime = parseTime(startTimeStr);
    request.endTime = parseTime(endTimeStr);

    if (request.startTime >= request.endTime) {
        throw QueryParseException("Start time must be before end time");
    }

    return request;
}

QueryRequest QueryParser::parseQueryString(const std::string& queryString) {
    if (queryString.empty()) {
        throw QueryParseException("Query string cannot be empty");
    }

    QueryRequest request;
    size_t pos = 0;

    // Skip leading whitespace
    skipWhitespace(queryString, pos);

    // Parse aggregation method (before ':')
    size_t colonPos = queryString.find(':', pos);
    if (colonPos == std::string::npos) {
        throw QueryParseException("Query needs to specify an aggregation method");
    }

    request.aggregation = parseAggregation(queryString.substr(pos, colonPos - pos));
    pos = colonPos + 1;

    // Parse measurement (before '(' or '{' or end)
    request.measurement = parseMeasurement(queryString, pos);

    if (request.measurement.empty()) {
        throw QueryParseException("Query measurement must be present");
    }

    // Parse required fields parentheses (can be empty for all fields)
    skipWhitespace(queryString, pos);
    if (pos >= queryString.length() || queryString[pos] != '(') {
        throw QueryParseException("Query missing field parentheses after measurement");
    }
    request.fields = parseFields(queryString, pos);

    // Parse optional scopes
    skipWhitespace(queryString, pos);
    if (pos < queryString.length() && queryString[pos] == '{') {
        request.scopes = parseScopes(queryString, pos);
    }

    // Parse optional group by
    skipWhitespace(queryString, pos);
    if (pos < queryString.length()) {
        // Check for "by" keyword (with word boundary — must be followed by
        // whitespace or '{' to avoid matching "bytes", "bypass", etc.)
        if (pos + 1 < queryString.length() && queryString[pos] == 'b' && queryString[pos + 1] == 'y' &&
            (pos + 2 >= queryString.length() || queryString[pos + 2] == ' ' || queryString[pos + 2] == '\t' ||
             queryString[pos + 2] == '{')) {
            pos += 2;
            skipWhitespace(queryString, pos);
            if (pos >= queryString.length() || queryString[pos] != '{') {
                throw QueryParseException("Query missing open brace on aggregation group");
            }
            request.groupByTags = parseGroupBy(queryString, pos);

            skipWhitespace(queryString, pos);
            if (pos < queryString.length()) {
                throw QueryParseException("Unexpected characters after group by: " + queryString.substr(pos));
            }
        } else {
            throw QueryParseException("Unexpected characters after query: " + queryString.substr(pos));
        }
    }

    return request;
}

uint64_t QueryParser::parseTime(const std::string& timeStr) {
    // Parse format: dd-mm-yyyy hh:mm:ss
    std::tm tm = {};
    std::istringstream ss(timeStr);

    // Parse components
    int day, month, year, hour, minute, second;
    char dash1, dash2, colon1, colon2;

    ss >> day >> dash1 >> month >> dash2 >> year >> hour >> colon1 >> minute >> colon2 >> second;

    if (ss.fail() || dash1 != '-' || dash2 != '-' || colon1 != ':' || colon2 != ':') {
        throw QueryParseException("Invalid time format. Expected: dd-mm-yyyy hh:mm:ss");
    }

    // Reject trailing characters after the expected format
    std::string remaining;
    if (ss >> remaining) {
        throw QueryParseException("Trailing characters in time string: " + timeStr);
    }

    // Validate ranges
    if (day < 1 || day > 31 || month < 1 || month > 12 || year < 1970 || hour < 0 || hour > 23 || minute < 0 ||
        minute > 59 || second < 0 || second > 59) {
        throw QueryParseException("Invalid time values in: " + timeStr);
    }

    tm.tm_mday = day;
    tm.tm_mon = month - 1;     // tm_mon is 0-based
    tm.tm_year = year - 1900;  // tm_year is years since 1900
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    tm.tm_isdst = 0;

    std::time_t time = timegm(&tm);
    if (time == -1) {
        throw QueryParseException("Failed to convert time: " + timeStr);
    }

    // Post-validation: timegm normalizes invalid dates (e.g., Feb 30 -> Mar 2).
    // Detect this by checking that the fields were not modified.
    if (tm.tm_mday != day || tm.tm_mon != (month - 1) || (tm.tm_year + 1900) != year) {
        throw QueryParseException("Invalid date: day " + std::to_string(day) +
            " is out of range for month " + std::to_string(month));
    }

    // Convert to nanoseconds
    return static_cast<uint64_t>(time) * 1000000000ULL;
}

// Case-insensitive comparison without allocating a lowercase copy.
static bool ciEqual(std::string_view a, std::string_view b) {
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (static_cast<unsigned char>(std::tolower(static_cast<unsigned char>(a[i]))) !=
            static_cast<unsigned char>(std::tolower(static_cast<unsigned char>(b[i]))))
            return false;
    }
    return true;
}

AggregationMethod QueryParser::parseAggregation(const std::string& method) {
    std::string_view sv = trimView(method);

    if (ciEqual(sv, "avg"))
        return AggregationMethod::AVG;
    if (ciEqual(sv, "min"))
        return AggregationMethod::MIN;
    if (ciEqual(sv, "max"))
        return AggregationMethod::MAX;
    if (ciEqual(sv, "sum"))
        return AggregationMethod::SUM;
    if (ciEqual(sv, "latest"))
        return AggregationMethod::LATEST;
    if (ciEqual(sv, "count"))
        return AggregationMethod::COUNT;
    if (ciEqual(sv, "first"))
        return AggregationMethod::FIRST;
    if (ciEqual(sv, "median"))
        return AggregationMethod::MEDIAN;
    if (ciEqual(sv, "stddev"))
        return AggregationMethod::STDDEV;
    if (ciEqual(sv, "stdvar"))
        return AggregationMethod::STDVAR;
    if (ciEqual(sv, "spread"))
        return AggregationMethod::SPREAD;

    throw QueryParseException(
        "Must be one of 'avg', 'min', 'max', 'sum', 'latest', 'count', 'first', 'median', 'stddev', 'stdvar', "
        "'spread'");
}

std::string QueryParser::parseMeasurement(const std::string& query, size_t& pos) {
    skipWhitespace(query, pos);
    size_t start = pos;

    // Measurement ends at '(', '{', or whitespace followed by 'by'
    while (pos < query.length()) {
        char c = query[pos];
        if (c == '(' || c == '{') {
            break;
        }
        if (std::isspace(static_cast<unsigned char>(c))) {
            // Check if followed by 'by' keyword
            size_t tempPos = pos;
            skipWhitespace(query, tempPos);
            if (tempPos + 1 < query.length() && query[tempPos] == 'b' && query[tempPos + 1] == 'y' &&
                (tempPos + 2 == query.length() || std::isspace(static_cast<unsigned char>(query[tempPos + 2])) || query[tempPos + 2] == '{')) {
                break;
            }
        }
        pos++;
    }

    // Trim trailing whitespace without extra allocation
    std::string_view sv = trimView(std::string_view(query).substr(start, pos - start));

    if (sv.empty()) {
        throw QueryParseException("Empty measurement name");
    }
    if (sv.find_first_of(" \t\n\r") != std::string_view::npos) {
        throw QueryParseException("Measurement name cannot contain whitespace: '" + std::string(sv) + "'");
    }

    return std::string(sv);
}

std::vector<std::string> QueryParser::parseFields(const std::string& query, size_t& pos) {
    std::vector<std::string> fields;

    if (query[pos] != '(') {
        throw QueryParseException("Expected '(' for fields");
    }
    pos++;  // Skip '('

    size_t closePos = query.find(')', pos);
    if (closePos == std::string::npos) {
        throw QueryParseException("Query missing close bracket on fields");
    }

    std::string_view fieldsView = trimView(std::string_view(query).substr(pos, closePos - pos));
    pos = closePos + 1;

    if (fieldsView.empty()) {
        return fields;
    }

    // Split by comma using string_view to avoid intermediate string allocations
    size_t start = 0;
    while (start < fieldsView.size()) {
        size_t end = fieldsView.find(',', start);
        if (end == std::string_view::npos)
            end = fieldsView.size();
        std::string_view token = trimView(fieldsView.substr(start, end - start));
        if (token.empty()) {
            throw QueryParseException("Empty field name in field list");
        }
        fields.emplace_back(token);
        start = end + 1;
    }

    return fields;
}

std::map<std::string, std::string> QueryParser::parseScopes(const std::string& query, size_t& pos) {
    std::map<std::string, std::string> scopes;

    if (query[pos] != '{') {
        throw QueryParseException("Expected '{' for scopes");
    }
    pos++;  // Skip '{'

    // NOTE: Regex scope values containing '}' (e.g., {host:/server-[0-9]{2,4}/})
    // are not supported. The parser finds the first '}' as the closing brace.
    // Workaround: use ~ prefix syntax instead: {host:~server-[0-9]\{2,4\}}
    size_t closePos = query.find('}', pos);
    if (closePos == std::string::npos) {
        throw QueryParseException("Query missing closing brace on scopes");
    }

    std::string_view scopesView = trimView(std::string_view(query).substr(pos, closePos - pos));
    pos = closePos + 1;

    if (scopesView.empty()) {
        return scopes;
    }

    // Parse comma-separated key:value pairs using string_view.
    // We must not split on commas that appear inside regex patterns:
    //   ~prefix regex:  e.g. host:~server-[a,b]+   — commas inside [...] are literal
    //   /regex/ style:  e.g. host:/server,[0-9]+/   — commas between /.../ are literal
    // Strategy: scan character by character, tracking whether we're inside a
    // /.../ regex or a [...] bracket expression. Only split on top-level commas.
    size_t start = 0;
    while (start < scopesView.size()) {
        size_t end = start;
        bool inSlashRegex = false;  // between /  .../
        int bracketDepth = 0;       // inside [...]
        bool seenColon = false;     // past the key: separator
        while (end < scopesView.size()) {
            char c = scopesView[end];
            if (!seenColon) {
                // Before the colon — just looking for the key:value separator
                if (c == ':') {
                    seenColon = true;
                    // Peek: does the value start with '/'?
                    if (end + 1 < scopesView.size() && scopesView[end + 1] == '/') {
                        inSlashRegex = true;
                        end += 2;  // skip ':' and opening '/'
                        continue;
                    }
                }
            } else if (inSlashRegex) {
                // Inside /regex/ — skip to closing unescaped '/'
                if (c == '/' && (end == 0 || scopesView[end - 1] != '\\')) {
                    inSlashRegex = false;
                }
            } else {
                // In the value portion (plain or ~regex)
                if (c == '[') {
                    bracketDepth++;
                } else if (c == ']' && bracketDepth > 0) {
                    bracketDepth--;
                } else if (c == ',' && bracketDepth == 0) {
                    break;  // top-level comma — this is a real delimiter
                }
            }
            end++;
        }
        std::string_view pair = trimView(scopesView.substr(start, end - start));

        size_t colonP = pair.find(':');
        if (colonP == std::string_view::npos) {
            throw QueryParseException("Invalid scope format. Expected 'key:value', got: " + std::string(pair));
        }

        std::string_view key = trimView(pair.substr(0, colonP));
        std::string_view value = trimView(pair.substr(colonP + 1));

        if (key.empty() || value.empty()) {
            throw QueryParseException("Empty key or value in scope: " + std::string(pair));
        }

        if (scopes.count(std::string(key))) {
            throw QueryParseException("Duplicate scope key: '" + std::string(key) + "'");
        }
        scopes[std::string(key)] = std::string(value);
        start = end + 1;
    }

    return scopes;
}

std::vector<std::string> QueryParser::parseGroupBy(const std::string& query, size_t& pos) {
    std::vector<std::string> tags;

    if (query[pos] != '{') {
        throw QueryParseException("Expected '{' for group by tags");
    }
    pos++;  // Skip '{'

    size_t closePos = query.find('}', pos);
    if (closePos == std::string::npos) {
        throw QueryParseException("Query missing closing brace on aggregation group");
    }

    std::string_view tagsView = trimView(std::string_view(query).substr(pos, closePos - pos));
    pos = closePos + 1;

    if (tagsView.empty()) {
        throw QueryParseException("Empty group by clause");
    }

    size_t start = 0;
    while (start < tagsView.size()) {
        size_t end = tagsView.find(',', start);
        if (end == std::string_view::npos)
            end = tagsView.size();
        std::string_view token = trimView(tagsView.substr(start, end - start));
        if (token.empty()) {
            throw QueryParseException("Empty tag name in group by list");
        }
        tags.emplace_back(token);
        start = end + 1;
    }

    return tags;
}

std::string_view QueryParser::trimView(std::string_view str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string_view::npos)
        return {};
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

void QueryParser::skipWhitespace(const std::string& str, size_t& pos) {
    while (pos < str.length() && std::isspace(static_cast<unsigned char>(str[pos]))) {
        pos++;
    }
}

}  // namespace timestar