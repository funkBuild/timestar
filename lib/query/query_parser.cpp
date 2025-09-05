#include "query_parser.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <iomanip>
#include <ctime>

namespace tsdb {

QueryRequest QueryParser::parse(
    const std::string& queryString,
    const std::string& startTimeStr,
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
    
    std::string aggMethod = queryString.substr(pos, colonPos - pos);
    request.aggregation = parseAggregation(trim(aggMethod));
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
        // Check for "by" keyword
        if (queryString.substr(pos, 2) == "by") {
            pos += 2;
            skipWhitespace(queryString, pos);
            if (pos >= queryString.length() || queryString[pos] != '{') {
                throw QueryParseException("Query missing open brace on aggregation group");
            }
            request.groupByTags = parseGroupBy(queryString, pos);
        } else {
            throw QueryParseException("Unexpected characters after query: " + 
                                    queryString.substr(pos));
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
    char dash1, dash2, space, colon1, colon2;
    
    ss >> day >> dash1 >> month >> dash2 >> year 
       >> space >> hour >> colon1 >> minute >> colon2 >> second;
    
    if (ss.fail() || dash1 != '-' || dash2 != '-' || 
        colon1 != ':' || colon2 != ':') {
        throw QueryParseException("Invalid time format. Expected: dd-mm-yyyy hh:mm:ss");
    }
    
    // Validate ranges
    if (day < 1 || day > 31 || month < 1 || month > 12 || year < 1970 ||
        hour < 0 || hour > 23 || minute < 0 || minute > 59 || 
        second < 0 || second > 59) {
        throw QueryParseException("Invalid time values in: " + timeStr);
    }
    
    tm.tm_mday = day;
    tm.tm_mon = month - 1;  // tm_mon is 0-based
    tm.tm_year = year - 1900;  // tm_year is years since 1900
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    
    std::time_t time = std::mktime(&tm);
    if (time == -1) {
        throw QueryParseException("Failed to convert time: " + timeStr);
    }
    
    // Convert to nanoseconds
    return static_cast<uint64_t>(time) * 1000000000ULL;
}

AggregationMethod QueryParser::parseAggregation(const std::string& method) {
    std::string lower = method;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    
    if (lower == "avg") return AggregationMethod::AVG;
    if (lower == "min") return AggregationMethod::MIN;
    if (lower == "max") return AggregationMethod::MAX;
    if (lower == "sum") return AggregationMethod::SUM;
    if (lower == "latest") return AggregationMethod::LATEST;
    
    throw QueryParseException("Must be one of 'avg', 'min', 'max', 'sum', 'latest'");
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
        if (std::isspace(c)) {
            // Check if followed by 'by' keyword
            size_t tempPos = pos;
            skipWhitespace(query, tempPos);
            if (tempPos + 2 <= query.length() && 
                query.substr(tempPos, 2) == "by") {
                break;
            }
        }
        pos++;
    }
    
    return trim(query.substr(start, pos - start));
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
    
    std::string fieldsStr = query.substr(pos, closePos - pos);
    pos = closePos + 1;  // Move past ')'
    
    // Empty parentheses means all fields
    fieldsStr = trim(fieldsStr);
    if (fieldsStr.empty()) {
        return fields;  // Return empty vector for "all fields"
    }
    
    // Split by comma
    fields = split(fieldsStr, ',');
    for (auto& field : fields) {
        field = trim(field);
        if (field.empty()) {
            throw QueryParseException("Empty field name in field list");
        }
    }
    
    return fields;
}

std::map<std::string, std::string> QueryParser::parseScopes(const std::string& query, size_t& pos) {
    std::map<std::string, std::string> scopes;
    
    if (query[pos] != '{') {
        throw QueryParseException("Expected '{' for scopes");
    }
    pos++;  // Skip '{'
    
    size_t closePos = query.find('}', pos);
    if (closePos == std::string::npos) {
        throw QueryParseException("Query missing closing brace on scopes");
    }
    
    std::string scopesStr = query.substr(pos, closePos - pos);
    pos = closePos + 1;  // Move past '}'
    
    // Empty braces means no filters
    scopesStr = trim(scopesStr);
    if (scopesStr.empty()) {
        return scopes;
    }
    
    // Split by comma
    std::vector<std::string> scopePairs = split(scopesStr, ',');
    for (const auto& pair : scopePairs) {
        size_t colonPos = pair.find(':');
        if (colonPos == std::string::npos) {
            throw QueryParseException("Invalid scope format. Expected 'key:value', got: " + pair);
        }
        
        std::string key = trim(pair.substr(0, colonPos));
        std::string value = trim(pair.substr(colonPos + 1));
        
        if (key.empty() || value.empty()) {
            throw QueryParseException("Empty key or value in scope: " + pair);
        }
        
        scopes[key] = value;
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
    
    std::string tagsStr = query.substr(pos, closePos - pos);
    pos = closePos + 1;  // Move past '}'
    
    tagsStr = trim(tagsStr);
    if (tagsStr.empty()) {
        throw QueryParseException("Empty group by clause");
    }
    
    // Split by comma
    tags = split(tagsStr, ',');
    for (auto& tag : tags) {
        tag = trim(tag);
        if (tag.empty()) {
            throw QueryParseException("Empty tag name in group by list");
        }
    }
    
    return tags;
}

std::string QueryParser::trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

std::vector<std::string> QueryParser::split(const std::string& str, char delimiter) {
    std::vector<std::string> result;
    std::stringstream ss(str);
    std::string item;
    
    while (std::getline(ss, item, delimiter)) {
        result.push_back(item);
    }
    
    return result;
}

void QueryParser::skipWhitespace(const std::string& str, size_t& pos) {
    while (pos < str.length() && std::isspace(str[pos])) {
        pos++;
    }
}

} // namespace tsdb