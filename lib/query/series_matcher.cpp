#include "series_matcher.hpp"
#include <regex>

namespace tsdb {

bool SeriesMatcher::matches(
    const std::map<std::string, std::string>& seriesTags,
    const std::map<std::string, std::string>& queryScopes) {
    
    // Empty scopes match everything
    if (queryScopes.empty()) {
        return true;
    }
    
    // Check each scope condition
    for (const auto& [scopeKey, scopeValue] : queryScopes) {
        auto it = seriesTags.find(scopeKey);
        
        // If series doesn't have the tag, it doesn't match
        if (it == seriesTags.end()) {
            return false;
        }
        
        // Check if the tag value matches (supports wildcards and regex)
        if (!matchesTag(it->second, scopeValue)) {
            return false;
        }
    }
    
    return true;
}

bool SeriesMatcher::matchesTag(
    const std::string& tagValue,
    const std::string& scopeValue) {
    
    // Check for regex pattern (starts with /)
    if (!scopeValue.empty() && scopeValue[0] == '/') {
        size_t endPos = scopeValue.rfind('/');
        if (endPos > 0) {
            std::string pattern = scopeValue.substr(1, endPos - 1);
            return matchesRegex(tagValue, pattern);
        }
    }
    
    // Check for wildcard pattern (contains * or ?)
    if (scopeValue.find('*') != std::string::npos || 
        scopeValue.find('?') != std::string::npos) {
        return matchesWildcard(tagValue, scopeValue);
    }
    
    // Exact match
    return tagValue == scopeValue;
}

bool SeriesMatcher::matchesWildcard(
    const std::string& value,
    const std::string& pattern) {
    
    // Convert wildcard pattern to regex
    std::string regexPattern = wildcardToRegex(pattern);
    
    try {
        std::regex re(regexPattern);
        return std::regex_match(value, re);
    } catch (const std::regex_error&) {
        // If regex is invalid, fall back to exact match
        return value == pattern;
    }
}

bool SeriesMatcher::matchesRegex(
    const std::string& value,
    const std::string& pattern) {
    
    try {
        std::regex re(pattern);
        return std::regex_match(value, re);
    } catch (const std::regex_error&) {
        // If regex is invalid, no match
        return false;
    }
}

std::string SeriesMatcher::wildcardToRegex(const std::string& pattern) {
    std::string regex;
    regex.reserve(pattern.size() * 2);
    
    for (char c : pattern) {
        switch (c) {
            case '*':
                regex += ".*";
                break;
            case '?':
                regex += ".";
                break;
            case '.':
            case '+':
            case '^':
            case '$':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
            case '|':
            case '\\':
                // Escape special regex characters
                regex += '\\';
                regex += c;
                break;
            default:
                regex += c;
                break;
        }
    }
    
    return "^" + regex + "$";
}

} // namespace tsdb