#ifndef __SERIES_MATCHER_H_INCLUDED__
#define __SERIES_MATCHER_H_INCLUDED__

#include <string>
#include <map>
#include <regex>

namespace tsdb {

class SeriesMatcher {
public:
    // Check if series tags match query scopes (exact match)
    static bool matches(
        const std::map<std::string, std::string>& seriesTags,
        const std::map<std::string, std::string>& queryScopes);
    
    // Check if a single tag matches (supports wildcards)
    static bool matchesTag(
        const std::string& tagValue,
        const std::string& scopeValue);
    
    // Support for wildcard matching (* and ?)
    static bool matchesWildcard(
        const std::string& value,
        const std::string& pattern);
    
    // Support for regex matching (pattern starts with /)
    static bool matchesRegex(
        const std::string& value,
        const std::string& pattern);
    
private:
    // Convert wildcard pattern to regex
    static std::string wildcardToRegex(const std::string& pattern);
};

} // namespace tsdb

#endif // __SERIES_MATCHER_H_INCLUDED__