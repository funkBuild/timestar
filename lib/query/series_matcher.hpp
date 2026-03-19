#pragma once

#include <map>
#include <regex>
#include <string>

namespace timestar {

enum class ScopeMatchType { EXACT, WILDCARD, REGEX };

class SeriesMatcher {
public:
    // Check if series tags match query scopes (exact match)
    static bool matches(const std::map<std::string, std::string>& seriesTags,
                        const std::map<std::string, std::string>& queryScopes);

    // Check if series tags match query scopes using pre-compiled regex patterns.
    // For scope keys present in compiledScopes, the pre-compiled regex is used
    // directly (avoiding per-call recompilation). For all other scope keys the
    // existing runtime-compile path in matchesTag() is used.
    static bool matches(const std::map<std::string, std::string>& seriesTags,
                        const std::map<std::string, std::string>& queryScopes,
                        const std::map<std::string, std::regex>& compiledScopes);

    // Check if a single tag matches (supports wildcards, ~regex, /regex/)
    static bool matchesTag(const std::string& tagValue, const std::string& scopeValue);

    // Support for wildcard matching (* and ?)
    static bool matchesWildcard(const std::string& value, const std::string& pattern);

    // Support for regex matching (pattern starts with /)
    static bool matchesRegex(const std::string& value, const std::string& pattern);

    // Classify a scope value as EXACT, WILDCARD, or REGEX
    static ScopeMatchType classifyScope(const std::string& scopeValue);

    // Returns true when pattern requires regex/wildcard matching rather than
    // a simple equality check. Used to decide which scope values need a
    // pre-compiled std::regex.
    static bool needsRegexMatch(const std::string& pattern);

    // Convert a wildcard/regex scope pattern to a std::regex-compatible string
    // suitable for passing to std::regex(). For wildcard patterns (* / ?) the
    // wildcards are translated to their regex equivalents. For ~regex or
    // /regex/ patterns the surrounding sigils are stripped.
    static std::string toRegexPattern(const std::string& pattern);

    // Validate that a regex pattern is safe from catastrophic backtracking
    // (ReDoS). Throws std::invalid_argument if the pattern exceeds the length
    // limit or contains nested quantifiers / quantified alternation.
    static void validateRegexSafety(const std::string& pattern);

    // Extract the longest literal prefix before the first metacharacter.
    // For wildcards: "server-*" -> "server-", "server-0?" -> "server-0"
    // For ~regex: "~server-[0-9]+" -> "server-", "~[a-z]+" -> ""
    // For /regex/: "/server-[0-9]+/" -> "server-"
    // For exact: returns the full value
    static std::string extractLiteralPrefix(const std::string& scopeValue);

private:
    // Convert wildcard pattern to regex
    static std::string wildcardToRegex(const std::string& pattern);
};

}  // namespace timestar
