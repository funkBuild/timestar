#include "series_matcher.hpp"

#include <regex>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

namespace timestar {

bool SeriesMatcher::matches(const std::map<std::string, std::string>& seriesTags,
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

bool SeriesMatcher::matches(const std::map<std::string, std::string>& seriesTags,
                            const std::map<std::string, std::string>& queryScopes,
                            const std::map<std::string, std::regex>& compiledScopes) {
    // Empty scopes match everything
    if (queryScopes.empty()) {
        return true;
    }

    for (const auto& [scopeKey, scopeValue] : queryScopes) {
        auto tagIt = seriesTags.find(scopeKey);
        if (tagIt == seriesTags.end()) {
            return false;
        }

        // Use the pre-compiled regex if available for this scope key.
        auto compiledIt = compiledScopes.find(scopeKey);
        if (compiledIt != compiledScopes.end()) {
            if (!std::regex_match(tagIt->second, compiledIt->second)) {
                return false;
            }
        } else {
            // Fall back to the runtime-compile path.
            if (!matchesTag(tagIt->second, scopeValue)) {
                return false;
            }
        }
    }

    return true;
}

bool SeriesMatcher::matchesTag(const std::string& tagValue, const std::string& scopeValue) {
    // Check for ~regex pattern (starts with ~)
    if (!scopeValue.empty() && scopeValue[0] == '~') {
        std::string pattern = scopeValue.substr(1);
        return matchesRegex(tagValue, pattern);
    }

    // Check for regex pattern (starts with /)
    if (!scopeValue.empty() && scopeValue[0] == '/') {
        size_t endPos = scopeValue.rfind('/');
        if (endPos > 0) {
            std::string pattern = scopeValue.substr(1, endPos - 1);
            return matchesRegex(tagValue, pattern);
        }
    }

    // Check for wildcard pattern (contains * or ?)
    if (scopeValue.find('*') != std::string::npos || scopeValue.find('?') != std::string::npos) {
        return matchesWildcard(tagValue, scopeValue);
    }

    // Exact match
    return tagValue == scopeValue;
}

// Maximum cached compiled regex patterns per thread. Prevents unbounded memory
// growth on long-running servers with diverse query patterns.  When the cache
// is full, it is cleared entirely (simple eviction; typical deployments use
// far fewer than 1024 unique patterns).
static constexpr size_t MAX_REGEX_CACHE_SIZE = 1024;

bool SeriesMatcher::matchesWildcard(const std::string& value, const std::string& pattern) {
    // Cache compiled regex per pattern to avoid ~10-100us recompilation per call.
    // Thread-local is safe under Seastar's one-thread-per-shard model.
    static thread_local std::unordered_map<std::string, std::regex> cache;

    auto it = cache.find(pattern);
    if (it == cache.end()) {
        if (cache.size() >= MAX_REGEX_CACHE_SIZE) {
            cache.clear();
        }
        std::string regexPattern = wildcardToRegex(pattern);
        try {
            auto [ins, _] = cache.emplace(pattern, std::regex(regexPattern));
            it = ins;
        } catch (const std::regex_error& e) {
            throw std::invalid_argument(std::string("invalid wildcard pattern '") + pattern + "': " + e.what());
        }
    }

    return std::regex_match(value, it->second);
}

bool SeriesMatcher::matchesRegex(const std::string& value, const std::string& pattern) {
    // Guard against ReDoS: limit pattern length and reject patterns with
    // nested quantifiers that cause catastrophic backtracking in std::regex
    // (which has no timeout mechanism). Since Seastar is single-threaded
    // per shard, a stuck regex freezes the entire shard.
    static constexpr size_t MAX_REGEX_LEN = 512;
    if (pattern.size() > MAX_REGEX_LEN) {
        throw std::invalid_argument("regex pattern exceeds maximum length of " + std::to_string(MAX_REGEX_LEN) +
                                    " characters");
    }

    // Detect patterns that cause exponential backtracking:
    // 1. Nested quantifiers: (a+)+, (a*)+, (a+)*, etc.
    // 2. Quantified alternation: (a|aa)+, (x|xy)*, etc. — overlapping
    //    alternatives with a quantifier cause the same combinatorial explosion.
    size_t depth = 0;
    bool hasQuantifierInGroup = false;
    bool hasAlternationInGroup = false;
    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (c == '\\' && i + 1 < pattern.size()) {
            ++i;  // skip escaped character
            continue;
        }
        if (c == '(') {
            depth++;
            hasQuantifierInGroup = false;
            hasAlternationInGroup = false;
        } else if (c == '+' || c == '*') {
            if (depth > 0) {
                hasQuantifierInGroup = true;
            }
        } else if (c == '|') {
            if (depth > 0) {
                hasAlternationInGroup = true;
            }
        } else if (c == ')') {
            if (depth > 0)
                depth--;
            // Check if a quantifier follows the closing paren of a group
            // that itself contains a quantifier or alternation
            if ((hasQuantifierInGroup || hasAlternationInGroup) && i + 1 < pattern.size()) {
                char next = pattern[i + 1];
                if (next == '+' || next == '*' || next == '{') {
                    throw std::invalid_argument(
                        "regex pattern rejected: quantified group with "
                        "nested quantifiers or alternation can cause "
                        "catastrophic backtracking");
                }
            }
            hasQuantifierInGroup = false;
            hasAlternationInGroup = false;
        }
    }

    // Cache compiled regex per pattern to avoid recompilation on each call.
    static thread_local std::unordered_map<std::string, std::regex> cache;

    auto it = cache.find(pattern);
    if (it == cache.end()) {
        if (cache.size() >= MAX_REGEX_CACHE_SIZE) {
            cache.clear();
        }
        try {
            auto [ins, _] = cache.emplace(pattern, std::regex(pattern, std::regex::optimize));
            it = ins;
        } catch (const std::regex_error& e) {
            throw std::invalid_argument(std::string("invalid regex pattern '") + pattern + "': " + e.what());
        }
    }

    return std::regex_match(value, it->second);
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

ScopeMatchType SeriesMatcher::classifyScope(const std::string& scopeValue) {
    if (scopeValue.empty()) {
        return ScopeMatchType::EXACT;
    }
    // ~regex or /regex/
    if (scopeValue[0] == '~') {
        return ScopeMatchType::REGEX;
    }
    if (scopeValue[0] == '/' && scopeValue.size() > 1 && scopeValue.rfind('/') > 0) {
        return ScopeMatchType::REGEX;
    }
    // Wildcard if contains * or ?
    if (scopeValue.find('*') != std::string::npos || scopeValue.find('?') != std::string::npos) {
        return ScopeMatchType::WILDCARD;
    }
    return ScopeMatchType::EXACT;
}

bool SeriesMatcher::needsRegexMatch(const std::string& pattern) {
    return classifyScope(pattern) != ScopeMatchType::EXACT;
}

std::string SeriesMatcher::toRegexPattern(const std::string& pattern) {
    auto type = classifyScope(pattern);
    switch (type) {
        case ScopeMatchType::WILDCARD:
            // Convert * / ? wildcards to their regex equivalents.
            return wildcardToRegex(pattern);
        case ScopeMatchType::REGEX:
            if (!pattern.empty() && pattern[0] == '~') {
                // ~regex — strip the leading ~
                return pattern.substr(1);
            }
            // /regex/ — strip surrounding slashes
            {
                size_t endPos = pattern.rfind('/');
                if (endPos > 0) {
                    return pattern.substr(1, endPos - 1);
                }
                // Malformed /regex pattern — treat the whole thing as the pattern
                return pattern;
            }
        case ScopeMatchType::EXACT:
        default: {
            // Exact patterns don't need regex; caller should have checked
            // needsRegexMatch() first.  Return an anchored literal anyway so
            // a caller that ignores this can still get correct behaviour.
            // Escape regex metacharacters so "foo.bar" matches literally.
            constexpr std::string_view metacharacters = R"(\.^$|()[]{}*+?)";
            std::string escaped;
            escaped.reserve(pattern.size() + 4);
            escaped += '^';
            for (char c : pattern) {
                if (metacharacters.find(c) != std::string_view::npos) {
                    escaped += '\\';
                }
                escaped += c;
            }
            escaped += '$';
            return escaped;
        }
    }
}

std::string SeriesMatcher::extractLiteralPrefix(const std::string& scopeValue) {
    if (scopeValue.empty()) {
        return "";
    }

    std::string raw;
    auto type = classifyScope(scopeValue);

    switch (type) {
        case ScopeMatchType::EXACT:
            return scopeValue;
        case ScopeMatchType::WILDCARD:
            // Return everything before the first * or ?
            for (char c : scopeValue) {
                if (c == '*' || c == '?')
                    break;
                raw += c;
            }
            return raw;
        case ScopeMatchType::REGEX: {
            // Strip the ~ or / prefix
            if (scopeValue[0] == '~') {
                raw = scopeValue.substr(1);
            } else {
                // /pattern/ — strip leading / and trailing /
                size_t endPos = scopeValue.rfind('/');
                raw = scopeValue.substr(1, endPos - 1);
            }
            // Scan for first regex metacharacter
            std::string prefix;
            for (char c : raw) {
                if (c == '[' || c == '(' || c == '.' || c == '*' || c == '+' || c == '?' || c == '{' || c == '|' ||
                    c == '\\' || c == '^' || c == '$') {
                    break;
                }
                prefix += c;
            }
            return prefix;
        }
    }
    return "";
}

}  // namespace timestar