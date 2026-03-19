#include "function_query_parser.hpp"

#include "function_registry.hpp"

#include <cctype>

namespace timestar::functions {

FunctionQueryParser::FunctionQueryParser() = default;

FunctionQueryParser::FunctionQueryParser(const FunctionRegistry& registry) : registry_(&registry) {}

// Returns true when the substring query[pos .. pos+len-1] forms a whole word,
// i.e. the character immediately before (if any) and immediately after (if any)
// are not alphanumeric and are not '_'.
static bool isWholeWord(const std::string& query, size_t pos, size_t len) {
    // Check left boundary
    if (pos > 0) {
        char before = query[pos - 1];
        if (std::isalnum(static_cast<unsigned char>(before)) || before == '_') {
            return false;
        }
    }
    // Check right boundary
    size_t end = pos + len;
    if (end < query.size()) {
        char after = query[end];
        if (std::isalnum(static_cast<unsigned char>(after)) || after == '_') {
            return false;
        }
    }
    return true;
}

// Returns true if the function name `name` appears as a whole word anywhere in `query`.
static bool containsWholeWord(const std::string& query, const std::string& name) {
    size_t pos = 0;
    while ((pos = query.find(name, pos)) != std::string::npos) {
        if (isWholeWord(query, pos, name.size())) {
            return true;
        }
        pos += 1;
    }
    return false;
}

std::vector<std::string> FunctionQueryParser::parse(const std::string& query) {
    // Collect (position, name) pairs and sort by position to preserve
    // the pipeline execution order from the query string.
    std::vector<std::pair<size_t, std::string>> matches;

    auto findAndCollect = [&](const std::string& name) {
        size_t pos = query.find(name);
        if (pos != std::string::npos && containsWholeWord(query, name)) {
            matches.emplace_back(pos, name);
        }
    };

    if (registry_) {
        for (const auto& name : registry_->getAllFunctionNames()) {
            findAndCollect(name);
        }
    } else {
        static const std::vector<std::string> builtins = {"sma", "ema", "add", "multiply"};
        for (const auto& name : builtins) {
            findAndCollect(name);
        }
    }

    std::sort(matches.begin(), matches.end());

    std::vector<std::string> functions;
    functions.reserve(matches.size());
    for (auto& [pos, name] : matches) {
        functions.push_back(std::move(name));
    }
    return functions;
}

}  // namespace timestar::functions