#pragma once

#include <string>
#include <vector>

namespace timestar::functions {

class FunctionRegistry;

class FunctionQueryParser {
public:
    FunctionQueryParser();
    explicit FunctionQueryParser(const FunctionRegistry& registry);

    std::vector<std::string> parse(const std::string& query);

private:
    const FunctionRegistry* registry_ = nullptr;
};

}  // namespace timestar::functions
