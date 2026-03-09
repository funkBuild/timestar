#ifndef FUNCTION_QUERY_PARSER_H_INCLUDED
#define FUNCTION_QUERY_PARSER_H_INCLUDED

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

} // namespace timestar::functions

#endif // FUNCTION_QUERY_PARSER_H_INCLUDED