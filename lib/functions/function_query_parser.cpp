#include "function_query_parser.hpp"

namespace tsdb::functions {

FunctionQueryParser::FunctionQueryParser() = default;

std::vector<std::string> FunctionQueryParser::parse(const std::string& query) {
    std::vector<std::string> functions;
    
    // Simple parsing for known functions
    if (query.find("sma") != std::string::npos) {
        functions.push_back("sma");
    }
    if (query.find("ema") != std::string::npos) {
        functions.push_back("ema");
    }
    if (query.find("add") != std::string::npos) {
        functions.push_back("add");
    }
    if (query.find("multiply") != std::string::npos) {
        functions.push_back("multiply");
    }
    
    return functions;
}

} // namespace tsdb::functions