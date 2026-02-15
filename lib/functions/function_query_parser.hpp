#ifndef FUNCTION_QUERY_PARSER_H_INCLUDED
#define FUNCTION_QUERY_PARSER_H_INCLUDED

#include <string>
#include <vector>

namespace tsdb::functions {

class FunctionQueryParser {
public:
    FunctionQueryParser();
    
    std::vector<std::string> parse(const std::string& query);
};

} // namespace tsdb::functions

#endif // FUNCTION_QUERY_PARSER_H_INCLUDED