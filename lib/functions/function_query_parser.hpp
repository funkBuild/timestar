#ifndef __FUNCTION_QUERY_PARSER_H_INCLUDED__
#define __FUNCTION_QUERY_PARSER_H_INCLUDED__

#include <string>
#include <vector>

namespace tsdb::functions {

class FunctionQueryParser {
public:
    FunctionQueryParser();
    
    std::vector<std::string> parse(const std::string& query);
};

} // namespace tsdb::functions

#endif // __FUNCTION_QUERY_PARSER_H_INCLUDED__