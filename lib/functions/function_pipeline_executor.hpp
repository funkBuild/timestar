#ifndef FUNCTION_PIPELINE_EXECUTOR_H_INCLUDED
#define FUNCTION_PIPELINE_EXECUTOR_H_INCLUDED

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <vector>
#include <map>
#include <string>
#include <variant>

// Forward declarations
class Engine;

namespace tsdb::functions {

class FunctionPipelineExecutor {
private:
    seastar::sharded<Engine>* engine_;

public:
    FunctionPipelineExecutor(seastar::sharded<Engine>* engine, void* index = nullptr);
    
    seastar::future<void> executeFunction(
        const std::string& functionName,
        const std::map<std::string, std::variant<int64_t, double, std::string>>& parameters,
        std::vector<double>& data);
};

} // namespace tsdb::functions

#endif // FUNCTION_PIPELINE_EXECUTOR_H_INCLUDED