#ifndef FUNCTION_REGISTRY_H_INCLUDED
#define FUNCTION_REGISTRY_H_INCLUDED

#include "function_types.hpp"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>

namespace tsdb::functions {

// Registry statistics
struct RegistryStats {
    size_t totalFunctions = 0;
    std::map<FunctionCategory, size_t> functionsByCategory;
    std::map<std::string, size_t> functionsByOutputType;
};

class FunctionRegistry {
private:
    std::map<std::string, std::unique_ptr<IFunction>> prototypes_;
    std::map<std::string, FunctionMetadata> metadata_;
    
public:
    static FunctionRegistry& getInstance();
    
    // Template registration method
    template<typename FunctionType>
    void registerFunction(const FunctionMetadata& metadata) {
        if (hasFunction(metadata.name)) {
            throw std::invalid_argument("Function already registered: " + metadata.name);
        }
        
        prototypes_[metadata.name] = std::make_unique<FunctionType>();
        metadata_[metadata.name] = metadata;
    }
    
    bool hasFunction(const std::string& name) const;
    std::vector<std::string> getAllFunctionNames() const;
    std::vector<std::string> getFunctionsByCategory(FunctionCategory category) const;
    std::vector<std::string> searchFunctions(const std::string& pattern) const;
    
    std::unique_ptr<IFunction> createFunction(const std::string& name) const;
    RegistryStats getStats() const;
    
    void clear();
};

} // namespace tsdb::functions

#endif // FUNCTION_REGISTRY_H_INCLUDED