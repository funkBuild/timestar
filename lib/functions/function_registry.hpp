#pragma once

#include "function_types.hpp"

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace timestar::functions {

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
    template <typename FunctionType>
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

}  // namespace timestar::functions
