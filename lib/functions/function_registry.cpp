#include "function_registry.hpp"
#include <algorithm>

namespace tsdb::functions {

FunctionRegistry& FunctionRegistry::getInstance() {
    static FunctionRegistry instance;
    return instance;
}

bool FunctionRegistry::hasFunction(const std::string& name) const {
    return prototypes_.find(name) != prototypes_.end();
}

std::vector<std::string> FunctionRegistry::getAllFunctionNames() const {
    std::vector<std::string> names;
    for (const auto& pair : prototypes_) {
        names.push_back(pair.first);
    }
    return names;
}

std::vector<std::string> FunctionRegistry::getFunctionsByCategory(FunctionCategory category) const {
    std::vector<std::string> names;
    for (const auto& pair : metadata_) {
        if (pair.second.category == category) {
            names.push_back(pair.first);
        }
    }
    return names;
}

std::vector<std::string> FunctionRegistry::searchFunctions(const std::string& pattern) const {
    std::vector<std::string> results;
    std::string lowerPattern = pattern;
    std::transform(lowerPattern.begin(), lowerPattern.end(), lowerPattern.begin(), ::tolower);
    
    for (const auto& pair : metadata_) {
        const auto& metadata = pair.second;
        
        // Search in name
        std::string lowerName = metadata.name;
        std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);
        if (lowerName.find(lowerPattern) != std::string::npos) {
            results.push_back(pair.first);
            continue;
        }
        
        // Search in description
        std::string lowerDesc = metadata.description;
        std::transform(lowerDesc.begin(), lowerDesc.end(), lowerDesc.begin(), ::tolower);
        if (lowerDesc.find(lowerPattern) != std::string::npos) {
            results.push_back(pair.first);
        }
    }
    
    return results;
}

std::unique_ptr<IFunction> FunctionRegistry::createFunction(const std::string& name) const {
    auto it = prototypes_.find(name);
    if (it == prototypes_.end()) {
        return nullptr;
    }
    
    auto metadataIt = metadata_.find(name);
    if (metadataIt == metadata_.end()) {
        return nullptr; // This shouldn't happen if registry is consistent
    }
    
    auto cloned = it->second->clone();
    
    // Check if it's a unary function and wrap appropriately
    auto unaryFunction = dynamic_cast<IUnaryFunction*>(cloned.get());
    if (unaryFunction) {
        // Release the raw pointer from the unique_ptr and wrap it
        cloned.release();
        return std::make_unique<RegistryUnaryFunctionWrapper>(
            std::unique_ptr<IUnaryFunction>(unaryFunction), 
            metadataIt->second
        );
    } else {
        // For other function types, use the generic wrapper
        return std::make_unique<RegistryFunctionWrapper>(std::move(cloned), metadataIt->second);
    }
}

RegistryStats FunctionRegistry::getStats() const {
    RegistryStats stats;
    stats.totalFunctions = prototypes_.size();
    
    for (const auto& pair : metadata_) {
        const auto& metadata = pair.second;
        stats.functionsByCategory[metadata.category]++;
        stats.functionsByOutputType[metadata.outputType]++;
    }
    
    return stats;
}

void FunctionRegistry::clear() {
    prototypes_.clear();
    metadata_.clear();
}

} // namespace tsdb::functions