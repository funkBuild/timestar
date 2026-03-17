#pragma once

#include <set>
#include <string>
#include <unordered_map>

namespace timestar::index {

// SchemaUpdate captures schema changes discovered during indexing on one shard.
// After local indexing, the update is broadcast to all shards so their in-memory
// schema caches (fields, tags, tag values, field types) stay synchronized.
struct SchemaUpdate {
    std::unordered_map<std::string, std::set<std::string>> newFields;     // measurement → fields
    std::unordered_map<std::string, std::set<std::string>> newTags;       // measurement → tag keys
    std::unordered_map<std::string, std::set<std::string>> newTagValues;  // "meas\0tagKey" → values
    std::unordered_map<std::string, std::string> newFieldTypes;           // "meas\0field" → type

    bool empty() const { return newFields.empty() && newTags.empty() && newTagValues.empty() && newFieldTypes.empty(); }

    void merge(const SchemaUpdate& other) {
        for (const auto& [k, v] : other.newFields) {
            newFields[k].insert(v.begin(), v.end());
        }
        for (const auto& [k, v] : other.newTags) {
            newTags[k].insert(v.begin(), v.end());
        }
        for (const auto& [k, v] : other.newTagValues) {
            newTagValues[k].insert(v.begin(), v.end());
        }
        for (const auto& [k, v] : other.newFieldTypes) {
            // Deterministic conflict resolution: lexicographically smaller type wins
            auto it = newFieldTypes.find(k);
            if (it == newFieldTypes.end() || v < it->second) {
                newFieldTypes[k] = v;
            }
        }
    }
};

}  // namespace timestar::index
