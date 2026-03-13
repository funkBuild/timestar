#pragma once

#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace timestar {

// Trait to estimate the heap size of a cached value.
// Specialize for types that own heap memory beyond sizeof(V).
template <typename V>
struct CacheSizeEstimator {
    static size_t estimate(const V& v) {
        (void)v;
        return sizeof(V);
    }
};

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class LRUCache {
public:
    explicit LRUCache(size_t maxBytes) : maxBytes_(maxBytes) {}

    // Returns nullptr if not found; promotes to front on hit
    Value* get(const Key& key) {
        auto it = map_.find(key);
        if (it == map_.end())
            return nullptr;
        // Promote to front
        list_.splice(list_.begin(), list_, it->second);
        return &it->second->value;
    }

    // Insert or update; evicts LRU entries until under budget
    void put(const Key& key, Value value) {
        size_t entrySize = estimateEntrySize(key, value);

        auto it = map_.find(key);
        if (it != map_.end()) {
            // Update existing: adjust size
            currentBytes_ -= it->second->entrySize;
            it->second->value = std::move(value);
            it->second->entrySize = entrySize;
            currentBytes_ += entrySize;
            list_.splice(list_.begin(), list_, it->second);
        } else {
            // Evict until we have room (but always allow at least 1 entry)
            while (!list_.empty() && currentBytes_ + entrySize > maxBytes_) {
                evictOne();
            }
            list_.push_front({key, std::move(value), entrySize});
            map_[key] = list_.begin();
            currentBytes_ += entrySize;
        }
    }

    bool erase(const Key& key) {
        auto it = map_.find(key);
        if (it == map_.end())
            return false;
        currentBytes_ -= it->second->entrySize;
        list_.erase(it->second);
        map_.erase(it);
        return true;
    }

    // Remove all entries whose key starts with the given prefix (for std::string keys)
    size_t clearByPrefix(const std::string& prefix) {
        size_t removed = 0;
        for (auto it = list_.begin(); it != list_.end();) {
            if constexpr (std::is_same_v<Key, std::string>) {
                if (it->key.size() >= prefix.size() &&
                    std::memcmp(it->key.data(), prefix.data(), prefix.size()) == 0) {
                    currentBytes_ -= it->entrySize;
                    map_.erase(it->key);
                    it = list_.erase(it);
                    ++removed;
                    continue;
                }
            }
            ++it;
        }
        return removed;
    }

    void clear() {
        list_.clear();
        map_.clear();
        currentBytes_ = 0;
    }

    size_t size() const { return map_.size(); }
    size_t currentBytes() const { return currentBytes_; }
    size_t maxBytes() const { return maxBytes_; }

private:
    struct Entry {
        Key key;
        Value value;
        size_t entrySize;
    };

    using ListType = std::list<Entry>;
    ListType list_;
    std::unordered_map<Key, typename ListType::iterator, Hash> map_;
    size_t maxBytes_;
    size_t currentBytes_ = 0;

    size_t estimateEntrySize(const Key& key, const Value& value) const {
        // Base overhead: list node + map entry + allocator overhead
        size_t overhead = sizeof(Entry) + sizeof(typename decltype(map_)::value_type) + 64;
        size_t keySize = CacheSizeEstimator<Key>::estimate(key);
        size_t valueSize = CacheSizeEstimator<Value>::estimate(value);
        return overhead + keySize + valueSize;
    }

    void evictOne() {
        if (list_.empty())
            return;
        auto& back = list_.back();
        currentBytes_ -= back.entrySize;
        map_.erase(back.key);
        list_.pop_back();
    }
};

// Specialization for std::string keys
template <>
struct CacheSizeEstimator<std::string> {
    static size_t estimate(const std::string& s) { return sizeof(std::string) + s.capacity(); }
};

}  // namespace timestar
