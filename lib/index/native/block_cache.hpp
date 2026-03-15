#ifndef NATIVE_INDEX_BLOCK_CACHE_H_INCLUDED
#define NATIVE_INDEX_BLOCK_CACHE_H_INCLUDED

#include <cstddef>
#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>

namespace timestar::index {

// Byte-budgeted LRU cache for decompressed SSTable data blocks.
// Single-threaded (Seastar shard-local) — no locking needed.
// Key: (cache_id, block_index) where cache_id is unique per SSTableReader.
class BlockCache {
public:
    explicit BlockCache(size_t maxBytes) : maxBytes_(maxBytes) {}

    // Look up a cached block. Returns pointer to block data (stable until
    // next put/evict call), or nullptr on miss. Promotes entry to MRU on hit.
    const std::string* get(uint64_t cacheId, size_t blockIndex) {
        auto it = map_.find({cacheId, blockIndex});
        if (it == map_.end()) return nullptr;
        list_.splice(list_.begin(), list_, it->second);
        return &it->second->data;
    }

    // Insert a block into the cache. Returns reference to the cached data.
    // May evict LRU entries to stay under budget.
    const std::string& put(uint64_t cacheId, size_t blockIndex, std::string data) {
        CacheKey key{cacheId, blockIndex};
        size_t entrySize = sizeof(Entry) + data.capacity() + 64;

        auto it = map_.find(key);
        if (it != map_.end()) {
            currentBytes_ -= it->second->entrySize;
            it->second->data = std::move(data);
            it->second->entrySize = entrySize;
            currentBytes_ += entrySize;
            list_.splice(list_.begin(), list_, it->second);
            return it->second->data;
        }

        while (!list_.empty() && currentBytes_ + entrySize > maxBytes_) {
            evictOne();
        }

        list_.push_front({key, std::move(data), entrySize});
        map_[key] = list_.begin();
        currentBytes_ += entrySize;
        return list_.front().data;
    }

    // Evict all entries for a given cache ID (called when SSTableReader is destroyed).
    void evict(uint64_t cacheId) {
        for (auto it = list_.begin(); it != list_.end();) {
            if (it->key.cacheId == cacheId) {
                currentBytes_ -= it->entrySize;
                map_.erase(it->key);
                it = list_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Generate a unique cache ID. Thread-local monotonic counter.
    static uint64_t nextCacheId() {
        static thread_local uint64_t counter = 0;
        return ++counter;
    }

    size_t size() const { return map_.size(); }
    size_t currentBytes() const { return currentBytes_; }
    size_t maxBytes() const { return maxBytes_; }

private:
    struct CacheKey {
        uint64_t cacheId;
        size_t blockIndex;
        bool operator==(const CacheKey& other) const {
            return cacheId == other.cacheId && blockIndex == other.blockIndex;
        }
    };

    struct CacheKeyHash {
        size_t operator()(const CacheKey& k) const {
            return std::hash<uint64_t>()(k.cacheId) ^ (std::hash<size_t>()(k.blockIndex) * 2654435761ULL);
        }
    };

    struct Entry {
        CacheKey key;
        std::string data;
        size_t entrySize;
    };

    using ListType = std::list<Entry>;
    ListType list_;
    std::unordered_map<CacheKey, typename ListType::iterator, CacheKeyHash> map_;
    size_t maxBytes_;
    size_t currentBytes_ = 0;

    void evictOne() {
        if (list_.empty()) return;
        auto& back = list_.back();
        currentBytes_ -= back.entrySize;
        map_.erase(back.key);
        list_.pop_back();
    }
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_BLOCK_CACHE_H_INCLUDED
