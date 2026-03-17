#pragma once

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace timestar::index {

// In-memory sorted key-value store for the native index.
// Backed by std::map for simplicity (adequate for metadata workload).
//
// Supports Put, Get, Delete (via tombstone), and sorted iteration.
// No locking needed — Seastar single-threaded per shard.
//
// A tombstone is represented by std::nullopt in the map value.
// This is important for correct merge behavior: a tombstone in the MemTable
// must suppress older values in SSTables.
class MemTable {
public:
    MemTable() = default;

    // Insert or update a key-value pair.
    void put(std::string_view key, std::string_view value);

    // Look up a key. Returns:
    //   - std::nullopt if the key is not present OR is a tombstone
    //   - The value string if the key is present and live
    // Use contains() to distinguish "not present" from "deleted".
    std::optional<std::string_view> get(std::string_view key) const;

    // Insert a tombstone for the key (marks it as deleted).
    void remove(std::string_view key);

    // Check if a key exists in the memtable (including tombstones).
    bool contains(std::string_view key) const;

    // Check if a key is a tombstone.
    bool isTombstone(std::string_view key) const;

    // Approximate memory usage in bytes.
    size_t approximateMemoryUsage() const { return approxMemory_; }

    // Number of entries (including tombstones).
    size_t size() const { return entries_.size(); }

    bool empty() const { return entries_.empty(); }

    // Sorted iteration over entries.
    class Iterator {
    public:
        Iterator() = default;

        void seekToFirst();
        void seek(std::string_view target);
        void next();

        bool valid() const { return valid_; }
        std::string_view key() const { return it_->first; }
        // Returns the value, or empty string_view for tombstones.
        std::string_view value() const { return it_->second ? std::string_view(*it_->second) : std::string_view(); }
        bool isTombstone() const { return !it_->second.has_value(); }

    private:
        friend class MemTable;
        using MapIter = std::map<std::string, std::optional<std::string>, std::less<>>::const_iterator;
        explicit Iterator(const MemTable* table);

        const MemTable* table_ = nullptr;
        MapIter it_;
        bool valid_ = false;
    };

    Iterator newIterator() const;

private:
    // Using std::less<> enables heterogeneous lookup with string_view
    std::map<std::string, std::optional<std::string>, std::less<>> entries_;
    size_t approxMemory_ = 0;
};

}  // namespace timestar::index
