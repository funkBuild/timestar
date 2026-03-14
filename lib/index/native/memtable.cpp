#include "memtable.hpp"

namespace timestar::index {

void MemTable::put(std::string_view key, std::string_view value) {
    auto [it, inserted] = entries_.emplace(std::string(key), std::string(value));
    if (inserted) {
        // New entry: account for key + value + map node overhead (~80 bytes)
        approxMemory_ += key.size() + value.size() + 80;
    } else {
        // Update: adjust for value size change
        size_t oldSize = it->second ? it->second->size() : 0;
        approxMemory_ -= oldSize;
        approxMemory_ += value.size();
        it->second = std::string(value);
    }
}

std::optional<std::string_view> MemTable::get(std::string_view key) const {
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    if (!it->second) {
        // Tombstone
        return std::nullopt;
    }
    return std::string_view(*it->second);
}

void MemTable::remove(std::string_view key) {
    auto [it, inserted] = entries_.emplace(std::string(key), std::nullopt);
    if (inserted) {
        approxMemory_ += key.size() + 80;
    } else {
        size_t oldSize = it->second ? it->second->size() : 0;
        approxMemory_ -= oldSize;
        it->second = std::nullopt;
    }
}

bool MemTable::contains(std::string_view key) const {
    return entries_.find(key) != entries_.end();
}

bool MemTable::isTombstone(std::string_view key) const {
    auto it = entries_.find(key);
    return it != entries_.end() && !it->second;
}

MemTable::Iterator MemTable::newIterator() const {
    return Iterator(this);
}

// --- Iterator ---

MemTable::Iterator::Iterator(const MemTable* table) : table_(table) {}

void MemTable::Iterator::seekToFirst() {
    if (!table_ || table_->entries_.empty()) {
        valid_ = false;
        return;
    }
    it_ = table_->entries_.begin();
    valid_ = true;
}

void MemTable::Iterator::seek(std::string_view target) {
    if (!table_) {
        valid_ = false;
        return;
    }
    it_ = table_->entries_.lower_bound(target);
    valid_ = (it_ != table_->entries_.end());
}

void MemTable::Iterator::next() {
    if (!valid_) return;
    ++it_;
    valid_ = (it_ != table_->entries_.end());
}

}  // namespace timestar::index
