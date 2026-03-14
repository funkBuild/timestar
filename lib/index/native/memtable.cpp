#include "memtable.hpp"

namespace timestar::index {

void MemTable::put(std::string_view key, std::string_view value) {
    // Use lower_bound + emplace_hint for O(1) amortized insertion when
    // keys arrive in sorted or nearly-sorted order (common in batch inserts).
    auto it = entries_.lower_bound(key);
    if (it != entries_.end() && it->first == key) {
        // Update existing entry
        size_t oldSize = it->second ? it->second->size() : 0;
        approxMemory_ -= oldSize;
        approxMemory_ += value.size();
        it->second = std::string(value);
    } else {
        // Insert new entry with position hint
        entries_.emplace_hint(it, std::string(key), std::string(value));
        approxMemory_ += key.size() + value.size() + 80;
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
    auto it = entries_.lower_bound(key);
    if (it != entries_.end() && it->first == key) {
        size_t oldSize = it->second ? it->second->size() : 0;
        approxMemory_ -= oldSize;
        it->second = std::nullopt;
    } else {
        entries_.emplace_hint(it, std::string(key), std::nullopt);
        approxMemory_ += key.size() + 80;
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
