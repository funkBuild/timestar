#ifndef NATIVE_INDEX_MERGE_ITERATOR_H_INCLUDED
#define NATIVE_INDEX_MERGE_ITERATOR_H_INCLUDED

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// Abstract interface for an iterator source used by MergeIterator.
// Both MemTable iterators and SSTable iterators implement this.
class IteratorSource {
public:
    virtual ~IteratorSource() = default;

    virtual seastar::future<> seek(std::string_view target) = 0;
    virtual seastar::future<> seekToFirst() = 0;
    virtual seastar::future<> next() = 0;

    virtual bool valid() const = 0;
    virtual std::string_view key() const = 0;
    virtual std::string_view value() const = 0;
    virtual bool isTombstone() const = 0;

    // Lower number = newer. MemTable = 0, newer SSTables = lower numbers.
    // Used to resolve duplicate keys: newest source wins.
    virtual int priority() const = 0;
};

// Merges multiple sorted iterator sources into a single sorted stream.
// - Duplicate keys: the source with the lowest priority() wins (newest).
// - Tombstones: suppressed in output (deleted keys produce no output).
//
// Uses a min-heap of sources, keyed by (current_key, priority).
class MergeIterator {
public:
    explicit MergeIterator(std::vector<std::unique_ptr<IteratorSource>> sources);

    seastar::future<> seek(std::string_view target);
    seastar::future<> seekToFirst();
    seastar::future<> next();

    bool valid() const { return valid_; }
    std::string_view key() const { return currentKey_; }
    std::string_view value() const { return currentValue_; }

private:
    struct HeapEntry {
        size_t sourceIndex;
    };

    std::vector<std::unique_ptr<IteratorSource>> sources_;
    std::vector<HeapEntry> heap_;
    bool valid_ = false;
    std::string currentKey_;
    std::string_view currentValue_;

    // Rebuild heap from all valid sources
    void rebuildHeap();

    // Standard min-heap operations
    void siftDown(size_t i);
    void siftUp(size_t i);
    bool heapLess(size_t a, size_t b) const;

    // Advance past duplicate keys and tombstones
    seastar::future<> findNext();
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_MERGE_ITERATOR_H_INCLUDED
