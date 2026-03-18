#include "merge_iterator.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>

namespace timestar::index {

MergeIterator::MergeIterator(std::vector<std::unique_ptr<IteratorSource>> sources) : sources_(std::move(sources)) {}

bool MergeIterator::heapLess(size_t a, size_t b) const {
    auto& sa = sources_[heap_[a].sourceIndex];
    auto& sb = sources_[heap_[b].sourceIndex];
    // Sort by key first, then by priority (lower = newer = preferred)
    int cmp = sa->key().compare(sb->key());
    if (cmp != 0)
        return cmp < 0;
    return sa->priority() < sb->priority();
}

void MergeIterator::siftDown(size_t i) {
    size_t n = heap_.size();
    while (true) {
        size_t smallest = i;
        size_t left = 2 * i + 1;
        size_t right = 2 * i + 2;
        if (left < n && heapLess(left, smallest))
            smallest = left;
        if (right < n && heapLess(right, smallest))
            smallest = right;
        if (smallest == i)
            break;
        std::swap(heap_[i], heap_[smallest]);
        i = smallest;
    }
}

void MergeIterator::siftUp(size_t i) {
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (heapLess(i, parent)) {
            std::swap(heap_[i], heap_[parent]);
            i = parent;
        } else {
            break;
        }
    }
}

void MergeIterator::rebuildHeap() {
    heap_.clear();
    for (size_t i = 0; i < sources_.size(); ++i) {
        if (sources_[i]->valid()) {
            heap_.push_back({i});
        }
    }
    // Build min-heap
    for (int i = static_cast<int>(heap_.size()) / 2 - 1; i >= 0; --i) {
        siftDown(static_cast<size_t>(i));
    }
}

seastar::future<> MergeIterator::seekToFirst() {
    for (auto& src : sources_) {
        co_await src->seekToFirst();
    }
    rebuildHeap();
    co_await findNext();
}

seastar::future<> MergeIterator::seek(std::string_view target) {
    for (auto& src : sources_) {
        co_await src->seek(target);
    }
    rebuildHeap();
    co_await findNext();
}

seastar::future<> MergeIterator::next() {
    if (!valid_)
        co_return;

    // Advance all sources that are positioned at the current key
    // (skip duplicates from lower-priority sources)
    while (!heap_.empty()) {
        auto& top = sources_[heap_[0].sourceIndex];
        if (top->key() != currentKey_)
            break;

        // Advance this source
        co_await top->next();
        if (top->valid()) {
            siftDown(0);
        } else {
            // Remove from heap: swap with last, pop, sift down
            heap_[0] = heap_.back();
            heap_.pop_back();
            if (!heap_.empty())
                siftDown(0);
        }
    }

    co_await findNext();
}

seastar::future<> MergeIterator::findNext() {
    // Skip tombstones: the top of the heap is the winning entry for each key.
    // If it's a tombstone, skip all sources at that key and try the next key.
    while (!heap_.empty()) {
        auto& winner = sources_[heap_[0].sourceIndex];
        if (!winner->isTombstone()) {
            // Live entry — set current and return
            currentKey_.assign(winner->key().data(), winner->key().size());
            currentValue_ = winner->value();
            valid_ = true;
            co_return;
        }

        // Tombstone: skip all sources at this key
        std::string tombstoneKey(winner->key());
        while (!heap_.empty() && sources_[heap_[0].sourceIndex]->key() == tombstoneKey) {
            auto& src = sources_[heap_[0].sourceIndex];
            co_await src->next();
            if (src->valid()) {
                siftDown(0);
            } else {
                heap_[0] = heap_.back();
                heap_.pop_back();
                if (!heap_.empty())
                    siftDown(0);
            }
        }
    }

    valid_ = false;
}

// ============================================================================
// Synchronous interface — same logic, no coroutine overhead.
// Used by kvPrefixScan where all sources are in-memory.
// ============================================================================

void MergeIterator::seekToFirstSync() {
    for (auto& src : sources_) {
        src->seekToFirstSync();
    }
    rebuildHeap();
    findNextSync();
}

void MergeIterator::seekSync(std::string_view target) {
    for (auto& src : sources_) {
        src->seekSync(target);
    }
    rebuildHeap();
    findNextSync();
}

void MergeIterator::nextSync() {
    if (!valid_)
        return;

    // Advance all sources positioned at the current key
    while (!heap_.empty()) {
        auto& top = sources_[heap_[0].sourceIndex];
        if (top->key() != currentKey_)
            break;

        top->nextSync();
        if (top->valid()) {
            siftDown(0);
        } else {
            heap_[0] = heap_.back();
            heap_.pop_back();
            if (!heap_.empty())
                siftDown(0);
        }
    }

    findNextSync();
}

void MergeIterator::findNextSync() {
    while (!heap_.empty()) {
        auto& winner = sources_[heap_[0].sourceIndex];
        if (!winner->isTombstone()) {
            currentKey_.assign(winner->key().data(), winner->key().size());
            currentValue_ = winner->value();
            valid_ = true;
            return;
        }

        // Tombstone: skip all sources at this key
        std::string tombstoneKey(winner->key());
        while (!heap_.empty() && sources_[heap_[0].sourceIndex]->key() == tombstoneKey) {
            auto& src = sources_[heap_[0].sourceIndex];
            src->nextSync();
            if (src->valid()) {
                siftDown(0);
            } else {
                heap_[0] = heap_.back();
                heap_.pop_back();
                if (!heap_.empty())
                    siftDown(0);
            }
        }
    }

    valid_ = false;
}

}  // namespace timestar::index
