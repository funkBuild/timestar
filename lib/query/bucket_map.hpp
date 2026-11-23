#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

namespace timestar {

// A map from bucket timestamp to per-bucket state, shaped for how aggregation
// actually uses one.
//
// Bucketed aggregation used std::unordered_map<uint64_t, State>, and with fine
// buckets that map WAS the query: a heap node per bucket, a hash and a
// cache-missing probe per access, nodes handed between maps one at a time and
// freed on a shard that did not allocate them — 1M buckets cost ~130 ms where
// reading the same 1M points raw cost 5.
//
// But the access pattern is not random. A series is decoded in time order, so
// its buckets are created in ascending order; when two series are folded
// together, the second's buckets are looked up in ascending order too. So:
//
//   - Entries live in insertion order in a few large blocks (no per-entry
//     allocation; a million entries is ~250 allocations, cheap to free from
//     any shard). For time-ordered input, insertion order is bucket order, and
//     sortedByKey() says so — consumers then need neither a hash nor a sort.
//   - A key greater than every key so far is known to be new without looking
//     anything up: it is appended.
//   - A lookup first tries the entry after the previous hit, which is what a
//     fold of one time-ordered series into another asks for every time.
//   - Any other lookup in a map that is still in key order gallops or bisects
//     from that hint. Only a map whose entries are out of order builds a hash
//     index, lazily, and only over the entries added since it was last needed.
//
// It keeps the parts of the unordered_map contract the code relies on:
// references and iterators to entries stay valid across insertions (blocks
// never move), operator[] default-constructs, iteration visits every entry
// once. Iteration order is insertion order. erase() moves the last entry into
// the hole, so it invalidates references to that entry and to end().
//
// Not thread-safe; lookups update the hint and the index (both mutable).
template <class V>
class BucketMap {
public:
    using key_type = uint64_t;
    using mapped_type = V;
    using value_type = std::pair<const uint64_t, V>;
    using size_type = size_t;

    template <bool Const>
    class Iter {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = BucketMap::value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = std::conditional_t<Const, const value_type*, value_type*>;
        using reference = std::conditional_t<Const, const value_type&, value_type&>;

        Iter() = default;
        // iterator -> const_iterator. A template so that it is never taken
        // for the copy constructor of the non-const iterator.
        template <bool OtherConst>
            requires(Const && !OtherConst)
        Iter(const Iter<OtherConst>& o) : map_(o.map_), index_(o.index_) {}

        reference operator*() const { return map_->entry(index_); }
        pointer operator->() const { return &map_->entry(index_); }
        Iter& operator++() {
            ++index_;
            return *this;
        }
        Iter operator++(int) {
            Iter copy = *this;
            ++index_;
            return copy;
        }
        friend bool operator==(const Iter& a, const Iter& b) { return a.index_ == b.index_; }
        friend bool operator!=(const Iter& a, const Iter& b) { return a.index_ != b.index_; }

    private:
        friend class BucketMap;
        template <bool>
        friend class Iter;
        using MapPtr = std::conditional_t<Const, const BucketMap*, BucketMap*>;
        Iter(MapPtr map, size_t index) : map_(map), index_(index) {}
        MapPtr map_ = nullptr;
        size_t index_ = 0;
    };
    using iterator = Iter<false>;
    using const_iterator = Iter<true>;

    BucketMap() = default;
    ~BucketMap() { destroyAll(); }

    BucketMap(const BucketMap& o) { copyFrom(o); }
    BucketMap& operator=(const BucketMap& o) {
        if (this != &o) {
            destroyAll();
            copyFrom(o);
        }
        return *this;
    }
    BucketMap(BucketMap&& o) noexcept { stealFrom(o); }
    BucketMap& operator=(BucketMap&& o) noexcept {
        if (this != &o) {
            destroyAll();
            stealFrom(o);
        }
        return *this;
    }

    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }

    // True when iteration order is ascending key order (always so for an
    // empty map and for one filled in key order).
    bool sortedByKey() const noexcept { return sorted_; }

    iterator begin() noexcept { return iterator(this, 0); }
    iterator end() noexcept { return iterator(this, size_); }
    const_iterator begin() const noexcept { return const_iterator(this, 0); }
    const_iterator end() const noexcept { return const_iterator(this, size_); }
    const_iterator cbegin() const noexcept { return begin(); }
    const_iterator cend() const noexcept { return end(); }

    // The most recently inserted entry — the largest key when sortedByKey().
    const value_type& back() const noexcept { return entry(size_ - 1); }

    // Storage grows by blocks and the index is built on demand, so there is
    // nothing useful to do ahead of time. Kept because callers sized the hash
    // table this replaces from the query's bucket count — an upper bound that
    // most series never approach.
    void reserve(size_t) noexcept {}

    V& operator[](uint64_t key) {
        const size_t found = findIndex(key);
        if (found != kNone) {
            return entry(found).second;
        }
        return append(key).second;
    }

    template <class... Args>
    std::pair<iterator, bool> try_emplace(uint64_t key, Args&&... args) {
        const size_t found = findIndex(key);
        if (found != kNone) {
            return {iterator(this, found), false};
        }
        append(key, std::forward<Args>(args)...);
        return {iterator(this, size_ - 1), true};
    }

    template <class... Args>
    std::pair<iterator, bool> emplace(uint64_t key, Args&&... args) {
        return try_emplace(key, std::forward<Args>(args)...);
    }

    iterator find(uint64_t key) {
        const size_t found = findIndex(key);
        return iterator(this, found == kNone ? size_ : found);
    }
    const_iterator find(uint64_t key) const {
        const size_t found = findIndex(key);
        return const_iterator(this, found == kNone ? size_ : found);
    }
    size_t count(uint64_t key) const { return findIndex(key) == kNone ? 0 : 1; }
    bool contains(uint64_t key) const { return findIndex(key) != kNone; }

    V& at(uint64_t key) {
        const size_t found = findIndex(key);
        if (found == kNone) {
            throw std::out_of_range("BucketMap::at");
        }
        return entry(found).second;
    }
    const V& at(uint64_t key) const {
        const size_t found = findIndex(key);
        if (found == kNone) {
            throw std::out_of_range("BucketMap::at");
        }
        return entry(found).second;
    }

    // Removes `key` if present by moving the last entry into its place.
    size_t erase(uint64_t key) {
        const size_t found = findIndex(key);
        if (found == kNone) {
            return 0;
        }
        const size_t last = size_ - 1;
        if (found != last) {
            // The key is const in value_type, so relocate rather than assign.
            value_type& hole = entry(found);
            value_type& tail = entry(last);
            hole.~value_type();
            ::new (static_cast<void*>(&hole)) value_type(tail.first, std::move(tail.second));
        }
        dropTail(1);
        // Order, maximum and index no longer describe the contents.
        sorted_ = true;
        maxKey_ = 0;
        for (size_t i = 0; i < size_; ++i) {
            const uint64_t k = entry(i).first;
            sorted_ = sorted_ && (i == 0 || k > maxKey_);
            maxKey_ = std::max(maxKey_, k);
        }
        return 1;
    }

    // Destroys the last `n` entries (all of them if n >= size()) and frees the
    // blocks that empties. Lets a consumer release a large map in bounded
    // steps instead of one long destructor.
    void dropTail(size_t n) noexcept {
        n = std::min(n, size_);
        for (size_t i = 0; i < n; ++i) {
            entry(size_ - 1 - i).~value_type();
        }
        size_ -= n;
        while (!blocks_.empty() && blockBase(blocks_.size() - 1) >= size_) {
            freeBlock(blocks_.size() - 1);
            blocks_.pop_back();
        }
        slots_.clear();
        indexed_ = 0;
        hint_ = 0;
        if (size_ == 0) {
            sorted_ = true;
            maxKey_ = 0;
        } else if (sorted_) {
            maxKey_ = entry(size_ - 1).first;
        }
        // Unsorted: maxKey_ may now exceed every key, which only makes the
        // "larger than everything" shortcut miss; it is never wrong.
    }

    void clear() noexcept { dropTail(size_); }

    // Frees all storage WITHOUT running the values' destructors. For a
    // consumer that has just visited every value and left each one owning
    // nothing (its destructor would be a no-op): destroying a million values
    // afterwards means touching every one of them again, cold, for nothing.
    // The keys are trivially destructible. If the consumer stops part-way and
    // never calls this, the ordinary destructor still does the right thing.
    void releaseWithoutDestroying() noexcept {
        for (size_t b = 0; b < blocks_.size(); ++b) {
            freeBlock(b);
        }
        blocks_.clear();
        slots_.clear();
        size_ = 0;
        indexed_ = 0;
        hint_ = 0;
        maxKey_ = 0;
        sorted_ = true;
    }

private:
    static constexpr size_t kNone = ~size_t{0};

    // Block b holds a power-of-two run of entries: 8, 8, 16, 32, ... up to
    // kMaxBlock, then kMaxBlock each. Small maps stay small; large ones settle
    // into ~512KB blocks (for a 128-byte entry).
    static constexpr unsigned kFirstLog = 3;
    static constexpr unsigned kMaxLog = 12;
    static constexpr size_t kMaxBlock = size_t{1} << kMaxLog;
    static constexpr size_t kGrowingBlocks = kMaxLog - kFirstLog + 1;  // blocks below the fixed-size regime

    static void locate(size_t index, size_t& block, size_t& offset) noexcept {
        if (index < (size_t{1} << kFirstLog)) {
            block = 0;
            offset = index;
        } else if (index < kMaxBlock) {
            const unsigned log = static_cast<unsigned>(std::bit_width(index)) - 1;
            block = log - kFirstLog + 1;
            offset = index - (size_t{1} << log);
        } else {
            block = kGrowingBlocks + ((index - kMaxBlock) >> kMaxLog);
            offset = (index - kMaxBlock) & (kMaxBlock - 1);
        }
    }
    static size_t blockBase(size_t block) noexcept {
        if (block == 0) {
            return 0;
        }
        if (block < kGrowingBlocks) {
            return size_t{1} << (block + kFirstLog - 1);
        }
        return kMaxBlock + ((block - kGrowingBlocks) << kMaxLog);
    }
    static size_t blockCapacity(size_t block) noexcept {
        if (block == 0) {
            return size_t{1} << kFirstLog;
        }
        if (block < kGrowingBlocks) {
            return size_t{1} << (block + kFirstLog - 1);
        }
        return kMaxBlock;
    }

    value_type& entry(size_t index) noexcept {
        size_t block, offset;
        locate(index, block, offset);
        return blocks_[block][offset];
    }
    const value_type& entry(size_t index) const noexcept {
        size_t block, offset;
        locate(index, block, offset);
        return blocks_[block][offset];
    }

    void freeBlock(size_t block) noexcept {
        std::allocator<value_type>().deallocate(blocks_[block], blockCapacity(block));
    }

    template <class... Args>
    value_type& append(uint64_t key, Args&&... args) {
        size_t block, offset;
        locate(size_, block, offset);
        if (block == blocks_.size()) {
            blocks_.push_back(std::allocator<value_type>().allocate(blockCapacity(block)));
        }
        value_type* slot = blocks_[block] + offset;
        ::new (static_cast<void*>(slot)) value_type(std::piecewise_construct, std::forward_as_tuple(key),
                                                    std::forward_as_tuple(std::forward<Args>(args)...));
        sorted_ = sorted_ && (size_ == 0 || key > maxKey_);
        maxKey_ = (size_ == 0) ? key : std::max(maxKey_, key);
        hint_ = size_;
        ++size_;
        return *slot;
    }

    size_t findIndex(uint64_t key) const noexcept {
        if (size_ == 0 || key > maxKey_) {
            return kNone;
        }
        // The previous hit, or the entry after it: what a fold of one
        // time-ordered series into another asks for every time.
        if (hint_ < size_) {
            if (entry(hint_).first == key) {
                return hint_;
            }
            const size_t next = hint_ + 1;
            if (next < size_ && entry(next).first == key) {
                hint_ = next;
                return next;
            }
        }
        // In key order, position is findable without an index: gallop forward
        // from the hint (lookups usually ascend) or bisect behind it. The hash
        // index is only for maps whose entries are out of order.
        const size_t found = sorted_ ? orderedLookup(key) : indexLookup(key);
        if (found != kNone) {
            hint_ = found;
        }
        return found;
    }

    size_t orderedLookup(uint64_t key) const noexcept {
        size_t lo = 0;
        size_t hi = size_;
        if (hint_ < size_) {
            if (key > entry(hint_).first) {
                lo = hint_ + 1;
                size_t step = 1;
                while (lo + step < size_ && entry(lo + step).first < key) {
                    lo += step + 1;
                    step <<= 1;
                }
                hi = std::min(lo + step + 1, size_);
            } else {
                hi = hint_;
            }
        }
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (entry(mid).first < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return (lo < size_ && entry(lo).first == key) ? lo : kNone;
    }

    // Fibonacci hash. The top bits pick the slot; the low 32 are kept in the
    // slot as a tag, so a probe compares tags and only touches an entry (128
    // bytes away in a cold block) when the key is almost certainly there.
    static uint64_t mix(uint64_t key) noexcept { return key * 0x9E3779B97F4A7C15ULL; }
    static uint64_t packSlot(uint64_t hash, size_t index) noexcept {
        return (hash << 32) | static_cast<uint64_t>(index + 1);
    }

    // Bring the index up to date with the entries, growing it when more than
    // half full.
    void syncIndex() const {
        if (indexed_ == size_ && !slots_.empty()) {
            return;
        }
        if (slots_.size() < size_ * 2 || slots_.empty()) {
            const size_t capacity = std::bit_ceil(std::max<size_t>(size_ * 2 + 2, 16));
            slots_.assign(capacity, 0);
            shift_ = 64 - static_cast<unsigned>(std::bit_width(capacity) - 1);
            indexed_ = 0;
        }
        const size_t mask = slots_.size() - 1;
        for (size_t i = indexed_; i < size_; ++i) {
            const uint64_t hash = mix(entry(i).first);
            size_t slot = static_cast<size_t>(hash >> shift_);
            while (slots_[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            slots_[slot] = packSlot(hash, i);
        }
        indexed_ = size_;
    }

    size_t indexLookup(uint64_t key) const {
        syncIndex();
        const size_t mask = slots_.size() - 1;
        const uint64_t hash = mix(key);
        const uint64_t tag = hash << 32;
        size_t slot = static_cast<size_t>(hash >> shift_);
        while (slots_[slot] != 0) {
            if ((slots_[slot] & 0xFFFFFFFF00000000ULL) == tag) {
                const size_t index = static_cast<size_t>(slots_[slot] & 0xFFFFFFFFULL) - 1;
                if (entry(index).first == key) {
                    return index;
                }
            }
            slot = (slot + 1) & mask;
        }
        return kNone;
    }

    void destroyAll() noexcept {
        for (size_t i = 0; i < size_; ++i) {
            entry(i).~value_type();
        }
        for (size_t b = 0; b < blocks_.size(); ++b) {
            freeBlock(b);
        }
        blocks_.clear();
        slots_.clear();
        size_ = 0;
        indexed_ = 0;
        hint_ = 0;
        maxKey_ = 0;
        sorted_ = true;
    }

    void copyFrom(const BucketMap& o) {
        for (size_t i = 0; i < o.size_; ++i) {
            const value_type& e = o.entry(i);
            append(e.first, e.second);
        }
        hint_ = 0;
    }

    void stealFrom(BucketMap& o) noexcept {
        blocks_ = std::move(o.blocks_);
        slots_ = std::move(o.slots_);
        size_ = o.size_;
        indexed_ = o.indexed_;
        shift_ = o.shift_;
        hint_ = o.hint_;
        maxKey_ = o.maxKey_;
        sorted_ = o.sorted_;
        o.blocks_.clear();
        o.slots_.clear();
        o.size_ = 0;
        o.indexed_ = 0;
        o.hint_ = 0;
        o.maxKey_ = 0;
        o.sorted_ = true;
    }

    std::vector<value_type*> blocks_;
    size_t size_ = 0;
    uint64_t maxKey_ = 0;  // largest key present (meaningful when size_ > 0)
    bool sorted_ = true;   // entries are in ascending key order

    // Lookup acceleration; none of it is part of the map's value.
    mutable size_t hint_ = 0;              // index of the last entry hit or appended
    mutable std::vector<uint64_t> slots_;  // open addressing: hash tag << 32 | entry index + 1; 0 = empty
    mutable size_t indexed_ = 0;           // entries [0, indexed_) are in slots_
    mutable unsigned shift_ = 64;
};

}  // namespace timestar
