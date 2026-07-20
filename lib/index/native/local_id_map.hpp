#pragma once

#include "series_id.hpp"

#include <tsl/robin_map.h>

#include <cassert>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace timestar::index {

// Bidirectional mapping between global SeriesId128 and shard-local uint32_t IDs.
// Local IDs are used as keys in roaring bitmaps (which require 32-bit integers).
// Per-shard, single-threaded — no synchronization needed.
class LocalIdMap {
public:
    // Get the local ID for a global ID, assigning a new one if not yet mapped.
    uint32_t getOrAssign(const SeriesId128& globalId) {
        auto it = globalToLocal_.find(globalId);
        if (it != globalToLocal_.end()) {
            return it->second;
        }
        if (nextId_ >= UINT32_MAX) [[unlikely]] {
            throw std::overflow_error("LocalIdMap: uint32_t ID space exhausted on this shard");
        }
        uint32_t localId = nextId_++;
        globalToLocal_.emplace(globalId, localId);
        localToGlobal_.push_back(globalId);
        return localId;
    }

    // Look up local ID without assigning. Returns nullopt if not mapped.
    std::optional<uint32_t> getLocalId(const SeriesId128& globalId) const {
        auto it = globalToLocal_.find(globalId);
        if (it != globalToLocal_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    // Reverse lookup: local ID → global ID. The caller must ensure localId < nextId().
    // After restore, some slots may contain zero SeriesId128 if local IDs were lost
    // (e.g., partial WAL truncation). Callers should check isValid() or handle zero IDs.
    const SeriesId128& getGlobalId(uint32_t localId) const {
        if (localId >= localToGlobal_.size()) [[unlikely]] {
            static const SeriesId128 zero{};
            return zero;
        }
        return localToGlobal_[localId];
    }

    // Check whether a local ID is validly mapped (in bounds and not a zero hole).
    // Use this before getGlobalId() when processing restored data that may have gaps.
    bool isValid(uint32_t localId) const {
        return localId < localToGlobal_.size() && !localToGlobal_[localId].isZero();
    }

    // The next local ID that will be assigned.
    uint32_t nextId() const { return nextId_; }

    // Number of mapped IDs.
    uint32_t size() const { return nextId_; }

    // Prepare for bulk restore: pre-allocate for expectedCount entries.
    // WARNING: pre-allocation default-constructs zero SeriesId128 entries.
    // If any local IDs are missing from persisted data (e.g., partial WAL truncation),
    // those slots will remain zero, creating "holes". Use isValid() to detect them.
    //
    // `nextId` comes from a 4-byte value read off disk with no range validation,
    // and localToGlobal_ is 16 bytes per slot while globalToLocal_ is ~24 -- so a
    // corrupt or truncated counter could ask for up to 275 GB here. This runs on
    // the STARTUP path, so unlike a query-time allocation failure the shard cannot
    // recover by restarting.
    //
    // Only the SPECULATIVE pre-allocation is clamped — never the counter itself:
    // getOrAssign() freely issues ids past this value at runtime, so throwing on
    // a large counter at restore would brick a legitimately large shard on its
    // next restart while it ran fine before. restoreEntry() grows the vector as
    // real entries arrive, so growth is bounded by the data actually persisted.
    static constexpr uint32_t kMaxSpeculativeRestoreIds = 10'000'000;

    // Ids further than this past the restored counter are rejected as corrupt:
    // forward keys and the counter are persisted in one atomic batch, so a real
    // entry can never be meaningfully newer than the counter. Without a bound,
    // one corrupt 4-byte scan key demands a resize of up to 64 GB.
    static constexpr uint32_t kRestoreIdSlack = 1'048'576;

    void restoreBegin(uint32_t nextId, uint32_t expectedCount) {
        nextId_ = nextId;
        restoreCounterBase_ = nextId;
        localToGlobal_.resize(std::min(nextId, kMaxSpeculativeRestoreIds));
        globalToLocal_.reserve(std::min(expectedCount, kMaxSpeculativeRestoreIds));
    }

    // Add a single mapping during restore (call between restoreBegin/restoreEnd).
    // Entries slightly beyond the restored counter GROW the map instead of being
    // dropped: dropping them would re-assign those local IDs to different series
    // while persisted bitmaps still reference the old assignment.
    //
    // Returns false when the id is implausibly far past the persisted counter
    // (a corrupt scan key) — the entry is skipped and the caller should log it.
    [[nodiscard]] bool restoreEntry(uint32_t localId, const SeriesId128& globalId) {
        if (static_cast<uint64_t>(localId) >= static_cast<uint64_t>(restoreCounterBase_) + kRestoreIdSlack) {
            return false;
        }
        if (localId >= nextId_) {
            nextId_ = localId + 1;
        }
        if (localId >= localToGlobal_.size()) {
            // Grow only as far as this entry needs — NOT to nextId_, which may
            // be a corrupt-huge counter whose speculative prealloc was capped.
            localToGlobal_.resize(static_cast<size_t>(localId) + 1);
        }
        localToGlobal_[localId] = globalId;
        globalToLocal_.emplace(globalId, localId);
        return true;
    }

private:
    uint32_t nextId_ = 0;
    // Counter value passed to restoreBegin(); plausibility bound for restoreEntry().
    uint32_t restoreCounterBase_ = 0;
    tsl::robin_map<SeriesId128, uint32_t, SeriesId128::Hash> globalToLocal_;
    std::vector<SeriesId128> localToGlobal_;  // Indexed by local ID, O(1) reverse lookup
};

}  // namespace timestar::index
