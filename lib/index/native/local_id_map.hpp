#pragma once

#include "series_id.hpp"

#include <tsl/robin_map.h>

#include <array>
#include <cassert>
#include <cstdint>
#include <memory>
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
        const uint32_t localId = nextId_;
        SeriesId128& reverse = reverseSlot(localId);
        globalToLocal_.emplace(globalId, localId);
        reverse = globalId;
        ++nextId_;
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
    // After restore, some slots may be absent if local IDs were lost (e.g., partial
    // WAL truncation). Callers should check isValid() or handle a zero ID.
    const SeriesId128& getGlobalId(uint32_t localId) const {
        const SeriesId128* globalId = reverseEntry(localId);
        return globalId ? *globalId : zeroSeriesId();
    }

    // Check whether a local ID is validly mapped (allocated and not a zero hole).
    // Use this before getGlobalId() when processing restored data that may have gaps.
    bool isValid(uint32_t localId) const {
        const SeriesId128* globalId = reverseEntry(localId);
        return globalId != nullptr && !globalId->isZero();
    }

    // The next local ID that will be assigned.
    uint32_t nextId() const { return nextId_; }

    // Number of issued ID slots, including any holes recovered from durable state.
    uint32_t size() const { return nextId_; }

    // The durable counter is a high-water mark, not proof that every reverse
    // entry is present. Reserve only a modest number of forward-map entries;
    // both maps grow from the entries actually restored. Reverse IDs live in
    // independently allocated chunks, so one sparse/high counter cannot demand
    // a multi-gigabyte contiguous vector on the startup path.
    static constexpr uint32_t kMaxSpeculativeRestoreEntries = 65'536;

    // Ids further than this past the restored counter are rejected as corrupt:
    // forward keys and the counter are persisted in one atomic batch, so a real
    // entry can never be meaningfully newer than the counter. Without a bound,
    // one corrupt 4-byte scan key demands a resize of up to 64 GB.
    static constexpr uint32_t kRestoreIdSlack = 1'048'576;

    void restoreBegin(uint32_t nextId, uint32_t expectedCount) {
        globalToLocal_.clear();
        localToGlobalChunks_.clear();
        nextId_ = nextId;
        restoreCounterBase_ = nextId;
        globalToLocal_.reserve(std::min(expectedCount, kMaxSpeculativeRestoreEntries));
    }

    // Add a single mapping during restore (call between restoreBegin/restoreEnd).
    // Entries slightly beyond the restored counter GROW the map instead of being
    // dropped: dropping them would re-assign those local IDs to different series
    // while persisted bitmaps still reference the old assignment.
    //
    // Returns false for an implausible, reserved-zero, or conflicting mapping;
    // the corrupt entry is skipped and the caller logs the startup fault.
    [[nodiscard]] bool restoreEntry(uint32_t localId, const SeriesId128& globalId) {
        if (localId == UINT32_MAX || globalId.isZero() ||
            static_cast<uint64_t>(localId) >= static_cast<uint64_t>(restoreCounterBase_) + kRestoreIdSlack) {
            return false;
        }

        if (const SeriesId128* existingGlobal = reverseEntry(localId);
            existingGlobal != nullptr && !existingGlobal->isZero()) {
            return *existingGlobal == globalId;
        }
        if (auto existingLocal = globalToLocal_.find(globalId); existingLocal != globalToLocal_.end()) {
            return existingLocal->second == localId;
        }

        SeriesId128& reverse = reverseSlot(localId);
        globalToLocal_.emplace(globalId, localId);
        reverse = globalId;
        if (localId >= nextId_) {
            nextId_ = localId + 1;
        }
        return true;
    }

private:
    static constexpr uint32_t kReverseChunkBits = 12;
    static constexpr uint32_t kReverseChunkSize = 1u << kReverseChunkBits;
    static constexpr uint32_t kReverseChunkMask = kReverseChunkSize - 1;
    using ReverseChunk = std::array<SeriesId128, kReverseChunkSize>;

    static const SeriesId128& zeroSeriesId() {
        static const SeriesId128 zero{};
        return zero;
    }

    const SeriesId128* reverseEntry(uint32_t localId) const {
        const size_t chunkIndex = localId >> kReverseChunkBits;
        if (chunkIndex >= localToGlobalChunks_.size() || !localToGlobalChunks_[chunkIndex]) {
            return nullptr;
        }
        return &(*localToGlobalChunks_[chunkIndex])[localId & kReverseChunkMask];
    }

    SeriesId128& reverseSlot(uint32_t localId) {
        const size_t chunkIndex = localId >> kReverseChunkBits;
        if (chunkIndex >= localToGlobalChunks_.size()) {
            localToGlobalChunks_.resize(chunkIndex + 1);
        }
        if (!localToGlobalChunks_[chunkIndex]) {
            localToGlobalChunks_[chunkIndex] = std::make_unique<ReverseChunk>();
        }
        return (*localToGlobalChunks_[chunkIndex])[localId & kReverseChunkMask];
    }

    uint32_t nextId_ = 0;
    // Counter value passed to restoreBegin(); plausibility bound for restoreEntry().
    uint32_t restoreCounterBase_ = 0;
    tsl::robin_map<SeriesId128, uint32_t, SeriesId128::Hash> globalToLocal_;
    // Sparse outer vector plus dense 64 KiB chunks: O(1) lookup without
    // allocating every hole below a high durable counter.
    std::vector<std::unique_ptr<ReverseChunk>> localToGlobalChunks_;
};

}  // namespace timestar::index
