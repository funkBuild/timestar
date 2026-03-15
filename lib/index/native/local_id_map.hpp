#ifndef LOCAL_ID_MAP_H_INCLUDED
#define LOCAL_ID_MAP_H_INCLUDED

#include "series_id.hpp"

#include <tsl/robin_map.h>

#include <cassert>
#include <cstdint>
#include <optional>
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
        assert(nextId_ < UINT32_MAX && "LocalIdMap: uint32_t overflow");
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
    const SeriesId128& getGlobalId(uint32_t localId) const {
        assert(localId < localToGlobal_.size());
        return localToGlobal_[localId];
    }

    // The next local ID that will be assigned.
    uint32_t nextId() const { return nextId_; }

    // Number of mapped IDs.
    uint32_t size() const { return nextId_; }

    // Prepare for bulk restore: pre-allocate for expectedCount entries.
    void restoreBegin(uint32_t nextId, uint32_t expectedCount) {
        nextId_ = nextId;
        localToGlobal_.resize(nextId);
        globalToLocal_.reserve(expectedCount);
    }

    // Add a single mapping during restore (call between restoreBegin/restoreEnd).
    void restoreEntry(uint32_t localId, const SeriesId128& globalId) {
        if (localId < nextId_) {
            localToGlobal_[localId] = globalId;
            globalToLocal_.emplace(globalId, localId);
        }
    }

    // Restore from persisted state (used during open() recovery).
    void restore(uint32_t nextId, std::vector<std::pair<uint32_t, SeriesId128>> mappings) {
        restoreBegin(nextId, static_cast<uint32_t>(mappings.size()));
        for (auto& [localId, globalId] : mappings) {
            restoreEntry(localId, globalId);
        }
    }

private:
    uint32_t nextId_ = 0;
    tsl::robin_map<SeriesId128, uint32_t, SeriesId128::Hash> globalToLocal_;
    std::vector<SeriesId128> localToGlobal_;  // Indexed by local ID, O(1) reverse lookup
};

}  // namespace timestar::index

#endif  // LOCAL_ID_MAP_H_INCLUDED
