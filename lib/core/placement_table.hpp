#pragma once

#include "series_id.hpp"

#include <array>
#include <cstdint>
#include <string>

namespace timestar {

static constexpr uint16_t VIRTUAL_SHARD_COUNT = 4096;
static constexpr uint16_t VIRTUAL_SHARD_MASK = VIRTUAL_SHARD_COUNT - 1;

struct VShardMapping {
    uint16_t serverId = 0;  // 0 = local (single-server)
    uint16_t coreId = 0;
};

class PlacementTable {
public:
    static PlacementTable buildLocal(unsigned coreCount);

    // Primary routing: hash -> core. Single-server: hash % coreCount_.
    unsigned coreForHash(size_t hash) const;
    unsigned coreCount() const { return coreCount_; }

    // Virtual shard number (for serialization / Phase 6)
    static uint16_t vshardForHash(size_t hash) {
        return static_cast<uint16_t>(hash & VIRTUAL_SHARD_MASK);
    }

    // Access the mapping table
    const VShardMapping& mapping(uint16_t vshard) const { return table_[vshard]; }

    // JSON serialization
    std::string toJson() const;
    static PlacementTable fromJson(const std::string& data);

private:
    unsigned coreCount_ = 0;
    std::array<VShardMapping, VIRTUAL_SHARD_COUNT> table_{};
};

// Global singleton — set once before app.run(), read from any shard.
void setGlobalPlacement(PlacementTable table);
const PlacementTable& placement();

// Persist placement table to disk
void savePlacement(const std::string& path);

// Convenience routing
inline unsigned routeToCore(const SeriesId128& id) {
    return placement().coreForHash(SeriesId128::Hash{}(id));
}

inline uint16_t virtualShard(const SeriesId128& id) {
    return PlacementTable::vshardForHash(SeriesId128::Hash{}(id));
}

}  // namespace timestar
