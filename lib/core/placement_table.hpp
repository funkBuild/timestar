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
    static uint16_t vshardForHash(size_t hash) { return static_cast<uint16_t>(hash & VIRTUAL_SHARD_MASK); }

    // Virtual-shard -> mapping. Single-server round-robin (serverId 0,
    // coreId = vshard % coreCount), derived on the fly. Previously this was a
    // materialised std::array<VShardMapping, 4096> (16 KB/shard) that the hot
    // router (coreForHash) never read — it routes by hash % coreCount directly.
    // Deriving here drops the dead 16 KB while preserving the (round-robin)
    // mapping semantics and JSON format. Phase 6 (per-vshard serverId) would
    // reintroduce a stored table behind this same accessor.
    VShardMapping mapping(uint16_t vshard) const {
        return VShardMapping{0, coreCount_ ? static_cast<uint16_t>(vshard % coreCount_) : uint16_t{0}};
    }

    // JSON serialization
    std::string toJson() const;
    static PlacementTable fromJson(const std::string& data);

private:
    unsigned coreCount_ = 0;
    unsigned coreMask_ = 0;  // (coreCount_ - 1) when power-of-2, else 0
};

// Global singleton — set once before app.run(), read from any shard.
void setGlobalPlacement(PlacementTable table);
const PlacementTable& placement();

// Persist placement table to disk.
// Uses blocking I/O — call from seastar::async or before reactor starts.
void savePlacement(const std::string& path);

// Convenience routing
inline unsigned routeToCore(const SeriesId128& id) {
    return placement().coreForHash(SeriesId128::Hash{}(id));
}

inline uint16_t virtualShard(const SeriesId128& id) {
    return PlacementTable::vshardForHash(SeriesId128::Hash{}(id));
}

}  // namespace timestar
