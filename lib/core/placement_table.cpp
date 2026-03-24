#include "placement_table.hpp"

#include <glaze/glaze.hpp>

#include <fstream>
#include <stdexcept>

namespace timestar {

// ---------------------------------------------------------------------------
// Global singleton — mirrors the pattern in timestar_config.cpp
// ---------------------------------------------------------------------------
static PlacementTable g_defaultPlacement;
static const PlacementTable* g_placement = nullptr;

void setGlobalPlacement(PlacementTable table) {
    static PlacementTable stored;
    stored = std::move(table);
    g_placement = &stored;
}

const PlacementTable& placement() {
    return g_placement ? *g_placement : g_defaultPlacement;
}

// NOTE: Uses blocking std::ofstream.  Must be called from a seastar::async
// context (Seastar thread) or before the reactor is running.
void savePlacement(const std::string& path) {
    std::ofstream ofs(path);
    if (!ofs) {
        throw std::runtime_error("savePlacement: cannot open file: " + path);
    }
    ofs << placement().toJson();
    ofs.flush();
    if (!ofs.good()) {
        throw std::runtime_error("savePlacement: write failed for file: " + path);
    }
}

// ---------------------------------------------------------------------------
// PlacementTable
// ---------------------------------------------------------------------------

PlacementTable PlacementTable::buildLocal(unsigned coreCount) {
    if (coreCount == 0)
        throw std::invalid_argument("PlacementTable::buildLocal: coreCount must be > 0");
    PlacementTable pt;
    pt.coreCount_ = coreCount;
    // Cache power-of-2 mask for fast routing (bitwise AND vs modulo)
    pt.coreMask_ = (coreCount > 1 && (coreCount & (coreCount - 1)) == 0) ? (coreCount - 1) : 0;
    for (uint16_t i = 0; i < VIRTUAL_SHARD_COUNT; ++i) {
        pt.table_[i].serverId = 0;
        pt.table_[i].coreId = coreCount > 0 ? static_cast<uint16_t>(i % coreCount) : 0;
    }
    return pt;
}

unsigned PlacementTable::coreForHash(size_t hash) const {
    if (coreCount_ <= 1)
        return 0;
    // Fast path: power-of-2 core counts use bitwise AND (1 cycle vs ~20 for modulo)
    if (coreMask_)
        return static_cast<unsigned>(hash & coreMask_);
    return static_cast<unsigned>(hash % coreCount_);
}

}  // namespace timestar

// ---------------------------------------------------------------------------
// JSON serialization via Glaze (must be at global scope for glz::meta)
// ---------------------------------------------------------------------------

struct PlacementJson {
    unsigned coreCount;
    uint16_t virtualShardCount;
    std::vector<uint16_t> serverIds;
    std::vector<uint16_t> coreIds;
};

template <>
struct glz::meta<PlacementJson> {
    using T = PlacementJson;
    static constexpr auto value = object("coreCount", &T::coreCount, "virtualShardCount", &T::virtualShardCount,
                                         "serverIds", &T::serverIds, "coreIds", &T::coreIds);
};

namespace timestar {

std::string PlacementTable::toJson() const {
    PlacementJson pj;
    pj.coreCount = coreCount_;
    pj.virtualShardCount = VIRTUAL_SHARD_COUNT;
    pj.serverIds.resize(VIRTUAL_SHARD_COUNT);
    pj.coreIds.resize(VIRTUAL_SHARD_COUNT);
    for (uint16_t i = 0; i < VIRTUAL_SHARD_COUNT; ++i) {
        pj.serverIds[i] = table_[i].serverId;
        pj.coreIds[i] = table_[i].coreId;
    }
    return glz::write_json(pj).value_or("{}");
}

PlacementTable PlacementTable::fromJson(const std::string& data) {
    PlacementJson pj;
    auto err = glz::read_json(pj, data);
    if (err) {
        throw std::runtime_error("Failed to parse placement JSON");
    }

    if (pj.coreCount == 0) {
        throw std::runtime_error("PlacementTable: coreCount must be > 0");
    }

    if (pj.virtualShardCount != VIRTUAL_SHARD_COUNT) {
        throw std::runtime_error("PlacementTable: virtualShardCount mismatch — file has " +
                                 std::to_string(pj.virtualShardCount) + " but binary expects " +
                                 std::to_string(VIRTUAL_SHARD_COUNT));
    }

    PlacementTable pt;
    pt.coreCount_ = pj.coreCount;
    pt.coreMask_ = (pj.coreCount > 1 && (pj.coreCount & (pj.coreCount - 1)) == 0) ? (pj.coreCount - 1) : 0;
    pt.table_.fill(VShardMapping{});
    size_t count = std::min({pj.serverIds.size(), pj.coreIds.size(), static_cast<size_t>(VIRTUAL_SHARD_COUNT)});
    for (size_t i = 0; i < count; ++i) {
        if (pj.coreIds[i] >= pj.coreCount) {
            throw std::runtime_error("PlacementTable: coreId " + std::to_string(pj.coreIds[i]) + " >= coreCount " +
                                     std::to_string(pj.coreCount) + " at vshard " + std::to_string(i));
        }
        pt.table_[i].serverId = pj.serverIds[i];
        pt.table_[i].coreId = pj.coreIds[i];
    }
    return pt;
}

}  // namespace timestar
