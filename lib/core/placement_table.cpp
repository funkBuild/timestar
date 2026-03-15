#include "placement_table.hpp"

#include <fstream>
#include <glaze/glaze.hpp>

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

void savePlacement(const std::string& path) {
    std::ofstream ofs(path);
    if (ofs) {
        ofs << placement().toJson();
    }
}

// ---------------------------------------------------------------------------
// PlacementTable
// ---------------------------------------------------------------------------

PlacementTable PlacementTable::buildLocal(unsigned coreCount) {
    PlacementTable pt;
    pt.coreCount_ = coreCount;
    for (uint16_t i = 0; i < VIRTUAL_SHARD_COUNT; ++i) {
        pt.table_[i].serverId = 0;
        pt.table_[i].coreId = coreCount > 0
            ? static_cast<uint16_t>(i % coreCount)
            : 0;
    }
    return pt;
}

unsigned PlacementTable::coreForHash(size_t hash) const {
    return coreCount_ <= 1 ? 0 : static_cast<unsigned>(hash % coreCount_);
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
    static constexpr auto value = object(
        "coreCount", &T::coreCount,
        "virtualShardCount", &T::virtualShardCount,
        "serverIds", &T::serverIds,
        "coreIds", &T::coreIds
    );
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

    PlacementTable pt;
    pt.coreCount_ = pj.coreCount;
    size_t count = std::min<size_t>(pj.serverIds.size(), VIRTUAL_SHARD_COUNT);
    for (size_t i = 0; i < count; ++i) {
        pt.table_[i].serverId = pj.serverIds[i];
        pt.table_[i].coreId = pj.coreIds[i];
    }
    return pt;
}

}  // namespace timestar
