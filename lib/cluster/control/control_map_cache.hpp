#pragma once

#include "../../core/vshard.hpp"
#include "../raft/raft_types.hpp"  // NodeId

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace timestar::control {

using timestar::raft::NodeId;

// The cluster-wide desired placement at a given map epoch: which nodes hold each
// VShard's replicas. This is the routing-relevant slice of group-0 state.
struct ControlMap {
    uint64_t epoch = 0;
    std::map<uint16_t, std::vector<NodeId>> placement;
    // Which Raft GROUP replicates each VShard (ADR 0004's prep step, debt D-11).
    // SPARSE and normally EMPTY: an absent entry means the identity mapping,
    // groupId == vshard, which is what every cluster runs today. It exists so a
    // later N:1 VShard:group consolidation is a rolling migration (the directory
    // simply starts carrying non-identity entries for the VShards that have moved
    // into a consolidated group) rather than a backup-and-rebuild -- see
    // docs/adr/0004-vshard-group-consolidation.md, "Migration path".
    //
    // Read it through VShardDirectory::groupOf(), never directly: the empty-means-
    // identity default is the whole reason this is safe to add now, and a direct
    // find() at a call site is how that default gets forgotten.
    std::map<uint16_t, uint16_t> groups;

    friend bool operator==(const ControlMap&, const ControlMap&) = default;
};

// Structural validation shared by notifications, serialized caches, and the
// durable serving-map store. A partial map is allowed here because bootstrap
// constructs the map incrementally; callers that intend to ROUTE from it must
// additionally require isCompleteControlMap().
inline bool isValidControlMap(const ControlMap& map) {
    for (const auto& [vshard, replicas] : map.placement) {
        if (vshard >= timestar::VIRTUAL_SHARD_COUNT || replicas.empty())
            return false;
        std::set<NodeId> unique;
        for (NodeId replica : replicas)
            if (replica == raft::kNoNode || !unique.insert(replica).second)
                return false;
    }
    for (const auto& [vshard, group] : map.groups)
        if (vshard >= timestar::VIRTUAL_SHARD_COUNT || group >= timestar::VIRTUAL_SHARD_COUNT)
            return false;
    return true;
}

inline bool isCompleteControlMap(const ControlMap& map) {
    if (map.epoch == 0 || map.placement.size() != timestar::VIRTUAL_SHARD_COUNT || !isValidControlMap(map))
        return false;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        if (!map.placement.contains(vshard))
            return false;
    return true;
}

// Every node caches the last valid control map (the plan) so it can keep routing
// from a committed view during a group-0 quorum outage. Updates NEVER regress the
// epoch -- a stale/restored notification with a lower epoch is rejected, which is
// the cheap first line of defence behind the epoch-regression guard.
class ControlMapCache {
public:
    // Adopt a fresh map iff its epoch is strictly AHEAD of the cached one.
    // Returns true if the cache changed. A same-epoch update is IMMUTABLE: the map
    // epoch uniquely determines the placement, so a same-epoch notification with a
    // different placement (a notifier race, retransmit, or an epoch-reusing
    // restore) is rejected rather than adopted -- otherwise two nodes could end on
    // different placements at the same epoch and both believe themselves current.
    bool update(ControlMap fresh) {
        if (!isValidControlMap(fresh) || fresh.epoch <= map_.epoch)
            return false;  // never regress; same epoch is immutable
        map_ = std::move(fresh);
        return true;
    }

    const ControlMap& current() const { return map_; }
    uint64_t epoch() const { return map_.epoch; }

    // Current v1 payload. The group count is mandatory, including zero; truncated
    // pre-v1 layouts are rejected rather than interpreted as identity mappings.
    std::string serialize() const {
        std::string out;
        auto put16 = [&](uint16_t v) {
            out.push_back(static_cast<char>(v & 0xff));
            out.push_back(static_cast<char>((v >> 8) & 0xff));
        };
        auto put64 = [&](uint64_t v) {
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
        };
        put64(map_.epoch);
        put64(map_.placement.size());
        for (const auto& [vs, reps] : map_.placement) {  // std::map: canonical order
            put16(vs);
            put64(reps.size());
            for (NodeId n : reps)
                put64(n);
        }
        put64(map_.groups.size());
        for (const auto& [vs, g] : map_.groups) {  // std::map: canonical order
            put16(vs);
            put16(g);
        }
        return out;
    }

    bool load(const std::string& s) {
        const char* p = s.data();
        const char* end = p + s.size();
        auto avail = [&](size_t n) { return static_cast<size_t>(end - p) >= n; };
        auto get16 = [&]() -> uint16_t {
            uint16_t v = static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8);
            p += 2;
            return v;
        };
        auto get64 = [&]() -> uint64_t {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
            p += 8;
            return v;
        };
        if (!avail(16))
            return false;
        ControlMap m;
        m.epoch = get64();
        uint64_t nvs = get64();
        for (uint64_t i = 0; i < nvs; ++i) {
            if (!avail(10))
                return false;  // vs(2) + count(8)
            uint16_t vs = get16();
            uint64_t nr = get64();
            if (nr > static_cast<uint64_t>(end - p) / 8)
                return false;  // non-wrapping bound
            std::vector<NodeId> reps;
            reps.reserve(nr);
            for (uint64_t j = 0; j < nr; ++j)
                reps.push_back(get64());
            if (!m.placement.emplace(vs, std::move(reps)).second)
                return false;
        }
        if (!avail(8))
            return false;
        uint64_t ng = get64();
        if (ng > static_cast<uint64_t>(end - p) / 4)
            return false;
        for (uint64_t i = 0; i < ng; ++i) {
            uint16_t vs = get16();
            uint16_t g = get16();
            if (!m.groups.emplace(vs, g).second)
                return false;
        }
        if (p != end || !isValidControlMap(m))
            return false;
        map_ = std::move(m);
        return true;
    }

private:
    ControlMap map_;
};

}  // namespace timestar::control
