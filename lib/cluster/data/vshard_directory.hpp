#pragma once

#include "../../core/placement_table.hpp"    // virtualShard, SeriesId128
#include "../control/control_map_cache.hpp"  // ControlMap
#include "../raft/raft_types.hpp"            // NodeId

#include <cstdint>
#include <set>

namespace timestar::data {

using ::SeriesId128;  // SeriesId128 lives in the global namespace
using timestar::control::ControlMap;
using timestar::raft::kNoNode;
using timestar::raft::NodeId;

// The data-plane view of the control map: which NODE owns each VShard, plus this
// node's own id. In Phase 4 (static RF=1) a VShard has exactly one replica, which
// is both its owner and its Raft leader, so `ownerOf` is the single routing
// authority for both writes and reads. A VShard absent from the map is
// unassigned (kNoNode) -- the caller treats that as a routing error, never a
// silent local fallback.
class VShardDirectory {
public:
    VShardDirectory(NodeId self, ControlMap map) : self_(self), map_(std::move(map)) {}

    NodeId self() const { return self_; }
    uint64_t epoch() const { return map_.epoch; }
    // The live control map. A caller MUST NOT retain this reference (or an
    // iterator into it) across a co_await: updateMap() move-reassigns map_, so a
    // reference held across a suspension can dangle. The router/coordinator
    // snapshot everything they need before their first await.
    const ControlMap& map() const { return map_; }

    // Owning (RF=1: sole-replica, == leader) node of a VShard; kNoNode if unassigned.
    NodeId ownerOf(uint16_t vshard) const {
        auto it = map_.placement.find(vshard);
        if (it == map_.placement.end() || it->second.empty())
            return kNoNode;
        return it->second.front();  // RF=1 owner; RF>1 primary is replica[0]
    }
    NodeId ownerOfSeries(const SeriesId128& id) const { return ownerOf(timestar::virtualShard(id)); }

    // The Raft GROUP that replicates a VShard (ADR 0004's prep step, debt D-11).
    //
    // Today this is ALWAYS the identity -- `ControlMap::groups` is empty on every
    // cluster in existence, so groupOf(v) == v and the whole system is byte- and
    // behaviour-identical to the version before this accessor existed. It is here
    // so that a later 1:N VShard:group consolidation needs no data migration: the
    // control map starts carrying explicit entries for the VShards that have moved
    // into a consolidated group, and every routing site already asks rather than
    // assumes. See docs/adr/0004-vshard-group-consolidation.md.
    //
    // NOTE the two things this does NOT decide, because conflating them is the
    // mistake this accessor exists to prevent:
    //   * WHICH NODES hold the data -- that is `ownerOf`, which stays per VShard
    //     (placement granularity is the VShard, consolidation or not; under
    //     consolidation the group's replica set is what constrains it).
    //   * WHICH ENGINE CORE holds the data -- that is `assignCore(vshard)`, which
    //     stays per VShard. Only the RAFT GROUP moves to `assignCore(groupOf(v))`.
    //     The apply path already hops (`EngineLocalStore` invoke_on's the VShard's
    //     own core), so the two are already decoupled.
    uint16_t groupOf(uint16_t vshard) const {
        auto it = map_.groups.find(vshard);
        return it == map_.groups.end() ? vshard : it->second;
    }

    // True iff every VShard maps to its own group (the only shape in production
    // today). Sites that must stay an integer op can assert on this.
    bool identityGroupMapping() const { return map_.groups.empty(); }

    bool isLocal(uint16_t vshard) const { return ownerOf(vshard) == self_; }
    bool isLocalSeries(const SeriesId128& id) const { return ownerOfSeries(id) == self_; }

    // Distinct owner nodes across all assigned VShards -- the fan-out target set
    // for a scatter-gather query. Excludes unassigned VShards.
    std::set<NodeId> ownerNodes() const {
        std::set<NodeId> out;
        for (const auto& [vs, reps] : map_.placement)
            if (!reps.empty())
                out.insert(reps.front());
        return out;
    }

    // Adopt a newer control map (epoch advance). Ignores a non-newer map so a
    // stale notification can never regress routing.
    bool updateMap(ControlMap fresh) {
        if (fresh.epoch <= map_.epoch)
            return false;
        map_ = std::move(fresh);
        return true;
    }

private:
    NodeId self_;
    ControlMap map_;
};

}  // namespace timestar::data
