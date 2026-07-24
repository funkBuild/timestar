#pragma once

#include "../../core/vshard.hpp"  // assignCore, VShardId
#include "vshard_directory.hpp"

#include <cstdint>
#include <set>
#include <vector>

namespace timestar::data {

// Read-only operator surface (the plan's minimum: `cluster status`,
// `vshard describe`, `placement explain`). Pure views over the control map +
// directory; the HTTP/CLI layer renders these. Per-group Raft metrics
// (role/term/commit) are surfaced separately by the group registry.

struct PlacementExplain {
    uint16_t vshard = 0;
    NodeId ownerNode = kNoNode;  // kNoNode if unassigned
    unsigned core = 0;           // derived local core = assignCore(vshard, coreCount)
    bool local = false;          // owned by this node
};

struct VShardDescribe {
    uint16_t vshard = 0;
    std::vector<NodeId> replicas;  // from the control map (primary first)
    NodeId owner = kNoNode;
    bool assigned = false;
};

struct ClusterStatus {
    NodeId self = kNoNode;
    uint64_t mapEpoch = 0;
    size_t nodeCount = 0;             // distinct owner nodes
    size_t assignedVShards = 0;       // VShards with a placement
    std::vector<NodeId> metaVoters;   // group-0 voters (supplied by the caller)
    bool controlPlaneHighlyAvailable = false;  // >= 3 meta voters
    bool clustered = false;           // any VShard owned off this node
};

class ClusterInspector {
public:
    explicit ClusterInspector(const VShardDirectory& dir) : dir_(dir) {}

    // Where a VShard routes: owning node + the node-local core it maps to.
    PlacementExplain explainVShard(uint16_t vshard, unsigned coreCount) const {
        PlacementExplain e;
        e.vshard = vshard;
        e.ownerNode = dir_.ownerOf(vshard);
        e.core = coreCount ? assignCore(timestar::VShardId{vshard}, coreCount) : 0u;
        e.local = e.ownerNode == dir_.self();
        return e;
    }
    PlacementExplain explainSeries(const SeriesId128& id, unsigned coreCount) const {
        return explainVShard(timestar::virtualShard(id), coreCount);
    }

    VShardDescribe describeVShard(uint16_t vshard) const {
        VShardDescribe d;
        d.vshard = vshard;
        auto it = dir_.map().placement.find(vshard);
        if (it != dir_.map().placement.end() && !it->second.empty()) {
            d.replicas = it->second;
            d.owner = it->second.front();
            d.assigned = true;
        }
        return d;
    }

    ClusterStatus status(std::vector<NodeId> metaVoters) const {
        ClusterStatus s;
        s.self = dir_.self();
        s.mapEpoch = dir_.epoch();
        const std::set<NodeId> owners = dir_.ownerNodes();
        s.nodeCount = owners.size();
        s.assignedVShards = 0;
        for (const auto& [vs, reps] : dir_.map().placement)
            if (!reps.empty())
                ++s.assignedVShards;
        s.metaVoters = std::move(metaVoters);
        s.controlPlaneHighlyAvailable = s.metaVoters.size() >= 3;
        for (NodeId o : owners)
            if (o != s.self)
                s.clustered = true;
        return s;
    }

private:
    const VShardDirectory& dir_;
};

}  // namespace timestar::data
