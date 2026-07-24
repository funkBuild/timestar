#pragma once

#include "../../config/timestar_config.hpp"  // ClusterConfig
#include "../data/vshard_directory.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace timestar::cluster {

using timestar::data::ControlMap;
using timestar::data::NodeId;
using timestar::data::VShardDirectory;

// The static, derived placement for VShard-partitioned RF=1 (integration plan M2),
// built deterministically on every node from the [cluster] config -- no group 0 /
// consensus yet. Every node computes the SAME map, so any node can route a write to
// the owner and fan a query out to the owners without coordination.
//
// placement[vs] = { NodeId((vs % N) + 1) }  -- vshard vs is owned by exactly one
// node, round-robin over the N configured peers. epoch is fixed at 1: changing the
// peer list is a NEW cluster (documented limitation until M3 brings real
// reconfiguration through group 0).
struct ClusterRuntime {
    NodeId selfId = 0;
    ControlMap map;                                // epoch 1, all 4096 VShards assigned
    std::map<NodeId, std::string> peerAddresses;   // NodeId -> "host:port" (1-based, includes self)

    VShardDirectory directory() const { return VShardDirectory(selfId, map); }

    // The VShards this node replicates (is a voter of), each with its full voter set.
    // ReplicatedVShardHost instantiates one Raft group per entry.
    std::map<uint16_t, std::vector<NodeId>> localReplicaGroups() const;

    // Build from the parsed [cluster] config. Throws std::invalid_argument if the
    // config is inconsistent (empty peers, node_id out of [1, N]) -- a
    // misconfigured node must fail to start, never route to a phantom owner.
    static ClusterRuntime fromConfig(const ClusterConfig& cfg);
};

}  // namespace timestar::cluster
