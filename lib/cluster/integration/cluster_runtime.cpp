#include "cluster_runtime.hpp"

#include "../../core/vshard.hpp"  // VIRTUAL_SHARD_COUNT

#include <stdexcept>

namespace timestar::cluster {

ClusterRuntime ClusterRuntime::fromConfig(const ClusterConfig& cfg) {
    const size_t n = cfg.peers.size();
    if (n == 0)
        throw std::invalid_argument("ClusterRuntime: [cluster] peers is empty");
    if (cfg.node_id < 1 || cfg.node_id > n)
        throw std::invalid_argument("ClusterRuntime: [cluster] node_id " + std::to_string(cfg.node_id) +
                                    " out of range [1, " + std::to_string(n) + "]");

    const uint16_t rf = cfg.replication_factor < 1 ? 1 : cfg.replication_factor;
    if (rf > n)
        throw std::invalid_argument("ClusterRuntime: replication_factor exceeds node count");

    ClusterRuntime rt;
    rt.selfId = static_cast<NodeId>(cfg.node_id);
    rt.map.epoch = 1;
    // Each VShard is owned by RF distinct nodes: a primary at (vs % N)+1 and the next
    // RF-1 nodes wrapping. placement[vs][0] is the primary (RF=1 owner / RF>1 initial
    // Raft leader preference); the whole vector is the voter set. Deterministic on
    // every node. (Failure-domain-aware rendezvous is the full-M3/group-0 refinement.)
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
        std::vector<NodeId> replicas;
        replicas.reserve(rf);
        for (uint16_t k = 0; k < rf; ++k)
            replicas.push_back(static_cast<NodeId>(((vs + k) % n) + 1));
        rt.map.placement[vs] = std::move(replicas);
    }
    for (size_t i = 0; i < n; ++i)
        rt.peerAddresses[static_cast<NodeId>(i + 1)] = cfg.peers[i];
    return rt;
}

std::map<uint16_t, std::vector<NodeId>> ClusterRuntime::localReplicaGroups() const {
    // The VShards this node is a REPLICA (voter) of, each with its full voter set --
    // exactly what ReplicatedVShardHost::addVShard consumes to instantiate its groups.
    std::map<uint16_t, std::vector<NodeId>> out;
    for (const auto& [vs, reps] : map.placement)
        for (NodeId r : reps)
            if (r == selfId) {
                out[vs] = reps;
                break;
            }
    return out;
}

}  // namespace timestar::cluster
