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

    ClusterRuntime rt;
    rt.selfId = static_cast<NodeId>(cfg.node_id);
    rt.map.epoch = 1;
    // Round-robin every VShard over the N nodes. Deterministic on every node.
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs)
        rt.map.placement[vs] = {static_cast<NodeId>((vs % n) + 1)};
    for (size_t i = 0; i < n; ++i)
        rt.peerAddresses[static_cast<NodeId>(i + 1)] = cfg.peers[i];
    return rt;
}

}  // namespace timestar::cluster
