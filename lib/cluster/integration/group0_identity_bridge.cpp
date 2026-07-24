#include "group0_identity_bridge.hpp"

#include <stdexcept>

namespace timestar::cluster {

control::NodeRecord nodeRecordFrom(const NodeIdentity& identity, raft::NodeId raftId,
                                   std::string address, std::string failureDomain,
                                   control::NodeState state) {
    if (identity.node_uuid.empty())
        throw std::invalid_argument("nodeRecordFrom: identity has no node_uuid");
    control::NodeRecord r;
    r.raftId = raftId;
    r.uuid = identity.node_uuid;
    r.address = std::move(address);
    r.failureDomain = std::move(failureDomain);
    r.state = state;
    return r;
}

bool bindClusterUuid(NodeIdentity& identity, const std::filesystem::path& dataDir,
                     const std::string& clusterUuid) {
    if (clusterUuid.empty())
        throw std::invalid_argument("bindClusterUuid: refusing to bind an empty cluster uuid");

    if (identity.cluster_uuid == clusterUuid)
        return false;  // already bound to this cluster -- idempotent, nothing to write

    if (!identity.cluster_uuid.empty())
        // Bound to a DIFFERENT cluster already: this is a cross-wired data_dir, not a
        // re-init. Overwriting would splice this node (and its data) into the wrong
        // cluster; fail loudly instead.
        throw std::runtime_error("bindClusterUuid: node is already bound to cluster '" +
                                 identity.cluster_uuid + "', refusing to rebind to '" + clusterUuid +
                                 "'");

    identity.cluster_uuid = clusterUuid;
    identity.persist(dataDir);
    return true;
}

}  // namespace timestar::cluster
