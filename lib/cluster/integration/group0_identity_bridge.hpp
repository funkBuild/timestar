#pragma once

#include "../control/group0_state.hpp"  // NodeRecord, NodeState
#include "../raft/raft_types.hpp"       // NodeId
#include "node_identity.hpp"

#include <filesystem>
#include <string>

namespace timestar::cluster {

// The seam between a node's persistent identity (node.json) and the gate-proven
// group-0 control plane (Group0Controller::initCluster / admitNode), integration
// plan M3. Building the control-plane NodeRecord from the SAME node_uuid that
// node.json persists is what makes a node stable across restarts to the rest of
// the cluster; a fresh data_dir (new node_uuid) is a new member, per the plan.

// Build the group-0 NodeRecord this node presents to `initCluster`/`admitNode`.
// `raftId` is the stable Raft replica id (from config / group-0 assignment),
// `address` the inter-node RPC endpoint, `failureDomain` the rack/az that drives
// cross-domain meta-voter selection.
control::NodeRecord nodeRecordFrom(const NodeIdentity& identity, raft::NodeId raftId, std::string address,
                                   std::string failureDomain, control::NodeState state = control::NodeState::Active);

// Close the bootstrap loop: after this node initializes or joins a cluster and
// group-0 reports the authoritative `clusterUuid`, bind it into node.json so a
// later restart knows this node already belongs to a cluster (and to which one).
// Refuses to REBIND a node already bound to a DIFFERENT cluster -- that indicates
// a data-dir cross-wire, not a legitimate re-init, and silently overwriting it
// would splice this node's data into the wrong cluster. Idempotent when the bound
// uuid already matches. Returns true iff node.json was (re)written.
bool bindClusterUuid(NodeIdentity& identity, const std::filesystem::path& dataDir, const std::string& clusterUuid);

}  // namespace timestar::cluster
