#pragma once

#include "../features/feature_gate.hpp"  // FeatureGate, VersionRange
#include "../raft/raft_group.hpp"
#include "control_command.hpp"
#include "group0_state_machine.hpp"
#include "meta_voters.hpp"

#include <cstdint>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace timestar::control {

// Orchestrates the group-0 control plane on top of its RaftGroup: the bootstrap
// ceremony, node admission, self-managed meta-voter membership (via joint
// consensus), and controller-term stamping. All mutations are ordinary group-0
// proposals, so they commit only under the current leader's term -- a deposed
// controller's late writes fail the Raft term check rather than interleaving
// (native fencing, per the plan). Every method that proposes is a no-op unless
// this node is the group-0 leader (the controller).
class Group0Controller {
public:
    Group0Controller(raft::RaftGroup& group0, Group0StateMachine& sm, unsigned metaTarget = 3)
        : g0_(group0), sm_(sm), metaTarget_(metaTarget) {}

    bool isController() const { return g0_.isLeader(); }
    raft::NodeId self() const { return g0_.node().id(); }

    // `cluster init`: mint the cluster UUID, record this node (Active), and mirror
    // the stable initial Raft voter set. A multi-voter initial group therefore
    // requires quorum for the bootstrap ceremony. Same-identity retries are
    // allowed; conflicting cluster/node identity fails closed.
    seastar::future<> initCluster(std::string clusterUuid, NodeRecord selfRecord);

    // Admit a node into the cluster (record it Active) and re-evaluate the meta
    // voter set (which may promote it to a group-0 voter).
    seastar::future<> admitNode(NodeRecord record);

    // Mint a group-0 join token (leader only) that a joining node must present.
    seastar::future<bool> mintJoinToken(std::string token);
    // Admit a node ONLY if it presents a valid unused token; the token is
    // consumed atomically. Reconcile is a separate step (as with admitNode).
    seastar::future<bool> admitNodeWithToken(NodeRecord record, std::string token);

    // Recompute the desired meta voters and, if they differ from the current
    // group-0 configuration, drive a joint-consensus membership change and mirror
    // the new set into the state machine. The acknowledgement waits for final
    // Cnew to apply, not merely for the joint entry to append. It also repairs a
    // stale mirror left by a controller that lost leadership after committing
    // the real configuration. Returns true iff reconciliation work committed.
    seastar::future<bool> reconcileMetaVoters();

    // Stamp the controller epoch (= our current group-0 term) into state the
    // first time we lead under it. Idempotent (SetControllerTerm is monotonic).
    seastar::future<> stampControllerTermIfLeader();

    // Propose a single control command (leader only). Returns true only after
    // this exact entry is quorum committed and applied on this controller;
    // false means it was rejected before append because this node was not leader.
    // Leadership loss after append is an ambiguous outcome and throws.
    seastar::future<bool> proposeCommand(ControlCommand cmd);

    // Activate wire/storage format `version` cluster-wide (rolling upgrade, decision
    // 8) ONLY if every current group-0 voter's supported range covers it (FeatureGate::
    // canActivate over `voterVersions`, one range per current voter in order). Refuses
    // (returns false, no proposal) if any voter cannot read it -- a node is never sent a
    // format it cannot decode. Also false if not leader or not an advance.
    seastar::future<bool> activateFormat(uint32_t version, const std::vector<features::VersionRange>& voterVersions);

private:
    raft::RaftGroup& g0_;
    Group0StateMachine& sm_;
    unsigned metaTarget_;
};

}  // namespace timestar::control
