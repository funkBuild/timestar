#pragma once

#include "../raft/raft_group.hpp"
#include "control_command.hpp"
#include "group0_state_machine.hpp"
#include "meta_voters.hpp"

#include <seastar/core/future.hh>
#include <string>

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
    // the sole-voter set. The fresh group 0 is already a group of one.
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
    // the new set into the state machine. Returns true iff a change was proposed.
    seastar::future<bool> reconcileMetaVoters();

    // Stamp the controller epoch (= our current group-0 term) into state the
    // first time we lead under it. Idempotent (SetControllerTerm is monotonic).
    seastar::future<> stampControllerTermIfLeader();

    // Propose a single control command (leader only). Returns false if not leader.
    seastar::future<bool> proposeCommand(const ControlCommand& cmd);

private:
    raft::RaftGroup& g0_;
    Group0StateMachine& sm_;
    unsigned metaTarget_;
};

}  // namespace timestar::control
