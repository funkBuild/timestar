#pragma once

#include "../raft/raft_group.hpp"
#include "control_command.hpp"
#include "group0_state_machine.hpp"
#include "meta_voters.hpp"

#include <chrono>
#include <cstdint>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <optional>
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
    static constexpr std::chrono::seconds kDefaultProposalTimeout{6};

    Group0Controller(raft::RaftGroup& group0, Group0StateMachine& sm, unsigned metaTarget = 3,
                     std::chrono::milliseconds proposalTimeout = kDefaultProposalTimeout);

    bool isController() const { return g0_.isLeader(); }
    raft::NodeId self() const { return g0_.node().id(); }

    // `cluster init`: mint the cluster UUID, record this node (Active), and mirror
    // the stable initial Raft voter set. A multi-voter initial group therefore
    // requires quorum for the bootstrap ceremony. Same-identity retries are
    // allowed; conflicting cluster/node identity fails closed.
    seastar::future<> initCluster(std::string clusterUuid, NodeRecord selfRecord);

    // Record a node as Joining. It is not eligible for meta-voter selection
    // until addLearner(), catch-up, and activateCaughtUpLearner() complete.
    seastar::future<> admitNode(NodeRecord record);

    // Mint a group-0 join token (leader only) that a joining node must present.
    seastar::future<bool> mintJoinToken(std::string token);
    // Admit a node as Joining ONLY if it presents a valid unused token; the
    // token is consumed atomically. Reconcile is a later step.
    seastar::future<bool> admitNodeWithToken(NodeRecord record, std::string token);

    // Add an already-admitted Joining or Active node as a NON-VOTING learner. Idempotent
    // when it is already a member. Promotion is deliberately separate: the
    // learner must first acknowledge the leader's complete current log.
    seastar::future<bool> addLearner(raft::NodeId node);
    bool learnerCaughtUp(raft::NodeId node) const;
    // Commit Joining -> Active only for a learner that has acknowledged the
    // leader's complete current tail. Idempotent once Active. The new state
    // command itself extends the log, so callers must observe catch-up once more
    // before reconcileMetaVoters may promote the learner.
    seastar::future<bool> activateCaughtUpLearner(raft::NodeId node);

    // Commit the complete epoch-1 serving map exactly once. Exact retries are
    // idempotent; any different/later map is refused until movement/cutover has
    // its own resumable protocol.
    seastar::future<bool> publishInitialServingMap(ControlMap map);

    // Atomically create one movement job from the current serving membership.
    // The controller, not the caller, supplies the authoritative source voters
    // and next map epoch so an operator cannot forge a stale removal plan.
    seastar::future<bool> planVShardMove(std::string jobId, uint16_t vshard, NodeId destination,
                                        NodeId victim = raft::kNoNode);

    // Publish the next serving-map epoch only after the named movement job is
    // durably Done. The state machine independently re-derives and validates the
    // one-VShard cutover before accepting it.
    seastar::future<bool> publishCompletedMove(std::string jobId);

    // Recompute the desired meta voters and, if they differ from the current
    // group-0 configuration, drive a joint-consensus membership change and mirror
    // the new set into the state machine. The acknowledgement waits for final
    // Cnew to apply, not merely for the joint entry to append. A newly selected
    // voter must already be a caught-up learner; reconciliation never adds an
    // unknown/lagging voter directly. It also repairs a
    // stale mirror left by a controller that lost leadership after committing
    // the real configuration. Returns true iff reconciliation work committed.
    seastar::future<bool> reconcileMetaVoters();

    // Stamp the controller epoch (= our current group-0 term) into state the
    // first time we lead under it. Idempotent (SetControllerTerm is monotonic).
    seastar::future<> stampControllerTermIfLeader();

    // Propose a single control command (leader only). Returns true only after
    // this exact entry is quorum committed and applied on this controller;
    // false means it was rejected before append because this node was not leader.
    // Leadership loss or the default bounded deadline after append is an
    // ambiguous outcome and throws. An explicit deadline replaces the default.
    seastar::future<bool> proposeCommand(
        ControlCommand cmd,
        std::optional<seastar::lowres_clock::time_point> deadline = std::nullopt);

    // Freeze a complete canonical pattern expansion in one group-0 entry before
    // any data-group proposal. A same-request retry returns the first stored
    // target vector even if a fresh catalog read found different series.
    seastar::future<FreezeDeletePlanResult> freezeDeletePlan(
        FrozenDeletePlan candidate,
        std::optional<seastar::lowres_clock::time_point> deadline = std::nullopt);

    // Consult an already-frozen plan before repeating catalog discovery. The
    // request identity carries an empty target vector; NotFound authorizes the
    // caller to discover and then race safely through freezeDeletePlan().
    FreezeDeletePlanResult lookupDeletePlan(const FrozenDeletePlan& request) const;

private:
    seastar::lowres_clock::time_point proposalDeadline() const;

    raft::RaftGroup& g0_;
    Group0StateMachine& sm_;
    unsigned metaTarget_;
    std::chrono::milliseconds proposalTimeout_;
};

}  // namespace timestar::control
