#include "group0_controller.hpp"

#include <seastar/core/coroutine.hh>

namespace timestar::control {

seastar::future<bool> Group0Controller::proposeCommand(const ControlCommand& cmd) {
    if (!g0_.isLeader())
        co_return false;
    co_return co_await g0_.propose(encodeCommand(cmd));
}

seastar::future<> Group0Controller::initCluster(std::string clusterUuid, NodeRecord selfRecord) {
    // A fresh group 0 is already a group of one (self is the sole voter). Record
    // the cluster identity, this node (Active), and mirror the sole-voter set.
    selfRecord.state = NodeState::Active;
    co_await proposeCommand(InitCluster{std::move(clusterUuid)});
    co_await proposeCommand(UpsertNode{selfRecord});
    co_await proposeCommand(SetMetaVoters{{selfRecord.raftId}});
    co_await stampControllerTermIfLeader();
}

seastar::future<> Group0Controller::admitNode(NodeRecord record) {
    // Record the node. Meta-voter reconciliation is a SEPARATE step
    // (reconcileMetaVoters) run once this command has COMMITTED and applied, so
    // selectMetaVoters observes the new node -- the controller's reconcile loop
    // drives it, not this call.
    record.state = NodeState::Active;
    co_await proposeCommand(UpsertNode{record});
}

seastar::future<bool> Group0Controller::reconcileMetaVoters() {
    if (!g0_.isLeader())
        co_return false;
    const std::vector<NodeId>& current = g0_.node().config().voters;
    const std::vector<NodeId> desired = selectMetaVoters(sm_.state().nodes, current, metaTarget_);
    if (desired.empty() || !metaVotersDiffer(current, desired))
        co_return false;
    // Self-managed membership: the actual voter change is a joint-consensus
    // group-0 configuration entry; the SetMetaVoters command only mirrors the
    // result into the state machine for readers (config entries are not applied
    // to the SM).
    const bool changed = co_await g0_.proposeConfChange(desired, /*learners=*/{});
    if (changed)
        co_await proposeCommand(SetMetaVoters{desired});
    co_return changed;
}

seastar::future<> Group0Controller::stampControllerTermIfLeader() {
    if (!g0_.isLeader())
        co_return;
    const raft::Term term = g0_.currentTerm();
    if (term > sm_.state().controllerTerm)
        co_await proposeCommand(SetControllerTerm{term, self()});
}

}  // namespace timestar::control
