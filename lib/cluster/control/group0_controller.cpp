#include "group0_controller.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <stdexcept>

namespace timestar::control {

seastar::future<bool> Group0Controller::proposeCommand(ControlCommand cmd) {
    if (!g0_.isLeader())
        co_return false;
    // A control-plane success is a published cluster decision, not merely a
    // local log append. In particular, callers use this return as the format
    // activation and placement-policy fence. Resolve only after this exact
    // entry is quorum committed and applied on the controller.
    co_return co_await g0_.proposeAndAwaitApplied(encodeCommand(cmd));
}

seastar::future<bool> Group0Controller::activateFormat(uint32_t version,
                                                       const std::vector<features::VersionRange>& voterVersions) {
    if (!g0_.isLeader())
        co_return false;
    if (version <= sm_.state().activeFormatVersion)
        co_return false;
    // The supplied ranges are positional metadata for the CURRENT stable voter
    // set. Missing a voter would make an unsafe activation look unanimous; an
    // extra range is equally ambiguous. Refuse changes during joint consensus,
    // where there is no single positional voter set to validate against.
    const auto& config = g0_.node().config();
    if (config.joint() || voterVersions.size() != config.voters.size())
        co_return false;
    // SAFETY GATE (decision 8): activate ONLY if every current voter can read the
    // format, so no node is ever sent data in a format it cannot decode. Refuse
    // otherwise -- the incompatible voter must be upgraded first.
    if (!features::FeatureGate::canActivate(version, voterVersions))
        co_return false;
    co_return co_await proposeCommand(SetActiveVersion{version});
}

seastar::future<> Group0Controller::initCluster(std::string clusterUuid, NodeRecord selfRecord) {
    if (!g0_.isLeader())
        co_return;
    if (clusterUuid.empty())
        throw std::invalid_argument("group0 init: cluster UUID must not be empty");
    if (selfRecord.raftId == raft::kNoNode || selfRecord.uuid.empty() || selfRecord.address.empty())
        throw std::invalid_argument("group0 init: self node id, UUID, and address must be set");

    const auto& config = g0_.node().config();
    if (config.joint() || std::find(config.voters.begin(), config.voters.end(), selfRecord.raftId) ==
                              config.voters.end())
        throw std::logic_error("group0 init: self must belong to a stable initial voter set");
    const std::vector<NodeId> initialVoters = config.voters;

    // Retrying an interrupted ceremony with the same identity is safe; using
    // `cluster init` to rewrite an existing cluster or node identity is not.
    const auto& state = sm_.state();
    if (!state.clusterUuid.empty() && state.clusterUuid != clusterUuid)
        throw std::runtime_error("group0 init: cluster is already initialized with a different UUID");
    if (auto it = state.nodes.find(selfRecord.raftId); it != state.nodes.end() && it->second.uuid != selfRecord.uuid)
        throw std::runtime_error("group0 init: Raft node id is already bound to a different node UUID");
    for (const auto& [id, record] : state.nodes)
        if (id != selfRecord.raftId && record.uuid == selfRecord.uuid)
            throw std::runtime_error("group0 init: node UUID is already bound to a different Raft id");

    // Record the cluster identity and this node, then mirror the actual initial
    // voter set. The initial group may deliberately contain several statically
    // configured voters so bootstrap itself requires quorum.
    selfRecord.state = NodeState::Active;
    if (sm_.state().clusterUuid.empty() && !co_await proposeCommand(InitCluster{clusterUuid}))
        co_return;
    if (!co_await proposeCommand(UpsertNode{selfRecord}))
        co_return;
    if (!co_await proposeCommand(SetMetaVoters{initialVoters}))
        co_return;
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

seastar::future<bool> Group0Controller::mintJoinToken(std::string token) {
    if (token.empty() || sm_.state().joinTokens.contains(token))
        co_return false;
    co_return co_await proposeCommand(MintJoinToken{std::move(token)});
}

seastar::future<bool> Group0Controller::admitNodeWithToken(NodeRecord record, std::string token) {
    // The token is validated + consumed atomically at apply time; the command
    // is a no-op if the token is invalid. Reconcile runs as a separate step.
    record.state = NodeState::Active;
    if (!g0_.isLeader())
        co_return false;
    if (!sm_.state().joinTokens.contains(token)) {
        // An ambiguous retry after the first command committed is idempotently
        // successful only for the exact node record that consumed it. A replay
        // attempting to admit another identity remains rejected.
        if (auto it = sm_.state().nodes.find(record.raftId); it != sm_.state().nodes.end())
            co_return it->second == record;
        co_return false;
    }
    if (!co_await proposeCommand(AdmitWithToken{record, std::move(token)}))
        co_return false;
    auto it = sm_.state().nodes.find(record.raftId);
    co_return it != sm_.state().nodes.end() && it->second == record;
}

seastar::future<bool> Group0Controller::reconcileMetaVoters() {
    if (!g0_.isLeader())
        co_return false;
    // Copy (not alias) the live config: proposeConfChange mutates it on append.
    const std::vector<NodeId> current = g0_.node().config().voters;
    const std::vector<NodeId> desired = selectMetaVoters(sm_.state().nodes, current, metaTarget_);
    if (desired.empty())
        co_return false;
    // Self-managed membership: the actual voter change is a joint-consensus
    // group-0 configuration entry; the SetMetaVoters command only mirrors the
    // result into the state machine for readers (config entries are not applied
    // to the SM).
    bool changed = false;
    if (metaVotersDiffer(current, desired)) {
        changed = co_await g0_.proposeConfChangeAndAwaitApplied(desired, /*learners=*/{});
        if (!changed)
            co_return false;
    }
    // Repair the state-machine mirror even when another controller committed
    // the real config and lost leadership before it could publish the mirror.
    // This proposal is ordered after the applied final configuration.
    if (metaVotersDiffer(sm_.state().metaVoters, desired)) {
        if (!co_await proposeCommand(SetMetaVoters{desired}))
            throw raft::LeadershipLostError("group0 membership committed but voter mirror was not published");
        changed = true;
    }
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
