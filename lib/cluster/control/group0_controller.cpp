#include "group0_controller.hpp"

#include "../data/journal_format.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <set>
#include <stdexcept>

namespace timestar::control {

namespace {

bool sameNodeIdentity(const NodeRecord& a, const NodeRecord& b) {
    return a.raftId == b.raftId && a.uuid == b.uuid && a.address == b.address &&
           a.failureDomain == b.failureDomain;
}

}  // namespace

seastar::future<bool> Group0Controller::proposeCommand(
    ControlCommand cmd, std::optional<seastar::lowres_clock::time_point> deadline) {
    if (!g0_.isLeader())
        co_return false;
    // A control-plane success is a published cluster decision, not merely a
    // local log append. In particular, callers use this return as the format
    // activation and placement-policy fence. Resolve only after this exact
    // entry is quorum committed and applied on the controller.
    co_return co_await g0_.proposeAndAwaitApplied(encodeCommand(cmd), deadline);
}

seastar::future<bool> Group0Controller::activateFormat(
    uint32_t version, const std::map<raft::NodeId, features::VersionRange>& nodeVersions) {
    if (!g0_.isLeader())
        co_return false;
    if (version <= sm_.state().activeFormatVersion)
        co_return false;
    const auto& config = g0_.node().config();
    if (config.joint() || metaVotersDiffer(sm_.state().metaVoters, config.voters) ||
        !isCompleteControlMap(sm_.state().servingMap))
        co_return false;
    std::set<raft::NodeId> required(config.voters.begin(), config.voters.end());
    for (const auto& placement : sm_.state().servingMap.placement)
        required.insert(placement.second.begin(), placement.second.end());
    std::vector<features::VersionRange> ranges;
    ranges.reserve(required.size());
    for (raft::NodeId voter : required) {
        auto capability = nodeVersions.find(voter);
        if (capability == nodeVersions.end())
            co_return false;
        ranges.push_back(capability->second);
    }
    if (!features::FeatureGate::canActivate(version, ranges))
        co_return false;
    std::vector<raft::NodeId> covered(required.begin(), required.end());
    if (!co_await proposeCommand(SetActiveVersion{version, std::move(covered)}))
        co_return false;
    co_return sm_.state().activeFormatVersion >= version;
}

seastar::future<FreezeDeletePlanResult> Group0Controller::freezeDeletePlan(
    FrozenDeletePlan candidate, std::optional<seastar::lowres_clock::time_point> deadline) {
    if (!g0_.isLeader())
        co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::NotLeader, {}};
    if (sm_.state().activeFormatVersion < data::kFrozenDeletePlanActivationVersion)
        co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::FormatInactive, {}};
    if (!validFrozenDeletePlan(candidate) || !isCompleteControlMap(sm_.state().servingMap))
        co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::Invalid, {}};

    const auto classifyExisting = [&](const FrozenDeletePlan& existing) {
        if (sameFrozenDeleteRequest(existing, candidate))
            return FreezeDeletePlanResult{FreezeDeletePlanStatus::Stored, existing};
        return FreezeDeletePlanResult{FreezeDeletePlanStatus::Conflict, existing};
    };
    if (auto found = sm_.state().frozenDeletePlans.find(candidate.requestId);
        found != sm_.state().frozenDeletePlans.end() &&
        !frozenDeletePlanExpiredAt(found->second, candidate.issuedAtMs))
        co_return classifyExisting(found->second);

    if (!co_await proposeCommand(StoreFrozenDeletePlan{candidate}, deadline))
        co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::NotLeader, {}};

    // A racing controller/request may have won the same key. Inspect the
    // applied state instead of assuming our proposal was the mutation: Raft
    // commits deterministic no-ops as successfully as insertions.
    if (auto found = sm_.state().frozenDeletePlans.find(candidate.requestId);
        found != sm_.state().frozenDeletePlans.end())
        co_return classifyExisting(found->second);
    co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::Capacity, {}};
}

FreezeDeletePlanResult Group0Controller::lookupDeletePlan(const FrozenDeletePlan& request) const {
    if (!g0_.isLeader())
        return {FreezeDeletePlanStatus::NotLeader, {}};
    if (sm_.state().activeFormatVersion < data::kFrozenDeletePlanActivationVersion)
        return {FreezeDeletePlanStatus::FormatInactive, {}};
    if (!validFrozenDeletePlan(request) || !request.targets.empty() ||
        !isCompleteControlMap(sm_.state().servingMap))
        return {FreezeDeletePlanStatus::Invalid, {}};
    auto found = sm_.state().frozenDeletePlans.find(request.requestId);
    if (found == sm_.state().frozenDeletePlans.end() ||
        frozenDeletePlanExpiredAt(found->second, request.issuedAtMs))
        return {FreezeDeletePlanStatus::NotFound, {}};
    if (sameFrozenDeleteRequest(found->second, request))
        return {FreezeDeletePlanStatus::Stored, found->second};
    return {FreezeDeletePlanStatus::Conflict, found->second};
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
    // Admission records identity but grants no serving/voting role. A separate
    // learner catch-up and committed state transition make it Active.
    record.state = NodeState::Joining;
    if (auto it = sm_.state().nodes.find(record.raftId); it != sm_.state().nodes.end()) {
        if (!sameNodeIdentity(it->second, record))
            throw std::runtime_error("group0 admission conflicts with the existing node identity");
        // Never regress Active/Draining/Down on an admission retry. A replacement
        // is a separate lifecycle operation, not an UpsertNode side effect.
        co_return;
    }
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
    record.state = NodeState::Joining;
    if (!g0_.isLeader())
        co_return false;
    if (auto it = sm_.state().nodes.find(record.raftId); it != sm_.state().nodes.end()) {
        if (!sameNodeIdentity(it->second, record))
            co_return false;
        // A missing token plus the exact committed identity is an ambiguous
        // retry after admission. If the token is still live, however, no
        // command consumed it: do not report a successful admission while
        // leaving reusable authority behind.
        co_return !sm_.state().joinTokens.contains(token);
    }
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

seastar::future<bool> Group0Controller::addLearner(raft::NodeId node) {
    if (!g0_.isLeader() || node == raft::kNoNode || node == self())
        co_return false;
    const auto record = sm_.state().nodes.find(node);
    if (record == sm_.state().nodes.end() ||
        (record->second.state != NodeState::Joining && record->second.state != NodeState::Active))
        co_return false;
    const auto& config = g0_.node().config();
    if (config.joint())
        co_return false;
    if (config.isVoter(node) || config.isLearner(node))
        co_return true;
    std::vector<NodeId> learners = config.learners;
    learners.push_back(node);
    std::sort(learners.begin(), learners.end());
    co_return co_await g0_.proposeConfChangeAndAwaitApplied(config.voters, std::move(learners));
}

bool Group0Controller::learnerCaughtUp(raft::NodeId node) const {
    if (!g0_.isLeader() || !g0_.node().isLearner(node))
        return false;
    // Equality is intentional. While a caller waits, new control proposals may
    // extend the target; promotion is safe only after an acknowledgement for the
    // leader's current tail, not a stale target sampled earlier.
    return g0_.node().matchIndexOf(node) == g0_.node().log().lastIndex() &&
           g0_.node().ticksSinceAck(node) <= g0_.node().heartbeatTimeout();
}

seastar::future<bool> Group0Controller::activateCaughtUpLearner(raft::NodeId node) {
    if (!g0_.isLeader())
        co_return false;
    const auto record = sm_.state().nodes.find(node);
    if (record == sm_.state().nodes.end())
        co_return false;
    if (record->second.state == NodeState::Active)
        co_return true;
    if (record->second.state != NodeState::Joining || !learnerCaughtUp(node))
        co_return false;
    co_return co_await proposeCommand(SetNodeState{node, NodeState::Active});
}

seastar::future<bool> Group0Controller::publishInitialServingMap(ControlMap map) {
    if (!g0_.isLeader() || map.epoch != 1 || !isCompleteControlMap(map))
        co_return false;
    if (sm_.state().servingMap.epoch != 0)
        co_return sm_.state().servingMap == map;
    if (!co_await proposeCommand(SetInitialServingMap{map}))
        co_return false;
    co_return sm_.state().servingMap == map;
}

seastar::future<bool> Group0Controller::reconcileMetaVoters() {
    if (!g0_.isLeader())
        co_return false;
    // Copy (not alias) the live config: proposeConfChange mutates it on append.
    const std::vector<NodeId> current = g0_.node().config().voters;
    const std::vector<NodeId> desired = selectMetaVoters(sm_.state().nodes, current, metaTarget_);
    if (desired.empty())
        co_return false;
    const auto& config = g0_.node().config();
    if (config.joint())
        co_return false;
    for (NodeId node : desired) {
        if (std::find(current.begin(), current.end(), node) != current.end())
            continue;
        if (!learnerCaughtUp(node))
            co_return false;
    }
    // Self-managed membership: the actual voter change is a joint-consensus
    // group-0 configuration entry; the SetMetaVoters command only mirrors the
    // result into the state machine for readers (config entries are not applied
    // to the SM).
    bool changed = false;
    if (metaVotersDiffer(current, desired)) {
        std::vector<NodeId> remainingLearners;
        for (NodeId learner : config.learners)
            if (std::find(desired.begin(), desired.end(), learner) == desired.end())
                remainingLearners.push_back(learner);
        changed = co_await g0_.proposeConfChangeAndAwaitApplied(desired, std::move(remainingLearners));
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
