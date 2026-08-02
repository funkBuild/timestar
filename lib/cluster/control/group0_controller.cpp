#include "group0_controller.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <seastar/core/coroutine.hh>
#include <set>
#include <stdexcept>

namespace timestar::control {

namespace {

bool sameNodeIdentity(const NodeRecord& a, const NodeRecord& b) {
    return a.raftId == b.raftId && a.uuid == b.uuid && a.address == b.address && a.failureDomain == b.failureDomain;
}

bool containsNode(const std::vector<NodeId>& nodes, NodeId node) {
    return std::find(nodes.begin(), nodes.end(), node) != nodes.end();
}

}  // namespace

DrainMoveDecision selectNextDrainMove(const Group0State& state) {
    DrainMoveDecision decision;
    std::vector<NodeId> draining;
    std::map<NodeId, size_t> replicaCounts;
    for (const auto& [id, record] : state.nodes) {
        if (record.state == NodeState::Draining)
            draining.push_back(id);
        if (record.state == NodeState::Active)
            replicaCounts.emplace(id, 0);
    }
    decision.drainingNodes = draining.size();

    for (const auto& [vshard, replicas] : state.servingMap.placement) {
        (void)vshard;
        for (NodeId replica : replicas) {
            if (containsNode(draining, replica))
                ++decision.remainingReferences;
            if (auto count = replicaCounts.find(replica); count != replicaCounts.end())
                ++count->second;
        }
    }
    if (decision.remainingReferences == 0)
        return decision;
    if (!isCompleteControlMap(state.servingMap) || state.mapEpoch != state.servingMap.epoch) {
        decision.state = DrainMoveState::InProgress;
        return decision;
    }

    // std::map iteration supplies the canonical node/VShard order. Skip an
    // individually blocked VShard so another safe replacement can still make
    // progress; once only blocked references remain the result is Blocked.
    for (NodeId victim : draining) {
        for (const auto& [vshard, replicas] : state.servingMap.placement) {
            if (!containsNode(replicas, victim))
                continue;

            std::set<std::string> occupiedDomains;
            bool sourceKnown = true;
            for (NodeId replica : replicas) {
                if (replica == victim)
                    continue;
                const auto record = state.nodes.find(replica);
                if (record == state.nodes.end() || record->second.failureDomain.empty()) {
                    sourceKnown = false;
                    break;
                }
                occupiedDomains.insert(record->second.failureDomain);
            }
            if (!sourceKnown)
                continue;

            NodeId destination = raft::kNoNode;
            size_t destinationLoad = std::numeric_limits<size_t>::max();
            for (const auto& [candidate, load] : replicaCounts) {
                const auto& record = state.nodes.at(candidate);
                if (containsNode(replicas, candidate) || record.failureDomain.empty() ||
                    occupiedDomains.contains(record.failureDomain))
                    continue;
                if (load < destinationLoad || (load == destinationLoad && candidate < destination)) {
                    destination = candidate;
                    destinationLoad = load;
                }
            }
            if (destination == raft::kNoNode)
                continue;
            decision.state = DrainMoveState::Ready;
            decision.victim = victim;
            decision.vshard = vshard;
            decision.destination = destination;
            return decision;
        }
    }
    decision.state = DrainMoveState::Blocked;
    return decision;
}

std::optional<StartRetentionSweep> selectNextRetentionSweep(const Group0State& state, uint64_t nowNanos) {
    if (state.retentionSweep || nowNanos == 0 || state.lastRetentionSweepId == UINT64_MAX)
        return std::nullopt;
    for (const auto& [key, cell] : state.policies) {
        const auto measurement = retentionMeasurementFromKey(key);
        if (!measurement || cell.value.empty())
            continue;
        const auto policy = decodeRetentionPolicyValue(cell.value);
        if (!policy || nowNanos <= policy->ttlNanos)
            continue;
        const uint64_t cutoff = nowNanos - policy->ttlNanos;
        uint64_t previous = 0;
        if (const auto found = state.retentionCutoffs.find(std::string(*measurement));
            found != state.retentionCutoffs.end())
            previous = found->second.cutoffTime;
        if (cutoff <= previous || (previous != 0 && cutoff - previous < kRetentionSweepIntervalNanos))
            continue;
        return StartRetentionSweep{state.lastRetentionSweepId + 1, std::string(*measurement), cell.version, nowNanos,
                                   cutoff};
    }
    return std::nullopt;
}

Group0Controller::Group0Controller(raft::RaftGroup& group0, Group0StateMachine& sm, unsigned metaTarget,
                                   std::chrono::milliseconds proposalTimeout)
    : g0_(group0), sm_(sm), metaTarget_(metaTarget), proposalTimeout_(proposalTimeout) {
    if (proposalTimeout_ <= std::chrono::milliseconds::zero())
        throw std::invalid_argument("group0 controller: proposal timeout must be positive");
}

seastar::lowres_clock::time_point Group0Controller::proposalDeadline() const {
    return seastar::lowres_clock::now() + proposalTimeout_;
}

seastar::future<bool> Group0Controller::proposeCommand(ControlCommand cmd,
                                                       std::optional<seastar::lowres_clock::time_point> deadline) {
    if (!g0_.isLeader())
        co_return false;
    // A control-plane success is a published cluster decision, not merely a
    // local log append. In particular, callers use this return as the format
    // activation and placement-policy fence. Resolve only after this exact
    // entry is quorum committed and applied on the controller.
    co_return co_await g0_.proposeAndAwaitApplied(encodeCommand(cmd), deadline.value_or(proposalDeadline()));
}

seastar::future<FreezeDeletePlanResult> Group0Controller::freezeDeletePlan(
    FrozenDeletePlan candidate, std::optional<seastar::lowres_clock::time_point> deadline) {
    if (!g0_.isLeader())
        co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::NotLeader, {}};
    if (!validFrozenDeletePlan(candidate) || !isCompleteControlMap(sm_.state().servingMap))
        co_return FreezeDeletePlanResult{FreezeDeletePlanStatus::Invalid, {}};

    const auto classifyExisting = [&](const FrozenDeletePlan& existing) {
        if (sameFrozenDeleteRequest(existing, candidate))
            return FreezeDeletePlanResult{FreezeDeletePlanStatus::Stored, existing};
        return FreezeDeletePlanResult{FreezeDeletePlanStatus::Conflict, existing};
    };
    if (auto found = sm_.state().frozenDeletePlans.find(candidate.requestId);
        found != sm_.state().frozenDeletePlans.end() && !frozenDeletePlanExpiredAt(found->second, candidate.issuedAtMs))
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
    if (!validFrozenDeletePlan(request) || !request.targets.empty() || !isCompleteControlMap(sm_.state().servingMap))
        return {FreezeDeletePlanStatus::Invalid, {}};
    auto found = sm_.state().frozenDeletePlans.find(request.requestId);
    if (found == sm_.state().frozenDeletePlans.end() || frozenDeletePlanExpiredAt(found->second, request.issuedAtMs))
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
    if (config.joint() ||
        std::find(config.voters.begin(), config.voters.end(), selfRecord.raftId) == config.voters.end())
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
        // Never regress Active/Draining/Removed on an admission retry. A replacement
        // is a separate lifecycle operation, not an UpsertNode side effect.
        co_return;
    }
    co_await proposeCommand(UpsertNode{record});
}

seastar::future<bool> Group0Controller::mintJoinToken(std::string token) {
    if (!validJoinToken(token) || sm_.state().joinTokens.size() >= kMaxOutstandingJoinTokens ||
        sm_.state().joinTokens.contains(token))
        co_return false;
    const std::string expected = token;
    if (!co_await proposeCommand(MintJoinToken{std::move(token)}))
        co_return false;
    co_return sm_.state().joinTokens.contains(expected);
}

seastar::future<bool> Group0Controller::admitNodeWithToken(NodeRecord record, std::string token) {
    // The token is validated + consumed atomically at apply time; the command
    // is a no-op if the token is invalid. Reconcile runs as a separate step.
    record.state = NodeState::Joining;
    if (!g0_.isLeader() || !validJoinToken(token))
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
    co_return co_await g0_.proposeConfChangeAndAwaitApplied(config.voters, std::move(learners), proposalDeadline());
}

bool Group0Controller::learnerCaughtUp(raft::NodeId node) const {
    return g0_.node().isLearner(node) && memberCaughtUp(node);
}

bool Group0Controller::memberCaughtUp(raft::NodeId node) const {
    if (!g0_.isLeader() || (!g0_.node().isVoter(node) && !g0_.node().isLearner(node)))
        return false;
    // Equality is intentional. While a caller waits, new control proposals may
    // extend the target. Replication alone is insufficient for final eviction:
    // the peer must report that its state machine applied the complete tail, so
    // its durable serving-map cache cannot remain stale after departure.
    return g0_.node().matchIndexOf(node) == g0_.node().log().lastIndex() &&
           g0_.node().appliedIndexOf(node) == g0_.node().log().lastIndex() &&
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

seastar::future<bool> Group0Controller::drainNode(raft::NodeId node) {
    if (!g0_.isLeader() || node == raft::kNoNode)
        co_return false;
    auto record = sm_.state().nodes.find(node);
    if (record == sm_.state().nodes.end())
        co_return false;
    if (record->second.state != NodeState::Draining) {
        if (record->second.state != NodeState::Active ||
            !co_await proposeCommand(SetNodeState{node, NodeState::Draining}))
            co_return false;
    }
    record = sm_.state().nodes.find(node);
    if (record == sm_.state().nodes.end() || record->second.state != NodeState::Draining)
        co_return false;

    // A retry also retries meta-voter eviction. The lifecycle decision is
    // already durable even when there are not yet enough caught-up Active
    // learners to replace this voter.
    (void)co_await reconcileMetaVoters();
    co_return true;
}

seastar::future<bool> Group0Controller::removeDrainedNode(raft::NodeId node) {
    if (!g0_.isLeader() || node == raft::kNoNode)
        co_return false;
    auto record = sm_.state().nodes.find(node);
    if (record == sm_.state().nodes.end())
        co_return false;
    if (record->second.state == NodeState::Removed) {
        // The authoritative lifecycle decision is complete. A retry also
        // accelerates the final learner eviction once the node reports that it
        // applied its own Removed record.
        if ((!g0_.node().config().isVoter(node) && !g0_.node().config().isLearner(node)) || memberCaughtUp(node))
            (void)co_await reconcileMetaVoters();
        co_return true;
    }
    if (record->second.state != NodeState::Draining)
        co_return false;

    for (const auto& [vshard, replicas] : sm_.state().servingMap.placement) {
        (void)vshard;
        if (containsNode(replicas, node))
            co_return false;
    }
    if (std::ranges::any_of(sm_.state().jobs, [](const auto& entry) { return !entry.second.done; }))
        co_return false;

    // Draining is removed from the voter set but deliberately retained as a
    // learner through evacuation. It must apply the final map before Removed is
    // committed. Removed itself is then replicated while the node is still a
    // learner; a later reconciliation evicts it only after that entry is also
    // reported applied.
    if (g0_.node().config().isVoter(node) || containsNode(sm_.state().metaVoters, node))
        (void)co_await reconcileMetaVoters();
    if (g0_.node().config().isVoter(node) || containsNode(sm_.state().metaVoters, node))
        co_return false;
    if (g0_.node().config().isLearner(node) && !memberCaughtUp(node))
        co_return false;
    if (!co_await proposeCommand(SetNodeState{node, NodeState::Removed}))
        co_return false;
    record = sm_.state().nodes.find(node);
    co_return record != sm_.state().nodes.end() && record->second.state == NodeState::Removed;
}

seastar::future<bool> Group0Controller::publishInitialServingMap(ControlMap map) {
    if (!g0_.isLeader() || map.epoch != 1 || !isCompleteControlMap(map))
        co_return false;
    if (sm_.state().servingMap.epoch != 0)
        co_return sm_.state().servingMap == map;
    if (!co_await proposeCommand(PublishServingMap{map, {}}))
        co_return false;
    co_return sm_.state().servingMap == map;
}

seastar::future<bool> Group0Controller::planVShardMove(std::string jobId, uint64_t expectedMapEpoch, uint16_t vshard,
                                                       NodeId destination, NodeId victim) {
    if (!g0_.isLeader() || !validControlJobId(jobId) || expectedMapEpoch == 0 ||
        expectedMapEpoch == std::numeric_limits<uint64_t>::max() || destination == raft::kNoNode ||
        vshard >= timestar::VIRTUAL_SHARD_COUNT)
        co_return false;
    if (const auto existing = sm_.state().jobs.find(jobId); existing != sm_.state().jobs.end()) {
        const auto move = movement::MoveJob::decode(existing->second.payload);
        co_return move && move->plan().mapEpoch == expectedMapEpoch + 1 && move->plan().vshard == vshard &&
            move->plan().dest == destination && move->plan().victim == victim;
    }
    if (!isCompleteControlMap(sm_.state().servingMap) || sm_.state().mapEpoch != sm_.state().servingMap.epoch ||
        sm_.state().mapEpoch != expectedMapEpoch)
        co_return false;
    const auto source = sm_.state().servingMap.placement.find(vshard);
    if (source == sm_.state().servingMap.placement.end())
        co_return false;
    movement::MovePlan plan{vshard, destination, victim, sm_.state().mapEpoch + 1, source->second};
    movement::MoveJob job(plan);
    if (!job.valid())
        co_return false;
    const std::string expectedId = jobId;
    if (!co_await proposeCommand(PlanVShardMove{std::move(jobId), std::move(plan)}))
        co_return false;
    const auto persisted = sm_.state().jobs.find(expectedId);
    if (persisted == sm_.state().jobs.end())
        co_return false;
    auto decoded = movement::MoveJob::decode(persisted->second.payload);
    co_return decoded && persisted->second.step == static_cast<uint32_t>(decoded->step()) &&
        persisted->second.done == decoded->done();
}

seastar::future<DrainMoveDecision> Group0Controller::planNextDrainMove() {
    DrainMoveDecision decision = selectNextDrainMove(sm_.state());
    if (!g0_.isLeader() || decision.state != DrainMoveState::Ready)
        co_return decision;
    const auto& config = g0_.node().config();
    if (config.joint()) {
        decision.state = DrainMoveState::InProgress;
        co_return decision;
    }
    if (!config.isVoter(decision.victim) && !config.isLearner(decision.victim)) {
        decision.state = DrainMoveState::Blocked;
        co_return decision;
    }
    const uint64_t expectedEpoch = sm_.state().servingMap.epoch;
    const std::string jobId = "drain-v1-" + std::to_string(decision.victim) + "-" + std::to_string(decision.vshard) +
                              "-" + std::to_string(expectedEpoch + 1);
    if (!co_await planVShardMove(jobId, expectedEpoch, decision.vshard, decision.destination, decision.victim)) {
        decision.state = g0_.isLeader() ? DrainMoveState::Blocked : DrainMoveState::InProgress;
        co_return decision;
    }
    co_return decision;
}

seastar::future<bool> Group0Controller::casRetentionPolicy(std::string measurement, uint64_t expectedVersion,
                                                           std::optional<RetentionPolicyValue> value) {
    if (!g0_.isLeader() || !validRetentionMeasurement(measurement) || expectedVersion == UINT64_MAX ||
        (value && !validRetentionPolicyValue(*value)))
        co_return false;
    const std::string key = retentionPolicyKey(measurement);
    const std::string encoded = value ? encodeRetentionPolicyValue(*value) : std::string{};
    if (const auto existing = sm_.state().policies.find(key); existing != sm_.state().policies.end() &&
                                                              existing->second.version == expectedVersion + 1 &&
                                                              existing->second.value == encoded)
        co_return true;
    if (!value && !sm_.state().policies.contains(key))
        co_return false;
    if (!co_await proposeCommand(CasPolicy{key, expectedVersion, encoded}))
        co_return false;
    const auto persisted = sm_.state().policies.find(key);
    co_return persisted != sm_.state().policies.end() && persisted->second.version == expectedVersion + 1 &&
        persisted->second.value == encoded;
}

seastar::future<bool> Group0Controller::startRetentionSweep(StartRetentionSweep sweep) {
    if (!g0_.isLeader() || sweep.sweepId == 0 || !validRetentionMeasurement(sweep.measurement) ||
        sweep.policyVersion == 0 || sweep.issuedAtNanos == 0 || sweep.cutoffTime == 0)
        co_return false;
    if (sm_.state().retentionSweep == std::optional<RetentionSweep>{RetentionSweep{
                                          sweep.sweepId, sweep.measurement, sweep.policyVersion, sweep.cutoffTime, 0}})
        co_return true;
    if (!co_await proposeCommand(sweep))
        co_return false;
    co_return sm_.state().retentionSweep ==
        std::optional<RetentionSweep>{
            RetentionSweep{sweep.sweepId, sweep.measurement, sweep.policyVersion, sweep.cutoffTime, 0}};
}

seastar::future<bool> Group0Controller::advanceRetentionSweep(uint32_t nextVShard) {
    if (!g0_.isLeader() || !sm_.state().retentionSweep)
        co_return false;
    const RetentionSweep current = *sm_.state().retentionSweep;
    if (nextVShard <= current.nextVShard || nextVShard > timestar::VIRTUAL_SHARD_COUNT ||
        nextVShard - current.nextVShard > kRetentionFanoutBatch)
        co_return false;
    if (!co_await proposeCommand(AdvanceRetentionSweep{current.sweepId, current.measurement, current.policyVersion,
                                                       current.cutoffTime, nextVShard}))
        co_return false;
    if (nextVShard == timestar::VIRTUAL_SHARD_COUNT) {
        const auto complete = sm_.state().retentionCutoffs.find(current.measurement);
        co_return !sm_.state().retentionSweep && complete != sm_.state().retentionCutoffs.end() &&
            complete->second == RetentionCutoffRecord{current.policyVersion, current.cutoffTime};
    }
    co_return sm_.state().retentionSweep&& sm_.state().retentionSweep->measurement ==
        current.measurement&& sm_.state().retentionSweep->sweepId ==
        current.sweepId&& sm_.state().retentionSweep->policyVersion ==
        current.policyVersion&& sm_.state().retentionSweep->cutoffTime ==
        current.cutoffTime&& sm_.state().retentionSweep->nextVShard == nextVShard;
}

seastar::future<bool> Group0Controller::publishCompletedMove(std::string jobId) {
    if (!g0_.isLeader() || !validControlJobId(jobId))
        co_return false;
    const auto persisted = sm_.state().jobs.find(jobId);
    if (persisted == sm_.state().jobs.end() || !persisted->second.done)
        co_return false;
    auto move = movement::MoveJob::decode(persisted->second.payload);
    if (!move || !move->done() || move->plan().mapEpoch != sm_.state().mapEpoch)
        co_return false;
    const auto current = sm_.state().servingMap.placement.find(move->plan().vshard);
    if (sm_.state().servingMap.epoch == move->plan().mapEpoch)
        co_return current != sm_.state().servingMap.placement.end() && current->second == move->targetVoters();
    if (sm_.state().servingMap.epoch + 1 != move->plan().mapEpoch)
        co_return false;
    ControlMap next = sm_.state().servingMap;
    next.epoch = move->plan().mapEpoch;
    next.placement[move->plan().vshard] = move->targetVoters();
    if (!co_await proposeCommand(PublishServingMap{next, std::move(jobId)}))
        co_return false;
    co_return sm_.state().servingMap == next;
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
    // Every admitted non-voter remains a learner so it continues to receive
    // Group-0 decisions. A Draining voter is always demoted to learner and kept
    // there until the explicit Removed decision. A Removed learner remains only
    // until it reports applying that decision, then final configuration eviction
    // is safe.
    std::set<NodeId> currentMembers(current.begin(), current.end());
    currentMembers.insert(config.learners.begin(), config.learners.end());
    std::vector<NodeId> desiredLearners;
    for (NodeId member : currentMembers) {
        if (containsNode(desired, member))
            continue;
        const auto record = sm_.state().nodes.find(member);
        if (record == sm_.state().nodes.end())
            continue;
        const bool retain = record->second.state == NodeState::Joining || record->second.state == NodeState::Active ||
                            record->second.state == NodeState::Draining ||
                            (record->second.state == NodeState::Removed && !memberCaughtUp(member));
        if (retain)
            desiredLearners.push_back(member);
    }

    bool changed = false;
    if (metaVotersDiffer(current, desired) || config.learners != desiredLearners) {
        changed = co_await g0_.proposeConfChangeAndAwaitApplied(desired, desiredLearners, proposalDeadline());
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
