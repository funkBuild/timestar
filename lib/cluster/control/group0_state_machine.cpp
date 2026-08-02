#include "group0_state_machine.hpp"

#include "../../core/vshard.hpp"

#include <seastar/core/coroutine.hh>
#include <limits>
#include <set>
#include <stdexcept>

namespace timestar::control {

namespace {

constexpr uint64_t kSnapshotMagic = 0x31504e53'30475354ull;  // "TSG0SNP1"

// State (snapshot) serialization -- distinct from command serialization: it
// encodes the whole Group0State so a snapshot fully reconstructs a node.
struct SW {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
    void u16(uint16_t v) {
        u8(v & 0xff);
        u8((v >> 8) & 0xff);
    }
    void str(const std::string& s) {
        u64(s.size());
        out.append(s);
    }
    void ids(const std::vector<NodeId>& v) {
        u64(v.size());
        for (NodeId n : v)
            u64(n);
    }
};

struct SR {
    const char* p;
    const char* end;
    bool ok = true;
    bool avail(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    uint8_t u8() {
        if (!avail(1)) {
            ok = false;
            return 0;
        }
        return static_cast<uint8_t>(*p++);
    }
    uint16_t u16() {
        uint16_t a = u8(), b = u8();
        return static_cast<uint16_t>(a | (b << 8));
    }
    uint64_t u64() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(u8()) << (8 * i);
        return v;
    }
    uint32_t u32() {
        const uint64_t v = u64();
        if (v > std::numeric_limits<uint32_t>::max()) {
            ok = false;
            return 0;
        }
        return static_cast<uint32_t>(v);
    }
    bool boolean() {
        const uint8_t v = u8();
        if (v > 1) {
            ok = false;
            return false;
        }
        return v != 0;
    }
    NodeState nodeState() {
        const auto state = static_cast<NodeState>(u8());
        if (!isValidNodeState(state))
            ok = false;
        return state;
    }
    std::string str(uint64_t maxBytes = std::numeric_limits<uint64_t>::max()) {
        uint64_t n = u64();
        if (!ok || n > maxBytes || !avail(n)) {
            ok = false;
            return {};
        }
        std::string s(p, p + n);
        p += n;
        return s;
    }
    std::vector<NodeId> ids() {
        uint64_t n = u64();
        std::vector<NodeId> v;
        // Non-wrapping bounds check (n*8 could overflow to bypass avail()).
        if (!ok || n > static_cast<uint64_t>(end - p) / 8) {
            ok = false;
            return v;
        }
        v.reserve(n);
        for (uint64_t i = 0; i < n; ++i)
            v.push_back(u64());
        return v;
    }
};

void writeControlMap(SW& w, const ControlMap& map) {
    w.u64(map.epoch);
    w.u64(map.placement.size());
    for (const auto& [vshard, replicas] : map.placement) {
        w.u16(vshard);
        w.ids(replicas);
    }
    w.u64(map.groups.size());
    for (const auto& [vshard, group] : map.groups) {
        w.u16(vshard);
        w.u16(group);
    }
}

ControlMap readControlMap(SR& r) {
    ControlMap map;
    map.epoch = r.u64();
    const uint64_t placements = r.u64();
    if (!r.ok || placements > static_cast<uint64_t>(r.end - r.p) / 10) {
        r.ok = false;
        return map;
    }
    for (uint64_t i = 0; i < placements && r.ok; ++i) {
        const uint16_t vshard = r.u16();
        auto replicas = r.ids();
        if (r.ok && !map.placement.emplace(vshard, std::move(replicas)).second)
            r.ok = false;
    }
    const uint64_t groups = r.u64();
    if (!r.ok || groups > static_cast<uint64_t>(r.end - r.p) / 4) {
        r.ok = false;
        return map;
    }
    for (uint64_t i = 0; i < groups && r.ok; ++i) {
        const uint16_t vshard = r.u16();
        const uint16_t group = r.u16();
        if (!map.groups.emplace(vshard, group).second)
            r.ok = false;
    }
    return map;
}

void writeFrozenDeletePlan(SW& w, const FrozenDeletePlan& plan) {
    w.str(plan.requestId);
    w.str(plan.requestFingerprint);
    w.u64(plan.issuedAtMs);
    w.u64(plan.targets.size());
    for (const auto& target : plan.targets) {
        w.str(target.seriesKey);
        w.u64(target.startTime);
        w.u64(target.endTime);
    }
}

FrozenDeletePlan readFrozenDeletePlan(SR& r) {
    FrozenDeletePlan plan;
    plan.requestId = r.str();
    plan.requestFingerprint = r.str();
    plan.issuedAtMs = r.u64();
    const uint64_t count = r.u64();
    constexpr uint64_t kMinimumTargetBytes = sizeof(uint64_t) * 3;
    if (!r.ok || count > kMaxFrozenDeletePlanTargets ||
        count > static_cast<uint64_t>(r.end - r.p) / kMinimumTargetBytes) {
        r.ok = false;
        return plan;
    }
    plan.targets.reserve(count);
    for (uint64_t i = 0; i < count && r.ok; ++i)
        plan.targets.push_back(FrozenDeleteTarget{r.str(), r.u64(), r.u64()});
    return plan;
}

bool validFrozenDeletePlanState(const Group0State& state) {
    if (state.frozenDeletePlans.empty())
        return true;
    if (!isCompleteControlMap(state.servingMap) || state.frozenDeletePlans.size() > kMaxFrozenDeletePlans)
        return false;
    size_t aggregateBytes = 0;
    for (const auto& [requestId, plan] : state.frozenDeletePlans) {
        const size_t planBytes = frozenDeletePlanBytes(plan);
        if (requestId != plan.requestId || !validFrozenDeletePlan(plan) ||
            aggregateBytes > kMaxFrozenDeletePlanAggregateBytes - planBytes)
            return false;
        aggregateBytes += planBytes;
    }
    return true;
}

bool validNodeRecord(const Group0State& state, const NodeRecord& record) {
    if (record.raftId == raft::kNoNode || record.uuid.empty() || record.address.empty() ||
        !isValidNodeState(record.state))
        return false;
    if (auto it = state.nodes.find(record.raftId); it != state.nodes.end() && it->second.uuid != record.uuid)
        return false;  // a Raft id is permanently bound to one node identity
    for (const auto& [id, existing] : state.nodes)
        if (id != record.raftId && existing.uuid == record.uuid)
            return false;  // one persistent node identity cannot occupy two ids
    return true;
}

bool validNodeSet(const std::vector<NodeId>& nodes);
std::optional<movement::MoveJob> decodeJob(const Job& job);

bool validSnapshotState(const Group0State& state) {
    std::set<std::string> nodeUuids;
    for (const auto& [id, record] : state.nodes) {
        if (id != record.raftId || record.raftId == raft::kNoNode || record.uuid.empty() || record.address.empty() ||
            !isValidNodeState(record.state) || !nodeUuids.insert(record.uuid).second)
            return false;
    }
    for (const auto& [vshard, replicas] : state.desiredPlacement)
        if (vshard >= timestar::VIRTUAL_SHARD_COUNT || !validNodeSet(replicas))
            return false;
    if ((state.servingMap.epoch == 0 &&
         (state.mapEpoch != 0 || !state.servingMap.placement.empty() || !state.servingMap.groups.empty())) ||
        (state.servingMap.epoch != 0 &&
         (!isCompleteControlMap(state.servingMap) || state.servingMap.epoch > state.mapEpoch ||
          state.mapEpoch - state.servingMap.epoch > 1)))
        return false;
    if (!state.metaVoters.empty() && !validNodeSet(state.metaVoters))
        return false;
    for (const auto& [key, cell] : state.policies)
        if (key.empty() || cell.version == 0)
            return false;
    size_t unfinishedJobs = 0;
    for (const auto& [id, job] : state.jobs) {
        const auto move = decodeJob(job);
        if (!validControlJobId(id) || id != job.id || !move || move->plan().mapEpoch > state.mapEpoch)
            return false;
        if (!job.done) {
            ++unfinishedJobs;
            if (move->plan().mapEpoch != state.mapEpoch || state.servingMap.epoch + 1 != state.mapEpoch)
                return false;
        }
    }
    if (unfinishedJobs > 1)
        return false;
    if (state.joinTokens.size() > kMaxOutstandingJoinTokens)
        return false;
    for (const auto& token : state.joinTokens)
        if (!validJoinToken(token))
            return false;
    if (!validFrozenDeletePlanState(state))
        return false;
    return (state.controllerTerm == 0) == (state.controllerLeader == raft::kNoNode);
}

bool validNodeSet(const std::vector<NodeId>& nodes) {
    if (nodes.empty())
        return false;
    std::set<NodeId> unique;
    for (NodeId id : nodes)
        if (id == raft::kNoNode || !unique.insert(id).second)
            return false;
    return true;
}

bool sameNodeSet(std::vector<NodeId> lhs, std::vector<NodeId> rhs) {
    std::sort(lhs.begin(), lhs.end());
    std::sort(rhs.begin(), rhs.end());
    return lhs == rhs;
}

std::optional<movement::MoveJob> decodeJob(const Job& job) {
    auto move = movement::MoveJob::decode(job.payload);
    if (!move || job.step != static_cast<uint32_t>(move->step()) || job.done != move->done())
        return std::nullopt;
    return move;
}

bool allJobsDone(const Group0State& state) {
    return std::ranges::all_of(state.jobs, [](const auto& item) { return item.second.done; });
}

bool validNewMove(const Group0State& state, const PlanVShardMove& command) {
    movement::MoveJob job(command.plan);
    if (!validControlJobId(command.jobId) || state.jobs.size() >= kMaxControlJobs || !job.valid() ||
        !isCompleteControlMap(state.servingMap) ||
        state.mapEpoch != state.servingMap.epoch || state.mapEpoch == std::numeric_limits<uint64_t>::max() ||
        command.plan.mapEpoch != state.mapEpoch + 1 || state.jobs.contains(command.jobId) || !allJobsDone(state))
        return false;

    const auto current = state.servingMap.placement.find(command.plan.vshard);
    const auto destination = state.nodes.find(command.plan.dest);
    const auto target = job.targetVoters();
    return current != state.servingMap.placement.end() && current->second == command.plan.sourceVoters &&
           destination != state.nodes.end() && destination->second.state == NodeState::Active &&
           validNodeSet(target) && !sameNodeSet(target, current->second);
}

}  // namespace

void Group0StateMachine::expectLocalIdentity(std::string clusterUuid, NodeRecord localRecord) {
    if (clusterUuid.empty() || localRecord.raftId == raft::kNoNode || localRecord.uuid.empty() ||
        localRecord.address.empty())
        throw std::invalid_argument("group0: local identity expectation is incomplete");
    if (expectedLocalRecord_)
        throw std::logic_error("group0: local identity expectation was configured more than once");
    expectedClusterUuid_ = std::move(clusterUuid);
    expectedLocalRecord_ = std::move(localRecord);
    if (!stateMatchesLocalExpectations(state_))
        throw std::runtime_error("group0: existing control state conflicts with the local persistent identity");
}

void Group0StateMachine::expectServingMap(ControlMap map) {
    if (!isCompleteControlMap(map))
        throw std::invalid_argument("group0: expected serving map is incomplete");
    if (expectedServingMap_)
        throw std::logic_error("group0: serving-map expectation was configured more than once");
    expectedServingMap_ = std::move(map);
    if (!stateMatchesLocalExpectations(state_))
        throw std::runtime_error("group0: existing control state conflicts with the durable serving map");
}

void Group0StateMachine::setServingMapObserver(ServingMapObserver observer) {
    if (!observer)
        throw std::invalid_argument("group0: serving-map observer is empty");
    if (servingMapObserver_)
        throw std::logic_error("group0: serving-map observer was configured more than once");
    servingMapObserver_ = std::move(observer);
}

bool Group0StateMachine::stateMatchesLocalExpectations(const Group0State& state) const {
    if (expectedLocalRecord_) {
        if (!state.clusterUuid.empty() && state.clusterUuid != expectedClusterUuid_)
            return false;
        const NodeRecord& local = *expectedLocalRecord_;
        for (const auto& [id, record] : state.nodes) {
            if (id == local.raftId &&
                (record.uuid != local.uuid || record.address != local.address ||
                 record.failureDomain != local.failureDomain))
                return false;
            if (id != local.raftId && record.uuid == local.uuid)
                return false;
        }
    }
    // A recovered Group-0 snapshot may legitimately predate the durable local
    // routing cache: publication is persisted before the committed entry's
    // applied boundary advances, and the retained log then catches Group 0 back
    // up. Only two different maps claiming the same epoch are irreconcilable.
    if (expectedServingMap_ && state.servingMap.epoch == expectedServingMap_->epoch &&
        state.servingMap != *expectedServingMap_)
        return false;
    return true;
}

void Group0StateMachine::rejectConflictingLocalCommand(const ControlCommand& command) const {
    if (const auto* serving = std::get_if<PublishServingMap>(&command)) {
        // The startup expectation fences the recovered/current map. Once that
        // exact state is established, a later quorum-committed movement cutover
        // may advance it and the observer durably replaces the local cache.
        if (expectedServingMap_ && serving->map.epoch == expectedServingMap_->epoch &&
            serving->map != *expectedServingMap_)
            throw std::runtime_error("group0: committed serving map conflicts with local durable placement");
        return;
    }
    if (!expectedLocalRecord_)
        return;
    if (const auto* init = std::get_if<InitCluster>(&command)) {
        if (init->clusterUuid != expectedClusterUuid_)
            throw std::runtime_error("group0: committed cluster UUID conflicts with local persistent identity");
        return;
    }
    const NodeRecord* record = nullptr;
    if (const auto* upsert = std::get_if<UpsertNode>(&command))
        record = &upsert->record;
    else if (const auto* admission = std::get_if<AdmitWithToken>(&command))
        record = &admission->record;
    if (!record)
        return;
    const NodeRecord& local = *expectedLocalRecord_;
    if ((record->raftId == local.raftId &&
         (record->uuid != local.uuid || record->address != local.address ||
          record->failureDomain != local.failureDomain)) ||
        (record->raftId != local.raftId && record->uuid == local.uuid))
        throw std::runtime_error("group0: committed node record conflicts with local persistent identity");
}

bool Group0StateMachine::applyCommand(const ControlCommand& cmd) {
    bool ok = true;
    std::visit(
        [&](const auto& c) {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, InitCluster>) {
                if (c.clusterUuid.empty() || !state_.clusterUuid.empty())
                    ok = false;
                else
                    state_.clusterUuid = c.clusterUuid;  // never re-init
            } else if constexpr (std::is_same_v<T, UpsertNode>) {
                if (!validNodeRecord(state_, c.record)) {
                    ok = false;
                } else if (auto it = state_.nodes.find(c.record.raftId);
                           it != state_.nodes.end() && it->second == c.record) {
                    ok = false;
                } else {
                    state_.nodes[c.record.raftId] = c.record;
                }
            } else if constexpr (std::is_same_v<T, SetNodeState>) {
                if (auto it = state_.nodes.find(c.raftId);
                    it != state_.nodes.end() && isValidNodeState(c.state) && it->second.state != c.state)
                    it->second.state = c.state;
                else
                    ok = false;
            } else if constexpr (std::is_same_v<T, PlanVShardMove>) {
                if (!validNewMove(state_, c)) {
                    ok = false;
                } else {
                    movement::MoveJob job(c.plan);
                    const std::string payload = job.encode();
                    state_.mapEpoch = c.plan.mapEpoch;
                    state_.desiredPlacement[c.plan.vshard] = job.targetVoters();
                    state_.jobs.emplace(c.jobId, Job{c.jobId, static_cast<uint32_t>(movement::MoveStep::Planned),
                                                     false, payload});
                }
            } else if constexpr (std::is_same_v<T, SetMetaVoters>) {
                if (!validNodeSet(c.voters) || state_.metaVoters == c.voters)
                    ok = false;
                else
                    state_.metaVoters = c.voters;
            } else if constexpr (std::is_same_v<T, CasPolicy>) {
                // Read the current version WITHOUT inserting -- a failed CAS must
                // leave state exactly unchanged (no phantom version-0 cell).
                auto it = state_.policies.find(c.key);
                const uint64_t curVer = (it == state_.policies.end()) ? 0 : it->second.version;
                if (!c.key.empty() && curVer == c.expectedVersion) {
                    PolicyCell& cell = state_.policies[c.key];  // insert only on success
                    ++cell.version;
                    cell.value = c.value;
                } else {
                    ok = false;  // lost update: CAS failed (state unchanged)
                }
            } else if constexpr (std::is_same_v<T, SetControllerTerm>) {
                if (c.term > state_.controllerTerm && c.leader != raft::kNoNode) {  // monotonic fencing
                    state_.controllerTerm = c.term;
                    state_.controllerLeader = c.leader;
                } else
                    ok = false;
            } else if constexpr (std::is_same_v<T, UpsertJob>) {
                const Job incoming{c.jobId, c.step, c.done, c.payload};
                const auto incomingMove = decodeJob(incoming);
                auto it = state_.jobs.find(c.jobId);
                if (!incomingMove || it == state_.jobs.end()) {
                    ok = false;
                } else {
                    Job& j = it->second;
                    const auto existingMove = decodeJob(j);
                    // A planned operation is immutable and advances exactly one
                    // durable step at a time. Skipping a step would turn the
                    // summary into an assertion that catch-up/config work happened
                    // without evidence; changing the plan would authorize a
                    // different removal under an existing job id.
                    if (!existingMove || j.done || c.step != j.step + 1 ||
                        incomingMove->plan() != existingMove->plan() ||
                        incomingMove->plan().mapEpoch != state_.mapEpoch) {
                        ok = false;
                    } else {
                        j.step = c.step;
                        j.payload = c.payload;
                        j.done = c.done;
                    }
                }
            } else if constexpr (std::is_same_v<T, MintJoinToken>) {
                if (!validJoinToken(c.token) || state_.joinTokens.size() >= kMaxOutstandingJoinTokens ||
                    !state_.joinTokens.insert(c.token).second)
                    ok = false;
            } else if constexpr (std::is_same_v<T, AdmitWithToken>) {
                // Admit ONLY on a valid unused token, consumed atomically. An
                // invalid/replayed token is a no-op (never implicitly initialize).
                auto it = state_.joinTokens.find(c.token);
                if (validJoinToken(c.token) && it != state_.joinTokens.end() &&
                    validNodeRecord(state_, c.record)) {
                    state_.joinTokens.erase(it);
                    NodeRecord record = c.record;
                    // Admission is a state-machine invariant, not merely a
                    // controller convention: no caller can encode Active and
                    // bypass learner catch-up.
                    record.state = NodeState::Joining;
                    state_.nodes[record.raftId] = std::move(record);
                } else {
                    ok = false;  // rejected: no such token
                }
            } else if constexpr (std::is_same_v<T, StoreFrozenDeletePlan>) {
                if (!isCompleteControlMap(state_.servingMap) || !validFrozenDeletePlan(c.plan)) {
                    ok = false;
                } else {
                    // A request timestamp may be up to five minutes in the
                    // future. Retain that extra skew so a future-dated request
                    // cannot evict a plan whose one-hour HTTP retry window is
                    // still open according to wall clock.
                    bool pruned = false;
                    std::erase_if(state_.frozenDeletePlans, [&](const auto& item) {
                        const bool expired = frozenDeletePlanExpiredAt(item.second, c.plan.issuedAtMs);
                        pruned = pruned || expired;
                        return expired;
                    });

                    if (auto found = state_.frozenDeletePlans.find(c.plan.requestId);
                        found != state_.frozenDeletePlans.end()) {
                        // The identity and original request bytes, not a later
                        // catalog result, define an idempotent retry. Preserve
                        // the first target vector even if re-expansion changed.
                        ok = pruned;
                    } else {
                        size_t aggregateBytes = 0;
                        for (const auto& [id, plan] : state_.frozenDeletePlans) {
                            (void)id;
                            aggregateBytes += frozenDeletePlanBytes(plan);
                        }
                        const size_t planBytes = frozenDeletePlanBytes(c.plan);
                        if (state_.frozenDeletePlans.size() == kMaxFrozenDeletePlans ||
                            aggregateBytes > kMaxFrozenDeletePlanAggregateBytes - planBytes) {
                            ok = pruned;
                        } else {
                            state_.frozenDeletePlans.emplace(c.plan.requestId, c.plan);
                        }
                    }
                }
            } else if constexpr (std::is_same_v<T, PublishServingMap>) {
                if (!isCompleteControlMap(c.map)) {
                    ok = false;
                } else if (state_.servingMap.epoch == 0) {
                    // Bootstrap is the only publication without a completed
                    // movement proof.
                    if (!c.completedJobId.empty() || c.map.epoch != 1 || state_.mapEpoch != 0)
                        ok = false;
                    else {
                        state_.mapEpoch = 1;
                        state_.servingMap = c.map;
                    }
                } else {
                    const auto persisted = state_.jobs.find(c.completedJobId);
                    const auto move = persisted == state_.jobs.end() ? std::optional<movement::MoveJob>{}
                                                                     : decodeJob(persisted->second);
                    const auto desired = move ? state_.desiredPlacement.find(move->plan().vshard)
                                              : state_.desiredPlacement.end();
                    if (!move || !persisted->second.done || move->plan().mapEpoch != state_.mapEpoch ||
                        c.map.epoch != state_.mapEpoch || state_.servingMap.epoch + 1 != c.map.epoch ||
                        desired == state_.desiredPlacement.end()) {
                        ok = false;
                    } else {
                        ControlMap expected = state_.servingMap;
                        expected.epoch = c.map.epoch;
                        expected.placement[move->plan().vshard] = desired->second;
                        if (desired->second != move->targetVoters() || c.map != expected)
                            ok = false;
                        else
                            state_.servingMap = c.map;
                    }
                }
            }
        },
        cmd);
    return ok;
}

seastar::future<> Group0StateMachine::apply(raft::LogEntry entry) {
    auto cmd = decodeCommand(entry.data);
    if (!cmd) {
        // A COMMITTED group-0 entry that this binary cannot decode is fatal: it
        // means a peer applied a command we don't understand (a version/format
        // mismatch), and silently skipping it while advancing appliedIndex would
        // diverge this replica from the others. Fail-stop instead (the driver
        // should quarantine the group rather than serve divergent control state).
        throw std::runtime_error("group0: undecodable committed control command");
    }
    // The replicated controller epoch is the Raft term that actually owns this
    // log entry, never a caller-provided integer. A controller can sample its
    // term before waiting for the group lock and append after a rapid election;
    // canonicalising here both closes that race and prevents an internal caller
    // from permanently fencing the cluster with a fabricated future term.
    if (auto* stamp = std::get_if<SetControllerTerm>(&*cmd))
        stamp->term = entry.term;
    rejectConflictingLocalCommand(*cmd);
    const auto* publication = std::get_if<PublishServingMap>(&*cmd);
    const bool advancesDurableServingMap = publication &&
                                           (!expectedServingMap_ ||
                                            publication->map.epoch > expectedServingMap_->epoch);
    const bool publishesServingMap = servingMapObserver_ && advancesDurableServingMap;
    std::optional<Group0State> previous;
    if (publishesServingMap)
        previous = state_;
    const bool applied = applyCommand(*cmd);
    if (applied && previous) {
        Group0State staged = std::move(state_);
        state_ = std::move(*previous);
        // Keep both the logical state and applied boundary private until every
        // node-local publication succeeds. A failure leaves the old state
        // visible, so replay deterministically reapplies and republishes the
        // exact same committed entry.
        if (publishesServingMap)
            co_await servingMapObserver_(staged.servingMap);
        expectedServingMap_ = staged.servingMap;
        state_ = std::move(staged);
    } else if (applied && publication &&
               (!expectedServingMap_ || state_.servingMap.epoch > expectedServingMap_->epoch)) {
        expectedServingMap_ = state_.servingMap;
    }
    state_.appliedIndex = entry.index;
    co_return;
}

std::string Group0StateMachine::snapshot() const {
    SW w;
    w.u64(kSnapshotMagic);
    w.str(state_.clusterUuid);
    w.u64(state_.mapEpoch);
    w.u64(state_.appliedIndex);
    w.u64(state_.controllerTerm);
    w.u64(state_.controllerLeader);
    w.u64(state_.nodes.size());
    for (const auto& [id, n] : state_.nodes) {
        w.u64(n.raftId);
        w.str(n.uuid);
        w.str(n.address);
        w.str(n.failureDomain);
        w.u8(static_cast<uint8_t>(n.state));
    }
    w.u64(state_.desiredPlacement.size());
    for (const auto& [vs, reps] : state_.desiredPlacement) {
        w.u16(vs);
        w.ids(reps);
    }
    w.ids(state_.metaVoters);
    w.u64(state_.policies.size());
    for (const auto& [k, cell] : state_.policies) {
        w.str(k);
        w.u64(cell.version);
        w.str(cell.value);
    }
    w.u64(state_.jobs.size());
    for (const auto& [id, j] : state_.jobs) {
        w.str(j.id);
        w.u64(j.step);
        w.u8(j.done ? 1 : 0);
        w.str(j.payload);
    }
    w.u64(state_.joinTokens.size());
    for (const auto& t : state_.joinTokens)  // std::set: canonical order
        w.str(t);
    writeControlMap(w, state_.servingMap);
    w.u64(state_.frozenDeletePlans.size());
    for (const auto& [requestId, plan] : state_.frozenDeletePlans) {
        (void)requestId;
        writeFrozenDeletePlan(w, plan);
    }
    return std::move(w.out);
}

bool Group0StateMachine::decodeSnapshot(const std::string& data, Group0State& decoded) const {
    SR r{data.data(), data.data() + data.size()};
    Group0State s;
    if (r.u64() != kSnapshotMagic)
        return false;
    s.clusterUuid = r.str();
    s.mapEpoch = r.u64();
    s.appliedIndex = r.u64();
    s.controllerTerm = r.u64();
    s.controllerLeader = r.u64();
    uint64_t nNodes = r.u64();
    if (!r.ok || nNodes > static_cast<uint64_t>(r.end - r.p) / 33)
        r.ok = false;  // minimum node: id + three string lengths + state
    for (uint64_t i = 0; i < nNodes && r.ok; ++i) {
        NodeRecord n;
        n.raftId = r.u64();
        n.uuid = r.str();
        n.address = r.str();
        n.failureDomain = r.str();
        n.state = r.nodeState();
        if (r.ok && !s.nodes.emplace(n.raftId, std::move(n)).second)
            r.ok = false;
    }
    uint64_t nPlace = r.u64();
    if (!r.ok || nPlace > static_cast<uint64_t>(r.end - r.p) / 10)
        r.ok = false;  // minimum placement: vshard + replica count
    for (uint64_t i = 0; i < nPlace && r.ok; ++i) {
        uint16_t vs = r.u16();
        auto replicas = r.ids();
        if (r.ok && !s.desiredPlacement.emplace(vs, std::move(replicas)).second)
            r.ok = false;
    }
    s.metaVoters = r.ids();
    uint64_t nPol = r.u64();
    if (!r.ok || nPol > static_cast<uint64_t>(r.end - r.p) / 24)
        r.ok = false;  // minimum policy: key length + version + value length
    for (uint64_t i = 0; i < nPol && r.ok; ++i) {
        std::string k = r.str();
        PolicyCell cell;
        cell.version = r.u64();
        cell.value = r.str();
        if (r.ok && !s.policies.emplace(std::move(k), std::move(cell)).second)
            r.ok = false;
    }
    uint64_t nJobs = r.u64();
    if (!r.ok || nJobs > static_cast<uint64_t>(r.end - r.p) / 25)
        r.ok = false;  // minimum job: id length + step + done + payload length
    for (uint64_t i = 0; i < nJobs && r.ok; ++i) {
        Job j;
        j.id = r.str();
        j.step = r.u32();
        j.done = r.boolean();
        j.payload = r.str();
        if (r.ok && !s.jobs.emplace(j.id, std::move(j)).second)
            r.ok = false;
    }
    uint64_t nTok = r.u64();
    if (!r.ok || nTok > kMaxOutstandingJoinTokens || nTok > static_cast<uint64_t>(r.end - r.p) / 8)
        r.ok = false;  // each token has at least its string length
    for (uint64_t i = 0; i < nTok && r.ok; ++i) {
        auto token = r.str(kMaxJoinTokenBytes);
        if (r.ok && (!validJoinToken(token) || !s.joinTokens.insert(std::move(token)).second))
            r.ok = false;
    }
    s.servingMap = readControlMap(r);
    const uint64_t frozenPlanCount = r.u64();
    if (!r.ok || frozenPlanCount > kMaxFrozenDeletePlans ||
        frozenPlanCount > static_cast<uint64_t>(r.end - r.p) / (sizeof(uint64_t) * 4)) {
        r.ok = false;
    } else {
        for (uint64_t i = 0; i < frozenPlanCount && r.ok; ++i) {
            FrozenDeletePlan plan = readFrozenDeletePlan(r);
            if (r.ok && !s.frozenDeletePlans.emplace(plan.requestId, std::move(plan)).second)
                r.ok = false;
        }
    }
    if (!r.ok || r.p != r.end || !validSnapshotState(s) || !stateMatchesLocalExpectations(s))
        return false;  // reject a corrupt snapshot without half-applying it
    decoded = std::move(s);
    return true;
}

bool Group0StateMachine::loadSnapshot(const std::string& data) {
    Group0State decoded;
    if (!decodeSnapshot(data, decoded))
        return false;
    if (decoded.servingMap.epoch != 0 &&
        (!expectedServingMap_ || decoded.servingMap.epoch > expectedServingMap_->epoch))
        expectedServingMap_ = decoded.servingMap;
    state_ = std::move(decoded);
    return true;
}

seastar::future<> Group0StateMachine::applySnapshot(raft::Snapshot snap) {
    // Raft has already accepted the snapshot boundary by the time the driver calls
    // this method. Treating a decode failure as success advances Raft's applied index
    // while leaving the old control state in service. That is silent control-plane
    // divergence, so fail-stop and let the group be quarantined.
    Group0State decoded;
    if (!decodeSnapshot(snap.data, decoded))
        throw std::runtime_error("group0: invalid control-state snapshot");
    const bool advancesDurableServingMap =
        decoded.servingMap.epoch != 0 &&
        (!expectedServingMap_ || decoded.servingMap.epoch > expectedServingMap_->epoch);
    if (servingMapObserver_ && advancesDurableServingMap)
        co_await servingMapObserver_(decoded.servingMap);
    if (advancesDurableServingMap)
        expectedServingMap_ = decoded.servingMap;
    // The snapshot boundary is at least this index.
    if (snap.index > decoded.appliedIndex)
        decoded.appliedIndex = snap.index;
    // Publish recovered replicated state only after every node-local observer
    // succeeds. On failure the old state and applied boundary remain visible,
    // and Raft can retry the same idempotent snapshot installation.
    state_ = std::move(decoded);
    co_return;
}

}  // namespace timestar::control
