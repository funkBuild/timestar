#include "group0_state_machine.hpp"

#include "../../core/vshard.hpp"

#include <seastar/core/coroutine.hh>
#include <limits>
#include <set>
#include <stdexcept>

namespace timestar::control {

namespace {

constexpr uint64_t kServingMapSnapshotMagic = 0x54534730'4d415031ull;  // "TSG0MAP1"

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
    std::string str() {
        uint64_t n = u64();
        if (!ok || !avail(n)) {
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
         (!state.servingMap.placement.empty() || !state.servingMap.groups.empty())) ||
        (state.servingMap.epoch != 0 &&
         (state.servingMap.epoch != 1 || !isCompleteControlMap(state.servingMap))))
        return false;
    if (!state.metaVoters.empty() && !validNodeSet(state.metaVoters))
        return false;
    for (const auto& [key, cell] : state.policies)
        if (key.empty() || cell.version == 0)
            return false;
    for (const auto& [id, job] : state.jobs)
        if (id.empty() || id != job.id)
            return false;
    for (const auto& token : state.joinTokens)
        if (token.empty())
            return false;
    if ((state.controllerTerm == 0) != (state.controllerLeader == raft::kNoNode))
        return false;
    return state.activeFormatVersion != 0;
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

void Group0StateMachine::expectInitialServingMap(ControlMap map) {
    if (map.epoch != 1 || !isCompleteControlMap(map))
        throw std::invalid_argument("group0: expected initial serving map is incomplete or not epoch 1");
    if (expectedInitialServingMap_)
        throw std::logic_error("group0: initial serving-map expectation was configured more than once");
    expectedInitialServingMap_ = std::move(map);
    if (!stateMatchesLocalExpectations(state_))
        throw std::runtime_error("group0: existing control state conflicts with the expected initial serving map");
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
    if (expectedInitialServingMap_ && state.servingMap.epoch != 0 &&
        state.servingMap != *expectedInitialServingMap_)
        return false;
    return true;
}

void Group0StateMachine::rejectConflictingLocalCommand(const ControlCommand& command) const {
    if (const auto* serving = std::get_if<SetInitialServingMap>(&command)) {
        if (expectedInitialServingMap_ && serving->map != *expectedInitialServingMap_)
            throw std::runtime_error("group0: committed initial serving map conflicts with local placement");
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
            } else if constexpr (std::is_same_v<T, SetDesiredPlacement>) {
                auto it = state_.desiredPlacement.find(c.vshard);
                if (c.vshard >= timestar::VIRTUAL_SHARD_COUNT || !validNodeSet(c.replicas) ||
                    (it != state_.desiredPlacement.end() && it->second == c.replicas)) {
                    ok = false;
                } else {
                    state_.desiredPlacement[c.vshard] = c.replicas;
                    ++state_.mapEpoch;  // only a real topology change bumps the epoch
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
                if (c.jobId.empty()) {
                    ok = false;
                } else if (auto it = state_.jobs.find(c.jobId); it == state_.jobs.end()) {
                    state_.jobs.emplace(c.jobId, Job{c.jobId, c.step, c.done, c.payload});
                } else {
                    Job& j = it->second;
                    // A step number identifies one immutable operation. Conflicting
                    // payloads at the same step are rejected, as are regressions and
                    // attempts to resurrect a completed job. The one allowed same-step
                    // mutation is making completion sticky.
                    if (j.done || c.step < j.step || (c.step == j.step && c.payload != j.payload)) {
                        ok = false;
                    } else if (c.step > j.step) {
                        j.step = c.step;
                        j.payload = c.payload;
                        j.done = c.done;
                    } else if (!j.done && c.done) {
                        j.done = true;
                    } else {
                        ok = false;  // exact idempotent replay
                    }
                }
            } else if constexpr (std::is_same_v<T, MintJoinToken>) {
                if (c.token.empty() || !state_.joinTokens.insert(c.token).second)
                    ok = false;
            } else if constexpr (std::is_same_v<T, AdmitWithToken>) {
                // Admit ONLY on a valid unused token, consumed atomically. An
                // invalid/replayed token is a no-op (never implicitly initialize).
                auto it = state_.joinTokens.find(c.token);
                if (it != state_.joinTokens.end() && validNodeRecord(state_, c.record)) {
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
            } else if constexpr (std::is_same_v<T, SetActiveVersion>) {
                // Monotonic: a stale/replayed lower version must never regress the
                // active format (nodes only ever move formats forward). The safety
                // check (every voter can read it) is enforced by the controller BEFORE
                // proposing; apply is the durable, monotonic record.
                if (c.version > state_.activeFormatVersion)
                    state_.activeFormatVersion = c.version;
                else
                    ok = false;  // no-op: not an advance
            } else if constexpr (std::is_same_v<T, SetInitialServingMap>) {
                // Single-assignment by construction. A later serving-map epoch
                // is a topology cutover and must go through the resumable data
                // movement protocol, which is intentionally not implemented by
                // this command.
                if (c.map.epoch != 1 || !isCompleteControlMap(c.map) || state_.servingMap.epoch != 0)
                    ok = false;
                else
                    state_.servingMap = c.map;
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
    rejectConflictingLocalCommand(*cmd);
    applyCommand(*cmd);
    if (servingMapObserver_ && std::holds_alternative<SetInitialServingMap>(*cmd) &&
        state_.servingMap.epoch != 0)
        co_await servingMapObserver_(state_.servingMap);
    state_.appliedIndex = entry.index;
    co_return;
}

std::string Group0StateMachine::snapshot() const {
    SW w;
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
    w.u64(state_.activeFormatVersion);  // trailing (back-compat: read only if present)
    if (state_.servingMap.epoch != 0) {
        w.u64(kServingMapSnapshotMagic);
        writeControlMap(w, state_.servingMap);
    }
    return std::move(w.out);
}

bool Group0StateMachine::loadSnapshot(const std::string& data) {
    SR r{data.data(), data.data() + data.size()};
    Group0State s;
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
    if (!r.ok || nTok > static_cast<uint64_t>(r.end - r.p) / 8)
        r.ok = false;  // each token has at least its string length
    for (uint64_t i = 0; i < nTok && r.ok; ++i) {
        auto token = r.str();
        if (r.ok && !s.joinTokens.insert(std::move(token)).second)
            r.ok = false;
    }
    // Trailing, optional for backward compatibility: a pre-format-version snapshot has
    // no field here, so default to 1. A current snapshot carries active version, then an
    // optional magic-tagged initial serving map. Unknown/partial trailers fail closed.
    size_t remaining = static_cast<size_t>(r.end - r.p);
    if (r.ok && remaining >= 8) {
        const uint64_t activeFormatVersion = r.u64();
        if (activeFormatVersion == 0 || activeFormatVersion > std::numeric_limits<uint32_t>::max())
            r.ok = false;
        else
            s.activeFormatVersion = static_cast<uint32_t>(activeFormatVersion);
    } else if (remaining != 0) {
        r.ok = false;
    }
    remaining = static_cast<size_t>(r.end - r.p);
    if (r.ok && remaining != 0) {
        if (remaining < 8 || r.u64() != kServingMapSnapshotMagic)
            r.ok = false;
        else
            s.servingMap = readControlMap(r);
    }
    if (!r.ok || r.p != r.end || !validSnapshotState(s) || !stateMatchesLocalExpectations(s))
        return false;  // reject a corrupt snapshot without half-applying it
    state_ = std::move(s);
    return true;
}

seastar::future<> Group0StateMachine::applySnapshot(raft::Snapshot snap) {
    // Raft has already accepted the snapshot boundary by the time the driver calls
    // this method. Treating a decode failure as success advances Raft's applied index
    // while leaving the old control state in service. That is silent control-plane
    // divergence, so fail-stop and let the group be quarantined.
    if (!loadSnapshot(snap.data))
        throw std::runtime_error("group0: invalid control-state snapshot");
    if (servingMapObserver_ && state_.servingMap.epoch != 0)
        co_await servingMapObserver_(state_.servingMap);
    // The snapshot boundary is at least this index.
    if (snap.index > state_.appliedIndex)
        state_.appliedIndex = snap.index;
    co_return;
}

}  // namespace timestar::control
