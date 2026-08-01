#include "group0_state_machine.hpp"

#include "../../core/vshard.hpp"

#include <seastar/core/coroutine.hh>
#include <set>
#include <stdexcept>

namespace timestar::control {

namespace {

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

bool validNodeRecord(const Group0State& state, const NodeRecord& record) {
    if (record.raftId == raft::kNoNode || record.uuid.empty() || record.address.empty())
        return false;
    if (auto it = state.nodes.find(record.raftId); it != state.nodes.end() && it->second.uuid != record.uuid)
        return false;  // a Raft id is permanently bound to one node identity
    for (const auto& [id, existing] : state.nodes)
        if (id != record.raftId && existing.uuid == record.uuid)
            return false;  // one persistent node identity cannot occupy two ids
    return true;
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
                    it != state_.nodes.end() && it->second.state != c.state)
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
                } else {
                    Job& j = state_.jobs[c.jobId];
                    j.id = c.jobId;
                    // Idempotent step: never move a job backwards, and keep the
                    // payload paired with the RETAINED step (an out-of-order replay of
                    // an older step must not overwrite the newer payload).
                    if (c.step >= j.step) {
                        j.step = c.step;
                        j.payload = c.payload;
                    }
                    if (c.done)
                        j.done = true;
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
                    NodeRecord rec = c.record;
                    rec.state = NodeState::Active;
                    state_.nodes[rec.raftId] = std::move(rec);
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
    applyCommand(*cmd);
    state_.appliedIndex = entry.index;
    return seastar::make_ready_future<>();
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
    for (uint64_t i = 0; i < nNodes && r.ok; ++i) {
        NodeRecord n;
        n.raftId = r.u64();
        n.uuid = r.str();
        n.address = r.str();
        n.failureDomain = r.str();
        n.state = static_cast<NodeState>(r.u8());
        s.nodes[n.raftId] = std::move(n);
    }
    uint64_t nPlace = r.u64();
    for (uint64_t i = 0; i < nPlace && r.ok; ++i) {
        uint16_t vs = r.u16();
        s.desiredPlacement[vs] = r.ids();
    }
    s.metaVoters = r.ids();
    uint64_t nPol = r.u64();
    for (uint64_t i = 0; i < nPol && r.ok; ++i) {
        std::string k = r.str();
        PolicyCell cell;
        cell.version = r.u64();
        cell.value = r.str();
        s.policies[k] = std::move(cell);
    }
    uint64_t nJobs = r.u64();
    for (uint64_t i = 0; i < nJobs && r.ok; ++i) {
        Job j;
        j.id = r.str();
        j.step = static_cast<uint32_t>(r.u64());
        j.done = r.u8() != 0;
        j.payload = r.str();
        s.jobs[j.id] = std::move(j);
    }
    uint64_t nTok = r.u64();
    for (uint64_t i = 0; i < nTok && r.ok; ++i)
        s.joinTokens.insert(r.str());
    // Trailing, optional for backward compatibility: a pre-format-version snapshot has
    // no field here, so default to 1 (s.activeFormatVersion's default) rather than fail.
    // Exactly eight trailing bytes is the only other valid shape. Previously a partial
    // field or arbitrary suffix was silently accepted, which could make an older binary
    // claim it had installed state it did not actually understand.
    const size_t remaining = static_cast<size_t>(r.end - r.p);
    if (r.ok && remaining == 8) {
        const uint64_t activeFormatVersion = r.u64();
        if (activeFormatVersion == 0 || activeFormatVersion > UINT32_MAX)
            r.ok = false;
        else
            s.activeFormatVersion = static_cast<uint32_t>(activeFormatVersion);
    } else if (remaining != 0) {
        r.ok = false;
    }
    if (!r.ok || r.p != r.end)
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
    // The snapshot boundary is at least this index.
    if (snap.index > state_.appliedIndex)
        state_.appliedIndex = snap.index;
    return seastar::make_ready_future<>();
}

}  // namespace timestar::control
