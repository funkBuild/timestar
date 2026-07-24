#include "group0_state_machine.hpp"

#include <seastar/core/coroutine.hh>

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
        if (!ok || !avail(n * 8)) {
            ok = false;
            return v;
        }
        for (uint64_t i = 0; i < n; ++i)
            v.push_back(u64());
        return v;
    }
};

}  // namespace

bool Group0StateMachine::applyCommand(const ControlCommand& cmd) {
    bool ok = true;
    std::visit(
        [&](const auto& c) {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, InitCluster>) {
                if (state_.clusterUuid.empty())
                    state_.clusterUuid = c.clusterUuid;  // never re-init
            } else if constexpr (std::is_same_v<T, UpsertNode>) {
                state_.nodes[c.record.raftId] = c.record;
            } else if constexpr (std::is_same_v<T, SetNodeState>) {
                if (auto it = state_.nodes.find(c.raftId); it != state_.nodes.end())
                    it->second.state = c.state;
            } else if constexpr (std::is_same_v<T, SetDesiredPlacement>) {
                state_.desiredPlacement[c.vshard] = c.replicas;
                ++state_.mapEpoch;  // topology change bumps the epoch
            } else if constexpr (std::is_same_v<T, SetMetaVoters>) {
                state_.metaVoters = c.voters;
            } else if constexpr (std::is_same_v<T, CasPolicy>) {
                PolicyCell& cell = state_.policies[c.key];
                if (cell.version == c.expectedVersion) {
                    ++cell.version;
                    cell.value = c.value;
                } else {
                    ok = false;  // lost update: CAS failed (state unchanged)
                }
            } else if constexpr (std::is_same_v<T, SetControllerTerm>) {
                if (c.term > state_.controllerTerm) {  // monotonic fencing
                    state_.controllerTerm = c.term;
                    state_.controllerLeader = c.leader;
                }
            } else if constexpr (std::is_same_v<T, UpsertJob>) {
                Job& j = state_.jobs[c.jobId];
                j.id = c.jobId;
                // Idempotent step: never move a job backwards or un-complete it.
                if (c.step >= j.step)
                    j.step = c.step;
                if (c.done)
                    j.done = true;
                j.payload = c.payload;
            }
        },
        cmd);
    return ok;
}

seastar::future<> Group0StateMachine::apply(raft::LogEntry entry) {
    if (auto cmd = decodeCommand(entry.data))
        applyCommand(*cmd);
    // A malformed command is a hard fault in a real deployment; here we skip it
    // rather than crash the reactor. appliedIndex tracks committed control state.
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
    if (!r.ok)
        return false;  // ignore a corrupt snapshot rather than half-apply
    state_ = std::move(s);
    return true;
}

seastar::future<> Group0StateMachine::applySnapshot(raft::Snapshot snap) {
    loadSnapshot(snap.data);
    // The snapshot boundary is at least this index.
    if (snap.index > state_.appliedIndex)
        state_.appliedIndex = snap.index;
    return seastar::make_ready_future<>();
}

}  // namespace timestar::control
