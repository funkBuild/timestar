#include "control_command.hpp"

namespace timestar::control {

namespace {

struct Writer {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u16(uint16_t v) {
        u8(v & 0xff);
        u8((v >> 8) & 0xff);
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            u8((v >> (8 * i)) & 0xff);
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

struct Reader {
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
        // Non-wrapping bounds check: n*8 could overflow, so compare against the
        // remaining byte budget divided by 8 instead of multiplying.
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

enum : uint8_t {
    kInitCluster = 1,
    kUpsertNode = 2,
    kSetNodeState = 3,
    kSetDesiredPlacement = 4,
    kSetMetaVoters = 5,
    kCasPolicy = 6,
    kSetControllerTerm = 7,
    kUpsertJob = 8,
};

void writeNode(Writer& w, const NodeRecord& r) {
    w.u64(r.raftId);
    w.str(r.uuid);
    w.str(r.address);
    w.str(r.failureDomain);
    w.u8(static_cast<uint8_t>(r.state));
}

NodeRecord readNode(Reader& r) {
    NodeRecord n;
    n.raftId = r.u64();
    n.uuid = r.str();
    n.address = r.str();
    n.failureDomain = r.str();
    n.state = static_cast<NodeState>(r.u8());
    return n;
}

}  // namespace

std::string encodeCommand(const ControlCommand& cmd) {
    Writer w;
    std::visit(
        [&](const auto& c) {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, InitCluster>) {
                w.u8(kInitCluster);
                w.str(c.clusterUuid);
            } else if constexpr (std::is_same_v<T, UpsertNode>) {
                w.u8(kUpsertNode);
                writeNode(w, c.record);
            } else if constexpr (std::is_same_v<T, SetNodeState>) {
                w.u8(kSetNodeState);
                w.u64(c.raftId);
                w.u8(static_cast<uint8_t>(c.state));
            } else if constexpr (std::is_same_v<T, SetDesiredPlacement>) {
                w.u8(kSetDesiredPlacement);
                w.u16(c.vshard);
                w.ids(c.replicas);
            } else if constexpr (std::is_same_v<T, SetMetaVoters>) {
                w.u8(kSetMetaVoters);
                w.ids(c.voters);
            } else if constexpr (std::is_same_v<T, CasPolicy>) {
                w.u8(kCasPolicy);
                w.str(c.key);
                w.u64(c.expectedVersion);
                w.str(c.value);
            } else if constexpr (std::is_same_v<T, SetControllerTerm>) {
                w.u8(kSetControllerTerm);
                w.u64(c.term);
                w.u64(c.leader);
            } else if constexpr (std::is_same_v<T, UpsertJob>) {
                w.u8(kUpsertJob);
                w.str(c.jobId);
                w.u64(c.step);
                w.u8(c.done ? 1 : 0);
                w.str(c.payload);
            }
        },
        cmd);
    return std::move(w.out);
}

std::optional<ControlCommand> decodeCommand(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    const uint8_t tag = r.u8();
    ControlCommand cmd;
    switch (tag) {
        case kInitCluster: {
            InitCluster c;
            c.clusterUuid = r.str();
            cmd = std::move(c);
            break;
        }
        case kUpsertNode: {
            UpsertNode c;
            c.record = readNode(r);
            cmd = std::move(c);
            break;
        }
        case kSetNodeState: {
            SetNodeState c;
            c.raftId = r.u64();
            c.state = static_cast<NodeState>(r.u8());
            cmd = c;
            break;
        }
        case kSetDesiredPlacement: {
            SetDesiredPlacement c;
            c.vshard = r.u16();
            c.replicas = r.ids();
            cmd = std::move(c);
            break;
        }
        case kSetMetaVoters: {
            SetMetaVoters c;
            c.voters = r.ids();
            cmd = std::move(c);
            break;
        }
        case kCasPolicy: {
            CasPolicy c;
            c.key = r.str();
            c.expectedVersion = r.u64();
            c.value = r.str();
            cmd = std::move(c);
            break;
        }
        case kSetControllerTerm: {
            SetControllerTerm c;
            c.term = r.u64();
            c.leader = r.u64();
            cmd = c;
            break;
        }
        case kUpsertJob: {
            UpsertJob c;
            c.jobId = r.str();
            c.step = static_cast<uint32_t>(r.u64());
            c.done = r.u8() != 0;
            c.payload = r.str();
            cmd = std::move(c);
            break;
        }
        default:
            return std::nullopt;
    }
    if (!r.ok)
        return std::nullopt;
    return cmd;
}

}  // namespace timestar::control
