#include "raft_codec.hpp"

#include <cstring>

namespace timestar::raft {

namespace {

// ---- little-endian writer ----
struct Writer {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u16(uint16_t v) {
        u8(v & 0xff);
        u8((v >> 8) & 0xff);
    }
    void u32(uint32_t v) {
        for (int i = 0; i < 4; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void str(const std::string& s) {
        u32(static_cast<uint32_t>(s.size()));
        out.append(s);
    }
    void ids(const std::vector<NodeId>& v) {
        u32(static_cast<uint32_t>(v.size()));
        for (NodeId n : v)
            u64(n);
    }
};

// ---- bounds-checked little-endian reader ----
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
        uint16_t a = u8();
        uint16_t b = u8();
        return static_cast<uint16_t>(a | (b << 8));
    }
    uint32_t u32() {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(u8()) << (8 * i);
        return v;
    }
    uint64_t u64() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(u8()) << (8 * i);
        return v;
    }
    std::string str() {
        uint32_t n = u32();
        if (!ok || !avail(n)) {
            ok = false;
            return {};
        }
        std::string s(p, p + n);
        p += n;
        return s;
    }
    std::vector<NodeId> ids() {
        uint32_t n = u32();
        std::vector<NodeId> v;
        if (!ok || !avail(static_cast<size_t>(n) * 8)) {
            ok = false;
            return v;
        }
        v.reserve(n);
        for (uint32_t i = 0; i < n; ++i)
            v.push_back(u64());
        return v;
    }
};

enum : uint8_t {
    kRequestVote = 1,
    kRequestVoteReply = 2,
    kAppendEntries = 3,
    kAppendEntriesReply = 4,
    kInstallSnapshot = 5,
    kInstallSnapshotReply = 6,
    kTimeoutNow = 7,
};

void writeConfig(Writer& w, const Config& c) {
    w.ids(c.voters);
    w.ids(c.votersOutgoing);
    w.ids(c.learners);
}

Config readConfig(Reader& r) {
    Config c;
    c.voters = r.ids();
    c.votersOutgoing = r.ids();
    c.learners = r.ids();
    return c;
}

void writeEntry(Writer& w, const LogEntry& e) {
    w.u64(e.term);
    w.u64(e.index);
    w.u8(static_cast<uint8_t>(e.type));
    w.str(e.data);
}

LogEntry readEntry(Reader& r) {
    LogEntry e;
    e.term = r.u64();
    e.index = r.u64();
    e.type = static_cast<EntryType>(r.u8());
    e.data = r.str();
    return e;
}

}  // namespace

std::string encodeEnvelope(const Envelope& env) {
    Writer w;
    w.u16(env.groupId);
    w.u64(env.message.to);
    w.u64(env.message.from);

    std::visit(
        [&](const auto& p) {
            using T = std::decay_t<decltype(p)>;
            if constexpr (std::is_same_v<T, RequestVote>) {
                w.u8(kRequestVote);
                w.u8(p.preVote ? 1 : 0);
                w.u64(p.term);
                w.u64(p.candidateId);
                w.u64(p.lastLogIndex);
                w.u64(p.lastLogTerm);
            } else if constexpr (std::is_same_v<T, RequestVoteReply>) {
                w.u8(kRequestVoteReply);
                w.u8(p.preVote ? 1 : 0);
                w.u64(p.term);
                w.u8(p.voteGranted ? 1 : 0);
            } else if constexpr (std::is_same_v<T, AppendEntries>) {
                w.u8(kAppendEntries);
                w.u64(p.term);
                w.u64(p.leaderId);
                w.u64(p.prevLogIndex);
                w.u64(p.prevLogTerm);
                w.u64(p.leaderCommit);
                w.u64(p.readSeq);
                w.u32(static_cast<uint32_t>(p.entries.size()));
                for (const auto& e : p.entries)
                    writeEntry(w, e);
            } else if constexpr (std::is_same_v<T, AppendEntriesReply>) {
                w.u8(kAppendEntriesReply);
                w.u64(p.term);
                w.u8(p.success ? 1 : 0);
                w.u64(p.matchIndex);
                w.u64(p.conflictIndex);
                w.u64(p.conflictTerm);
                w.u64(p.readSeq);
            } else if constexpr (std::is_same_v<T, InstallSnapshot>) {
                w.u8(kInstallSnapshot);
                w.u64(p.term);
                w.u64(p.leaderId);
                w.u64(p.lastIncludedIndex);
                w.u64(p.lastIncludedTerm);
                writeConfig(w, p.config);
                w.str(p.data);
            } else if constexpr (std::is_same_v<T, InstallSnapshotReply>) {
                w.u8(kInstallSnapshotReply);
                w.u64(p.term);
                w.u64(p.matchIndex);
            } else if constexpr (std::is_same_v<T, TimeoutNow>) {
                w.u8(kTimeoutNow);
                w.u64(p.term);
                w.u64(p.leaderId);
            }
        },
        env.message.payload);
    return std::move(w.out);
}

std::optional<Envelope> decodeEnvelope(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    Envelope env;
    env.groupId = r.u16();
    env.message.to = r.u64();
    env.message.from = r.u64();
    const uint8_t tag = r.u8();

    switch (tag) {
        case kRequestVote: {
            RequestVote p;
            p.preVote = r.u8() != 0;
            p.term = r.u64();
            p.candidateId = r.u64();
            p.lastLogIndex = r.u64();
            p.lastLogTerm = r.u64();
            env.message.payload = p;
            break;
        }
        case kRequestVoteReply: {
            RequestVoteReply p;
            p.preVote = r.u8() != 0;
            p.term = r.u64();
            p.voteGranted = r.u8() != 0;
            env.message.payload = p;
            break;
        }
        case kAppendEntries: {
            AppendEntries p;
            p.term = r.u64();
            p.leaderId = r.u64();
            p.prevLogIndex = r.u64();
            p.prevLogTerm = r.u64();
            p.leaderCommit = r.u64();
            p.readSeq = r.u64();
            uint32_t n = r.u32();
            // Guard against a bogus count before reserving/allocating.
            if (!r.ok || !r.avail(static_cast<size_t>(n)))  // each entry is >= 1 byte
                return std::nullopt;
            for (uint32_t i = 0; i < n; ++i) {
                p.entries.push_back(readEntry(r));
                if (!r.ok)
                    return std::nullopt;
            }
            env.message.payload = std::move(p);
            break;
        }
        case kAppendEntriesReply: {
            AppendEntriesReply p;
            p.term = r.u64();
            p.success = r.u8() != 0;
            p.matchIndex = r.u64();
            p.conflictIndex = r.u64();
            p.conflictTerm = r.u64();
            p.readSeq = r.u64();
            env.message.payload = p;
            break;
        }
        case kInstallSnapshot: {
            InstallSnapshot p;
            p.term = r.u64();
            p.leaderId = r.u64();
            p.lastIncludedIndex = r.u64();
            p.lastIncludedTerm = r.u64();
            p.config = readConfig(r);
            p.data = r.str();
            env.message.payload = std::move(p);
            break;
        }
        case kInstallSnapshotReply: {
            InstallSnapshotReply p;
            p.term = r.u64();
            p.matchIndex = r.u64();
            env.message.payload = p;
            break;
        }
        case kTimeoutNow: {
            TimeoutNow p;
            p.term = r.u64();
            p.leaderId = r.u64();
            env.message.payload = p;
            break;
        }
        default:
            return std::nullopt;  // unknown tag
    }

    if (!r.ok)
        return std::nullopt;
    return env;
}

}  // namespace timestar::raft
