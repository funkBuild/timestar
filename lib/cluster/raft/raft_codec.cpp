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
    // A RequestVote carrying campaignTransfer (ADR 0005). SAME BODY LAYOUT as
    // kRequestVote -- only the type byte differs -- and that is the entire mixed-version
    // mechanism: this envelope has NO version field (it never had one, the transport had
    // one verb and one format), so adding a byte INSIDE kRequestVote would make an old
    // decoder mis-frame every field after it and produce a valid-LOOKING vote request
    // with a garbage term and log index. An unknown TYPE, by contrast, falls into
    // decodeEnvelope's `default:` and the envelope is dropped whole.
    //
    // So an old voter FAILS CLOSED: it never sees the transfer vote, never votes, and
    // the transfer degrades to exactly the pre-bypass behaviour (the outgoing leader
    // abandons the transfer after one election timeout -- RaftNode::tick -- and the
    // group elects normally). Slow, not wedged, and never a misparse.
    //
    // What is NOT covered here is mechanism (c) of the ADR: gating activation on the
    // cluster-wide committed format version, so CheckQuorum itself stays off until every
    // voter can read this tag. Until that lands, a MIXED-VERSION cluster running with
    // CheckQuorum on gets slow transfers during the rolling window (see the register row
    // for D-9).
    kRequestVoteTransfer = 8,
    // A CHUNK of an InstallSnapshot, and its progress reply (debt D-5). Same reasoning as
    // tag 8, and here it is not a nicety but the whole mixed-version story.
    //
    // The chunk fields could NOT be appended inside kInstallSnapshot's body. `data` is the
    // last field of that body and `decodeEnvelope` does not require the reader to have
    // consumed every byte, so an old decoder handed a chunked message would parse the
    // header, read `data`, IGNORE the offset/total/done trailer entirely -- and install a
    // PARTIAL snapshot as if it were complete. That is state corruption, not a misparse:
    // the follower would report `matchIndex == lastIncludedIndex` for data it does not
    // have. An unknown TYPE instead falls into `decodeEnvelope`'s `default:` and the
    // envelope is dropped whole.
    //
    // So an old follower FAILS CLOSED on a chunked transfer: it never sees the chunk,
    // never replies, and the leader's stall timer restarts the transfer forever (visibly
    // -- `snapshotTransfersRestarted`) while the follower stays uncaught-up. Slow and
    // counted, never wrong.
    //
    // AND THE COMMON CASE STILL INTEROPERATES, which is the point of `isWholePayload()`:
    // a snapshot that fits in ONE chunk is emitted under the OLD tag with the OLD body,
    // byte-for-byte, so an un-upgraded peer is caught up normally. Only a genuinely
    // multi-chunk transfer needs a peer that knows tag 9.
    //
    // The reply is tagged in the same spirit, and the pairing is exact: a tag-9 request
    // is answered with tag 10, a tag-5 request with tag 6. A completed install always
    // answers under tag 6 (both progress fields are at their defaults), so the ONE reply
    // an old leader must be able to read is the one it can.
    kInstallSnapshotChunk = 9,
    kInstallSnapshotChunkReply = 10,
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
                // The transfer flag is carried by the TYPE, not by an extra field: same
                // body, different tag (see kRequestVoteTransfer). A node only ever emits
                // the new tag when it is campaigning FROM a TimeoutNow, so an old peer
                // sees the new byte only in exactly the case the bypass exists for.
                w.u8(p.campaignTransfer ? kRequestVoteTransfer : kRequestVote);
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
                // A one-message snapshot keeps the ORIGINAL tag and body so an
                // un-upgraded peer can still install it; only a real chunk needs tag 9.
                const bool whole = p.isWholePayload();
                w.u8(whole ? kInstallSnapshot : kInstallSnapshotChunk);
                w.u64(p.term);
                w.u64(p.leaderId);
                w.u64(p.lastIncludedIndex);
                w.u64(p.lastIncludedTerm);
                writeConfig(w, p.config);
                w.str(p.data);
                if (!whole) {
                    w.u64(p.offset);
                    w.u64(p.totalBytes);
                    w.u8(p.done ? 1 : 0);
                }
            } else if constexpr (std::is_same_v<T, InstallSnapshotReply>) {
                // An install OUTCOME (or a stale-snapshot answer) is the original
                // two-field reply, which an old leader reads; only mid-transfer progress
                // needs tag 10.
                const bool outcome = p.isInstallOutcome();
                w.u8(outcome ? kInstallSnapshotReply : kInstallSnapshotChunkReply);
                w.u64(p.term);
                w.u64(p.matchIndex);
                if (!outcome) {
                    w.u64(p.pendingSnapshotIndex);
                    w.u64(p.stagedBytes);
                }
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
        case kRequestVote:
        case kRequestVoteTransfer: {
            RequestVote p;
            const bool preVoteByte = r.u8() != 0;
            // A TRANSFER VOTE IS NEVER A PREVOTE, whatever the byte says. The transfer
            // campaign enters becomeCandidate() directly (it must: a straw poll would
            // reset the disruption guard problem one level down), so the only shape that
            // can legitimately carry this tag is a real vote. Forcing it here keeps the
            // lease bypass confined to the real-vote path even if a peer -- buggy or
            // hostile -- sets both bits.
            p.preVote = (tag == kRequestVoteTransfer) ? false : preVoteByte;
            p.campaignTransfer = (tag == kRequestVoteTransfer);
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
        case kInstallSnapshot:
        case kInstallSnapshotChunk: {
            InstallSnapshot p;
            p.term = r.u64();
            p.leaderId = r.u64();
            p.lastIncludedIndex = r.u64();
            p.lastIncludedTerm = r.u64();
            p.config = readConfig(r);
            p.data = r.str();
            if (tag == kInstallSnapshotChunk) {
                p.offset = r.u64();
                p.totalBytes = r.u64();
                p.done = r.u8() != 0;
                // A chunk that claims to run past the payload it declares is malformed:
                // the receiver sizes its staging buffer from `totalBytes`, so trusting
                // this pair unchecked is how a hostile peer would make it allocate
                // without bound. Reject the envelope rather than clamp -- a chunked
                // transfer that has to be guessed at is one the leader should restart.
                if (p.totalBytes < p.offset || p.data.size() > p.totalBytes - p.offset)
                    return std::nullopt;
                // `done` must agree with the arithmetic, for the same reason: it is what
                // triggers the INSTALL, and a peer must not be able to say "complete" of
                // a prefix.
                if (p.done != (p.offset + p.data.size() == p.totalBytes))
                    return std::nullopt;
            } else {
                // The one-message shape: normalize to the chunked representation so the
                // node only ever handles one form.
                p.offset = 0;
                p.totalBytes = p.data.size();
                p.done = true;
            }
            env.message.payload = std::move(p);
            break;
        }
        case kInstallSnapshotReply:
        case kInstallSnapshotChunkReply: {
            InstallSnapshotReply p;
            p.term = r.u64();
            p.matchIndex = r.u64();
            if (tag == kInstallSnapshotChunkReply) {
                p.pendingSnapshotIndex = r.u64();
                p.stagedBytes = r.u64();
                // Tag 10 exists ONLY to carry progress; an "outcome-shaped" tag-10 reply
                // is a peer contradicting itself, and letting it through would have the
                // leader treat a mid-transfer ack as a completed install.
                if (p.isInstallOutcome())
                    return std::nullopt;
            }
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
