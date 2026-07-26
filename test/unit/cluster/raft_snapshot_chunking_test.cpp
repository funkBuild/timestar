// CHUNKED InstallSnapshot (debt D-5).
//
// Before this, one InstallSnapshot carried an ENTIRE VShard snapshot, which is why the
// Raft size chain had to be sized around "however big a VShard happens to be" (96 MiB
// send / 128 MiB admission) and why `snapshotVShard` had to refuse to COMPACT above that
// bound -- compaction discards the log prefix the snapshot replaces, so an undeliverable
// snapshot means a follower nothing can ever catch up.
//
// What these tests pin, in order of how much they matter:
//
//   1. A PARTIAL SNAPSHOT IS NEVER INSTALLED. Chunks accumulate in a staging buffer and
//      nothing observable moves -- not the log, not commitIndex, not the state machine --
//      until the chunk that COMPLETES the payload.
//   2. NOTHING ADVANCES nextIndex_ ON THE STRENGTH OF A SEND. That is the F3a discipline,
//      and chunking makes it stronger rather than weaker: the pre-D-5 code advanced
//      optimistically before the send, so a refusal became a hot loop. Now only an
//      install-outcome reply advances it, which is what makes abandoning a transfer a
//      genuine no-op.
//   3. A DROPPED CHUNK IS RECOVERED. The transport is fire-and-forget, so a lost chunk is
//      silent; the leader's stall timer resends from the offset the FOLLOWER reports, not
//      from the one the leader assumed.
//   4. A RESTARTED TRANSFER DISCARDS A STALE PARTIAL, keyed by (index, term), so one
//      snapshot's bytes can never be spliced onto another's.
//   5. MIXED VERSIONS FAIL CLOSED. A one-message snapshot still goes out under the
//      ORIGINAL wire tag (so an un-upgraded peer is caught up normally); a real chunk
//      rides a NEW tag that an old decoder drops whole rather than misparsing -- which it
//      would otherwise do, installing a prefix as if it were complete.
#include "../../../lib/cluster/raft/raft_codec.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"  // kMaxProposalBytes
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace timestar::raft;

namespace {

RaftOptions chunkedOpts(size_t chunkBytes) {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;  // deterministic; campaigns are driven explicitly
    o.heartbeatTimeout = 1;
    o.maxSnapshotChunkBytes = chunkBytes;
    o.snapshotChunkTimeout = 3;
    o.maxSnapshotResends = 4;
    return o;
}

// A deterministic two-plus-node network with PER-MESSAGE delivery control, which is what
// the drop/duplicate cases need (the shared Network in raft_cluster_test.cpp can only
// isolate a whole node).
class ChunkNet {
public:
    ChunkNet(std::vector<NodeId> voters, RaftOptions opts, std::vector<NodeId> learners = {}) : ids_(voters) {
        ids_.insert(ids_.end(), learners.begin(), learners.end());
        for (NodeId id : ids_) {
            nodes_[id] = std::make_unique<RaftNode>(id, voters, RaftLog{}, HardState{}, opts, learners);
            installed_[id] = {};
        }
    }

    RaftNode& node(NodeId id) { return *nodes_.at(id); }
    // The snapshot payloads this node's driver was told to install, in order.
    const std::vector<std::string>& installed(NodeId id) const { return installed_.at(id); }

    void isolate(NodeId id) { isolated_.insert(id); }
    void heal(NodeId id) { isolated_.erase(id); }

    // Drop the next `n` InstallSnapshot chunks addressed to `to` (simulating an
    // over-budget peer / a reset connection: no reply, no error).
    void dropChunksTo(NodeId to, unsigned n) { dropChunks_[to] = n; }

    // One round: drain every node, route, step. Returns false when quiescent.
    bool step() {
        bool progress = false;
        for (NodeId id : ids_) {
            RaftNode& n = *nodes_[id];
            if (!n.hasReady())
                continue;
            RaftNode::Ready rd = n.ready();
            n.advance(rd);
            if (rd.snapshot)
                installed_[id].push_back(rd.snapshot->data);
            for (auto& m : rd.messages) {
                if (isolated_.count(m.from) || isolated_.count(m.to))
                    continue;
                if (std::holds_alternative<InstallSnapshot>(m.payload)) {
                    auto d = dropChunks_.find(m.to);
                    if (d != dropChunks_.end() && d->second > 0) {
                        --d->second;
                        ++chunksDropped_;
                        continue;  // silence, exactly as the no_wait transport gives
                    }
                }
                inflight_.push_back(m);
            }
            progress = true;
        }
        std::deque<Message> batch;
        batch.swap(inflight_);
        for (auto& m : batch) {
            if (nodes_.count(m.to)) {
                nodes_[m.to]->step(m);
                progress = true;
            }
        }
        return progress;
    }

    void run(int maxRounds = 100000) {
        for (int i = 0; i < maxRounds && step(); ++i) {}
    }
    void tickAll() {
        for (NodeId id : ids_)
            nodes_[id]->tick();
    }
    unsigned chunksDropped() const { return chunksDropped_; }

private:
    std::vector<NodeId> ids_;
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
    std::map<NodeId, std::vector<std::string>> installed_;
    std::set<NodeId> isolated_;
    std::map<NodeId, unsigned> dropChunks_;
    unsigned chunksDropped_ = 0;
    std::deque<Message> inflight_;
};

// A snapshot payload big enough to need several chunks, and self-describing so a
// mis-assembled one is detectable rather than merely the wrong length.
std::string payloadOf(size_t bytes) {
    std::string s;
    s.reserve(bytes);
    for (size_t i = 0; i < bytes; ++i)
        s.push_back(static_cast<char>('a' + (i % 26)));
    return s;
}

template <class T>
const T* payloadIf(const Message& m) {
    return std::get_if<T>(&m.payload);
}

// Drain a node's Ready AND advance it -- `ready()` alone leaves the pending messages in
// place, so a test that only peeks sees the previous round's messages again.
RaftNode::Ready drain(RaftNode& n) {
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    return rd;
}

Envelope wrap(uint16_t gid, NodeId to, NodeId from, MessagePayload p) {
    Envelope e;
    e.groupId = gid;
    e.message.to = to;
    e.message.from = from;
    e.message.payload = std::move(p);
    return e;
}

// The message-type byte sits after the envelope header (u16 group + u64 to + u64 from).
constexpr size_t kTagOffset = 2 + 8 + 8;
uint8_t tagOf(const std::string& bytes) {
    return static_cast<uint8_t>(bytes.at(kTagOffset));
}

// A model of a decoder that predates D-5: it knows tags 1..8 and nothing else. This is
// the ONLY way to assert the mixed-version property, since the real old decoder is not in
// this tree -- and the property it proves is the reason the chunk needed a new tag at all.
bool preD5DecoderAccepts(const std::string& bytes) {
    return bytes.size() > kTagOffset && tagOf(bytes) >= 1 && tagOf(bytes) <= 8;
}

}  // namespace

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

TEST(RaftSnapshotChunkingTest, AOneMessageSnapshotStillUsesTheOriginalWireTag) {
    // THE MIXED-VERSION ESCAPE HATCH. A snapshot that fits in one chunk must be
    // byte-identical to what a pre-D-5 leader emitted, or every un-upgraded peer loses
    // snapshot catch-up the moment one node in the cluster is upgraded.
    InstallSnapshot is;
    is.term = 4;
    is.leaderId = 1;
    is.lastIncludedIndex = 9;
    is.lastIncludedTerm = 3;
    is.config.voters = {1, 2, 3};
    is.data = "whole-payload";
    // The defaults ARE the whole-payload shape, which is also what a pre-D-5 peer produces.
    ASSERT_TRUE(is.isWholePayload());
    const std::string enc = encodeEnvelope(wrap(7, 2, 1, is));
    EXPECT_EQ(tagOf(enc), 5) << "a one-message snapshot must keep the original tag";
    EXPECT_TRUE(preD5DecoderAccepts(enc));

    // ... and it decodes back into the normalized chunked representation.
    auto back = decodeEnvelope(enc);
    ASSERT_TRUE(back.has_value());
    const auto* got = std::get_if<InstallSnapshot>(&back->message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->data, "whole-payload");
    EXPECT_EQ(got->offset, 0u);
    EXPECT_EQ(got->totalBytes, is.data.size());
    EXPECT_TRUE(got->done);
}

TEST(RaftSnapshotChunkingTest, ARealChunkRidesANewTagAnOldDecoderDropsWhole) {
    // WHY THE CHUNK FIELDS COULD NOT GO INSIDE TAG 5. `data` is the last field of that
    // body and decodeEnvelope does not require every byte to be consumed, so an old
    // decoder would read the header, take `data`, ignore the offset/total/done trailer --
    // and INSTALL A PREFIX AS COMPLETE, reporting matchIndex for data it does not hold.
    // An unknown tag is dropped whole instead: slow, counted, never wrong.
    InstallSnapshot chunk;
    chunk.term = 4;
    chunk.leaderId = 1;
    chunk.lastIncludedIndex = 9;
    chunk.lastIncludedTerm = 3;
    chunk.config.voters = {1, 2, 3};
    chunk.data = "first-half";
    chunk.offset = 0;
    chunk.totalBytes = 20;
    chunk.done = false;
    ASSERT_FALSE(chunk.isWholePayload());

    const std::string enc = encodeEnvelope(wrap(7, 2, 1, chunk));
    EXPECT_EQ(tagOf(enc), 9);
    EXPECT_FALSE(preD5DecoderAccepts(enc)) << "an old peer must DROP this, not misparse it";

    auto back = decodeEnvelope(enc);
    ASSERT_TRUE(back.has_value());
    const auto* got = std::get_if<InstallSnapshot>(&back->message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->data, "first-half");
    EXPECT_EQ(got->offset, 0u);
    EXPECT_EQ(got->totalBytes, 20u);
    EXPECT_FALSE(got->done);

    // The tail chunk round-trips too, with done set by the arithmetic.
    InstallSnapshot tail = chunk;
    tail.data = "second-half";
    tail.offset = 10;
    tail.totalBytes = 21;
    tail.done = true;
    auto back2 = decodeEnvelope(encodeEnvelope(wrap(7, 2, 1, tail)));
    ASSERT_TRUE(back2.has_value());
    EXPECT_TRUE(std::get<InstallSnapshot>(back2->message.payload).done);
}

TEST(RaftSnapshotChunkingTest, AChunkThatContradictsItsOwnArithmeticIsRejected) {
    // The receiver sizes its staging from `totalBytes`, so this pair is not a hint: a peer
    // that could make offset/total/done disagree could make the receiver allocate without
    // bound, or make it INSTALL a prefix by claiming `done`.
    auto encWith = [](uint64_t offset, uint64_t total, bool done, std::string data) {
        InstallSnapshot c;
        c.term = 1;
        c.leaderId = 1;
        c.lastIncludedIndex = 5;
        c.lastIncludedTerm = 1;
        c.data = std::move(data);
        c.offset = offset;
        c.totalBytes = total;
        c.done = done;
        return encodeEnvelope(wrap(1, 2, 1, c));
    };
    // Runs past the declared payload.
    EXPECT_FALSE(decodeEnvelope(encWith(8, 10, false, "0123456789")).has_value());
    // offset beyond total.
    EXPECT_FALSE(decodeEnvelope(encWith(20, 10, false, "x")).has_value());
    // Claims completion of a prefix.
    EXPECT_FALSE(decodeEnvelope(encWith(0, 100, true, "short")).has_value());
    // Denies completion of the whole thing.
    EXPECT_FALSE(decodeEnvelope(encWith(0, 5, false, "12345")).has_value());
    // The consistent shape is accepted.
    EXPECT_TRUE(decodeEnvelope(encWith(0, 10, false, "12345")).has_value());
}

TEST(RaftSnapshotChunkingTest, AnInstallOutcomeReplyKeepsTheOriginalTagAndProgressDoesNot) {
    // The ONE reply an old leader must be able to read is the completion, and it can: both
    // progress fields sit at their defaults for an outcome, so it encodes as the original
    // two-field reply. Progress rides tag 10, which pairs exactly with the tag-9 request.
    InstallSnapshotReply outcome;
    outcome.term = 4;
    outcome.matchIndex = 42;
    ASSERT_TRUE(outcome.isInstallOutcome());
    const std::string encOutcome = encodeEnvelope(wrap(1, 1, 2, outcome));
    EXPECT_EQ(tagOf(encOutcome), 6);
    EXPECT_TRUE(preD5DecoderAccepts(encOutcome));

    InstallSnapshotReply progress;
    progress.term = 4;
    progress.matchIndex = 7;
    progress.pendingSnapshotIndex = 42;
    progress.stagedBytes = 4096;
    ASSERT_FALSE(progress.isInstallOutcome());
    const std::string encProgress = encodeEnvelope(wrap(1, 1, 2, progress));
    EXPECT_EQ(tagOf(encProgress), 10);
    EXPECT_FALSE(preD5DecoderAccepts(encProgress));

    auto back = decodeEnvelope(encProgress);
    ASSERT_TRUE(back.has_value());
    const auto& got = std::get<InstallSnapshotReply>(back->message.payload);
    EXPECT_EQ(got.pendingSnapshotIndex, 42u);
    EXPECT_EQ(got.stagedBytes, 4096u);
    EXPECT_EQ(got.matchIndex, 7u);
}

TEST(RaftSnapshotChunkingTest, AnOutcomeShapedProgressReplyIsRejected) {
    // A tag-10 reply with both progress fields at their defaults is a peer contradicting
    // itself, and admitting it would have the leader read a mid-transfer ack as a
    // completed install -- the exact confusion the two tags exist to make impossible.
    std::string bytes;
    auto u16 = [&](uint16_t v) {
        bytes.push_back(static_cast<char>(v & 0xff));
        bytes.push_back(static_cast<char>((v >> 8) & 0xff));
    };
    auto u64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i)
            bytes.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    };
    u16(1);
    u64(1);
    u64(2);
    bytes.push_back(static_cast<char>(10));  // kInstallSnapshotChunkReply
    u64(4);                                  // term
    u64(9);                                  // matchIndex
    u64(0);                                  // pendingSnapshotIndex -- outcome-shaped
    u64(0);                                  // stagedBytes
    EXPECT_FALSE(decodeEnvelope(bytes).has_value());
}

// ---------------------------------------------------------------------------
// The size chain (the payoff for D-5)
// ---------------------------------------------------------------------------

TEST(RaftSnapshotChunkingTest, TheSizeChainHoldsAndIsSizedForAnAppendNotASnapshot) {
    // The chain, as stated in raft_types.hpp: producer payload + envelope headroom <= send
    // refusal <= peer admission. A break anywhere makes a message the peer's `no_wait`
    // admission DROPS WITH NO REPLY, i.e. a silent lost replica.
    static_assert(kMaxRaftPayloadBytes + kRaftEnvelopeHeadroomBytes == kMaxRaftSendBytes);
    // A snapshot chunk is no longer the binding producer -- that is the whole point.
    static_assert(kMaxSnapshotChunkBytes < kMaxRaftPayloadBytes,
                  "a chunk must fit a message with room to spare, or chunking bought nothing");
    static_assert(RaftGroup::kMaxProposalBytes == kMaxRaftPayloadBytes,
                  "the largest log entry is what the chain is now sized for");
    // A chunk must be much smaller than the entry bound, or the chain is still being
    // dragged up by snapshots.
    static_assert(kMaxSnapshotChunkBytes * 4 <= kMaxRaftPayloadBytes);
    // The compaction/memory ceiling is a MULTIPLE of a chunk (so a big snapshot ships as
    // a pipeline) and is well ABOVE the old effective ceiling (so compaction that used to
    // be refused now runs).
    static_assert(kMaxVShardSnapshotBytes / kMaxSnapshotChunkBytes >= 8);
    static_assert(kMaxVShardSnapshotBytes > kMaxRaftPayloadBytes,
                  "the snapshot ceiling must have RISEN above the per-message bound it replaced");
}

// ---------------------------------------------------------------------------
// Receiver: staging, atomicity, resumability
// ---------------------------------------------------------------------------

TEST(RaftSnapshotChunkingTest, APartialSnapshotIsStagedAndNeverInstalled) {
    // INVARIANT 1. Nothing observable may move until the payload is complete.
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, chunkedOpts(4));
    const std::string full = payloadOf(10);

    auto chunkAt = [&](uint64_t off, size_t len) {
        InstallSnapshot is;
        is.term = 2;
        is.leaderId = 2;
        is.lastIncludedIndex = 5;
        is.lastIncludedTerm = 2;
        is.config.voters = {1, 2, 3};
        is.data = full.substr(off, len);
        is.offset = off;
        is.totalBytes = full.size();
        is.done = (off + len == full.size());
        return Message{.to = 1, .from = 2, .payload = is};
    };

    n.step(chunkAt(0, 4));
    EXPECT_EQ(n.stagedSnapshotBytes(), 4u);
    EXPECT_EQ(n.commitIndex(), kNoIndex) << "commit must not move on a partial snapshot";
    EXPECT_EQ(n.log().snapshotIndex(), kNoIndex);
    EXPECT_EQ(n.snapshotsInstalled(), 0u);
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    EXPECT_FALSE(rd.snapshot.has_value()) << "no snapshot may be surfaced to the driver yet";
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* rep = std::get_if<InstallSnapshotReply>(&rd.messages[0].payload);
    ASSERT_NE(rep, nullptr);
    EXPECT_EQ(rep->pendingSnapshotIndex, 5u);
    EXPECT_EQ(rep->stagedBytes, 4u);
    EXPECT_FALSE(rep->isInstallOutcome());

    n.step(chunkAt(4, 4));
    EXPECT_EQ(n.stagedSnapshotBytes(), 8u);
    EXPECT_EQ(n.snapshotsInstalled(), 0u);
    rd = n.ready();
    n.advance(rd);
    EXPECT_FALSE(rd.snapshot.has_value());

    // The FINAL chunk, and only it, installs.
    n.step(chunkAt(8, 2));
    EXPECT_EQ(n.stagedSnapshotBytes(), 0u) << "staging is released on install";
    EXPECT_EQ(n.snapshotsInstalled(), 1u);
    EXPECT_EQ(n.commitIndex(), 5u);
    EXPECT_EQ(n.log().snapshotIndex(), 5u);
    rd = n.ready();
    n.advance(rd);
    ASSERT_TRUE(rd.snapshot.has_value());
    EXPECT_EQ(rd.snapshot->data, full) << "the installed payload must be the whole thing, in order";
    ASSERT_FALSE(rd.messages.empty());
    const auto* done = std::get_if<InstallSnapshotReply>(&rd.messages.back().payload);
    ASSERT_NE(done, nullptr);
    EXPECT_TRUE(done->isInstallOutcome());
    EXPECT_EQ(done->matchIndex, 5u);
}

TEST(RaftSnapshotChunkingTest, AChunkForADifferentBoundaryDiscardsTheStagedPartial) {
    // INVARIANT 4. Reachable without any malice: the leader compacts again mid-transfer,
    // or leadership moves and the new leader's boundary differs. Splicing the two payloads
    // would install a snapshot that never existed anywhere.
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, chunkedOpts(4));
    const std::string a = payloadOf(8);
    const std::string b = payloadOf(4);

    InstallSnapshot c1;
    c1.term = 2;
    c1.leaderId = 2;
    c1.lastIncludedIndex = 5;
    c1.lastIncludedTerm = 2;
    c1.data = a.substr(0, 4);
    c1.offset = 0;
    c1.totalBytes = a.size();
    c1.done = false;
    n.step(Message{.to = 1, .from = 2, .payload = c1});
    ASSERT_EQ(n.stagedSnapshotBytes(), 4u);
    drain(n);

    // A DIFFERENT snapshot (higher boundary), whole in one chunk.
    InstallSnapshot other;
    other.term = 2;
    other.leaderId = 2;
    other.lastIncludedIndex = 9;
    other.lastIncludedTerm = 2;
    other.config.voters = {1, 2, 3};
    other.data = b;
    other.offset = 0;
    other.totalBytes = b.size();
    other.done = true;
    n.step(Message{.to = 1, .from = 2, .payload = other});
    EXPECT_EQ(n.snapshotsInstalled(), 1u);
    EXPECT_EQ(n.log().snapshotIndex(), 9u);
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    ASSERT_TRUE(rd.snapshot.has_value());
    EXPECT_EQ(rd.snapshot->data, b) << "the first snapshot's staged prefix must not be spliced in";
}

TEST(RaftSnapshotChunkingTest, ARestartedTransferAndADuplicateChunkAreBothIdempotent) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, chunkedOpts(4));
    const std::string full = payloadOf(12);
    auto chunkAt = [&](uint64_t off, size_t len) {
        InstallSnapshot is;
        is.term = 2;
        is.leaderId = 2;
        is.lastIncludedIndex = 5;
        is.lastIncludedTerm = 2;
        is.config.voters = {1, 2, 3};
        is.data = full.substr(off, len);
        is.offset = off;
        is.totalBytes = full.size();
        is.done = (off + len == full.size());
        return Message{.to = 1, .from = 2, .payload = is};
    };

    n.step(chunkAt(0, 4));
    n.step(chunkAt(4, 4));
    ASSERT_EQ(n.stagedSnapshotBytes(), 8u);
    drain(n);

    // A DUPLICATE of an already-staged chunk: not applied (the staged prefix is the only
    // state), and answered with where we really are so the leader resumes from the truth.
    n.step(chunkAt(4, 4));
    EXPECT_EQ(n.stagedSnapshotBytes(), 8u) << "a duplicate must not extend the buffer";
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* rep = std::get_if<InstallSnapshotReply>(&rd.messages[0].payload);
    ASSERT_NE(rep, nullptr);
    EXPECT_EQ(rep->stagedBytes, 8u);

    // A chunk from the FUTURE (a gap) is likewise refused and answered with our offset.
    n.step(chunkAt(8, 4));  // ... this one is contiguous, so accept it
    EXPECT_EQ(n.snapshotsInstalled(), 1u);
    drain(n);

    // And a RESTART -- offset 0 again -- discards whatever was staged and starts clean.
    RaftNode m(2, {1, 2, 3}, RaftLog{}, HardState{}, chunkedOpts(4));
    m.step(chunkAt(0, 4));
    m.step(chunkAt(4, 4));
    ASSERT_EQ(m.stagedSnapshotBytes(), 8u);
    drain(m);
    m.step(chunkAt(0, 4));
    EXPECT_EQ(m.stagedSnapshotBytes(), 4u) << "chunk 0 restarts the transfer";
    m.step(chunkAt(4, 4));
    m.step(chunkAt(8, 4));
    EXPECT_EQ(m.snapshotsInstalled(), 1u);
    RaftNode::Ready rd2 = m.ready();
    m.advance(rd2);
    ASSERT_TRUE(rd2.snapshot.has_value());
    EXPECT_EQ(rd2.snapshot->data, full);
}

TEST(RaftSnapshotChunkingTest, AGapChunkWithNothingStagedAsksForTheTransferFromZero) {
    // The follower restarted (its staging is deliberately volatile) or the leader's first
    // chunks were dropped. Either way it must say "I have nothing", not stage a hole.
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, chunkedOpts(4));
    InstallSnapshot mid;
    mid.term = 2;
    mid.leaderId = 2;
    mid.lastIncludedIndex = 5;
    mid.lastIncludedTerm = 2;
    mid.data = "mid!";
    mid.offset = 8;
    mid.totalBytes = 16;
    mid.done = false;
    n.step(Message{.to = 1, .from = 2, .payload = mid});
    EXPECT_EQ(n.stagedSnapshotBytes(), 0u);
    EXPECT_EQ(n.snapshotsInstalled(), 0u);
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* rep = std::get_if<InstallSnapshotReply>(&rd.messages[0].payload);
    ASSERT_NE(rep, nullptr);
    EXPECT_EQ(rep->pendingSnapshotIndex, 5u);
    EXPECT_EQ(rep->stagedBytes, 0u);
}

TEST(RaftSnapshotChunkingTest, AReceiverRefusesToStageMoreThanItsMemoryBound) {
    // Defence in depth: a correctly configured leader refuses to BUILD such a snapshot, so
    // reaching this needs a mismatched or hostile peer. Refuse and COUNT rather than
    // allocate on a stranger's say-so.
    RaftOptions o = chunkedOpts(4);
    o.maxSnapshotBytes = 16;
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, o);
    InstallSnapshot big;
    big.term = 2;
    big.leaderId = 2;
    big.lastIncludedIndex = 5;
    big.lastIncludedTerm = 2;
    big.data = "abcd";
    big.offset = 0;
    big.totalBytes = 1024;  // over the bound
    big.done = false;
    n.step(Message{.to = 1, .from = 2, .payload = big});
    EXPECT_EQ(n.snapshotsRefusedTooLarge(), 1u);
    EXPECT_EQ(n.stagedSnapshotBytes(), 0u);
    EXPECT_EQ(n.snapshotsInstalled(), 0u);
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* rep = std::get_if<InstallSnapshotReply>(&rd.messages[0].payload);
    ASSERT_NE(rep, nullptr);
    EXPECT_TRUE(rep->isInstallOutcome()) << "nothing was installed AND nothing is pending";
}

// ---------------------------------------------------------------------------
// Leader: pacing, nextIndex discipline, stall recovery
// ---------------------------------------------------------------------------

TEST(RaftSnapshotChunkingTest, ALaggingFollowerIsCaughtUpByAPacedChunkPipeline) {
    ChunkNet net({1, 2, 3}, chunkedOpts(8));
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());

    net.isolate(3);
    for (int i = 0; i < 5; ++i)
        net.node(1).propose("entry");
    net.run();
    const LogIndex commit = net.node(1).commitIndex();
    ASSERT_GT(commit, 0u);

    // Compact past node 3's position: the entries it needs no longer exist as log
    // entries, so only a snapshot can catch it up.
    const std::string payload = payloadOf(50);  // 7 chunks at 8 bytes
    net.node(1).compact(commit, payload);
    ASSERT_GT(net.node(1).log().snapshotIndex(), 0u);

    net.heal(3);
    net.tickAll();
    net.run();

    ASSERT_EQ(net.installed(3).size(), 1u);
    EXPECT_EQ(net.installed(3)[0], payload);
    EXPECT_EQ(net.node(3).snapshotsInstalled(), 1u);
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());
    // PACED, not blasted: 50 bytes at 8 bytes per chunk is 7 chunks, and the leader sends
    // them one at a time (each released by the previous one's ack). Anything close to 1
    // would mean it was not chunked; anything much above 7 would mean chunk 0 was being
    // re-blasted by the heartbeat.
    EXPECT_GE(net.node(1).snapshotChunksSent(), 7u);
    EXPECT_LE(net.node(1).snapshotChunksSent(), 12u);
    EXPECT_FALSE(net.node(1).snapshotTransferInFlight(3)) << "the transfer must be retired on completion";
    EXPECT_EQ(net.node(1).snapshotTransfersAbandoned(), 0u);

    // Normal replication resumes on top of the installed snapshot.
    net.node(1).propose("after");
    net.run();
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftSnapshotChunkingTest, TheHeartbeatDoesNotRestartATransferAndTheStallTimerResendsExactlyOnce) {
    // FLOW CONTROL. Without the in-flight guard, every heartbeat's bcastAppend ->
    // sendAppend -> sendInstallSnapshot would resend chunk 0 and no snapshot would EVER
    // complete: the leader would blast the same first chunk at heartbeat cadence forever.
    // With it, exactly ONE chunk is unacked per peer, and the only thing that may add
    // another is either the peer's ack (progress) or the stall timer (recovery).
    ChunkNet net({1, 2, 3}, chunkedOpts(8));
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    net.node(1).compact(net.node(1).commitIndex(), payloadOf(80));

    net.heal(3);
    net.tickAll();  // the heartbeat starts the transfer
    net.step();     // ... and the first chunk leaves
    const uint64_t afterFirst = net.node(1).snapshotChunksSent();
    ASSERT_GE(afterFirst, 1u);
    ASSERT_TRUE(net.node(1).snapshotTransferInFlight(3));

    // Cut node 3 off so its ack never returns: from here the leader is in the "silence"
    // case, which on a fire-and-forget transport is indistinguishable from a lost chunk.
    net.isolate(3);
    // WITHIN the stall window (snapshotChunkTimeout == 3), heartbeats add nothing.
    net.node(1).tick();
    net.node(1).tick();
    net.step();
    EXPECT_EQ(net.node(1).snapshotChunksSent(), afterFirst)
        << "the heartbeat must not re-blast a chunk while one is unacked";
    EXPECT_EQ(net.node(1).snapshotTransfersRestarted(), 0u);

    // PAST the window, exactly one resend -- not one per heartbeat.
    net.node(1).tick();
    net.step();
    EXPECT_EQ(net.node(1).snapshotChunksSent(), afterFirst + 1);
    EXPECT_EQ(net.node(1).snapshotTransfersRestarted(), 1u);
    net.node(1).tick();
    net.node(1).tick();
    net.step();
    EXPECT_EQ(net.node(1).snapshotChunksSent(), afterFirst + 1) << "the window restarts after a resend";
}

TEST(RaftSnapshotChunkingTest, AFollowerBeingFedChunksDoesNotCampaignAgainstItsLeader) {
    // A CONSEQUENCE OF CHUNKING THAT IS EASY TO MISS. While a transfer is in flight, that
    // peer is served snapshot CHUNKS INSTEAD OF HEARTBEATS -- sendAppend hands off to
    // sendInstallSnapshot, which is a no-op while a chunk is unacked. So the chunk (and,
    // if one is lost, its resend) is the ONLY thing resetting that follower's election
    // clock, and a transfer slower than an election timeout would have the follower
    // campaign against a perfectly healthy leader mid-install.
    //
    // What makes it safe is the ORDERING of three timeouts, which production wires as
    // heartbeat 25 < snapshotChunkTimeout 50 << electionTimeoutMin 125 ticks: a chunk or a
    // resend always arrives well inside the election window. This test asserts the
    // property rather than the numbers -- a transfer that spans MANY election timeouts'
    // worth of ticks must leave the leader in place.
    RaftOptions o = chunkedOpts(4);
    o.electionTimeoutMin = 6;
    o.electionTimeoutMax = 6;
    o.snapshotChunkTimeout = 2;  // resend well inside the election window
    ChunkNet net({1, 2, 3}, o);
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    const std::string payload = payloadOf(200);  // 50 chunks: far more ticks than one timeout
    net.node(1).compact(net.node(1).commitIndex(), payload);

    net.heal(3);
    for (int round = 0; round < 200; ++round) {
        net.tickAll();
        net.run();
        if (!net.installed(3).empty())
            break;
    }
    ASSERT_EQ(net.installed(3).size(), 1u);
    EXPECT_EQ(net.installed(3)[0], payload);
    EXPECT_TRUE(net.node(1).isLeader()) << "the install must not cost the leader its term";
    EXPECT_EQ(net.node(1).currentTerm(), 1u) << "nobody may have campaigned during the transfer";
}

TEST(RaftSnapshotChunkingTest, ADroppedChunkIsResentFromWhereTheFollowerActuallyIs) {
    // INVARIANT 3. The transport is fire-and-forget: a dropped chunk produces no reply and
    // no error, and because only one chunk is in flight, silence means STOPPED. Only the
    // stall timer notices, and it must resume from the follower's reported offset.
    ChunkNet net({1, 2, 3}, chunkedOpts(8));
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    const std::string payload = payloadOf(40);  // 5 chunks
    net.node(1).compact(net.node(1).commitIndex(), payload);

    net.heal(3);
    net.dropChunksTo(3, 2);  // the first two chunks vanish
    net.tickAll();
    // Drive several stall windows: each resend is one tick sweep away.
    for (int round = 0; round < 40; ++round) {
        net.run();
        net.tickAll();
    }
    net.run();

    EXPECT_GT(net.chunksDropped(), 0u) << "the test must actually have dropped something";
    ASSERT_EQ(net.installed(3).size(), 1u);
    EXPECT_EQ(net.installed(3)[0], payload) << "the recovered transfer must assemble the SAME bytes";
    EXPECT_GT(net.node(1).snapshotTransfersRestarted(), 0u) << "the stall timer must have fired";
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftSnapshotChunkingTest, NothingAdvancesNextIndexOnASendAndAbandonmentIsANoOp) {
    // INVARIANT 2, and the F3a discipline made stronger. The pre-D-5 code advanced
    // nextIndex_ BEFORE the send, so a refused or lost send became a hot loop: advance ->
    // follower rejects the next append -> rewind -> re-encode the whole snapshot -> refuse.
    // Now only an install OUTCOME advances it, so a transfer that is abandoned leaves the
    // peer exactly as far behind as it truly is -- observable here because once the peer is
    // reachable again the leader still reaches for a SNAPSHOT and the install completes,
    // which is only possible if nextIndex_ never moved past the boundary.
    // Node 3 is a LEARNER here, for one reason: a learner never campaigns. A VOTER cut off
    // for 200 tick rounds bumps its own term ~20 times, and after healing the resulting
    // election can hand leadership to node 2 -- which never compacted and so catches node 3
    // up with ordinary appends. That would make this test about elections rather than about
    // nextIndex discipline. A lagging learner is also the realistic shape (a joining node).
    ChunkNet net({1, 2}, chunkedOpts(8), {3});
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    const LogIndex boundary = net.node(1).commitIndex();
    const std::string payload = payloadOf(80);
    net.node(1).compact(boundary, payload);
    const LogIndex matchBefore = net.node(1).matchIndexOf(3);
    ASSERT_LT(matchBefore, boundary) << "node 3 must really be behind the boundary";

    // Node 3 stays unreachable for long enough that transfers are started and given up on
    // repeatedly. (It is ISOLATED rather than merely chunk-dropped so it cannot campaign;
    // a follower that hears NOTHING campaigning is correct Raft behaviour and would just
    // make this test about elections.)
    for (int round = 0; round < 200; ++round) {
        net.tickAll();
        net.run();
    }
    EXPECT_GT(net.node(1).snapshotTransfersAbandoned(), 0u) << "a hopeless transfer must be given up on";
    EXPECT_EQ(net.installed(3).size(), 0u);
    EXPECT_EQ(net.node(1).matchIndexOf(3), matchBefore) << "no send may be recorded as replication";
    EXPECT_LT(net.node(1).matchIndexOf(3), boundary);

    // Reachable again: the leader still chooses a snapshot and the install completes.
    net.heal(3);
    for (int round = 0; round < 200; ++round) {
        net.tickAll();
        net.run();
        if (!net.installed(3).empty())
            break;
    }
    ASSERT_EQ(net.installed(3).size(), 1u) << "once the peer is reachable the transfer completes";
    EXPECT_EQ(net.installed(3)[0], payload);
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftSnapshotChunkingTest, AFollowerThatLOSTItsStagingRestartsTheTransferInsteadOfLivelocking) {
    // REVIEW F1, and it is the case volatile staging exists FOR. A follower that restarts
    // mid-transfer comes back with nothing staged while the LEADER's SnapshotTransfer
    // survives. The leader sends its remembered offset, the follower takes the "chunk from
    // the middle with nothing staged" branch and answers `stagedBytes = 0` -- and the
    // leader must believe it.
    //
    // It used to not: `if (rr.stagedBytes > t.acked)` discarded the backwards report as
    // non-monotonic, kept the stale offset, and resent it -- while every reply reset
    // `idleTicks`, so the tick sweep never fired either. One 4 MiB chunk per round trip,
    // forever, no restart counted, no abandonment, and no other way for that follower to
    // catch up because its log prefix is gone. An IMMORTAL LIVELOCK on the recovery path.
    //
    // This drives the leader by hand rather than through the network harness, because the
    // point is precisely a follower whose state does NOT match what the leader remembers.
    RaftOptions o = chunkedOpts(8);
    o.maxSnapshotResends = 3;
    RaftNode leader(1, {1, 2, 3}, RaftLog{}, HardState{}, o);
    leader.campaign();
    drain(leader);
    // Give the other two voters' grants so it really becomes leader.
    for (NodeId v : {2u, 3u}) {
        RequestVoteReply g;
        g.term = leader.currentTerm();
        g.voteGranted = true;
        leader.step(Message{.to = 1, .from = v, .payload = g});
    }
    drain(leader);
    ASSERT_TRUE(leader.isLeader());
    // Ack the term-start no-op from BOTH followers, so node 3's nextIndex sits just above
    // it; then commit three more entries on node 2's ack ALONE, so node 3 falls behind.
    auto ackAppend = [&](NodeId v, LogIndex match) {
        AppendEntriesReply a;
        a.term = leader.currentTerm();
        a.success = true;
        a.matchIndex = match;
        leader.step(Message{.to = 1, .from = v, .payload = a});
    };
    ackAppend(2, leader.log().lastIndex());
    ackAppend(3, leader.log().lastIndex());
    drain(leader);
    for (const char* c : {"a", "b", "c"})
        leader.propose(c);
    drain(leader);
    ackAppend(2, leader.log().lastIndex());  // quorum without node 3
    drain(leader);
    ASSERT_GE(leader.commitIndex(), 4u);

    // Compact PAST node 3's position, so only a snapshot can catch it up.
    const std::string payload = payloadOf(40);  // 5 chunks at 8 bytes
    leader.compact(leader.commitIndex(), payload);
    ASSERT_GT(leader.log().snapshotIndex(), 1u);
    const LogIndex boundary = leader.log().snapshotIndex();
    const Term boundaryTerm = leader.log().snapshotTerm();

    // Start the transfer and walk it forward to a NONZERO offset by acking normally.
    leader.tick();
    drain(leader);
    auto ackProgress = [&](uint64_t staged) {
        InstallSnapshotReply r;
        r.term = leader.currentTerm();
        r.matchIndex = 0;
        r.pendingSnapshotIndex = boundary;
        r.stagedBytes = staged;
        leader.step(Message{.to = 1, .from = 3, .payload = r});
        return drain(leader);
    };
    ackProgress(8);
    ackProgress(16);
    RaftNode::Ready rd = ackProgress(24);
    const InstallSnapshot* sent = nullptr;
    for (const auto& m : rd.messages)
        if (m.to == 3 && (sent = payloadIf<InstallSnapshot>(m)))
            break;
    ASSERT_NE(sent, nullptr);
    ASSERT_EQ(sent->offset, 24u) << "the transfer must really be at a nonzero offset";
    ASSERT_TRUE(leader.snapshotTransferInFlight(3));
    (void)boundaryTerm;

    // NOW THE FOLLOWER RESTARTS: nothing staged, so it reports zero.
    rd = ackProgress(0);
    sent = nullptr;
    for (const auto& m : rd.messages)
        if (m.to == 3 && (sent = payloadIf<InstallSnapshot>(m)))
            break;
    ASSERT_NE(sent, nullptr);
    EXPECT_EQ(sent->offset, 0u) << "the leader must RESTART from where the follower says it is, not from its own "
                                   "stale idea of it -- pinning the offset here is the livelock";
    EXPECT_EQ(leader.snapshotTransfersRestarted(), 1u) << "a no-progress reply must be COUNTED, not silently absorbed";

    // And a peer that keeps reporting no progress must eventually be ABANDONED rather than
    // fed one chunk per round trip forever. Every reply resets idleTicks, so the tick sweep
    // cannot be what catches this -- the no-progress budget has to.
    for (int i = 0; i < 10 && leader.snapshotTransferInFlight(3); ++i)
        ackProgress(0);
    EXPECT_FALSE(leader.snapshotTransferInFlight(3)) << "an answering-but-never-progressing peer must be given up on";
    EXPECT_GE(leader.snapshotTransfersAbandoned(), 1u);
    // Abandonment is a no-op on progress, as ever: the peer is left exactly as far behind
    // as it truly is (it acked the term-start no-op at index 1 and nothing since).
    EXPECT_LT(leader.matchIndexOf(3), boundary) << "no send may be recorded as replication";
}

TEST(RaftSnapshotChunkingTest, RecompactingInvalidatesEveryInFlightTransfer) {
    // REVIEW F3. `compact` REPLACES `snapshot_.data` in place, so a transfer mid-way
    // through the old payload would continue at its old offset into the new one. At an
    // UNCHANGED (index, term) -- which a group with a stalled flush watermark produces on
    // every sweep -- the reply path's "our snapshot moved on" guard does not fire either,
    // so the follower would be handed a SPLICE of two snapshots and, if the lengths lined
    // up, would install it as valid.
    ChunkNet net({1, 2, 3}, chunkedOpts(8));
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    net.node(1).compact(net.node(1).commitIndex(), payloadOf(80));
    net.heal(3);
    net.tickAll();
    net.step();
    ASSERT_TRUE(net.node(1).snapshotTransferInFlight(3));

    // Re-compact at the SAME boundary with DIFFERENT bytes: the transfer must be dropped,
    // not continued into the new payload.
    net.node(1).compact(net.node(1).commitIndex(), payloadOf(80));
    EXPECT_FALSE(net.node(1).snapshotTransferInFlight(3));

    // ... and the follower still ends up with a WHOLE, consistent payload.
    const std::string finalPayload = payloadOf(96);
    net.node(1).compact(net.node(1).commitIndex(), finalPayload);
    for (int round = 0; round < 200 && net.installed(3).empty(); ++round) {
        net.tickAll();
        net.run();
    }
    ASSERT_EQ(net.installed(3).size(), 1u);
    EXPECT_EQ(net.installed(3)[0], finalPayload) << "never a splice of two snapshots";
}

TEST(RaftSnapshotChunkingTest, ASnapshotOverTheTotalBoundIsRefusedWithoutTouchingProgress) {
    // The F3c/F3a refusal SURVIVES chunking; only its reason changed (memory, not message
    // size -- see kMaxVShardSnapshotBytes). It must not build the message, must not create
    // transfer state, and must not move nextIndex_.
    RaftOptions o = chunkedOpts(8);
    o.maxSnapshotBytes = 32;
    ChunkNet net({1, 2, 3}, o);
    net.node(1).campaign();
    net.run();
    ASSERT_TRUE(net.node(1).isLeader());
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    const LogIndex boundary = net.node(1).commitIndex();
    const LogIndex matchBefore = net.node(1).matchIndexOf(3);
    net.node(1).compact(boundary, payloadOf(200));  // over the bound

    net.heal(3);
    net.tickAll();
    net.run();
    EXPECT_GT(net.node(1).undeliverableSnapshots(), 0u);
    EXPECT_EQ(net.node(1).snapshotChunksSent(), 0u) << "the doomed message must never be built";
    EXPECT_FALSE(net.node(1).snapshotTransferInFlight(3));
    EXPECT_EQ(net.node(1).matchIndexOf(3), matchBefore) << "a refusal must not move progress";
    EXPECT_LT(net.node(1).matchIndexOf(3), boundary);
    EXPECT_EQ(net.installed(3).size(), 0u);
}

TEST(RaftSnapshotChunkingTest, ChunkingDisabledKeepsThePreD5SingleMessageBehaviour) {
    // Chunking off is what a peer predating tag 9 gets, and what the core's other unit
    // tests run under. One message, the original tag, and the ORIGINAL maxMessageBytes
    // refusal applied to the whole payload.
    ChunkNet net({1, 2, 3}, chunkedOpts(0));
    net.node(1).campaign();
    net.run();
    net.isolate(3);
    net.node(1).propose("x");
    net.run();
    const std::string payload = payloadOf(100);
    net.node(1).compact(net.node(1).commitIndex(), payload);
    net.heal(3);
    net.tickAll();
    net.run();
    ASSERT_EQ(net.installed(3).size(), 1u);
    EXPECT_EQ(net.installed(3)[0], payload);
    EXPECT_EQ(net.node(1).snapshotChunksSent(), 1u) << "unchunked == exactly one message";

    // ... and the whole-payload refusal still fires there.
    RaftOptions o = chunkedOpts(0);
    o.maxMessageBytes = 10;
    ChunkNet tight({1, 2, 3}, o);
    tight.node(1).campaign();
    tight.run();
    tight.isolate(3);
    tight.node(1).propose("x");
    tight.run();
    tight.node(1).compact(tight.node(1).commitIndex(), payloadOf(100));
    tight.heal(3);
    tight.tickAll();
    tight.run();
    EXPECT_GT(tight.node(1).undeliverableSnapshots(), 0u);
    EXPECT_EQ(tight.node(1).snapshotChunksSent(), 0u);
}
