#include "../../../lib/cluster/raft/raft_codec.hpp"

#include "../../../lib/cluster/raft/raft_group.hpp"  // kMaxProposalBytes

#include <gtest/gtest.h>

using namespace timestar::raft;

namespace {

Envelope roundTrip(const Envelope& e) {
    auto decoded = decodeEnvelope(encodeEnvelope(e));
    EXPECT_TRUE(decoded.has_value());
    return decoded.value_or(Envelope{});
}

Envelope wrap(uint16_t group, NodeId to, NodeId from, MessagePayload p) {
    Envelope e;
    e.groupId = group;
    e.message = Message{.to = to, .from = from, .payload = std::move(p)};
    return e;
}

}  // namespace

TEST(RaftCodecTest, RequestVoteRoundTrip) {
    auto e = roundTrip(wrap(7, 2, 1, RequestVote{true, 5, 1, 9, 4}));
    EXPECT_EQ(e.groupId, 7);
    EXPECT_EQ(e.message.to, 2u);
    EXPECT_EQ(e.message.from, 1u);
    const auto* rv = std::get_if<RequestVote>(&e.message.payload);
    ASSERT_NE(rv, nullptr);
    EXPECT_TRUE(rv->preVote);
    EXPECT_EQ(rv->term, 5u);
    EXPECT_EQ(rv->lastLogIndex, 9u);
    EXPECT_EQ(rv->lastLogTerm, 4u);
}

TEST(RaftCodecTest, RequestVoteReplyRoundTrip) {
    auto e = roundTrip(wrap(0, 1, 2, RequestVoteReply{false, 6, true}));
    const auto* rr = std::get_if<RequestVoteReply>(&e.message.payload);
    ASSERT_NE(rr, nullptr);
    EXPECT_FALSE(rr->preVote);
    EXPECT_EQ(rr->term, 6u);
    EXPECT_TRUE(rr->voteGranted);
}

TEST(RaftCodecTest, AppendEntriesWithEntriesRoundTrip) {
    AppendEntries ae;
    ae.term = 3;
    ae.leaderId = 1;
    ae.prevLogIndex = 10;
    ae.prevLogTerm = 2;
    ae.leaderCommit = 8;
    LogEntry a;
    a.term = 3;
    a.index = 11;
    a.data = "hello";
    LogEntry b;
    b.term = 3;
    b.index = 12;
    b.type = EntryType::ConfigChange;
    b.data = "v=1,2;o=;l=";
    ae.entries = {a, b};
    auto e = roundTrip(wrap(4095, 3, 1, ae));
    const auto* got = std::get_if<AppendEntries>(&e.message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->prevLogIndex, 10u);
    EXPECT_EQ(got->leaderCommit, 8u);
    ASSERT_EQ(got->entries.size(), 2u);
    EXPECT_EQ(got->entries[0].data, "hello");
    EXPECT_EQ(got->entries[0].index, 11u);
    EXPECT_EQ(got->entries[1].type, EntryType::ConfigChange);
    EXPECT_EQ(got->entries[1].data, "v=1,2;o=;l=");
}

TEST(RaftCodecTest, AppendEntriesEmptyHeartbeatRoundTrip) {
    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 2;
    ae.prevLogIndex = 0;
    ae.prevLogTerm = 0;
    ae.leaderCommit = 0;
    auto e = roundTrip(wrap(1, 1, 2, ae));
    const auto* got = std::get_if<AppendEntries>(&e.message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_TRUE(got->entries.empty());
}

TEST(RaftCodecTest, AppendEntriesReplyRoundTrip) {
    auto e = roundTrip(wrap(2, 1, 3, AppendEntriesReply{4, false, 0, 7, 2}));
    const auto* got = std::get_if<AppendEntriesReply>(&e.message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_FALSE(got->success);
    EXPECT_EQ(got->conflictIndex, 7u);
    EXPECT_EQ(got->conflictTerm, 2u);
}

TEST(RaftCodecTest, InstallSnapshotRoundTrip) {
    InstallSnapshot is;
    is.term = 5;
    is.leaderId = 1;
    is.lastIncludedIndex = 100;
    is.lastIncludedTerm = 4;
    is.config.voters = {1, 2, 3};
    is.config.learners = {9};
    is.data = std::string("\0\1\2binary\xff", 9);
    auto e = roundTrip(wrap(3, 2, 1, is));
    const auto* got = std::get_if<InstallSnapshot>(&e.message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->lastIncludedIndex, 100u);
    EXPECT_EQ(got->config.voters, (std::vector<NodeId>{1, 2, 3}));
    EXPECT_EQ(got->config.learners, (std::vector<NodeId>{9}));
    EXPECT_EQ(got->data, std::string("\0\1\2binary\xff", 9));
}

TEST(RaftCodecTest, TimeoutNowRoundTrip) {
    auto e = roundTrip(wrap(1, 2, 1, TimeoutNow{7, 1}));
    const auto* got = std::get_if<TimeoutNow>(&e.message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->term, 7u);
    EXPECT_EQ(got->leaderId, 1u);
}

// ---------------------------------------------------------------------------
// The leader-transfer vote (ADR 0005 / debt D-9). It travels under its OWN type byte,
// and these three tests are mechanism (b)'s whole claim, asserted rather than assumed.

TEST(RaftCodecTest, TransferVoteRoundTripsAndKeepsItsFlag) {
    RequestVote rv;
    rv.term = 5;
    rv.candidateId = 2;
    rv.lastLogIndex = 9;
    rv.lastLogTerm = 4;
    rv.campaignTransfer = true;
    auto e = roundTrip(wrap(7, 3, 2, rv));
    const auto* got = std::get_if<RequestVote>(&e.message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_TRUE(got->campaignTransfer);
    EXPECT_FALSE(got->preVote);  // a transfer campaign is never a straw poll
    EXPECT_EQ(got->term, 5u);
    EXPECT_EQ(got->candidateId, 2u);
    EXPECT_EQ(got->lastLogIndex, 9u);
    EXPECT_EQ(got->lastLogTerm, 4u);

    // An ORDINARY vote is unchanged on the wire -- same tag, same bytes as before the
    // flag existed -- so nothing about the old format moved.
    RequestVote plain = rv;
    plain.campaignTransfer = false;
    const std::string plainBytes = encodeEnvelope(wrap(7, 3, 2, plain));
    const std::string xferBytes = encodeEnvelope(wrap(7, 3, 2, rv));
    ASSERT_EQ(plainBytes.size(), xferBytes.size());  // same body, different tag only
    const size_t tagOff = 2 + 8 + 8;                 // groupId + to + from
    EXPECT_NE(plainBytes[tagOff], xferBytes[tagOff]);
    EXPECT_EQ(plainBytes.substr(tagOff + 1), xferBytes.substr(tagOff + 1));
    const Envelope plainRound = roundTrip(wrap(7, 3, 2, plain));
    const auto* plainBack = std::get_if<RequestVote>(&plainRound.message.payload);
    ASSERT_NE(plainBack, nullptr);
    EXPECT_FALSE(plainBack->campaignTransfer);
}

// THE MIXED-VERSION CLAIM. A decoder that predates the flag knows tags 1-7 and answers
// `default: return nullopt` for anything else. Model it exactly (the bodies of tags 1-7
// are untouched, so deferring to the real decoder for them is faithful) and assert that
// a transfer vote is DROPPED WHOLE rather than misparsed into a plausible-looking vote
// request with a garbage term -- which is what adding a byte inside kRequestVote would
// have produced, since the envelope has no version field.
TEST(RaftCodecTest, AnOldDecoderDropsTheTransferVoteAndStillReadsAnOrdinaryOne) {
    auto decodeAsOldBinary = [](const std::string& bytes) -> std::optional<Envelope> {
        if (bytes.size() < 19)
            return std::nullopt;
        const uint8_t tag = static_cast<uint8_t>(bytes[2 + 8 + 8]);
        if (tag == 0 || tag > 7)
            return std::nullopt;  // the pre-flag switch had no case for it
        return decodeEnvelope(bytes);
    };

    RequestVote rv;
    rv.term = 5;
    rv.candidateId = 2;
    rv.lastLogIndex = 9;
    rv.lastLogTerm = 4;
    rv.campaignTransfer = true;
    EXPECT_FALSE(decodeAsOldBinary(encodeEnvelope(wrap(7, 3, 2, rv))).has_value())
        << "an old voter must DROP the transfer vote, not decode a garbled one";

    // The same node's ordinary votes still reach that old peer untouched, so only the
    // transfer degrades -- and it degrades to the pre-bypass path (the outgoing leader
    // abandons the transfer after one election timeout and the group elects normally),
    // never to a wedge and never to a misparse.
    rv.campaignTransfer = false;
    auto old = decodeAsOldBinary(encodeEnvelope(wrap(7, 3, 2, rv)));
    ASSERT_TRUE(old.has_value());
    const auto* got = std::get_if<RequestVote>(&old->message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->term, 5u);
    EXPECT_EQ(got->lastLogIndex, 9u);
}

// A peer that sets BOTH bits cannot use the transfer tag to bypass a lease on a PREVOTE:
// the tag decides, and the tag means "real vote".
TEST(RaftCodecTest, TheTransferTagIsNeverAPreVoteEvenIfTheByteSaysSo) {
    RequestVote rv;
    rv.preVote = true;
    rv.term = 5;
    rv.campaignTransfer = true;
    std::string bytes = encodeEnvelope(wrap(7, 3, 2, rv));
    bytes[2 + 8 + 8 + 1] = 1;  // force the preVote byte on, whatever the encoder wrote
    auto decoded = decodeEnvelope(bytes);
    ASSERT_TRUE(decoded.has_value());
    const auto* got = std::get_if<RequestVote>(&decoded->message.payload);
    ASSERT_NE(got, nullptr);
    EXPECT_TRUE(got->campaignTransfer);
    EXPECT_FALSE(got->preVote);
}

TEST(RaftCodecTest, TruncatedInputIsRejected) {
    std::string full = encodeEnvelope(wrap(7, 2, 1, RequestVote{true, 5, 1, 9, 4}));
    // Every proper prefix must decode to nullopt, never a fabricated message.
    for (size_t n = 0; n < full.size(); ++n) {
        EXPECT_FALSE(decodeEnvelope(full.substr(0, n)).has_value()) << "prefix len " << n;
    }
    // The full frame decodes.
    EXPECT_TRUE(decodeEnvelope(full).has_value());
}

TEST(RaftCodecTest, UnknownTagAndBogusEntryCountRejected) {
    // Unknown payload tag.
    std::string s;
    s.push_back(1);  // group lo
    s.push_back(0);  // group hi
    for (int i = 0; i < 16; ++i)
        s.push_back(0);  // to + from
    s.push_back(99);     // unknown tag
    EXPECT_FALSE(decodeEnvelope(s).has_value());

    // AppendEntries claiming a huge entry count with no bytes behind it.
    AppendEntries ae;
    ae.term = 1;
    std::string good = encodeEnvelope(wrap(1, 1, 2, ae));
    // Corrupt the entry-count field (last 4 bytes are count==0) to a large value.
    good[good.size() - 4] = static_cast<char>(0xff);
    good[good.size() - 3] = static_cast<char>(0xff);
    EXPECT_FALSE(decodeEnvelope(good).has_value());
}

// The transport refuses on the ENCODED ENVELOPE; every producer checks a RAW PAYLOAD.
// The gap between the two is what `kRaftEnvelopeHeadroomBytes` reserves, and this pins
// that the reserve actually covers the framing -- otherwise a snapshot in the band
// between the payload bound and the send bound passes `sendInstallSnapshot`'s check and
// is refused on the wire, which is exactly the nextIndex hot loop F3a removed.
TEST(RaftCodecTest, EnvelopeHeadroomCoversTheFramingOfEveryPayloadProducer) {
    static_assert(kMaxRaftPayloadBytes + kRaftEnvelopeHeadroomBytes == kMaxRaftSendBytes);
    static_assert(kMaxRaftPayloadBytes < kMaxRaftSendBytes);
    static_assert(RaftGroup::kMaxProposalBytes == kMaxRaftPayloadBytes,
                  "the proposal producer must share the one payload bound");

    // A config far larger than any placement this system builds (RF is single digits),
    // so the measured overhead is a pessimistic bound on the real one.
    Config fat;
    for (NodeId n = 1; n <= 4096; ++n) {
        fat.voters.push_back(n);
        fat.votersOutgoing.push_back(n + 100000);
        fat.learners.push_back(n + 200000);
    }

    const std::string payload(1u << 20, 'x');  // 1 MiB stand-in for the real payload

    InstallSnapshot is;
    is.term = 9;
    is.leaderId = 1;
    is.lastIncludedIndex = 1u << 30;
    is.lastIncludedTerm = 7;
    is.config = fat;
    is.data = payload;
    const size_t isOverhead = encodeEnvelope(wrap(4095, 2, 1, is)).size() - payload.size();
    EXPECT_LE(isOverhead, kRaftEnvelopeHeadroomBytes)
        << "an InstallSnapshot at kMaxRaftPayloadBytes would encode over kMaxRaftSendBytes";

    // The proposal producer's framing: one entry carrying the payload, inside an
    // AppendEntries, inside an envelope.
    AppendEntries ae;
    ae.term = 9;
    ae.leaderId = 1;
    ae.prevLogIndex = 1u << 30;
    ae.prevLogTerm = 7;
    ae.leaderCommit = 1u << 30;
    LogEntry entry;
    entry.term = 9;
    entry.index = (1u << 30) + 1;
    entry.data = payload;
    ae.entries.push_back(std::move(entry));
    const size_t aeOverhead = encodeEnvelope(wrap(4095, 2, 1, ae)).size() - payload.size();
    EXPECT_LE(aeOverhead, kRaftEnvelopeHeadroomBytes)
        << "a proposal at kMaxProposalBytes would encode over kMaxRaftSendBytes";
}
