#include "../../../lib/cluster/raft/raft_codec.hpp"

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
