#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

using namespace timestar::raft;

namespace {

RaftOptions fixedTimeout(unsigned t) {
    RaftOptions o;
    o.electionTimeoutMin = t;
    o.electionTimeoutMax = t;  // deterministic
    return o;
}

// Build a log by appending entries with the given terms (indices 1..n).
RaftLog logOfTerms(std::vector<Term> terms) {
    RaftLog log;
    std::vector<LogEntry> es;
    for (Term t : terms) {
        LogEntry e;
        e.term = t;
        es.push_back(e);
    }
    if (!es.empty())
        log.append(std::move(es));
    return log;
}

// Drain a node's Ready, returning it, and advance so state moves forward.
RaftNode::Ready drain(RaftNode& n) {
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    return rd;
}

template <class T>
const T* payloadIf(const Message& m) {
    return std::get_if<T>(&m.payload);
}

}  // namespace

TEST(RaftNodeTest, SingleVoterElectsItselfImmediately) {
    RaftNode n(1, {1}, RaftLog{}, HardState{}, fixedTimeout(5));
    EXPECT_EQ(n.role(), Role::Follower);
    n.campaign();
    EXPECT_EQ(n.role(), Role::Leader);
    EXPECT_EQ(n.currentTerm(), 1u);
    EXPECT_EQ(n.leader(), 1u);
}

TEST(RaftNodeTest, ElectionTimeoutStartsCandidacyAndRequestsVotes) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(3));
    for (int i = 0; i < 2; ++i) {
        n.tick();
        EXPECT_EQ(n.role(), Role::Follower);
    }
    n.tick();  // 3rd tick hits the timeout
    EXPECT_EQ(n.role(), Role::Candidate);
    EXPECT_EQ(n.currentTerm(), 1u);

    RaftNode::Ready rd = drain(n);
    ASSERT_TRUE(rd.hardState.has_value());
    EXPECT_EQ(rd.hardState->currentTerm, 1u);
    EXPECT_EQ(rd.hardState->votedFor, 1u);  // voted for self
    // One RequestVote to each other voter.
    int votes = 0;
    for (const auto& m : rd.messages) {
        if (const auto* rv = payloadIf<RequestVote>(m)) {
            EXPECT_EQ(rv->term, 1u);
            EXPECT_EQ(rv->candidateId, 1u);
            EXPECT_FALSE(rv->preVote);
            EXPECT_NE(m.to, 1u);
            ++votes;
        }
    }
    EXPECT_EQ(votes, 2);
}

TEST(RaftNodeTest, GrantsVoteToUpToDateCandidateAndPersistsIt) {
    // Follower at term 5 with log [t5@1, t5@2].
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, fixedTimeout(10));
    RequestVote rv{.preVote = false, .term = 5, .candidateId = 2, .lastLogIndex = 2, .lastLogTerm = 5};
    n.step(Message{.to = 1, .from = 2, .payload = rv});

    RaftNode::Ready rd = drain(n);
    ASSERT_TRUE(rd.hardState.has_value());
    EXPECT_EQ(rd.hardState->votedFor, 2u);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* reply = payloadIf<RequestVoteReply>(rd.messages[0]);
    ASSERT_NE(reply, nullptr);
    EXPECT_TRUE(reply->voteGranted);
    EXPECT_EQ(reply->term, 5u);
    EXPECT_EQ(rd.messages[0].to, 2u);
}

TEST(RaftNodeTest, RejectsSecondCandidateSameTerm) {
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, fixedTimeout(10));
    n.step(Message{.to = 1, .from = 2,
                   .payload = RequestVote{false, 5, 2, 2, 5}});
    drain(n);
    // Node 3 asks for the same term after we voted for 2.
    n.step(Message{.to = 1, .from = 3,
                   .payload = RequestVote{false, 5, 3, 2, 5}});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* reply = payloadIf<RequestVoteReply>(rd.messages[0]);
    ASSERT_NE(reply, nullptr);
    EXPECT_FALSE(reply->voteGranted);
}

TEST(RaftNodeTest, RejectsCandidateWithStaleLog) {
    // Our log ends at term 5; candidate's ends at term 4 -> not up to date.
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, fixedTimeout(10));
    n.step(Message{.to = 1, .from = 2,
                   .payload = RequestVote{false, 5, 2, 9, 4}});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    EXPECT_FALSE(payloadIf<RequestVoteReply>(rd.messages[0])->voteGranted);
}

TEST(RaftNodeTest, LowerTermVoteRequestRejectedWithOurTerm) {
    RaftNode n(1, {1, 2, 3}, logOfTerms({5}), HardState{5, kNoNode}, fixedTimeout(10));
    n.step(Message{.to = 1, .from = 2,
                   .payload = RequestVote{false, 3, 2, 1, 3}});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* reply = payloadIf<RequestVoteReply>(rd.messages[0]);
    ASSERT_NE(reply, nullptr);
    EXPECT_FALSE(reply->voteGranted);
    EXPECT_EQ(reply->term, 5u);  // tells the stale candidate the real term
    EXPECT_EQ(n.currentTerm(), 5u);  // we did NOT adopt term 3
}

TEST(RaftNodeTest, CandidateWinsOnMajority) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(1));
    n.tick();  // -> candidate term 1
    ASSERT_EQ(n.role(), Role::Candidate);
    drain(n);
    // One grant from node 2 reaches quorum (2 of 3, self + 2).
    n.step(Message{.to = 1, .from = 2,
                   .payload = RequestVoteReply{false, 1, true}});
    EXPECT_EQ(n.role(), Role::Leader);
    EXPECT_EQ(n.leader(), 1u);
}

TEST(RaftNodeTest, VoteReplyFromNonVoterIsNotCounted) {
    // A stray/decommissioned replica (id 7, not in the voter set) must never help
    // a candidate reach quorum -- else it could win on fewer than a real majority.
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(1));
    n.tick();  // -> candidate term 1, self-vote (1 of 3)
    ASSERT_EQ(n.role(), Role::Candidate);
    drain(n);
    n.step(Message{.to = 1, .from = 7, .payload = RequestVoteReply{false, 1, true}});
    EXPECT_EQ(n.role(), Role::Candidate);  // still short of quorum, not leader
    // A real voter then puts us over the top.
    n.step(Message{.to = 1, .from = 2, .payload = RequestVoteReply{false, 1, true}});
    EXPECT_EQ(n.role(), Role::Leader);
}

TEST(RaftNodeTest, CandidateStepsDownOnMajorityReject) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(1));
    n.tick();
    drain(n);
    n.step(Message{.to = 1, .from = 2, .payload = RequestVoteReply{false, 1, false}});
    n.step(Message{.to = 1, .from = 3, .payload = RequestVoteReply{false, 1, false}});
    EXPECT_EQ(n.role(), Role::Follower);
    EXPECT_EQ(n.currentTerm(), 1u);
}

TEST(RaftNodeTest, HigherTermStepsLeaderDown) {
    RaftNode n(1, {1}, RaftLog{}, HardState{}, fixedTimeout(5));
    n.campaign();
    ASSERT_EQ(n.role(), Role::Leader);
    // A vote request at a higher term forces us to follower.
    n.step(Message{.to = 1, .from = 2,
                   .payload = RequestVote{false, 9, 2, 0, 0}});
    EXPECT_EQ(n.role(), Role::Follower);
    EXPECT_EQ(n.currentTerm(), 9u);
}

TEST(RaftNodeTest, FollowerAppendsEntriesAndAdvancesCommit) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(10));
    AppendEntries ae;
    ae.term = 2;
    ae.leaderId = 2;
    ae.prevLogIndex = 0;
    ae.prevLogTerm = 0;
    LogEntry e1;
    e1.term = 2;
    e1.data = "x";
    LogEntry e2;
    e2.term = 2;
    e2.data = "y";
    ae.entries = {e1, e2};
    ae.leaderCommit = 1;  // leader has committed index 1
    n.step(Message{.to = 1, .from = 2, .payload = ae});

    EXPECT_EQ(n.currentTerm(), 2u);
    EXPECT_EQ(n.leader(), 2u);
    EXPECT_EQ(n.log().lastIndex(), 2u);
    EXPECT_EQ(n.commitIndex(), 1u);

    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.entries.size(), 2u);  // to persist
    ASSERT_EQ(rd.committed.size(), 1u);  // index 1 committed -> apply
    EXPECT_EQ(rd.committed[0].data, "x");
    // A success reply with the new match index.
    const AppendEntriesReply* rep = nullptr;
    for (const auto& m : rd.messages)
        if ((rep = payloadIf<AppendEntriesReply>(m)))
            break;
    ASSERT_NE(rep, nullptr);
    EXPECT_TRUE(rep->success);
    EXPECT_EQ(rep->matchIndex, 2u);
}

TEST(RaftNodeTest, FollowerRejectsAppendOnShortLogWithHint) {
    RaftNode n(1, {1, 2, 3}, logOfTerms({1}), HardState{1, kNoNode}, fixedTimeout(10));
    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 2;
    ae.prevLogIndex = 5;  // we only have index 1
    ae.prevLogTerm = 1;
    n.step(Message{.to = 1, .from = 2, .payload = ae});
    RaftNode::Ready rd = drain(n);
    const AppendEntriesReply* rep = nullptr;
    for (const auto& m : rd.messages)
        if ((rep = payloadIf<AppendEntriesReply>(m)))
            break;
    ASSERT_NE(rep, nullptr);
    EXPECT_FALSE(rep->success);
    EXPECT_EQ(rep->conflictTerm, 0u);       // "log too short"
    EXPECT_EQ(rep->conflictIndex, 2u);      // our lastIndex()+1
}

TEST(RaftNodeTest, FollowerRejectsAppendOnTermConflictWithHint) {
    // Our log: t1@1, t1@2, t2@3, t2@4. Leader claims prevLogIndex 4 term 3.
    RaftNode n(1, {1, 2, 3}, logOfTerms({1, 1, 2, 2}), HardState{2, kNoNode}, fixedTimeout(10));
    AppendEntries ae;
    ae.term = 3;
    ae.leaderId = 2;
    ae.prevLogIndex = 4;
    ae.prevLogTerm = 3;  // conflicts with our term 2 at index 4
    n.step(Message{.to = 1, .from = 2, .payload = ae});
    RaftNode::Ready rd = drain(n);
    const AppendEntriesReply* rep = nullptr;
    for (const auto& m : rd.messages)
        if ((rep = payloadIf<AppendEntriesReply>(m)))
            break;
    ASSERT_NE(rep, nullptr);
    EXPECT_FALSE(rep->success);
    EXPECT_EQ(rep->conflictTerm, 2u);   // the conflicting term at index 4
    EXPECT_EQ(rep->conflictIndex, 3u);  // first index of term 2
}

TEST(RaftNodeTest, ReadyReportsEntriesOnceThenClears) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(10));
    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 2;
    LogEntry e;
    e.term = 1;
    e.data = "a";
    ae.entries = {e};
    n.step(Message{.to = 1, .from = 2, .payload = ae});
    RaftNode::Ready rd1 = drain(n);
    EXPECT_EQ(rd1.entries.size(), 1u);
    EXPECT_FALSE(n.hasReady());  // nothing left after advance
    RaftNode::Ready rd2 = n.ready();
    EXPECT_TRUE(rd2.entries.empty());
}
