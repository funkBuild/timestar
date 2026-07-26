#include "../../../lib/cluster/raft/raft_config.hpp"
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

TEST(RaftConfigTest, EncodeDecodeRoundTrip) {
    Config c;
    c.voters = {1, 2, 3};
    c.votersOutgoing = {4, 5};
    c.learners = {9};
    Config back = decodeConfig(encodeConfig(c));
    EXPECT_EQ(back.voters, c.voters);
    EXPECT_EQ(back.votersOutgoing, c.votersOutgoing);
    EXPECT_EQ(back.learners, c.learners);
    EXPECT_TRUE(back.joint());

    Config simple;
    simple.voters = {7};
    Config back2 = decodeConfig(encodeConfig(simple));
    EXPECT_EQ(back2.voters, (std::vector<NodeId>{7}));
    EXPECT_TRUE(back2.votersOutgoing.empty());
    EXPECT_TRUE(back2.learners.empty());
    EXPECT_FALSE(back2.joint());
}

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

// ---------------------------------------------------------------------------
// The leader-transfer lease bypass (ADR 0005 / debt D-9), from the VOTER's side.
//
// The claim being pinned is narrow and it is the reason the flag is safe: a
// campaignTransfer vote stands the CheckQuorum lease down and buys NOTHING ELSE. The
// distinction the assertions turn on is that the lease refuses SILENTLY (no reply, no
// term bump), while every other rule refuses with a reply -- so "we got a rejection" is
// itself proof the bypass happened and the rejection came from a real vote rule.

namespace {

RaftOptions checkQuorumTimeout(unsigned t) {
    RaftOptions o = fixedTimeout(t);
    o.checkQuorum = true;
    return o;
}

// Put `n` inside its CheckQuorum lease: it has just heard a valid AppendEntries from
// `leader` at its own term, so leaderId_ is set and electionElapsed_ is 0.
void hearFromLeader(RaftNode& n, NodeId leader, Term term, LogIndex prevIdx, Term prevTerm) {
    AppendEntries ae;
    ae.term = term;
    ae.leaderId = leader;
    ae.prevLogIndex = prevIdx;
    ae.prevLogTerm = prevTerm;
    n.step(Message{.to = n.id(), .from = leader, .payload = ae});
    drain(n);  // discard the ack
    ASSERT_EQ(n.leader(), leader);
}

RequestVote transferVote(Term term, NodeId candidate, LogIndex lastIdx, Term lastTerm) {
    RequestVote rv;
    rv.term = term;
    rv.candidateId = candidate;
    rv.lastLogIndex = lastIdx;
    rv.lastLogTerm = lastTerm;
    rv.campaignTransfer = true;
    return rv;
}

}  // namespace

TEST(RaftNodeTest, TheLeaseStillDropsAnOrdinaryVoteFromALiveFollower) {
    // The control for the three below: WITHOUT the flag, nothing changes. The vote is
    // dropped silently -- no reply, no term bump -- which is exactly the behaviour that
    // broke leadership transfer and forced 1f2e752.
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, checkQuorumTimeout(10));
    hearFromLeader(n, 2, 5, 2, 5);
    n.step(Message{.to = 1, .from = 3, .payload = RequestVote{false, 6, 3, 2, 5}});
    RaftNode::Ready rd = drain(n);
    EXPECT_TRUE(rd.messages.empty()) << "the lease refuses silently";
    EXPECT_EQ(n.currentTerm(), 5u) << "and without even bumping our term";
    EXPECT_EQ(n.leader(), 2u);
}

TEST(RaftNodeTest, TransferVoteBypassesTheLeaseAndIsGranted) {
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, checkQuorumTimeout(10));
    hearFromLeader(n, 2, 5, 2, 5);
    n.step(Message{.to = 1, .from = 3, .payload = transferVote(6, 3, 2, 5)});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* reply = payloadIf<RequestVoteReply>(rd.messages[0]);
    ASSERT_NE(reply, nullptr);
    EXPECT_TRUE(reply->voteGranted);
    EXPECT_EQ(n.currentTerm(), 6u);
    ASSERT_TRUE(rd.hardState.has_value());
    EXPECT_EQ(rd.hardState->votedFor, 3u);
}

TEST(RaftNodeTest, TransferVoteStillObeysTheLogUpToDateCheck) {
    // §5.4.1 is not a lease and the flag does not touch it. Note the term DOES bump:
    // that is the proof the lease stood aside and the refusal came from the log check.
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, checkQuorumTimeout(10));
    hearFromLeader(n, 2, 5, 2, 5);
    n.step(Message{.to = 1, .from = 3, .payload = transferVote(6, 3, /*lastIdx=*/1, /*lastTerm=*/4)});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* reply = payloadIf<RequestVoteReply>(rd.messages[0]);
    ASSERT_NE(reply, nullptr);
    EXPECT_FALSE(reply->voteGranted) << "a stale log loses the election, flag or no flag";
    EXPECT_EQ(n.currentTerm(), 6u);
}

TEST(RaftNodeTest, TransferVoteStillObeysOneVotePerTerm) {
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, checkQuorumTimeout(10));
    hearFromLeader(n, 2, 5, 2, 5);
    n.step(Message{.to = 1, .from = 3, .payload = transferVote(6, 3, 2, 5)});
    ASSERT_TRUE(payloadIf<RequestVoteReply>(drain(n).messages.at(0))->voteGranted);
    // A SECOND transferee at the same term cannot also be voted for -- which is the
    // property that makes two leaders in one term impossible.
    n.step(Message{.to = 1, .from = 2, .payload = transferVote(6, 2, 2, 5)});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    EXPECT_FALSE(payloadIf<RequestVoteReply>(rd.messages[0])->voteGranted);
    EXPECT_EQ(n.currentTerm(), 6u);
}

TEST(RaftNodeTest, TransferVoteAtAStaleTermIsRejectedWithOurTerm) {
    RaftNode n(1, {1, 2, 3}, logOfTerms({5, 5}), HardState{5, kNoNode}, checkQuorumTimeout(10));
    hearFromLeader(n, 2, 5, 2, 5);
    n.step(Message{.to = 1, .from = 3, .payload = transferVote(3, 3, 2, 5)});
    RaftNode::Ready rd = drain(n);
    ASSERT_EQ(rd.messages.size(), 1u);
    const auto* reply = payloadIf<RequestVoteReply>(rd.messages[0]);
    ASSERT_NE(reply, nullptr);
    EXPECT_FALSE(reply->voteGranted);
    EXPECT_EQ(reply->term, 5u);
    EXPECT_EQ(n.currentTerm(), 5u) << "a stale transfer vote never moves our term";
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

TEST(RaftNodeTest, FollowerInstallsSnapshotAheadOfItsLog) {
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(10));
    InstallSnapshot is;
    is.term = 2;
    is.leaderId = 2;
    is.lastIncludedIndex = 5;
    is.lastIncludedTerm = 2;
    is.data = "snap-bytes";
    n.step(Message{.to = 1, .from = 2, .payload = is});

    EXPECT_EQ(n.currentTerm(), 2u);
    EXPECT_EQ(n.leader(), 2u);
    EXPECT_EQ(n.log().snapshotIndex(), 5u);
    EXPECT_EQ(n.log().lastIndex(), 5u);
    EXPECT_EQ(n.commitIndex(), 5u);

    RaftNode::Ready rd = drain(n);
    ASSERT_TRUE(rd.snapshot.has_value());
    EXPECT_EQ(rd.snapshot->index, 5u);
    EXPECT_EQ(rd.snapshot->term, 2u);
    EXPECT_EQ(rd.snapshot->data, "snap-bytes");
    const InstallSnapshotReply* rep = nullptr;
    for (const auto& m : rd.messages)
        if ((rep = payloadIf<InstallSnapshotReply>(m)))
            break;
    ASSERT_NE(rep, nullptr);
    EXPECT_EQ(rep->matchIndex, 5u);
    EXPECT_FALSE(n.hasReady());  // fully drained

    // A subsequent AppendEntries branches off the installed snapshot boundary.
    AppendEntries ae;
    ae.term = 2;
    ae.leaderId = 2;
    ae.prevLogIndex = 5;
    ae.prevLogTerm = 2;
    LogEntry e;
    e.term = 2;
    e.data = "post-snap";
    ae.entries = {e};
    ae.leaderCommit = 6;
    n.step(Message{.to = 1, .from = 2, .payload = ae});
    EXPECT_EQ(n.log().lastIndex(), 6u);
    EXPECT_EQ(n.commitIndex(), 6u);
}

TEST(RaftNodeTest, KeepSuffixInstallStillPersistsPendingTail) {
    // Regression: a snapshot that keeps a suffix must NOT advance the persistence
    // watermark past retained tail entries -- they are not in the snapshot payload
    // and would be silently lost on restart otherwise.
    RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, fixedTimeout(10));
    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 2;
    ae.prevLogIndex = 0;
    ae.prevLogTerm = 0;
    for (int i = 0; i < 5; ++i) {
        LogEntry e;
        e.term = 1;
        e.data = "e" + std::to_string(i + 1);
        ae.entries.push_back(e);
    }
    ae.leaderCommit = 0;  // nothing committed yet
    n.step(Message{.to = 1, .from = 2, .payload = ae});
    // Deliberately do NOT drain: entries 1..5 are pending persistence.

    InstallSnapshot is;
    is.term = 1;
    is.leaderId = 2;
    is.lastIncludedIndex = 3;
    is.lastIncludedTerm = 1;  // matches our entry 3 -> keep-suffix path
    is.data = "snap@3";
    n.step(Message{.to = 1, .from = 2, .payload = is});

    ASSERT_EQ(n.log().snapshotIndex(), 3u);
    ASSERT_EQ(n.log().lastIndex(), 5u);  // entries 4,5 retained

    RaftNode::Ready rd = n.ready();
    ASSERT_TRUE(rd.snapshot.has_value());
    // The retained tail (4,5) MUST still be surfaced for persistence.
    ASSERT_EQ(rd.entries.size(), 2u);
    EXPECT_EQ(rd.entries[0].index, 4u);
    EXPECT_EQ(rd.entries[0].data, "e4");
    EXPECT_EQ(rd.entries[1].index, 5u);
}

TEST(RaftNodeTest, StaleSnapshotIsIgnored) {
    // A snapshot at or below our commit is already covered; we keep our log.
    RaftNode n(1, {1, 2, 3}, logOfTerms({1, 1, 1}), HardState{1, kNoNode}, fixedTimeout(10));
    // Push commit to 3 via an AppendEntries heartbeat from the leader.
    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 2;
    ae.prevLogIndex = 3;
    ae.prevLogTerm = 1;
    ae.leaderCommit = 3;
    n.step(Message{.to = 1, .from = 2, .payload = ae});
    drain(n);
    ASSERT_EQ(n.commitIndex(), 3u);

    InstallSnapshot is;
    is.term = 1;
    is.leaderId = 2;
    is.lastIncludedIndex = 2;  // behind our commit
    is.lastIncludedTerm = 1;
    n.step(Message{.to = 1, .from = 2, .payload = is});
    RaftNode::Ready rd = drain(n);
    EXPECT_FALSE(rd.snapshot.has_value());  // not installed
    EXPECT_EQ(n.log().lastIndex(), 3u);     // log intact
    const InstallSnapshotReply* rep = nullptr;
    for (const auto& m : rd.messages)
        if ((rep = payloadIf<InstallSnapshotReply>(m)))
            break;
    ASSERT_NE(rep, nullptr);
    EXPECT_EQ(rep->matchIndex, 3u);
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
