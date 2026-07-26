// THE BALANCER'S CAUGHT-UP GUARD MUST NOT BE EXACT EQUALITY (debt D-1).
//
// WHAT THIS PINS. `ShardRaftPlane::rebalance` refuses to hand leadership to a peer that
// is not a safe transfer target. It used to test `matchIndexOf(target) ==
// log().lastIndex()`, which is true of a healthy peer only in the instant between the
// leader replicating an entry and appending the next one. Under sustained writes that
// instant essentially never coincides with a balancer pass, so leadership balancing
// became LOAD-DEPENDENT: it converged on a quiet cluster and could only move COLD groups
// on a busy one.
//
// The replacement is two gates, and this file pins the primitives both are built on:
//
//   1. a bounded matchIndex LAG (`matchIndexOf`, already pinned elsewhere), and
//   2. LIVENESS -- `RaftNode::ticksSinceAck`, added for this.
//
// (2) is the half a lag bound cannot supply. On a write-IDLE group a dead peer sits at
// lag zero indefinitely: it was caught up when it died, and lastIndex never moves again,
// so it passes any delta test -- including the old exact-equality one. That is a
// PRE-EXISTING hole, not one the relaxation opens, and the heartbeat closes it: the
// leader bcastAppends every `heartbeatTimeout` ticks whether or not it has anything to
// send, so a live peer's ack clock resets on its own and a dead peer's does not.
//
// THE WINDOW IS ONE HEARTBEAT ROUND, not three, and that is a measured number: at three
// rounds `fault_injection_gate.sh` lost 1 of 2000 writes where the pre-change tree took a
// heavier storm with zero, because the balancer could still target a peer whose
// connection had just been RST. See ShardRaftPlane::rebalance for the mechanism.
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace timestar::raft;

namespace {

RaftOptions livenessOpts() {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;  // deterministic
    o.heartbeatTimeout = 2;
    return o;
}

RaftNode makeLeader(const std::vector<NodeId>& voters) {
    RaftNode leader(1, voters, RaftLog{}, HardState{}, livenessOpts());
    leader.campaign();
    for (NodeId v : voters) {
        if (v == 1)
            continue;
        RequestVoteReply reply;
        reply.term = leader.currentTerm();
        reply.voteGranted = true;
        leader.step(Message{.to = 1, .from = v, .payload = reply});
    }
    return leader;
}

void ackFully(RaftNode& leader, NodeId peer) {
    AppendEntriesReply r;
    r.term = leader.currentTerm();
    r.success = true;
    r.matchIndex = leader.log().lastIndex();
    leader.step(Message{.to = 1, .from = peer, .payload = r});
}

// The balancer's gate, spelled out here exactly as ShardRaftPlane::rebalance spells it,
// so this file fails if either half is dropped.
bool isTransferTarget(const RaftNode& leader, NodeId peer) {
    constexpr LogIndex kMaxTransferLagEntries = 64;
    const LogIndex last = leader.log().lastIndex();
    const LogIndex match = leader.matchIndexOf(peer);
    const bool caughtUp = match >= last || (last - match) <= kMaxTransferLagEntries;
    const bool live = leader.ticksSinceAck(peer) <= leader.heartbeatTimeout();
    return caughtUp && live;
}

}  // namespace

TEST(RaftPeerLivenessTest, AFreshLeaderTrustsNobodyUntilTheFirstAck) {
    RaftNode leader = makeLeader({1, 2, 3});
    ASSERT_TRUE(leader.isLeader());
    // Nothing has replied yet. Erring toward "not live" is the safe direction: the
    // balancer skips a pass rather than targeting a peer whose state it cannot see.
    EXPECT_EQ(leader.ticksSinceAck(2), RaftNode::kNeverAcked);
    EXPECT_EQ(leader.ticksSinceAck(3), RaftNode::kNeverAcked);
    EXPECT_FALSE(isTransferTarget(leader, 2));

    ackFully(leader, 2);
    EXPECT_EQ(leader.ticksSinceAck(2), 0u);
    EXPECT_TRUE(isTransferTarget(leader, 2));
}

TEST(RaftPeerLivenessTest, ARejectedAppendStillProvesLiveness) {
    RaftNode leader = makeLeader({1, 2, 3});
    // A peer whose log conflicts answers with success == false. It is still UP, and the
    // liveness signal must say so -- otherwise a peer in the middle of a log-repair
    // round trip reads as dead.
    AppendEntriesReply r;
    r.term = leader.currentTerm();
    r.success = false;
    r.conflictIndex = 1;
    r.conflictTerm = kNoTerm;
    leader.step(Message{.to = 1, .from = 2, .payload = r});
    EXPECT_EQ(leader.ticksSinceAck(2), 0u);
}

TEST(RaftPeerLivenessTest, TheAckClockDecaysWithTicksAndResetsOnAReply) {
    RaftNode leader = makeLeader({1, 2, 3});
    ackFully(leader, 2);
    for (int i = 0; i < 5; ++i)
        leader.tick();
    EXPECT_EQ(leader.ticksSinceAck(2), 5u);
    ackFully(leader, 2);
    EXPECT_EQ(leader.ticksSinceAck(2), 0u);
}

TEST(RaftPeerLivenessTest, ADeadPeerOnAnIdleGroupIsExcludedDespiteZeroLag) {
    // THE CASE A PURE LAG BOUND CANNOT CATCH. Both peers ack everything, then node 3
    // dies. No further writes arrive, so lastIndex is frozen and node 3's matchIndex
    // stays EQUAL to it forever -- it would pass exact equality and it would pass any
    // delta bound. Only the ack clock moves.
    RaftNode leader = makeLeader({1, 2, 3});
    ackFully(leader, 2);
    ackFully(leader, 3);
    const LogIndex frozen = leader.log().lastIndex();
    ASSERT_EQ(leader.matchIndexOf(3), frozen);
    ASSERT_TRUE(isTransferTarget(leader, 3));

    // Node 3 goes away; node 2 keeps answering heartbeats. No proposals in between.
    for (unsigned t = 0; t <= leader.heartbeatTimeout(); ++t) {
        leader.tick();
        ackFully(leader, 2);
    }
    EXPECT_EQ(leader.log().lastIndex(), frozen) << "the group must stay write-idle for this to be the idle case";
    EXPECT_EQ(leader.matchIndexOf(3), frozen) << "a dead peer's matchIndex never moves -- that is the whole problem";
    EXPECT_FALSE(isTransferTarget(leader, 3)) << "the dead peer must be excluded by liveness, not by lag";
    EXPECT_TRUE(isTransferTarget(leader, 2)) << "the live peer must stay eligible";
}

TEST(RaftPeerLivenessTest, ALaggingButLivePeerIsStillAValidTargetUnderWrites) {
    // THE CASE THE OLD GUARD GOT WRONG. Node 2 is healthy and answering, but the leader
    // has appended past what node 2 has acked -- the normal steady state of a group
    // taking writes. Exact equality rejected it; the lag bound accepts it while the
    // shortfall is small enough that TimeoutNow fires on the next ack.
    RaftNode leader = makeLeader({1, 2, 3});
    ackFully(leader, 2);
    for (int i = 0; i < 8; ++i)
        ASSERT_TRUE(leader.propose("entry-" + std::to_string(i)));

    ASSERT_LT(leader.matchIndexOf(2), leader.log().lastIndex()) << "the old exact-equality guard would reject here";
    EXPECT_TRUE(isTransferTarget(leader, 2));
}

TEST(RaftPeerLivenessTest, APeerFarBehindIsExcludedByTheLagBound) {
    // A restarted peer still streaming its backlog is live but NOT a prompt handoff:
    // transferring to it costs the group its proposals for the whole catch-up. The lag
    // bound is what keeps it out.
    RaftNode leader = makeLeader({1, 2, 3});
    ackFully(leader, 2);
    for (int i = 0; i < 200; ++i)
        ASSERT_TRUE(leader.propose("entry-" + std::to_string(i)));

    EXPECT_EQ(leader.ticksSinceAck(2), 0u) << "still live";
    EXPECT_FALSE(isTransferTarget(leader, 2)) << "200 entries behind is past the 64-entry bound";
}

TEST(RaftPeerLivenessTest, LosingLeadershipClearsTheAckClock) {
    // Liveness is per-term: the map is leader-only bookkeeping, and carrying a previous
    // term's acks into a new one would make a peer that has said nothing THIS term look
    // fresh. A higher-term message steps us down.
    RaftNode leader = makeLeader({1, 2, 3});
    ackFully(leader, 2);
    ASSERT_EQ(leader.ticksSinceAck(2), 0u);

    AppendEntries ae;
    ae.term = leader.currentTerm() + 1;
    ae.leaderId = 3;
    ae.prevLogIndex = kNoIndex;
    ae.prevLogTerm = kNoTerm;
    leader.step(Message{.to = 1, .from = 3, .payload = ae});
    ASSERT_FALSE(leader.isLeader());
    EXPECT_EQ(leader.ticksSinceAck(2), RaftNode::kNeverAcked);
}
