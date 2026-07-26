// A LEADER TRANSFER TO A PEER THAT NEVER ACKS MUST BE ABANDONED (§3.10).
//
// WHAT THIS PINS. While `leadTransferee_` is set, `RaftNode::propose` returns false --
// correct for a handoff that is about to complete, catastrophic for one that never will.
// `handleAppendEntriesReply` clears `leadTransferee_` only when the target's matchIndex
// reaches lastIndex, so a transfer aimed at a DEAD peer left it set FOREVER: the group
// kept its leadership (so no election rescued it) and refused every write, permanently.
//
// HOW IT WAS REACHED IN PRODUCTION, with no operator action. `ShardRaftPlane::rebalance`
// picks the peer with the largest leadership deficit, and a dead peer leads nothing --
// so it is the most attractive target on every 5 s balancer pass. The restart-catch-up
// gate (test/cluster_gates/restart_catchup_gate.sh) surfaced it as a sustained ~26% of
// writes failing with "1 VShard slice(s) uncommitted after 6 attempt(s) (last:
// not-leader)" for as long as one of three nodes was down -- against a healthy 2-of-3
// quorum that should have served every one of them.
//
// The fix is etcd's: give up after one election timeout. Nothing about the transfer is
// unsafe; it simply did not happen, and the leader must go back to accepting writes.
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace timestar::raft;

namespace {

RaftOptions transferOpts() {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;  // deterministic
    o.heartbeatTimeout = 1;
    return o;
}

// Drive a leader to power with two peers that then go silent.
RaftNode makeIsolatedLeader(const std::vector<NodeId>& voters) {
    RaftNode leader(1, voters, RaftLog{}, HardState{}, transferOpts());
    leader.campaign();
    // Grant the votes by hand: nothing else in this test needs a network.
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

// A peer acks everything the leader has, so it is a legitimate transfer target.
void ackFully(RaftNode& leader, NodeId peer) {
    AppendEntriesReply r;
    r.term = leader.currentTerm();
    r.success = true;
    r.matchIndex = leader.log().lastIndex();
    leader.step(Message{.to = 1, .from = peer, .payload = r});
}

}  // namespace

TEST(RaftTransferAbortTest, ATransferToASilentPeerIsAbandonedAndWritesResume) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ASSERT_TRUE(leader.isLeader());
    ackFully(leader, 2);  // node 2 is alive; node 3 will never ack

    ASSERT_TRUE(leader.propose("before")) << "the leader should accept writes before any transfer";

    leader.transferLeadership(3);  // node 3 is DOWN: matchIndex will never advance
    EXPECT_FALSE(leader.propose("during"))
        << "a leader with a transfer in flight must refuse proposals (this part is correct)";

    // One election timeout of ticks with no ack from the transferee.
    for (unsigned i = 0; i < leader.electionTimeout(); ++i)
        leader.tick();

    EXPECT_TRUE(leader.propose("after"))
        << "the transfer never completed and was never abandoned: this group refuses writes forever while "
        << "remaining leader, so no election rescues it either (write-scaleout 5.4 follow-up)";
    EXPECT_TRUE(leader.isLeader()) << "abandoning a transfer must not cost leadership";
}

// The abort must not race a transfer that IS progressing: a target that acks within the
// window still gets leadership, so the fix cannot be "stop transferring".
TEST(RaftTransferAbortTest, ATransferToACaughtUpPeerStillCompletes) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ASSERT_TRUE(leader.isLeader());
    ackFully(leader, 2);
    ackFully(leader, 3);

    leader.transferLeadership(3);
    // transferLeadership sends TimeoutNow immediately for an already-caught-up target.
    RaftNode::Ready rd = leader.ready();
    bool sawTimeoutNow = false;
    for (const auto& m : rd.messages)
        if (m.to == 3 && std::holds_alternative<TimeoutNow>(m.payload))
            sawTimeoutNow = true;
    EXPECT_TRUE(sawTimeoutNow) << "a caught-up target must be told to elect at once";
}

// The timer restarts per transfer: a second attempt gets its own full window rather than
// inheriting the elapsed ticks of the first.
TEST(RaftTransferAbortTest, EachTransferGetsAFreshWindow) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);

    leader.transferLeadership(3);
    for (unsigned i = 0; i < leader.electionTimeout() - 1; ++i)
        leader.tick();
    EXPECT_FALSE(leader.propose("still in the first window"));

    leader.transferLeadership(3);  // restart
    leader.tick();
    EXPECT_FALSE(leader.propose("one tick into the second window"))
        << "the second transfer inherited the first one's elapsed ticks and aborted early";
}
