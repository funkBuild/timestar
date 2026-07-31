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
// The fix was etcd's: give up after one election timeout. Nothing about the transfer is
// unsafe; it simply did not happen, and the leader must go back to accepting writes.
//
// THE WINDOW IS NO LONGER ONE ELECTION TIMEOUT (debt D-20). It is `transferTimeout`, two
// heartbeat intervals by default -- 1 s at the production cadence against an election
// timeout of 2.5-5 s and a 1.5 s write deadline. The etcd bound is LONGER than the
// deadline, so one transfer aimed at a peer that went unreachable after the balancer
// picked it costs the group a whole batch of writes; that is what forced D-1's target
// filter to be conservative rather than merely correct. The tests below pin the new bound
// AND the safety properties that make shortening it legitimate, because a consensus-timing
// change validated only by "the number got smaller" is exactly the kind D-1's fault-gate
// history says must not be trusted.
#include "../../../lib/cluster/data/write_errors.hpp"  // the class a refusal is reported as
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <string>
#include <variant>
#include <vector>

using namespace timestar::raft;

namespace {

RaftOptions transferOpts() {
    RaftOptions o;
    o.electionTimeoutMin = 20;
    o.electionTimeoutMax = 20;  // deterministic
    // 3 ticks per heartbeat => a 6-tick transfer window, unambiguously shorter than the
    // 20-tick election timeout. The production ratio is the same shape (25 vs 125-250).
    o.heartbeatTimeout = 3;
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

// A peer acks up to `upto` only -- still catching up.
void ackUpTo(RaftNode& leader, NodeId peer, LogIndex upto) {
    AppendEntriesReply r;
    r.term = leader.currentTerm();
    r.success = true;
    r.matchIndex = upto;
    leader.step(Message{.to = 1, .from = peer, .payload = r});
}

// Drain the node's outbound messages, answering the question "did it tell anyone to do
// anything?" -- the only evidence available for "nothing was told to campaign".
std::vector<Message> drain(RaftNode& n) {
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    return rd.messages;
}

bool sawTimeoutNowTo(const std::vector<Message>& msgs, NodeId to) {
    for (const auto& m : msgs)
        if (m.to == to && std::holds_alternative<TimeoutNow>(m.payload))
            return true;
    return false;
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

    // The window is the TRANSFER timeout, not the election timeout (debt D-20) -- and the
    // refusal must last for all of it, so the bound is a bound and not a rounding.
    for (unsigned i = 0; i + 1 < leader.transferTimeout(); ++i) {
        leader.tick();
        EXPECT_FALSE(leader.propose("inside the window")) << "abandoned early, at tick " << (i + 1);
    }
    leader.tick();

    EXPECT_TRUE(leader.propose("after"))
        << "the transfer never completed and was never abandoned: this group refuses writes forever while "
        << "remaining leader, so no election rescues it either (write-scaleout 5.4 follow-up)";
    EXPECT_TRUE(leader.isLeader()) << "abandoning a transfer must not cost leadership";
    EXPECT_LT(leader.transferTimeout(), leader.electionTimeout())
        << "the abandon window is back to one election timeout, which is longer than the write deadline it has "
        << "to fit inside (debt D-20)";
}

// THE BOUND ITSELF (debt D-20): two heartbeat intervals by default, explicitly settable,
// and never as long as an election timeout whatever it is set to.
TEST(RaftTransferAbortTest, TheAbandonWindowIsTwoHeartbeatsAndNeverAnElectionTimeout) {
    {  // derived
        RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, transferOpts());
        EXPECT_EQ(n.transferTimeout(), 2u * transferOpts().heartbeatTimeout);
    }
    {  // explicitly configured
        RaftOptions o = transferOpts();
        o.transferTimeout = 4;
        RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, o);
        EXPECT_EQ(n.transferTimeout(), 4u);
    }
    {  // clamped: a window at or past an election timeout is the pre-D-20 behaviour
        RaftOptions o = transferOpts();
        o.transferTimeout = 10'000;
        RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, o);
        EXPECT_EQ(n.transferTimeout(), o.electionTimeoutMin);
        EXPECT_LE(n.transferTimeout(), n.electionTimeout());
    }
    {  // never zero, whatever the cadence: a zero window would abandon before the
       // TimeoutNow it just sent could possibly be answered
        RaftOptions o = transferOpts();
        o.heartbeatTimeout = 0;
        RaftNode n(1, {1, 2, 3}, RaftLog{}, HardState{}, o);
        EXPECT_GE(n.transferTimeout(), 1u);
    }
}

// A TRANSFER THAT IS ACTUALLY PROGRESSING MUST NOT BE CUT OFF BY THE SHORTER WINDOW. The
// catch-up path is the one at risk: `transferLeadership` sends an append rather than a
// TimeoutNow, and the handoff only fires when the target's ack reaches lastIndex. Note
// that the backlog it has to cover is FIXED -- `propose` refuses while transferring, so
// lastIndex cannot run away from the transferee -- which is what makes a window of a few
// heartbeats sufficient rather than optimistic.
TEST(RaftTransferAbortTest, ACatchUpTransferThatAcksInsideTheWindowStillCompletes) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);
    ASSERT_TRUE(leader.propose("payload"));
    const LogIndex last = leader.log().lastIndex();
    ackUpTo(leader, 3, last - 1);  // node 3 is alive but one entry behind

    leader.transferLeadership(3);
    EXPECT_TRUE(leader.transferInFlight());
    EXPECT_FALSE(sawTimeoutNowTo(drain(leader), 3)) << "a lagging target must be caught up first, not told to elect";

    // ONE FULL HEARTBEAT ROUND passes before the target answers -- an ABSOLUTE bound, not
    // one derived from the window under test, or a window shortened to nothing would still
    // "pass" by shrinking this loop with it. A live peer answers the cadence it is served
    // on, so the window has to be at least that long to be a transfer bound at all.
    EXPECT_GE(leader.transferTimeout(), leader.heartbeatTimeout())
        << "the abandon window is shorter than the cadence a healthy peer answers on: a transfer can now be "
        << "abandoned before its target has had a single chance to ack";
    for (unsigned i = 0; i < leader.heartbeatTimeout(); ++i)
        leader.tick();
    ackFully(leader, 3);  // the catch-up ack lands inside the window

    EXPECT_TRUE(sawTimeoutNowTo(drain(leader), 3)) << "a transfer that caught up INSIDE the window was abandoned "
                                                   << "anyway, so the shorter bound broke leadership transfer";
    EXPECT_FALSE(leader.transferInFlight()) << "the handoff completed: leadTransferee_ is cleared by the ack";
}

// ABANDON, THEN THE TRANSFEREE WINS ANYWAY. This is the sequence the shorter window makes
// more likely and the one every "is this safe?" question is really about: the old leader
// resumed proposing in term T while the transferee, holding a TimeoutNow, campaigned.
//
// It is safe, and not because of anything in this file. A campaign runs at T+1, which no
// leader holds, so two leaders in one term remains impossible; the old leader steps down
// on the higher term exactly as it would for any election it lost. And a write it ACKED
// after abandoning cannot be lost, because §5.4.1 refuses the vote to a candidate whose
// log is missing a committed entry -- asserted here rather than assumed, since it is the
// entire safety argument for abandoning early.
TEST(RaftTransferAbortTest, AbandoningThenLosingToTheTransfereeStepsDownCleanly) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);
    ackFully(leader, 3);
    const Term term = leader.currentTerm();

    leader.transferLeadership(3);
    ASSERT_TRUE(sawTimeoutNowTo(drain(leader), 3)) << "the caught-up target was told to elect at once";
    for (unsigned i = 0; i < leader.transferTimeout(); ++i)
        leader.tick();  // the transferee's campaign is in flight; we abandon meanwhile
    ASSERT_FALSE(leader.transferInFlight());

    // A write accepted AND ACKED in the resumed window: proposed here, committed by the
    // 2-of-3 quorum that node 2 completes.
    ASSERT_TRUE(leader.propose("acked after the abandon"));
    const LogIndex acked = leader.log().lastIndex();
    ackFully(leader, 2);
    ASSERT_GE(leader.commitIndex(), acked) << "the resumed write never committed, so this proves nothing";
    drain(leader);

    // Now the transferee campaigns at T+1 WITHOUT that entry (it stopped replicating when
    // it took the TimeoutNow).
    RequestVote rv;
    rv.term = term + 1;
    rv.candidateId = 3;
    rv.campaignTransfer = true;
    rv.lastLogIndex = acked - 1;
    rv.lastLogTerm = term;
    leader.step(Message{.to = 1, .from = 3, .payload = rv});

    EXPECT_FALSE(leader.isLeader()) << "a higher term must depose us, transfer or not";
    EXPECT_EQ(leader.currentTerm(), term + 1);
    EXPECT_GE(leader.commitIndex(), acked) << "a COMMITTED write was rolled back by the transferee's campaign";
    bool granted = true;
    for (const auto& m : drain(leader))
        if (const auto* r = std::get_if<RequestVoteReply>(&m.payload); r && m.to == 3)
            granted = r->voteGranted;
    EXPECT_FALSE(granted) << "we voted for a candidate whose log is missing a committed entry (§5.4.1): the "
                          << "write acked after the abandon could be lost";
}

// ABANDON WHEN THE TRANSFEREE NEVER GOT A TimeoutNow AT ALL -- the common case, since a
// target that cannot be caught up is never told to elect. Nothing anywhere believes it may
// lead: we keep the term, keep the leadership, and the group resumes committing on the
// quorum that is actually alive. There is no second candidate in this story to split with.
TEST(RaftTransferAbortTest, AbandonWithNoTimeoutNowDeliveredResumesWithoutSplitBrain) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);
    const Term term = leader.currentTerm();
    ASSERT_TRUE(leader.propose("before"));
    ackFully(leader, 2);

    std::vector<Message> all = drain(leader);
    leader.transferLeadership(3);  // node 3 has never acked: the catch-up path, no TimeoutNow
    for (unsigned i = 0; i < leader.transferTimeout(); ++i) {
        leader.tick();
        for (auto& m : drain(leader))
            all.push_back(std::move(m));
    }

    EXPECT_FALSE(sawTimeoutNowTo(all, 3)) << "the transferee was told to elect and then abandoned -- the one shape "
                                          << "that puts a campaign in flight behind a leader that resumed";
    EXPECT_TRUE(leader.isLeader());
    EXPECT_EQ(leader.currentTerm(), term) << "abandoning must not bump the term";
    EXPECT_FALSE(leader.transferInFlight());

    ASSERT_TRUE(leader.propose("after"));
    const LogIndex idx = leader.log().lastIndex();
    ackFully(leader, 2);
    EXPECT_GE(leader.commitIndex(), idx) << "the group did not resume committing on its live 2-of-3 quorum";
}

// THE REFUSAL ITSELF IS UNCHANGED -- only its DURATION is (debt D-20). A proposal inside
// the window still fails as "I am the leader and I am standing down", which
// `ReplicatedVShardHost::classifyRefusal` reads off exactly this triple (propose refused,
// leader() == self) and reports as LeaderRefused: retryable, election-shaped, and never
// pointed back at this node. A shorter window that changed the CLASS would move the write
// path's retry policy without anyone asking.
TEST(RaftTransferAbortTest, AProposalInsideTheShortenedWindowIsStillLeaderRefused) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);
    leader.transferLeadership(3);

    for (unsigned i = 0; i < leader.transferTimeout(); ++i) {
        EXPECT_FALSE(leader.propose("refused")) << "at tick " << i;
        // The exact inputs classifyRefusal keys on: we ARE the leader, and we refused.
        EXPECT_TRUE(leader.isLeader());
        EXPECT_EQ(leader.leader(), static_cast<NodeId>(1));
        EXPECT_TRUE(leader.transferInFlight());
        leader.tick();
    }
    EXPECT_TRUE(leader.propose("resumed"));
    // ...and that class is one a WAIT can cure, which is what keeps a refusal inside the
    // window absorbed by the write path's retry budget rather than reported to a client.
    EXPECT_TRUE(timestar::data::isElectionWaitFailure(timestar::data::WriteFailure::LeaderRefused));
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

// THE ABORT WINDOW MUST NOT BE RE-ARMABLE BY REPEATING THE SAME TRANSFER.
//
// This is the door the abort can be walked around through, and it is not hypothetical: the
// leadership balancer runs every ~5 s against a 2.5-5 s election timeout, so a balancer
// that keeps choosing the same (down) target re-requests the transfer faster than the
// window expires. If each request reset the clock, `leadTransferee_` would never clear and
// the group would refuse writes forever -- exactly the failure tick()'s abort exists to
// bound, reached through a different door. etcd ignores a repeat request for the transfer
// already in flight; so do we.
//
// The previous version of this test never ran a tick after the second request, so it
// asserted nothing about the window at all.
TEST(RaftTransferAbortTest, RepeatingTheSameTransferDoesNotReArmTheAbortWindow) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);

    leader.transferLeadership(3);  // node 3 never acks
    for (unsigned i = 0; i < leader.transferTimeout() - 1; ++i) {
        leader.transferLeadership(3);  // the balancer, asking again on every pass
        leader.tick();
    }
    EXPECT_FALSE(leader.propose("still inside the one and only window"));

    leader.transferLeadership(3);  // ...and once more, right before the deadline
    leader.tick();
    EXPECT_TRUE(leader.propose("the window expired despite the repeat requests"))
        << "a repeated transfer request to the SAME target re-armed the abort window, so the group "
        << "refuses proposals for as long as something keeps asking (write-scaleout 5 review, F2)";
}

// ...AND THE REPEAT REQUEST MUST REPORT THAT IT STARTED NOTHING (debt D-24). The guard
// above is silent, so a caller keeping a counter -- `ShardRaftPlane::rebalance`, whose
// `transfers_initiated` the deposed-primary gate asserts an anti-vacuity FLOOR on --
// counted every re-request as a transfer. D-12 closed the non-voter door into the same
// counter; this is the other one.
TEST(RaftTransferAbortTest, TransferLeadershipReportsWhetherItStartedOne) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);

    EXPECT_FALSE(leader.transferLeadership(1)) << "transferring to ourselves starts nothing";
    EXPECT_FALSE(leader.transferLeadership(9)) << "node 9 is not a voter of this group (the D-12 door)";
    EXPECT_TRUE(leader.transferLeadership(3)) << "a genuine transfer must report itself started";
    EXPECT_FALSE(leader.transferLeadership(3)) << "the F2 re-arm guard started nothing and must say so";
    EXPECT_TRUE(leader.transferLeadership(2)) << "a NEW target is a new transfer";

    // A follower cannot transfer anything either.
    RaftNode follower(2, {1, 2, 3}, RaftLog{}, HardState{}, transferOpts());
    EXPECT_FALSE(follower.transferLeadership(3));
}

// THE COUNTER, UNDER THE BALANCER'S OWN CALL PATTERN. `rebalance` runs periodically and
// re-picks the same peer while its deficit stands, so "ask on every pass" is the production
// shape, not a corner. Counting the CALLS reported one transfer per pass; counting what the
// call ARMED reports one per window. The ground truth here is the number of times
// `leadTransferee_` actually went from unset to set, sampled independently of the return
// value -- so the test cannot pass by trusting the thing it is checking.
TEST(RaftTransferAbortTest, RepeatedPassesCountOneTransferPerWindowNotOnePerPass) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);  // node 3 never acks: the transfer can only ever be abandoned

    unsigned reported = 0, actuallyArmed = 0, passes = 0;
    for (unsigned tick = 0; tick < 4 * leader.transferTimeout(); ++tick) {
        if (tick % 2 == 0) {  // a balancer pass, faster than the window expires
            ++passes;
            const bool before = leader.transferInFlight();
            const bool started = leader.transferLeadership(3);
            if (!before && leader.transferInFlight())
                ++actuallyArmed;
            if (started)
                ++reported;
        }
        leader.tick();
    }

    EXPECT_EQ(reported, actuallyArmed) << "transfers_initiated counted " << reported << " transfers where "
                                       << actuallyArmed << " were armed (debt D-24)";
    EXPECT_GT(actuallyArmed, 0u) << "nothing was ever armed, so the equality above is vacuous";
    EXPECT_LT(reported, passes) << "every pass was counted, so this pattern cannot show the difference";
}

// ...but a transfer to a DIFFERENT target is a new transfer and gets its own full window.
TEST(RaftTransferAbortTest, ChangingTheTargetRestartsTheWindow) {
    RaftNode leader = makeIsolatedLeader({1, 2, 3});
    ackFully(leader, 2);  // node 2 is caught up but we aim at 3, which never acks

    leader.transferLeadership(3);
    for (unsigned i = 0; i < leader.transferTimeout() - 1; ++i)
        leader.tick();

    leader.transferLeadership(2);  // a genuinely new target
    leader.tick();
    EXPECT_FALSE(leader.propose("one tick into the new target's window"))
        << "the new transfer inherited the previous target's elapsed ticks and aborted early";
}
