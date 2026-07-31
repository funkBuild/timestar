// THE SHARD-LEVEL SNAPSHOT TRANSFER BUDGET (debt D-37).
//
// D-5 paced each transfer at one unacked chunk PER PEER PER GROUP. Every link of that
// holds; what nothing bounded was the AGGREGATE. A shard hosts ~1365 groups, so a node
// returning from an outage long enough for its peers to have compacted could be sent that
// many 4 MiB chunks at once -- the peer's inbound semaphore queues rather than drops, so
// the result is every transfer crawling behind every other rather than any one completing.
//
// What these tests pin, in order of how much they matter:
//
//   1. THE CAP BINDS. Only `cap` transfers ship chunks at once, however many groups want
//      to; the rest queue.
//   2. FAIRNESS IS FIFO. Groups tick in map order, so "whoever asks when a slot frees"
//      would let the lowest-numbered group win every time. The OLDEST waiter takes the
//      freed slot, not whichever happens to tick first.
//   3. NOTHING LEAKS. A ticket abandoned in the queue head-of-line blocks every other
//      group on the shard, so EVERY path that drops a transfer -- completion, abandonment,
//      role change, re-compaction, a peer catching up another way -- must give it back.
//   4. A DEFERRED PEER IS NOT SILENT. While a transfer waits, chunks are not arriving, and
//      chunks are what that follower's election clock is living on. It gets a keep-alive
//      probe on the heartbeat cadence instead.
//   5. NO BUDGET == THE OLD BEHAVIOUR, so a core with no host (i.e. every other test)
//      cannot have been changed by any of this.
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace timestar::raft;

namespace {

RaftOptions budgetOpts(SnapshotTransferBudget* budget) {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;
    o.heartbeatTimeout = 2;
    o.maxSnapshotChunkBytes = 8;  // several chunks per payload
    o.snapshotChunkTimeout = 3;
    o.maxSnapshotResends = 2;
    o.snapshotBudget = budget;
    return o;
}

std::string payloadOf(size_t bytes) {
    std::string s;
    s.reserve(bytes);
    for (size_t i = 0; i < bytes; ++i)
        s.push_back(static_cast<char>('a' + (i % 26)));
    return s;
}

RaftNode::Ready drain(RaftNode& n) {
    RaftNode::Ready rd = n.ready();
    n.advance(rd);
    return rd;
}

// ONE GROUP, standing in for one of the ~1365 a shard hosts: a single-voter leader with
// one LEARNER peer, which is enough to exercise the leader's send path without a network.
// The learner is then reported as being below the compaction boundary, which is exactly
// the situation `sendInstallSnapshot` exists for.
class Group {
public:
    Group(NodeId self, SnapshotTransferBudget* budget)
        : n_(self, {self}, RaftLog{}, HardState{}, budgetOpts(budget), {kPeer}) {
        n_.campaign();
        drain(n_);
        EXPECT_TRUE(n_.isLeader());
        for (int i = 0; i < 3; ++i) {
            n_.propose("entry");
            drain(n_);
        }
        n_.compact(n_.commitIndex(), payloadOf(40));  // 5 chunks at 8 bytes
        drain(n_);
    }

    RaftNode& node() { return n_; }

    // Tell the leader its peer is below the boundary: a rejected append walks nextIndex_
    // down past the compacted prefix, and `sendAppend` hands off to `sendInstallSnapshot`.
    // Returns what the leader emitted in response.
    RaftNode::Ready askForSnapshot() {
        AppendEntriesReply rr;
        rr.term = n_.currentTerm();
        rr.success = false;
        rr.matchIndex = kNoIndex;
        rr.conflictIndex = 1;
        rr.conflictTerm = kNoTerm;
        n_.step(Message{.to = n_.id(), .from = kPeer, .payload = rr});
        return drain(n_);
    }

    // Answer the in-flight chunk as a follower would, staging everything it was sent.
    RaftNode::Ready ackChunk(const InstallSnapshot& is) {
        InstallSnapshotReply rr;
        rr.term = n_.currentTerm();
        rr.pendingSnapshotIndex = is.lastIncludedIndex;
        rr.stagedBytes = is.offset + is.data.size();
        rr.matchIndex = kNoIndex;
        n_.step(Message{.to = n_.id(), .from = kPeer, .payload = rr});
        return drain(n_);
    }

    // Answer as "installed", which ENDS the transfer.
    void reportInstalled() {
        InstallSnapshotReply rr;
        rr.term = n_.currentTerm();
        rr.pendingSnapshotIndex = kNoIndex;  // outcome-shaped
        rr.stagedBytes = 0;
        rr.matchIndex = n_.servableSnapshot().index;
        n_.step(Message{.to = n_.id(), .from = kPeer, .payload = rr});
        drain(n_);
    }

    static constexpr NodeId kPeer = 99;

private:
    RaftNode n_;
};

const InstallSnapshot* chunkIn(const RaftNode::Ready& rd) {
    for (const auto& m : rd.messages)
        if (const auto* is = std::get_if<InstallSnapshot>(&m.payload))
            return is;
    return nullptr;
}

const AppendEntries* appendIn(const RaftNode::Ready& rd) {
    for (const auto& m : rd.messages)
        if (const auto* ae = std::get_if<AppendEntries>(&m.payload))
            return ae;
    return nullptr;
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. The cap binds
// ---------------------------------------------------------------------------

TEST(RaftSnapshotBudgetTest, OnlyCapManyTransfersShipChunksAtOnce) {
    SnapshotTransferBudget budget(2);
    std::vector<std::unique_ptr<Group>> groups;
    for (int i = 0; i < 6; ++i)
        groups.push_back(std::make_unique<Group>(1, &budget));

    int shipping = 0, queued = 0;
    for (auto& g : groups) {
        RaftNode::Ready rd = g->askForSnapshot();
        if (chunkIn(rd) != nullptr)
            ++shipping;
        else
            ++queued;
    }

    EXPECT_EQ(shipping, 2) << "the cap is what decides how many ship, not how many asked";
    EXPECT_EQ(queued, 4);
    EXPECT_EQ(budget.active(), 2u);
    EXPECT_EQ(budget.waiting(), 4u);
    // A queued group still has a transfer RECORD, so the heartbeat cannot start a second
    // one for the same peer -- the pre-existing flow control is unchanged by queueing.
    for (auto& g : groups)
        EXPECT_TRUE(g->node().snapshotTransferInFlight(Group::kPeer));
    uint64_t deferred = 0;
    for (auto& g : groups)
        deferred += g->node().snapshotTransfersDeferred();
    EXPECT_EQ(deferred, 4u);
}

// ---------------------------------------------------------------------------
// 2. Fairness
// ---------------------------------------------------------------------------

TEST(RaftSnapshotBudgetTest, AFreedSlotGoesToTheOldestWaiterNotTheFirstToAsk) {
    SnapshotTransferBudget budget(1);
    Group a(1, &budget), b(2, &budget), c(3, &budget);

    ASSERT_NE(chunkIn(a.askForSnapshot()), nullptr) << "a asked first and there was a slot";
    ASSERT_EQ(chunkIn(b.askForSnapshot()), nullptr);
    ASSERT_EQ(chunkIn(c.askForSnapshot()), nullptr);
    ASSERT_EQ(budget.waiting(), 2u);

    // `a` finishes and gives the slot back.
    a.reportInstalled();
    EXPECT_EQ(budget.active(), 0u);

    // Now tick the waiters in the REVERSE of the order they queued in -- which is what a
    // registry ticking groups in map order would do to a queue that was filled in some
    // other order. `c` must NOT take the slot: `b` has been waiting longer.
    c.node().tick(4);
    RaftNode::Ready rc = drain(c.node());
    EXPECT_EQ(chunkIn(rc), nullptr) << "c jumped the queue -- FIFO is what stops a group starving forever";

    b.node().tick(4);
    RaftNode::Ready rb = drain(b.node());
    EXPECT_NE(chunkIn(rb), nullptr) << "the oldest waiter takes the freed slot";
    EXPECT_EQ(budget.active(), 1u);
    EXPECT_EQ(budget.waiting(), 1u);

    // ...and once `b` is done, `c` gets its turn -- nobody is starved, they are ordered.
    b.reportInstalled();
    c.node().tick(4);
    EXPECT_NE(chunkIn(drain(c.node())), nullptr);
    EXPECT_EQ(budget.waiting(), 0u);
}

// ---------------------------------------------------------------------------
// 3. Nothing leaks
// ---------------------------------------------------------------------------

TEST(RaftSnapshotBudgetTest, EveryPathThatDropsATransferGivesTheBudgetBack) {
    // COMPLETION.
    {
        SnapshotTransferBudget budget(1);
        Group g(1, &budget);
        ASSERT_NE(chunkIn(g.askForSnapshot()), nullptr);
        g.reportInstalled();
        EXPECT_EQ(budget.active(), 0u);
        EXPECT_EQ(budget.waiting(), 0u);
    }
    // ABANDONMENT after the resend budget is spent (an ACTIVE transfer).
    {
        SnapshotTransferBudget budget(1);
        Group g(1, &budget);
        ASSERT_NE(chunkIn(g.askForSnapshot()), nullptr);
        for (int i = 0; i < 12; ++i) {
            g.node().tick(4);  // > snapshotChunkTimeout, with no reply
            drain(g.node());
        }
        EXPECT_GT(g.node().snapshotTransfersAbandoned(), 0u);
        // The same tick's heartbeat starts a FRESH transfer (D-5: abandoning leaves the
        // peer exactly as far behind as it is), so the live count is 1 again -- and that
        // is what makes this a leak test worth having: had the abandonment kept its slot,
        // the restart would have found the cap full and QUEUED instead.
        EXPECT_TRUE(g.node().snapshotTransferInFlight(Group::kPeer));
        EXPECT_EQ(budget.active(), 1u) << "the restarted transfer holds the slot the abandoned one gave back";
        EXPECT_EQ(budget.waiting(), 0u);
    }
    // ROLE CHANGE, with the transfer still QUEUED -- the leak that would matter most,
    // since a cancelled ticket nobody reclaims blocks the whole shard's queue.
    {
        SnapshotTransferBudget budget(1);
        Group holder(1, &budget), waiter(2, &budget);
        ASSERT_NE(chunkIn(holder.askForSnapshot()), nullptr);
        ASSERT_EQ(chunkIn(waiter.askForSnapshot()), nullptr);
        ASSERT_EQ(budget.waiting(), 1u);

        // A higher term arrives: the waiter steps down and its queued transfer is gone.
        AppendEntries ae;
        ae.term = waiter.node().currentTerm() + 5;
        ae.leaderId = 77;
        ae.prevLogIndex = kNoIndex;
        ae.prevLogTerm = kNoTerm;
        waiter.node().step(Message{.to = waiter.node().id(), .from = 77, .payload = ae});
        drain(waiter.node());
        EXPECT_FALSE(waiter.node().isLeader());
        EXPECT_EQ(budget.waiting(), 0u) << "a ticket left in the queue head-of-line blocks every other group";
    }
    // RE-COMPACTION drops live transfers (D-5 F3), and must return the slot too.
    {
        SnapshotTransferBudget budget(1);
        Group g(1, &budget);
        ASSERT_NE(chunkIn(g.askForSnapshot()), nullptr);
        ASSERT_EQ(budget.active(), 1u);
        g.node().propose("more");
        drain(g.node());
        g.node().compact(g.node().commitIndex(), payloadOf(24));
        drain(g.node());
        EXPECT_EQ(budget.active(), 0u);
        EXPECT_EQ(budget.waiting(), 0u);
    }
    // THE PEER TURNS OUT NOT TO NEED A SNAPSHOT: it answers the keep-alive probe with a
    // success, so the queued transfer is pointless and its ticket must not be kept.
    {
        SnapshotTransferBudget budget(1);
        Group holder(1, &budget), waiter(2, &budget);
        ASSERT_NE(chunkIn(holder.askForSnapshot()), nullptr);
        ASSERT_EQ(chunkIn(waiter.askForSnapshot()), nullptr);
        AppendEntriesReply ok;
        ok.term = waiter.node().currentTerm();
        ok.success = true;
        ok.matchIndex = waiter.node().servableSnapshot().index;
        waiter.node().step(Message{.to = waiter.node().id(), .from = Group::kPeer, .payload = ok});
        drain(waiter.node());
        EXPECT_FALSE(waiter.node().snapshotTransferInFlight(Group::kPeer));
        EXPECT_EQ(budget.waiting(), 0u);
    }
}

// ---------------------------------------------------------------------------
// 4. A deferred peer still hears from its leader
// ---------------------------------------------------------------------------

TEST(RaftSnapshotBudgetTest, ADeferredPeerGetsAKeepAliveProbeInsteadOfSilence) {
    SnapshotTransferBudget budget(1);
    Group holder(1, &budget), waiter(2, &budget);
    ASSERT_NE(chunkIn(holder.askForSnapshot()), nullptr);

    // The very first deferral answers immediately -- the peer must not wait a heartbeat
    // for its first sign of life.
    RaftNode::Ready first = waiter.askForSnapshot();
    ASSERT_EQ(chunkIn(first), nullptr);
    const AppendEntries* probe = appendIn(first);
    ASSERT_NE(probe, nullptr) << "a deferred peer that hears NOTHING campaigns after an election timeout, and "
                                 "chunks are what its election clock lives on";
    // Anchored at the snapshot boundary -- the lowest index whose term we can still name.
    EXPECT_EQ(probe->prevLogIndex, waiter.node().servableSnapshot().index);
    EXPECT_EQ(probe->prevLogTerm, waiter.node().servableSnapshot().term);
    EXPECT_TRUE(probe->entries.empty()) << "a probe carries no entries: the follower cannot use them";

    // ...and it repeats on the HEARTBEAT cadence while the transfer stays queued.
    waiter.node().tick(1);
    EXPECT_EQ(appendIn(drain(waiter.node())), nullptr) << "not every tick -- that would be a message storm";
    waiter.node().tick(1);
    EXPECT_NE(appendIn(drain(waiter.node())), nullptr) << "heartbeatTimeout is 2 ticks";
    EXPECT_EQ(budget.waiting(), 1u) << "probing must not consume the queue slot";
}

// ---------------------------------------------------------------------------
// 5. No budget == the pre-D-37 behaviour
// ---------------------------------------------------------------------------

TEST(RaftSnapshotBudgetTest, WithNoBudgetEveryGroupStartsImmediately) {
    std::vector<std::unique_ptr<Group>> groups;
    for (int i = 0; i < 6; ++i)
        groups.push_back(std::make_unique<Group>(1, nullptr));
    for (auto& g : groups) {
        EXPECT_NE(chunkIn(g->askForSnapshot()), nullptr);
        EXPECT_EQ(g->node().snapshotTransfersDeferred(), 0u);
    }
}
