// THE READ FENCE'S POLICY (debt D-36), which decides whether a node may answer a query
// at all: it must have applied everything it had already committed, or the answer omits
// acknowledged writes while reporting success.
//
// This is the half of the fence that is subtle, and it is extracted from
// ReplicatedVShardHost precisely so it can be tested without a live 3-node cluster --
// the first version of the fence shipped with ZERO unit coverage and a hole big enough
// to drive a whole recovered log through (see NoCurrentTermCommit below).
#include "../../../lib/cluster/integration/apply_fence.hpp"

#include <gtest/gtest.h>

#include <map>

using namespace timestar::cluster;

namespace {

// A settable world of groups, so a test can move one watermark at a time.
struct World {
    std::map<uint16_t, FenceGroupState> groups;
    FenceGroupState operator()(uint16_t id) const {
        auto it = groups.find(id);
        return it == groups.end() ? FenceGroupState{} : it->second;
    }
};

FenceGroupState caughtUp(uint64_t at = 10) {
    return FenceGroupState{/*hosted=*/true, /*hasCurrentTermCommit=*/true, /*commitIndex=*/at, /*appliedIndex=*/at};
}
FenceGroupState behind(uint64_t commit, uint64_t applied) {
    return FenceGroupState{true, true, commit, applied};
}

}  // namespace

// ---------------------------------------------------------------------------
// THE FAST PATH. A node that has applied everything it committed enrolls nothing, so a
// healthy cluster never suspends and never pays for the fence. Without this the fence
// would be a latency tax on every read, and the counter it is built on would be crying
// wolf -- which is the failure mode that makes people delete fences.
// ---------------------------------------------------------------------------
TEST(ApplyFenceTest, ACaughtUpNodeEnrollsNothing) {
    ApplyFencePolicy fence;
    for (uint16_t g = 1; g <= 5; ++g)
        fence.enroll(g, caughtUp(100 + g));
    EXPECT_TRUE(fence.clear()) << "a node with no lag must not wait at all";
    EXPECT_EQ(fence.pendingCount(), 0u);
}

// ---------------------------------------------------------------------------
// REASON 1: committed past applied. The bar is the ENTRY-TIME commit index, and it must
// STAY there -- a bar that tracked the live commit index would never be reached on a node
// taking writes, which is a livelock rather than a bound. A query owes the caller every
// write acknowledged BEFORE it started and nothing that races it.
// ---------------------------------------------------------------------------
TEST(ApplyFenceTest, TheBarIsTheEntryTimeCommitAndDoesNotChase) {
    ApplyFencePolicy fence;
    fence.enroll(7, behind(/*commit=*/50, /*applied=*/40));
    ASSERT_FALSE(fence.clear());
    EXPECT_EQ(fence.barFor(7), std::optional<uint64_t>(50)) << "the bar is sampled once, at entry";

    World w;
    // Writes keep arriving (commit climbs to 90) and apply reaches only the ORIGINAL bar.
    w.groups[7] = behind(/*commit=*/90, /*applied=*/50);
    EXPECT_TRUE(fence.refresh(w)) << "reaching the entry-time bar is enough; chasing the live commit index would "
                                     "never terminate on a node under continuous ingest";
}

TEST(ApplyFenceTest, AGroupStillBelowItsBarKeepsTheFenceClosed) {
    ApplyFencePolicy fence;
    fence.enroll(7, behind(50, 40));
    World w;
    w.groups[7] = behind(50, 49);
    EXPECT_FALSE(fence.refresh(w)) << "one index short of the bar is still short";
    EXPECT_EQ(fence.pendingCount(), 1u);
    w.groups[7] = behind(50, 50);
    EXPECT_TRUE(fence.refresh(w));
}

// ---------------------------------------------------------------------------
// REASON 2 -- THE CRITICAL ONE, and the hole the first version of this fence shipped with.
//
// commitIndex is NOT persisted, so a restarted node starts at its snapshot boundary; and
// `maybeAdvanceCommitAsLeader` refuses to raise commitIndex until the new leader's no-op
// reaches a majority, while `becomeLeader` already reports the node as leader. In that
// window commit == applied and a fence built only on "commit minus applied" sees a lag of
// ZERO on a group holding an entire unapplied recovered log -- and lets the read through
// with HTTP 200. It is not a restart-only edge: it reopens on EVERY leadership change.
//
// NEGATIVE CONTROL: delete the `hasCurrentTermCommit` branch from enroll() and this test
// fails on its first assertion -- the group enrolls as caught up.
// ---------------------------------------------------------------------------
TEST(ApplyFenceTest, AGroupWithNoCurrentTermCommitIsPendingEvenThoughItLooksCaughtUp) {
    ApplyFencePolicy fence;
    // Exactly the post-restart / freshly-elected shape: commit == applied == the snapshot
    // boundary, so the naive lag is 0, but nothing has been committed in this term.
    FenceGroupState fresh{/*hosted=*/true, /*hasCurrentTermCommit=*/false, /*commitIndex=*/12, /*appliedIndex=*/12};
    fence.enroll(3, fresh);

    ASSERT_FALSE(fence.clear()) << "commit == applied is NOT caught up when commitIndex proves nothing -- this is the "
                                   "window a freshly elected leader answers out of a whole unapplied log in";
    EXPECT_EQ(fence.barFor(3), std::nullopt) << "there is no meaningful bar to sample yet";

    World w;
    // Still no current-term commit: the no-op has not reached a majority.
    w.groups[3] = fresh;
    EXPECT_FALSE(fence.refresh(w));

    // The no-op commits: commitIndex jumps to the real end of the recovered log, and the
    // bar is sampled THEN.
    w.groups[3] = FenceGroupState{true, true, /*commit=*/900, /*applied=*/12};
    EXPECT_FALSE(fence.refresh(w)) << "the bar is now real and this node is 888 entries below it";
    EXPECT_EQ(fence.barFor(3), std::optional<uint64_t>(900)) << "sampled at the moment a current-term commit appeared";

    w.groups[3] = FenceGroupState{true, true, 900, 900};
    EXPECT_TRUE(fence.refresh(w));
}

// ---------------------------------------------------------------------------
// DE-ENROLMENT: a group that leaves this node (movement) cannot hold its read back --
// it is no longer part of this node's answer. Without this a moved-away group would fail
// every read on the node it left, permanently.
// ---------------------------------------------------------------------------
TEST(ApplyFenceTest, AGroupThatLeavesTheNodeStopsHoldingTheFence) {
    ApplyFencePolicy fence;
    fence.enroll(4, behind(50, 10));
    fence.enroll(5, behind(50, 10));
    World w;
    w.groups[4] = FenceGroupState{};  // no longer hosted
    w.groups[5] = behind(50, 50);
    EXPECT_TRUE(fence.refresh(w)) << "an un-hosted group is dropped, not waited on forever";
}

TEST(ApplyFenceTest, AnUnhostedGroupIsNeverEnrolled) {
    ApplyFencePolicy fence;
    FenceGroupState notMine{};  // hosted = false
    notMine.commitIndex = 999;
    fence.enroll(9, notMine);
    EXPECT_TRUE(fence.clear()) << "a VShard this node does not host says nothing about its own answer";
}

// ---------------------------------------------------------------------------
// BUDGET EXPIRY / FAIL-CLOSED: the policy keeps reporting pending for as long as a group
// is behind, so the caller's deadline is what ends the wait -- and it ends it by FAILING,
// which is what turns a silent short answer into a QUERY_INCOMPLETE. Pinned here as the
// policy's half of that contract: it must never spontaneously "give up" and report clear.
// ---------------------------------------------------------------------------
TEST(ApplyFenceTest, ThePolicyNeverGivesUpOnItsOwn) {
    ApplyFencePolicy fence;
    fence.enroll(1, behind(100, 0));
    World w;
    w.groups[1] = behind(100, 0);
    for (int pass = 0; pass < 500; ++pass)
        ASSERT_FALSE(fence.refresh(w)) << "pass " << pass
                                       << ": a stuck group must stay pending so the CALLER's budget decides, and the "
                                          "read fails closed rather than quietly succeeding";
    EXPECT_EQ(fence.pendingCount(), 1u);
    EXPECT_EQ(fence.pendingGroups(), (std::vector<uint16_t>{1})) << "and the fail-closed log line can name it";
}

// A mixed node: most groups fine, one behind for each reason. The fence is only as open
// as its worst group.
TEST(ApplyFenceTest, TheFenceIsOnlyAsOpenAsItsWorstGroup) {
    ApplyFencePolicy fence;
    fence.enroll(1, caughtUp());
    fence.enroll(2, behind(50, 10));                        // reason 1
    fence.enroll(3, FenceGroupState{true, false, 12, 12});  // reason 2
    fence.enroll(4, caughtUp());
    EXPECT_EQ(fence.pendingCount(), 2u) << "the caught-up groups must not be enrolled";
    EXPECT_EQ(fence.pendingGroups(), (std::vector<uint16_t>{2, 3}));

    World w;
    w.groups[2] = behind(50, 50);
    w.groups[3] = FenceGroupState{true, false, 12, 12};
    EXPECT_FALSE(fence.refresh(w)) << "one group short is the whole node short";
    EXPECT_EQ(fence.pendingGroups(), (std::vector<uint16_t>{3}));

    w.groups[3] = FenceGroupState{true, true, 12, 12};
    EXPECT_TRUE(fence.refresh(w)) << "a group whose current-term commit lands already applied clears immediately";
}
