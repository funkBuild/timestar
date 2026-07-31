// THE RECOVERY/APPLY INVARIANTS BEHIND THE ACK CONTRACT (debt D-36).
//
// An acknowledged write is durable at COMMIT and readable only at APPLY, and everything
// here is about the gap between those two moments. It was found the expensive way: after a
// heavy RF=3 write campaign, a kill -9 and a restart, `snapshot_durability_gate.sh` read
// back 173 of 200 acknowledged points -- and could not say whether the other 27 were LOST
// or merely not applied yet, because it read exactly once.
//
// They were not lost. `restart_readback_gate.sh` re-reads, and the count climbs to 200
// within ~10 s while `apply_lag_entries` falls to 0. What was actually wrong was three
// separate things, and this file pins the two that live below the cluster:
//
//   1. Apply is allowed to throw (`Engine::insertBatch` refuses while the shard's ingest
//      or compaction backlog is over its ceiling), and the throw used to propagate out of
//      `RaftGroupRegistry::tickAll` -- aborting the WHOLE pass. `groups_` is an ordered
//      map, so the same low group id aborted the pass at the same place every time and
//      every higher id was never ticked at all. Measured: 23 aborted passes, 16,511
//      group-ticks never taken.
//   2. Nothing counted or named the stall, so from outside the process it was
//      indistinguishable from data loss.
//
// The third -- a query answering HTTP 200 out of state behind its own committed log -- is
// the read fence in `ReplicatedVShardHost::awaitApplyCatchUp`, which needs a live Engine
// and is covered by the gate.
#include "../../../lib/cluster/raft/raft_group_registry.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <stdexcept>
#include <string>
#include <vector>

using namespace timestar::raft;
using namespace std::chrono_literals;

namespace {

class NoopPersistence : public RaftPersistence {
public:
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<LogEntry>) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistSnapshot(Snapshot, bool) override { return seastar::make_ready_future<>(); }
    seastar::future<> sync() override { return seastar::make_ready_future<>(); }
};

// A single-voter group needs no peers to commit, so nothing has to be delivered.
class NullTransport : public RaftTransport {
public:
    seastar::future<> send(Envelope) override { return seastar::make_ready_future<>(); }
};

// Refuses to apply while `refusing` is set -- the shape `Engine::insertBatch` has when the
// shard's ingest backlog is over its ceiling. The entry is NOT recorded when it refuses,
// which is what makes "did this group ever become readable" answerable.
class RefusingSM : public RaftStateMachine {
public:
    bool refusing = false;
    std::vector<std::string> applied;
    uint64_t refusals = 0;

    seastar::future<> apply(LogEntry e) override {
        if (refusing) {
            ++refusals;
            return seastar::make_exception_future<>(std::runtime_error("ingest backlog: refusing to apply"));
        }
        applied.push_back(e.data);
        return seastar::make_ready_future<>();
    }
    seastar::future<> applySnapshot(Snapshot) override { return seastar::make_ready_future<>(); }
};

// N single-voter groups on one registry, ids 1..N, so each one commits whatever it is
// proposed as soon as it is ticked -- and the ONLY thing that can stop group K making
// progress is something outside group K.
struct SoloGroups {
    NullTransport transport;
    std::vector<std::unique_ptr<NoopPersistence>> persistence;
    std::vector<std::unique_ptr<RefusingSM>> sms;
    std::unique_ptr<RaftGroupRegistry> registry;

    explicit SoloGroups(uint16_t n) {
        RaftOptions opts;
        opts.electionTimeoutMin = 2;
        opts.electionTimeoutMax = 3;
        opts.heartbeatTimeout = 1;
        registry = std::make_unique<RaftGroupRegistry>(transport, 1ms);
        // Hibernation off: it is irrelevant here and would only add a reason for a group
        // to miss a tick that is not the reason under test.
        registry->setHibernation(0);
        for (uint16_t g = 1; g <= n; ++g) {
            persistence.push_back(std::make_unique<NoopPersistence>());
            sms.push_back(std::make_unique<RefusingSM>());
            RaftNode node(/*id=*/1, /*voters=*/{1}, RaftLog{}, HardState{}, opts);
            registry->addGroup(g, std::move(node), *persistence.back(), *sms.back());
        }
    }

    RefusingSM& sm(uint16_t g) { return *sms[g - 1]; }
    RaftGroup& group(uint16_t g) { return *registry->group(g); }

    // Drive every group's timers by hand (no timer, so a test never depends on wall time).
    seastar::future<> tickAll(int passes) {
        for (int i = 0; i < passes; ++i)
            co_await registry->tickAllForTest();
    }
};

// ---------------------------------------------------------------------------
// 1. A GROUP WHOSE APPLY THROWS MUST NOT STOP THE OTHER GROUPS TICKING.
//
// NEGATIVE CONTROL: restore the `throw;` in tickAll's catch and this fails on the FIRST
// assertion -- group 3 never elects, never commits and never applies, because the pass
// dies at group 1 every single time.
// ---------------------------------------------------------------------------
seastar::future<> testAThrowingGroupDoesNotStarveTheOthers() {
    SoloGroups s(3);
    // ALL THREE refuse to start with, so every group ends up holding a committed entry it
    // has not applied -- i.e. every group needs a LATER TICK to make progress. That is
    // what makes this a test of the pass rather than of propose(): if the entries applied
    // inline during propose(), starving the pass would cost nothing and the test would
    // pass against the broken code.
    for (uint16_t g = 1; g <= 3; ++g)
        s.sm(g).refusing = true;

    co_await s.tickAll(10);  // elect (no committed entries yet, so nothing applies)
    EXPECT_TRUE(s.group(1).isLeader()) << "premise: a single voter elects itself";
    EXPECT_TRUE(s.group(3).isLeader());

    for (uint16_t g = 1; g <= 3; ++g) {
        try {
            (void)co_await s.group(g).propose("payload-" + std::to_string(g));
        } catch (const std::exception&) {
            // Expected: propose() drains, the drain applies, and the apply refuses. The
            // entry is committed and durable regardless -- which is the whole point.
        }
    }
    for (uint16_t g = 1; g <= 3; ++g)
        EXPECT_GT(s.group(g).applyLag(), 0u) << "premise: group " << g << " holds a committed, unapplied entry";

    // The backlog drains for groups 2 and 3. Group 1 stays refusing -- it is the group
    // whose tick throws, and under the old code it aborted the pass before 2 and 3 were
    // ever reached, every pass, because `groups_` is ordered by id.
    s.sm(2).refusing = false;
    s.sm(3).refusing = false;
    co_await s.tickAll(10);

    EXPECT_TRUE(s.sm(1).applied.empty()) << "premise: the still-refusing group really did not apply";
    EXPECT_GT(s.sm(1).refusals, 0u) << "premise: it actually threw";
    // THE INVARIANT.
    EXPECT_EQ(s.sm(2).applied.size(), 1u)
        << "group 2 was never ticked because group 1's apply threw and aborted the whole pass";
    EXPECT_EQ(s.sm(3).applied.size(), 1u)
        << "group 3 was never ticked because group 1's apply threw and aborted the whole pass";
    EXPECT_EQ(s.group(2).applyLag(), 0u);
    EXPECT_EQ(s.group(3).applyLag(), 0u);
    EXPECT_GT(s.registry->tickErrors(), 0u) << "the failure must be COUNTED, not merely survived";
}

// ---------------------------------------------------------------------------
// 2. THE STALL IS TEMPORARY AND SELF-HEALING: the refused entry is retried, and the group
//    catches up once the refusal clears. This is what makes "not readable" the right word
//    and "lost" the wrong one.
//
// NEGATIVE CONTROL: mark the entry applied before calling apply() (i.e. advance the Ready
// past a throw) and the retry never happens -- `applied` stays empty forever.
// ---------------------------------------------------------------------------
seastar::future<> testARefusedApplyIsRetriedAndCatchesUp() {
    SoloGroups s(1);
    s.sm(1).refusing = true;

    co_await s.tickAll(10);
    EXPECT_TRUE(s.group(1).isLeader());
    try {
        (void)co_await s.group(1).propose("acked-write");
    } catch (const std::exception&) {
        // Expected while refusing; the entry is committed and durable either way.
    }
    co_await s.tickAll(10);

    EXPECT_TRUE(s.sm(1).applied.empty()) << "premise: refused";
    EXPECT_GT(s.group(1).applyLag(), 0u)
        << "a committed-but-unapplied entry MUST show as apply lag -- that number is the only thing that "
           "distinguishes a node still catching up from one that lost the write";
    EXPECT_GT(s.group(1).applyFailures(), 0u) << "and the refusal itself must be counted";

    s.sm(1).refusing = false;  // the backlog drains
    co_await s.tickAll(10);

    EXPECT_EQ(s.sm(1).applied.size(), 1u) << "the entry was durable the whole time; it must be re-applied, not lost";
    EXPECT_EQ(s.sm(1).applied.front(), "acked-write");
    EXPECT_EQ(s.group(1).applyLag(), 0u) << "and the lag must clear once it applies";
}

// ---------------------------------------------------------------------------
// 3. THE LAG COUNTER MUST NOT CRY WOLF. A caught-up group reports zero, so a non-zero
//    reading is always a real gap. Without this, a fence built on applyLag() would fail
//    every read on a healthy cluster and the counter would be worse than useless.
// ---------------------------------------------------------------------------
seastar::future<> testACaughtUpGroupReportsNoLagAndNoFailures() {
    SoloGroups s(1);
    co_await s.tickAll(10);
    EXPECT_TRUE(s.group(1).isLeader());
    for (int i = 0; i < 5; ++i)
        (void)co_await s.group(1).propose("p" + std::to_string(i));
    co_await s.tickAll(10);

    EXPECT_EQ(s.sm(1).applied.size(), 5u);
    EXPECT_EQ(s.group(1).applyLag(), 0u) << "a group that has applied everything it committed reports no lag";
    EXPECT_EQ(s.group(1).applyFailures(), 0u);
    EXPECT_EQ(s.registry->tickErrors(), 0u) << "and a healthy pass records no tick error";
}

}  // namespace

TEST(ApplyStallTest, AThrowingGroupDoesNotStarveTheOthers) {
    testAThrowingGroupDoesNotStarveTheOthers().get();
}

TEST(ApplyStallTest, ARefusedApplyIsRetriedAndCatchesUp) {
    testARefusedApplyIsRetriedAndCatchesUp().get();
}

TEST(ApplyStallTest, ACaughtUpGroupReportsNoLagAndNoFailures) {
    testACaughtUpGroupReportsNoLagAndNoFailures().get();
}
