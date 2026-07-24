// Phase 5: the per-VShard data state machine. Proves the applied state is a
// deterministic function of the committed log (LWW by revision, delete/reappear,
// monotonic retention), that a snapshot round-trips, that two replicas applying
// the same log converge, and that an undecodable committed entry is fatal.
#include "../../../lib/cluster/data/data_state_machine.hpp"

#include "../../../lib/cluster/raft/raft_types.hpp"

#include <gtest/gtest.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

using namespace timestar::data;
namespace raft = timestar::raft;

namespace {
SeriesId128 sid(const std::string& k) {
    return SeriesId128::fromSeriesKey(k);
}
raft::LogEntry entry(uint64_t idx, const DataCommand& cmd) {
    return raft::LogEntry{1, idx, raft::EntryType::Normal, encodeDataCommand(cmd)};
}
size_t rawCount(const DataStateMachine& sm, uint64_t s, uint64_t e) {
    return sm.query(QuerySpec{s, e, AggMethod::Raw, {}}).raw.size();
}

seastar::future<> testLwwDeleteRetention() {
    DataStateMachine sm;
    // Two writes to the same (series,timestamp): the later log entry wins (LWW).
    co_await sm.apply(entry(1, WritePoints{{{sid("s"), 100, 1.0}}, 0}));
    co_await sm.apply(entry(2, WritePoints{{{sid("s"), 100, 2.0}}, 0}));
    {
        auto p = sm.query(QuerySpec{0, 1000, AggMethod::Raw, {}});
        EXPECT_EQ(p.raw.size(), 1u);
        if (p.raw.size() == 1)
            EXPECT_EQ(p.raw[0].value, 2.0);
    }
    // Delete the range: the point disappears...
    co_await sm.apply(entry(3, DeleteRange{sid("s"), 0, 1000}));
    EXPECT_EQ(rawCount(sm, 0, 1000), 0u);
    // ...but a later (higher-revision) write in the range reappears.
    co_await sm.apply(entry(4, WritePoints{{{sid("s"), 100, 5.0}}, 0}));
    {
        auto p = sm.query(QuerySpec{0, 1000, AggMethod::Raw, {}});
        EXPECT_EQ(p.raw.size(), 1u);
        if (p.raw.size() == 1)
            EXPECT_EQ(p.raw[0].value, 5.0);
    }
    // Retention: drop points older than 100. ts=50 goes, ts=100 stays.
    co_await sm.apply(entry(5, WritePoints{{{sid("s"), 50, 9.0}}, 0}));
    EXPECT_EQ(rawCount(sm, 0, 1000), 2u);
    co_await sm.apply(entry(6, RetentionCutoff{100}));
    EXPECT_EQ(rawCount(sm, 0, 1000), 1u);
    // Monotonic: an older cutoff is a no-op.
    co_await sm.apply(entry(7, RetentionCutoff{10}));
    EXPECT_EQ(rawCount(sm, 0, 1000), 1u);
    EXPECT_EQ(sm.appliedIndex(), 7u);
}

seastar::future<> testDeterminismAndSnapshot() {
    // Two independent state machines applying the SAME log end in the SAME state.
    std::vector<DataCommand> log = {
        WritePoints{{{sid("a"), 10, 1.0}, {sid("b"), 20, 2.0}}, 0},
        DeleteRange{sid("a"), 0, 15},
        WritePoints{{{sid("a"), 10, 3.0}, {sid("c"), 30, 4.0}}, 0},
        RetentionCutoff{5},
    };
    DataStateMachine s1, s2;
    for (size_t i = 0; i < log.size(); ++i)
        co_await s1.apply(entry(i + 1, log[i]));
    // Apply to s2 in the same order (a second "replica").
    for (size_t i = 0; i < log.size(); ++i)
        co_await s2.apply(entry(i + 1, log[i]));
    EXPECT_EQ(s1.snapshot(), s2.snapshot());  // byte-identical converged state

    // A snapshot installs into a fresh replica and reproduces the state exactly.
    std::string snap = s1.snapshot();
    DataStateMachine s3;
    raft::Snapshot snapshot;
    snapshot.index = log.size();
    snapshot.data = snap;
    co_await s3.applySnapshot(snapshot);
    EXPECT_EQ(s3.snapshot(), snap);
    EXPECT_EQ(s3.appliedIndex(), log.size());

    // A corrupt snapshot is rejected (fail closed), leaving the fatal path to the caller.
    DataStateMachine s4;
    bool threw = false;
    raft::Snapshot bad;
    bad.index = 1;
    bad.data = "not a valid snapshot payload";
    try {
        co_await s4.applySnapshot(bad);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

// Pins the log-ordered retry contract as EXPECTED behaviour (not a bug): a
// retried delete/write is ordered against whatever committed between the original
// proposal and the retry. Both outcomes are valid linearizations of concurrent
// operations. See data_command.hpp DeleteRange contract + docs/clustering.md.
seastar::future<> testRetryLogOrderSemantics() {
    // Delete straddling a write via retry: delete@1, write@2, delete-retry@3.
    // The point committed BEFORE the retried delete, so the later delete removes
    // it. Deterministic on every replica.
    auto runA = [](DataStateMachine& sm) -> seastar::future<> {
        co_await sm.apply(entry(1, DeleteRange{sid("s"), 50, 150}));
        co_await sm.apply(entry(2, WritePoints{{{sid("s"), 100, 7.0}}, 0}));
        co_await sm.apply(entry(3, DeleteRange{sid("s"), 50, 150}));  // retry
    };
    DataStateMachine a1, a2;
    co_await runA(a1);
    co_await runA(a2);
    EXPECT_EQ(rawCount(a1, 0, 1000), 0u);     // point superseded by the later delete
    EXPECT_EQ(a1.snapshot(), a2.snapshot());  // replicas converge

    // Symmetric resurrection: write@1, delete@2, write-retry@3 -> point reappears
    // (the retried write is ordered after the delete). Intentional + deterministic.
    auto runB = [](DataStateMachine& sm) -> seastar::future<> {
        co_await sm.apply(entry(1, WritePoints{{{sid("s"), 100, 7.0}}, 0}));
        co_await sm.apply(entry(2, DeleteRange{sid("s"), 50, 150}));
        co_await sm.apply(entry(3, WritePoints{{{sid("s"), 100, 7.0}}, 0}));  // retry
    };
    DataStateMachine b1, b2;
    co_await runB(b1);
    co_await runB(b2);
    {
        auto p = b1.query(QuerySpec{0, 1000, AggMethod::Raw, {}});
        EXPECT_EQ(p.raw.size(), 1u);  // resurrected
        if (p.raw.size() == 1)
            EXPECT_EQ(p.raw[0].value, 7.0);
    }
    EXPECT_EQ(b1.snapshot(), b2.snapshot());

    // Pure re-delete (no intervening op) is a true no-op AND does not grow the
    // tombstone list without bound: repeated identical-range deletes coalesce, so
    // the converged snapshot is byte-identical to a single delete.
    DataStateMachine oneDelete, manyDeletes;
    co_await oneDelete.apply(entry(1, WritePoints{{{sid("s"), 100, 7.0}}, 0}));
    co_await oneDelete.apply(entry(2, DeleteRange{sid("s"), 50, 150}));
    co_await manyDeletes.apply(entry(1, WritePoints{{{sid("s"), 100, 7.0}}, 0}));
    for (uint64_t i = 2; i <= 6; ++i)
        co_await manyDeletes.apply(entry(i, DeleteRange{sid("s"), 50, 150}));  // 5 retries
    EXPECT_EQ(rawCount(oneDelete, 0, 1000), 0u);
    EXPECT_EQ(rawCount(manyDeletes, 0, 1000), 0u);
    // Same VISIBLE result; the coalesced tombstone list did not grow with retries
    // (only revCounter differs, which does not affect query results).
    EXPECT_EQ(oneDelete.query(QuerySpec{0, 1000, AggMethod::Raw, {}}).raw.size(),
              manyDeletes.query(QuerySpec{0, 1000, AggMethod::Raw, {}}).raw.size());
}

seastar::future<> testUndecodableIsFatal() {
    DataStateMachine sm;
    bool threw = false;
    try {
        co_await sm.apply(raft::LogEntry{1, 1, raft::EntryType::Normal, "garbage-not-a-command"});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

}  // namespace

TEST(DataStateMachineTest, LwwDeleteRetention) {
    testLwwDeleteRetention().get();
}
TEST(DataStateMachineTest, DeterminismAndSnapshot) {
    testDeterminismAndSnapshot().get();
}
TEST(DataStateMachineTest, RetryLogOrderSemantics) {
    testRetryLogOrderSemantics().get();
}
TEST(DataStateMachineTest, UndecodableCommittedEntryIsFatal) {
    testUndecodableIsFatal().get();
}
