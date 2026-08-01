// Integration M3: EngineDataStateMachine applies a replicated command log through the
// REAL Engine. Proves apply() lands a WriteBatch (with log-index-derived revisions)
// visibly, a DeleteRangeKey removes it, log-ordered LWW holds (a higher-index write
// wins), and an undecodable committed entry is fail-stop.
#include "../../../lib/cluster/integration/engine_data_state_machine.hpp"

#include "../../../lib/core/placement_table.hpp"  // routeToCore
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <seastar/core/thread.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class EngineDataStateMachineTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

raft::LogEntry writeEntry(uint64_t index, const std::string& key, double value) {
    data::WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {BASE};
    s.values = std::vector<double>{value};
    data::WriteBatch b;
    b.series = {std::move(s)};
    raft::LogEntry e;
    e.index = index;
    e.type = raft::EntryType::Normal;
    e.data = data::encodeReplicatedCommand(data::ReplicatedCommand{std::move(b)});
    return e;
}

raft::LogEntry deleteEntry(uint64_t index, data::DeleteRangeKey command) {
    raft::LogEntry entry;
    entry.index = index;
    entry.type = raft::EntryType::Normal;
    entry.data = data::encodeReplicatedCommand(data::ReplicatedCommand{std::move(command)});
    return entry;
}

raft::LogEntry deleteBatchEntry(uint64_t index, data::DeleteRangeBatch command) {
    raft::LogEntry entry;
    entry.index = index;
    entry.type = raft::EntryType::Normal;
    entry.data = data::encodeReplicatedCommand(data::ReplicatedCommand{std::move(command)});
    return entry;
}

std::string keyOnVShard(const std::string& measurement, uint16_t vshard) {
    for (unsigned i = 0;; ++i) {
        auto key = buildSeriesKey(measurement, {{"host", "h" + std::to_string(i)}}, "value");
        if (timestar::virtualShard(SeriesId128::fromSeriesKey(key)) == vshard)
            return key;
    }
}

double latest(seastar::sharded<Engine>& eng, const std::string& m, const std::string& f) {
    http::HttpQueryHandler h(&eng);
    QueryRequest q;
    q.aggregation = AggregationMethod::LATEST;
    q.measurement = m;
    q.fields = {f};
    q.startTime = BASE - 1'000'000'000ULL;
    q.endTime = BASE + 1'000'000'000ULL;
    auto r = h.executeQuery(q).get();
    if (!r.success || r.series.empty())
        return -1;
    auto* v = std::get_if<std::vector<double>>(&r.series[0].fields.at(f).second);
    return (v && !v->empty()) ? (*v)[0] : -1;
}
}  // namespace

TEST_F(EngineDataStateMachineTest, AppliesWriteDeleteAndLwwFromLog) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        const std::string key = buildSeriesKey("temp", {{"host", "h1"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{vshard});

        // Entry 5: write 10.0.
        sm.apply(writeEntry(5, key, 10.0)).get();
        EXPECT_EQ(sm.appliedIndex(), 5u);
        EXPECT_DOUBLE_EQ(latest(*eng, "temp", "value"), 10.0);

        // Entry 9: overwrite same (series, timestamp) with 20.0 -- higher log index
        // wins LWW.
        sm.apply(writeEntry(9, key, 20.0)).get();
        EXPECT_EQ(sm.appliedIndex(), 9u);
        EXPECT_DOUBLE_EQ(latest(*eng, "temp", "value"), 20.0);

        // Entry 12: delete the range -> gone.
        data::DeleteRangeKey d{key, BASE - 1, BASE + 1};
        raft::LogEntry del;
        del.index = 12;
        del.type = raft::EntryType::Normal;
        del.data = data::encodeReplicatedCommand(data::ReplicatedCommand{d});
        sm.apply(std::move(del)).get();
        EXPECT_EQ(sm.appliedIndex(), 12u);
        EXPECT_DOUBLE_EQ(latest(*eng, "temp", "value"), -1);  // absent
    }).get();
}

TEST_F(EngineDataStateMachineTest, IdempotentDeleteRetryCannotEraseAnInterveningWrite) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        const std::string key = buildSeriesKey("delete_retry", {{"host", "h1"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{vshard});
        const SeriesId128 operationId = SeriesId128::fromHex("123456789abcdef0123456789abcdef0");
        data::DeleteRangeKey command{key, BASE - 1, BASE + 1, operationId};

        sm.apply(writeEntry(5, key, 10.0)).get();
        sm.apply(deleteEntry(9, command)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_retry", "value"), -1);

        sm.apply(writeEntry(10, key, 20.0)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_retry", "value"), 20.0);
        sm.apply(deleteEntry(12, command)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_retry", "value"), 20.0)
            << "a client retry physically deleted a write ordered after the first attempt";

        const auto receipts = sm.deleteReceiptsThrough(9);
        ASSERT_EQ(receipts.size(), 1u);
        EXPECT_EQ(receipts[0].operationId, operationId);
        EXPECT_EQ(receipts[0].appliedIndex, 9u);
        EXPECT_TRUE(sm.deleteReceiptsThrough(8).empty());

        auto conflicting = command;
        conflicting.endTime = BASE + 2;
        EXPECT_THROW(sm.apply(deleteEntry(13, std::move(conflicting))).get(), std::runtime_error)
            << "operation-ID reuse for another target must fail-stop";
    }).get();
}

TEST_F(EngineDataStateMachineTest, RestoredDeleteReceiptProtectsPostSnapshotWrites) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        const std::string key = buildSeriesKey("delete_snapshot_retry", {{"host", "h1"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{vshard});
        const SeriesId128 operationId = SeriesId128::fromHex("abcdef0123456789abcdef0123456789");
        data::DeleteRangeKey command{key, BASE - 1, BASE + 1, operationId};
        sm.restoreDeleteReceipts({{operationId, 7, data::deleteRangeCommandHash(command)}}, 7);

        sm.apply(writeEntry(8, key, 30.0)).get();
        sm.apply(deleteEntry(9, command)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_snapshot_retry", "value"), 30.0)
            << "a retry after snapshot compaction forgot its durable receipt";
    }).get();
}

TEST_F(EngineDataStateMachineTest, OneBatchReceiptProtectsEveryTargetFromRetry) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        const std::string first = buildSeriesKey("delete_batch_a", {{"host", "h1"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(first));
        const std::string second = keyOnVShard("delete_batch_b", vshard);
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{vshard});
        data::DeleteRangeBatch command;
        command.operationId = SeriesId128::fromHex("1029384756abcdef1029384756abcdef");
        command.targets = {{first, BASE, BASE}, {second, BASE, BASE}};
        std::sort(command.targets.begin(), command.targets.end());

        sm.apply(writeEntry(3, first, 10.0)).get();
        sm.apply(writeEntry(4, second, 11.0)).get();
        sm.apply(deleteBatchEntry(5, command)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_batch_a", "value"), -1);
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_batch_b", "value"), -1);

        sm.apply(writeEntry(6, first, 20.0)).get();
        sm.apply(writeEntry(7, second, 21.0)).get();
        sm.apply(deleteBatchEntry(8, command)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_batch_a", "value"), 20.0);
        EXPECT_DOUBLE_EQ(latest(*eng, "delete_batch_b", "value"), 21.0);

        const auto receipts = sm.deleteReceiptsThrough(5);
        ASSERT_EQ(receipts.size(), 1u) << "one VShard batch must consume one durable receipt";
        EXPECT_EQ(receipts[0].operationId, command.operationId);
        EXPECT_EQ(receipts[0].commandHash, data::deleteRangeCommandHash(command));
    }).get();
}

TEST_F(EngineDataStateMachineTest, RejectsACommittedCommandForAnotherVShard) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        const std::string owned = buildSeriesKey("temp", {{"host", "owned"}}, "value");
        const uint16_t ownedVShard = timestar::virtualShard(SeriesId128::fromSeriesKey(owned));
        std::string foreign;
        for (unsigned i = 0; i < 4096; ++i) {
            foreign = buildSeriesKey("temp", {{"host", "foreign" + std::to_string(i)}}, "value");
            if (timestar::virtualShard(SeriesId128::fromSeriesKey(foreign)) != ownedVShard)
                break;
        }
        ASSERT_NE(timestar::virtualShard(SeriesId128::fromSeriesKey(foreign)), ownedVShard);

        cluster::EngineDataStateMachine sm(store, timestar::VShardId{ownedVShard});
        EXPECT_THROW(sm.apply(writeEntry(1, foreign, 5.0)).get(), std::runtime_error)
            << "cross-VShard commands must fail-stop instead of contaminating this group's state";
        EXPECT_DOUBLE_EQ(latest(*eng, "temp", "value"), -1);
    }).get();
}

TEST_F(EngineDataStateMachineTest, UndecodableCommittedEntryIsFailStop) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{0});

        raft::LogEntry bad;
        bad.index = 3;
        bad.type = raft::EntryType::Normal;
        bad.data = "not a valid command frame";
        bool threw = false;
        try {
            sm.apply(std::move(bad)).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "an undecodable committed entry must fail-stop, not be skipped";
    }).get();
}

// Regression for the review's HIGH-1: after a restart flips assignRevisions_ on, the
// Engine must NOT re-stamp the state machine's log-index revisions with its local
// per-shard counter -- doing so breaks the ADR-0003 "revision = log index, identical
// on every replica" contract. Observable: applying a log-index-revision write leaves
// the per-shard revision counter UNCHANGED (the stamp guard skipped it).
TEST_F(EngineDataStateMachineTest, AppliedRevisionsAreNotReStampedByEngineCounter) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        // Simulate the post-restart state: revision assignment enabled on every shard.
        eng->invoke_on_all([](Engine& e) {
               e.setRevisionAssignment(true);
               return seastar::make_ready_future<>();
           })
            .get();

        cluster::EngineLocalStore store(*eng);
        const std::string key = buildSeriesKey("temp", {{"host", "h1"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{vshard});
        const unsigned core = timestar::routeToCore(SeriesId128::fromSeriesKey(key));
        const uint64_t before = eng->invoke_on(core, [](Engine& e) { return e.nextRevision(); }).get();

        // Apply a committed entry at log index 77 -> the state machine stamps
        // revision = 77 on the point and passes it through.
        sm.apply(writeEntry(77, key, 42.5)).get();
        EXPECT_DOUBLE_EQ(latest(*eng, "temp", "value"), 42.5);

        const uint64_t after = eng->invoke_on(core, [](Engine& e) { return e.nextRevision(); }).get();
        // The local counter must be untouched: the log-index revision was honored, not
        // clobbered. (Pre-fix, insertBatch would have burned a counter value here.)
        EXPECT_EQ(after, before) << "Engine re-stamped a caller-provided (log-index) revision";
    }).get();
}
