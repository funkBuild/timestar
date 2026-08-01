// Integration M3 (snapshots): the full consumer chain. EngineLocalStore::
// buildVShardSnapshot produces a self-contained payload; EngineDataStateMachine::
// applySnapshot decodes it and installs it into a FRESH replica's Engine, and a
// query reproduces the data. A malformed payload is fail-stop. The series is
// chosen so its VShard maps to core 0, keeping the internal invoke_on(assignCore)
// inline (no cross-shard payload transfer) exactly as the real apply path -- which
// runs ON the VShard's core -- guarantees. Source and dest engines are scoped so
// only one exists at a time.
#include "../../../lib/cluster/data/snapshot_payload.hpp"
#include "../../../lib/cluster/integration/engine_data_state_machine.hpp"
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"           // assignCore
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

// A float series whose VShard maps to core 0 under the current smp::count, so the
// snapshot build/install (which invoke_on assignCore(vshard)) run inline on shard 0.
data::WriteSeries seriesOnCore0(std::string* outKey, SeriesId128* outSid, VShardId* outVs) {
    for (int i = 0;; ++i) {
        std::string key = buildSeriesKey("snapsm", {{"host", "h" + std::to_string(i)}}, "value");
        const auto sid = SeriesId128::fromSeriesKey(key);
        const uint16_t vs = virtualShard(sid);
        if (assignCore(VShardId{vs}, seastar::smp::count) == 0) {
            *outKey = key;
            *outSid = sid;
            *outVs = VShardId{vs};
            data::WriteSeries s;
            s.seriesKey = key;
            s.type = TSMValueType::Float;
            s.timestamps = {BASE, BASE + 1000};
            s.values = std::vector<double>{7.0, 8.0};
            // Snapshot production compacts at maxFlushedRevision - 1. Keep the
            // payload and the test's Raft snapshot index (42) on that exact
            // production boundary.
            s.revisions = {43, 43};
            return s;
        }
    }
}

data::WriteSeries deletedSeriesOnVShard(VShardId vshard, std::string* outKey) {
    for (int i = 0;; ++i) {
        std::string key = buildSeriesKey("snapsm_deleted", {{"host", "gone" + std::to_string(i)}}, "value");
        if (virtualShard(SeriesId128::fromSeriesKey(key)) != vshard.value())
            continue;
        *outKey = key;
        data::WriteSeries series;
        series.seriesKey = std::move(key);
        series.type = TSMValueType::Float;
        series.timestamps = {BASE, BASE + 1000};
        series.values = std::vector<double>{70.0, 80.0};
        series.revisions = {43, 43};
        return series;
    }
}
}  // namespace

TEST(EngineSnapshotApply, ApplySnapshotInstallsDataOnFreshReplica) {
    seastar::thread([] {
        std::filesystem::remove_all("snapsm_src");
        std::filesystem::remove_all("snapsm_dst");

        std::string key;
        std::string deletedKey;
        SeriesId128 sid;
        VShardId vs{0};
        data::SnapshotPayload payload;

        // --- SOURCE: write + flush + build snapshot (engine destroyed before dest) ---
        {
            ScopedShardedEngine srcEng;
            srcEng.startAt("snapsm_src");
            (*srcEng)
                .invoke_on_all([](Engine& e) {
                    e.setRevisionAssignment(true);
                    return seastar::make_ready_future<>();
                })
                .get();
            cluster::EngineLocalStore srcStore(*srcEng);

            data::WriteBatch b;
            b.series.push_back(seriesOnCore0(&key, &sid, &vs));
            b.series.push_back(deletedSeriesOnVShard(vs, &deletedKey));
            srcStore.applyWrites(std::move(b)).get();

            (*srcEng).invoke_on_all([](Engine& e) { return e.rolloverMemoryStore(); }).get();
            for (int i = 0; i < 300; ++i) {
                auto n = (*srcEng).invoke_on(0u, [](Engine& e) { return e.getTSMFileCount(); }).get();
                if (n > 0)
                    break;
                seastar::sleep(std::chrono::milliseconds(100)).get();
            }
            ASSERT_TRUE(srcStore.applyDelete(deletedKey, 0, UINT64_MAX).get())
                << "the source tombstone must be durable before snapshot materialisation";
            payload = srcStore.buildVShardSnapshot(vs).get();
        }
        ASSERT_FALSE(payload.files.empty()) << "flushed data must be shipped";

        // --- DEST: fresh replica installs via the state machine ---
        {
            ScopedShardedEngine dstEng;
            dstEng.startAt("snapsm_dst");
            cluster::EngineLocalStore dstStore(*dstEng);
            cluster::EngineDataStateMachine sm(dstStore, vs);

            raft::Snapshot snap;
            snap.index = 42;
            snap.data = data::encodeSnapshotPayload(payload);
            sm.applySnapshot(snap).get();
            EXPECT_EQ(sm.appliedIndex(), 42u) << "applied watermark advances to the snapshot index";

            // The data is now queryable by seriesId on the VShard's core (0).
            auto resultOpt =
                (*dstEng).invoke_on(0u, [key, sid](Engine& e) { return e.query(key, sid, 0, UINT64_MAX); }).get();
            ASSERT_TRUE(resultOpt.has_value());
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps, (std::vector<uint64_t>{BASE, BASE + 1000}));
            ASSERT_EQ(result.values.size(), 2u);
            EXPECT_DOUBLE_EQ(result.values[0], 7.0);
            EXPECT_DOUBLE_EQ(result.values[1], 8.0);

            // The catalog/index is part of the snapshot: normal discovery by
            // measurement+tags+field must work on an empty data directory,
            // without carrying a precomputed SeriesId from the donor.
            auto discovered =
                (*dstEng)
                    .invoke_on(0u,
                               [key](Engine& e) {
                                   auto parsed = TimeStarInsert<double>::fromSeriesKey(key);
                                   return e.queryBySeries(std::move(parsed.measurement), std::move(parsed.tags),
                                                          std::move(parsed.field), 0, UINT64_MAX);
                               })
                    .get();
            auto& discoveredFloat = std::get<QueryResult<double>>(discovered);
            EXPECT_EQ(discoveredFloat.timestamps, (std::vector<uint64_t>{BASE, BASE + 1000}));

            // The payload carries the resolved logical view, not the donor's
            // raw TSM plus an out-of-band tombstone sidecar. A completely
            // deleted series must therefore be absent from both installed data
            // and the rebuilt discovery catalog on a truly empty receiver.
            auto deleted =
                (*dstEng)
                    .invoke_on(0u,
                               [deletedKey](Engine& e) {
                                   auto parsed = TimeStarInsert<double>::fromSeriesKey(deletedKey);
                                   return e.queryBySeries(std::move(parsed.measurement), std::move(parsed.tags),
                                                          std::move(parsed.field), 0, UINT64_MAX);
                               })
                    .get();
            const auto& deletedFloat = std::get<QueryResult<double>>(deleted);
            EXPECT_TRUE(deletedFloat.timestamps.empty());
            EXPECT_TRUE(deletedFloat.values.empty());

            // The separately transported Raft snapshot metadata must describe
            // this exact payload boundary, even when both envelopes are valid.
            raft::Snapshot wrongBoundary;
            wrongBoundary.index = 41;
            wrongBoundary.data = data::encodeSnapshotPayload(payload);
            EXPECT_THROW(sm.applySnapshot(wrongBoundary).get(), std::runtime_error);

            // Fail-stop: a malformed payload must throw, not silently drop state.
            raft::Snapshot bad;
            bad.index = 43;
            bad.data = "this is not a valid snapshot payload";
            EXPECT_THROW(sm.applySnapshot(bad).get(), std::runtime_error);
        }

        std::filesystem::remove_all("snapsm_src");
        std::filesystem::remove_all("snapsm_dst");
    })
        .join()
        .get();
}
