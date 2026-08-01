// Integration M3 (snapshots): the full consumer chain. EngineLocalStore::
// buildVShardSnapshot produces a self-contained payload; EngineDataStateMachine::
// applySnapshot decodes it and installs it into an Engine, and a query
// reproduces the data. Coverage below includes both fresh catch-up and live
// generation replacement. A malformed payload is fail-stop. The series is
// chosen so its VShard maps to core 0, keeping the internal invoke_on(assignCore)
// inline (no cross-shard payload transfer) exactly as the real apply path -- which
// runs ON the VShard's core -- guarantees. Source and dest engines are scoped so
// only one exists at a time.
#include "../../../lib/cluster/data/snapshot_payload.hpp"
#include "../../../lib/cluster/integration/engine_data_state_machine.hpp"
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"           // assignCore
#include "../../../lib/storage/series_catalog.hpp"
#include "../../../lib/storage/vshard_snapshot_builder.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <stdexcept>

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

struct TestSeries {
    data::WriteSeries write;
    std::string key;
    SeriesId128 id;
};

TestSeries floatSeriesOnVShard(std::string measurement, VShardId vshard, std::string tagStem, uint64_t timestampOffset,
                               double value, uint64_t revision) {
    for (uint64_t i = 0;; ++i) {
        std::string key = buildSeriesKey(measurement, {{"host", tagStem + std::to_string(i)}}, "value");
        const auto id = SeriesId128::fromSeriesKey(key);
        if (virtualShard(id) != vshard.value())
            continue;
        data::WriteSeries series;
        series.seriesKey = key;
        series.type = TSMValueType::Float;
        series.timestamps = {BASE + timestampOffset, BASE + timestampOffset + 1000};
        series.values = std::vector<double>{value, value + 1.0};
        series.revisions = {revision, revision};
        return TestSeries{std::move(series), std::move(key), id};
    }
}

VShardId anotherVShardOnCore(VShardId target) {
    const unsigned core = assignCore(target, seastar::smp::count);
    for (uint16_t value = 0; value < VIRTUAL_SHARD_COUNT; ++value) {
        VShardId candidate{value};
        if (candidate != target && assignCore(candidate, seastar::smp::count) == core)
            return candidate;
    }
    throw std::runtime_error("test requires two VShards on the target core");
}

void flushAndWaitForTsm(seastar::sharded<Engine>& engines, unsigned core) {
    engines.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
    for (int i = 0; i < 300; ++i) {
        if (engines.invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get() > 0)
            return;
        seastar::sleep(std::chrono::milliseconds(20)).get();
    }
    throw std::runtime_error("WAL conversion did not publish a TSM file");
}

struct FloatRead {
    bool found = false;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
};

FloatRead readFloat(seastar::sharded<Engine>& engines, const TestSeries& series) {
    const unsigned core = assignCore(VShardId{virtualShard(series.id)}, seastar::smp::count);
    auto result =
        engines
            .invoke_on(core, [&series](Engine& engine) { return engine.query(series.key, series.id, 0, UINT64_MAX); })
            .get();
    if (!result)
        return {};
    const auto* values = std::get_if<QueryResult<double>>(&*result);
    if (!values)
        throw std::runtime_error("test float series resolved to a different value type");
    return FloatRead{true, values->timestamps, values->values};
}

struct BuiltSnapshot {
    data::SnapshotPayload payload;
    TestSeries series;
    VShardId vshard{0};
};

BuiltSnapshot buildNonEmptySnapshot(const std::string& root, std::string measurement = "snapshot_new",
                                    double value = 7.0, uint64_t revision = 43, uint64_t timestampOffset = 0) {
    std::filesystem::remove_all(root);
    BuiltSnapshot built;
    {
        ScopedShardedEngine source;
        source.startAt(root);
        cluster::EngineLocalStore store(*source);

        std::string unusedKey;
        SeriesId128 unusedId;
        built.vshard = VShardId{0};
        (void)seriesOnCore0(&unusedKey, &unusedId, &built.vshard);
        built.series =
            floatSeriesOnVShard(std::move(measurement), built.vshard, "new", timestampOffset, value, revision);

        data::WriteBatch batch;
        batch.series.push_back(built.series.write);
        store.applyWrites(std::move(batch)).get();
        flushAndWaitForTsm(*source, assignCore(built.vshard, seastar::smp::count));
        built.payload = store.buildVShardSnapshot(built.vshard).get();
    }
    std::filesystem::remove_all(root);
    if (built.payload.files.size() != 1)
        throw std::runtime_error("non-empty snapshot test fixture did not produce exactly one object");
    return built;
}

data::SnapshotPayload emptySnapshot(VShardId vshard) {
    SeriesCatalog catalog;
    data::SnapshotPayload payload;
    payload.catalog = catalog.snapshot();
    VShardSnapshotBuilder builder(vshard);
    payload.manifest = builder.build({}, SeriesCatalog::snapshotHash(payload.catalog));
    return payload;
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

TEST(EngineSnapshotApply, ReplacesLiveWalAndMixedTsmGenerationExactly) {
    seastar::thread([] {
        const std::string sourceRoot = "snapsm_replace_src";
        const std::string destRoot = "snapsm_replace_dst";
        auto incoming = buildNonEmptySnapshot(sourceRoot);
        std::filesystem::remove_all(destRoot);

        {
            ScopedShardedEngine dest;
            dest.startAt(destRoot);
            cluster::EngineLocalStore store(*dest);
            const unsigned core = assignCore(incoming.vshard, seastar::smp::count);
            const VShardId foreignVShard = anotherVShardOnCore(incoming.vshard);
            const auto oldDisk =
                floatSeriesOnVShard("snapshot_old_disk", incoming.vshard, "old-disk", 10'000, 10.0, 10);
            const auto foreignDisk =
                floatSeriesOnVShard("snapshot_foreign_disk", foreignVShard, "foreign-disk", 20'000, 20.0, 20);
            const auto oldWal = floatSeriesOnVShard("snapshot_old_wal", incoming.vshard, "old-wal", 30'000, 30.0, 30);
            const auto foreignWal =
                floatSeriesOnVShard("snapshot_foreign_wal", foreignVShard, "foreign-wal", 40'000, 40.0, 40);

            data::WriteBatch diskBatch;
            diskBatch.series = {oldDisk.write, foreignDisk.write};
            store.applyWrites(std::move(diskBatch)).get();
            flushAndWaitForTsm(*dest, core);

            data::WriteBatch walBatch;
            walBatch.series = {oldWal.write, foreignWal.write};
            store.applyWrites(std::move(walBatch)).get();

            ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, incoming.payload).get());

            const auto installed = readFloat(*dest, incoming.series);
            EXPECT_EQ(installed.timestamps, (std::vector<uint64_t>{BASE, BASE + 1000}));
            EXPECT_EQ(installed.values, (std::vector<double>{7.0, 8.0}));
            EXPECT_TRUE(readFloat(*dest, oldDisk).timestamps.empty());
            EXPECT_TRUE(readFloat(*dest, oldWal).timestamps.empty());
            EXPECT_EQ(readFloat(*dest, foreignDisk).values, (std::vector<double>{20.0, 21.0}));
            EXPECT_EQ(readFloat(*dest, foreignWal).values, (std::vector<double>{40.0, 41.0}));

            auto measurements = store.queryMetadata({data::MetadataKind::Measurements, "", "", ""}).get().items;
            EXPECT_NE(std::find(measurements.begin(), measurements.end(), "snapshot_new"), measurements.end());
            EXPECT_NE(std::find(measurements.begin(), measurements.end(), "snapshot_foreign_disk"), measurements.end());
            EXPECT_NE(std::find(measurements.begin(), measurements.end(), "snapshot_foreign_wal"), measurements.end());
            EXPECT_EQ(std::find(measurements.begin(), measurements.end(), "snapshot_old_disk"), measurements.end());
            EXPECT_EQ(std::find(measurements.begin(), measurements.end(), "snapshot_old_wal"), measurements.end());
            auto cardinality =
                store.queryMetadata({data::MetadataKind::MeasurementCardinality, "snapshot_new", "", ""}).get();
            EXPECT_DOUBLE_EQ(cardinality.cardinality, 1.0);

            // An exact retry must reuse the already-published object. Compare
            // after two retries so the first can finish the unrelated VShard's
            // asynchronous rollover conversion without making this assertion racy.
            ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, incoming.payload).get());
            const size_t filesAfterRetry =
                (*dest).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get();
            ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, incoming.payload).get());
            EXPECT_EQ((*dest).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get(),
                      filesAfterRetry);
            EXPECT_EQ(readFloat(*dest, incoming.series).values, (std::vector<double>{7.0, 8.0}));
        }

        std::filesystem::remove_all(destRoot);
    })
        .join()
        .get();
}

TEST(EngineSnapshotApply, InvalidPreflightDoesNotMutateAndEmptySnapshotClearsOnlyTarget) {
    seastar::thread([] {
        const std::string sourceRoot = "snapsm_preflight_src";
        const std::string destRoot = "snapsm_preflight_dst";
        auto incoming = buildNonEmptySnapshot(sourceRoot);
        std::filesystem::remove_all(destRoot);

        {
            ScopedShardedEngine dest;
            dest.startAt(destRoot);
            cluster::EngineLocalStore store(*dest);
            const VShardId foreignVShard = anotherVShardOnCore(incoming.vshard);
            const auto oldTarget =
                floatSeriesOnVShard("snapshot_preflight_old", incoming.vshard, "old", 50'000, 50.0, 50);
            const auto foreign =
                floatSeriesOnVShard("snapshot_preflight_foreign", foreignVShard, "foreign", 60'000, 60.0, 60);
            data::WriteBatch batch;
            batch.series = {oldTarget.write, foreign.write};
            store.applyWrites(std::move(batch)).get();

            auto corrupt = incoming.payload;
            ASSERT_FALSE(corrupt.catalog.empty());
            corrupt.catalog.back() ^= 0x01;
            EXPECT_FALSE(store.installVShardSnapshot(incoming.vshard, std::move(corrupt)).get());
            EXPECT_EQ(readFloat(*dest, oldTarget).values, (std::vector<double>{50.0, 51.0}));
            EXPECT_EQ(readFloat(*dest, foreign).values, (std::vector<double>{60.0, 61.0}));
            auto before = store.queryMetadata({data::MetadataKind::Measurements, "", "", ""}).get().items;
            EXPECT_NE(std::find(before.begin(), before.end(), "snapshot_preflight_old"), before.end());

            ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, emptySnapshot(incoming.vshard)).get());
            EXPECT_TRUE(readFloat(*dest, oldTarget).timestamps.empty());
            EXPECT_EQ(readFloat(*dest, foreign).values, (std::vector<double>{60.0, 61.0}));
            auto after = store.queryMetadata({data::MetadataKind::Measurements, "", "", ""}).get().items;
            EXPECT_EQ(std::find(after.begin(), after.end(), "snapshot_preflight_old"), after.end());
            EXPECT_NE(std::find(after.begin(), after.end(), "snapshot_preflight_foreign"), after.end());
            EXPECT_TRUE(store.installVShardSnapshot(incoming.vshard, emptySnapshot(incoming.vshard)).get());
        }

        std::filesystem::remove_all(destRoot);
    })
        .join()
        .get();
}

TEST(EngineSnapshotApply, EveryDurableCheckpointCanRetryToTheExactGeneration) {
    seastar::thread([] {
        const std::string sourceRoot = "snapsm_checkpoint_src";
        auto incoming = buildNonEmptySnapshot(sourceRoot);
        const std::array checkpoints{
            SnapshotInstallCheckpoint::WalGenerationDeleted, SnapshotInstallCheckpoint::TsmGenerationDeleted,
            SnapshotInstallCheckpoint::DataPublished,        SnapshotInstallCheckpoint::CatalogPruned,
            SnapshotInstallCheckpoint::CatalogInstalled,     SnapshotInstallCheckpoint::SupersededObjectsRetired,
        };

        for (size_t index = 0; index < checkpoints.size(); ++index) {
            const std::string destRoot = "snapsm_checkpoint_dst_" + std::to_string(index);
            std::filesystem::remove_all(destRoot);
            bool injected = false;
            {
                ScopedShardedEngine dest;
                dest.startAt(destRoot);
                cluster::EngineLocalStore store(*dest);
                const auto oldTarget = floatSeriesOnVShard("snapshot_checkpoint_old", incoming.vshard,
                                                           "old" + std::to_string(index), 70'000, 70.0, 20);
                data::WriteBatch batch;
                batch.series.push_back(oldTarget.write);
                store.applyWrites(std::move(batch)).get();
                flushAndWaitForTsm(*dest, assignCore(incoming.vshard, seastar::smp::count));

                (*dest)
                    .invoke_on(assignCore(incoming.vshard, seastar::smp::count),
                               [checkpoint = checkpoints[index], &injected](Engine& engine) {
                                   engine.setSnapshotInstallCheckpointForTesting(
                                       [checkpoint, &injected](SnapshotInstallCheckpoint reached) {
                                           if (!injected && reached == checkpoint) {
                                               injected = true;
                                               throw std::runtime_error("injected snapshot-install interruption");
                                           }
                                       });
                               })
                    .get();
                EXPECT_THROW(store.installVShardSnapshot(incoming.vshard, incoming.payload).get(), std::runtime_error);
                EXPECT_TRUE(injected);
            }

            // Discard every in-memory manager/index decision before retrying.
            // This makes each hook a recovery boundary, including the
            // pre-publication fences and post-catalog source retirement.
            {
                ScopedShardedEngine reopened;
                reopened.startAt(destRoot);
                cluster::EngineLocalStore store(*reopened);
                ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, incoming.payload).get());
                EXPECT_EQ(readFloat(*reopened, incoming.series).values, (std::vector<double>{7.0, 8.0}));
                auto measurements = store.queryMetadata({data::MetadataKind::Measurements, "", "", ""}).get().items;
                EXPECT_EQ(std::find(measurements.begin(), measurements.end(), "snapshot_checkpoint_old"),
                          measurements.end());
            }
            std::filesystem::remove_all(destRoot);
        }
    })
        .join()
        .get();
}

TEST(EngineSnapshotApply, NewGenerationDurablyRetiresSupersededPureSnapshotObject) {
    seastar::thread([] {
        const std::string firstSourceRoot = "snapsm_retire_src_1";
        const std::string secondSourceRoot = "snapsm_retire_src_2";
        const std::string destRoot = "snapsm_retire_dst";
        auto first = buildNonEmptySnapshot(firstSourceRoot);
        auto second = buildNonEmptySnapshot(secondSourceRoot, "snapshot_newer", 9.0, 44, 100'000);
        ASSERT_EQ(first.vshard, second.vshard);
        std::filesystem::remove_all(destRoot);

        {
            ScopedShardedEngine dest;
            dest.startAt(destRoot);
            cluster::EngineLocalStore store(*dest);
            const unsigned core = assignCore(first.vshard, seastar::smp::count);
            ASSERT_TRUE(store.installVShardSnapshot(first.vshard, first.payload).get());
            ASSERT_EQ((*dest).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get(), 1u);

            ASSERT_TRUE(store.installVShardSnapshot(second.vshard, second.payload).get());
            EXPECT_EQ((*dest).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get(), 1u);
            EXPECT_TRUE(readFloat(*dest, first.series).timestamps.empty());
            EXPECT_EQ(readFloat(*dest, second.series).values, (std::vector<double>{9.0, 10.0}));
        }

        // Reopen proves source-name retirement, its directory barrier, and
        // sidecar cleanup did not merely hide the old object in the live map.
        {
            ScopedShardedEngine reopened;
            reopened.startAt(destRoot);
            const unsigned core = assignCore(first.vshard, seastar::smp::count);
            EXPECT_EQ((*reopened).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get(), 1u);
            EXPECT_TRUE(readFloat(*reopened, first.series).timestamps.empty());
            EXPECT_EQ(readFloat(*reopened, second.series).values, (std::vector<double>{9.0, 10.0}));
        }

        std::filesystem::remove_all(destRoot);
    })
        .join()
        .get();
}

TEST(EngineSnapshotApply, RetryReconcilesPublishedObjectAfterDirectoryBarrierFailure) {
    seastar::thread([] {
        const std::string sourceRoot = "snapsm_orphan_src";
        const std::string destRoot = "snapsm_orphan_dst";
        auto incoming = buildNonEmptySnapshot(sourceRoot);
        std::filesystem::remove_all(destRoot);

        {
            ScopedShardedEngine dest;
            dest.startAt(destRoot);
            cluster::EngineLocalStore store(*dest);
            const unsigned core = assignCore(incoming.vshard, seastar::smp::count);
            bool failed = false;
            (*dest)
                .invoke_on(core,
                           [&failed](Engine& engine) {
                               engine.setSnapshotDirectorySyncForTesting([&failed](const std::string&) {
                                   failed = true;
                                   return seastar::make_exception_future<>(
                                       std::runtime_error("injected directory barrier failure after publish"));
                               });
                           })
                .get();

            EXPECT_THROW(store.installVShardSnapshot(incoming.vshard, incoming.payload).get(), std::runtime_error);
            EXPECT_TRUE(failed);

            (*dest)
                .invoke_on(core,
                           [](Engine& engine) {
                               engine.setSnapshotDirectorySyncForTesting(
                                   [](const std::string&) { return seastar::make_ready_future<>(); });
                           })
                .get();
            ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, incoming.payload).get());
            EXPECT_EQ(readFloat(*dest, incoming.series).values, (std::vector<double>{7.0, 8.0}));
            const size_t fileCount =
                (*dest).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get();
            ASSERT_TRUE(store.installVShardSnapshot(incoming.vshard, incoming.payload).get());
            EXPECT_EQ((*dest).invoke_on(core, [](Engine& engine) { return engine.getTSMFileCount(); }).get(),
                      fileCount);
        }

        std::filesystem::remove_all(destRoot);
    })
        .join()
        .get();
}
