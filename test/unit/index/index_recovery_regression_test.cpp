#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers/native_index_test_access.hpp"

#include <filesystem>
#include <seastar/core/coroutine.hh>

using namespace timestar::index;
namespace ke = timestar::index::keys;

class IndexRecoveryRegressionTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

SEASTAR_TEST_F(IndexRecoveryRegressionTest, OpenDurablyClearsCleanMarkerBeforeReturning) {
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    NativeIndexTestAccess::cancelTimers(index);
    EXPECT_TRUE(index.openedCleanly());
    auto marker = co_await NativeIndexTestAccess::durableValue(index, ke::encodeCleanShutdownKey());
    EXPECT_FALSE(marker.has_value()) << "a crash now must not resurrect the previous clean marker";
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, MetadataBatchSuccessImpliesRecoverableSeries) {
    NativeIndex index(0);
    co_await index.open();
    NativeIndexTestAccess::cancelTimers(index);
    MetadataOp op;
    op.measurement = "durable";
    op.fieldName = "v";
    op.tags = {{"host", "h"}};
    op.valueType = TSMValueType::Float;
    op.minTs = op.maxTs = 2000 * ke::NS_PER_DAY;
    std::vector<MetadataOp> ops{op};
    co_await index.indexMetadataBatchWithSchema(ops);
    auto id = co_await index.getSeriesId(op.measurement, op.tags, op.fieldName);
    EXPECT_TRUE(id.has_value());
    if (id) {
        auto meta = co_await NativeIndexTestAccess::durableValue(index, ke::encodeSeriesMetadataKey(*id));
        EXPECT_TRUE(meta.has_value()) << "the HTTP metadata barrier must cover the index WAL";
    }
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, IndexSyncWaitsForInflightBitmapFlushEvenWithNoDirtyKeys) {
    NativeIndex index(0);
    co_await index.open();
    NativeIndexTestAccess::cancelTimers(index);
    co_await index.sync();
    // A flush owns this lock after clearing dirty flags but before its batch
    // reaches the WAL (for example while rebuilding a bloom from an SST).
    auto flushing = co_await NativeIndexTestAccess::holdIndexFlush(index);
    auto barrier = index.sync();
    EXPECT_FALSE(barrier.available());
    flushing.return_all();
    co_await std::move(barrier);
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, ConcurrentWalSyncWaitsForInflightDurability) {
    auto wal = co_await IndexWAL::open("shard_0/native_index/sync_test");
    IndexWriteBatch batch;
    batch.put("key", "value");
    co_await wal.append(batch);
    auto first = wal.sync();
    EXPECT_FALSE(first.available()) << "precondition: first sync is suspended in file I/O";
    auto second = wal.sync();
    EXPECT_FALSE(second.available()) << "a concurrent barrier cannot acknowledge the first sync's pending I/O";
    co_await std::move(first);
    co_await std::move(second);
    co_await wal.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, IndexWriteDuringRotationBelongsToTheNewWal) {
    NativeIndex index(0);
    co_await index.open();
    NativeIndexTestAccess::cancelTimers(index);
    auto compacting = index.compact();
    EXPECT_FALSE(compacting.available());
    auto id = SeriesId128::fromSeriesKey("rotating,host=new v");
    auto writing = index.putSeriesValueType(id, TSMValueType::Float);
    co_await std::move(compacting);
    co_await std::move(writing);
    co_await index.sync();
    EXPECT_TRUE((co_await NativeIndexTestAccess::durableValue(index, ke::encodeSeriesValueTypeKey(id))).has_value());
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, WalRotationDoesNotForgetConcurrentBufferedAppends) {
    const std::string path = "shard_0/native_index/rotation_test";
    auto wal = co_await IndexWAL::open(path);
    IndexWriteBatch seed;
    seed.put("seed", "durable");
    co_await wal.append(seed);
    co_await wal.sync();
    auto rotating = wal.rotate();
    EXPECT_FALSE(rotating.available());
    IndexWriteBatch next;
    next.put("next", "also durable");
    co_await wal.append(next);
    co_await std::move(rotating);
    co_await wal.sync();
    auto reader = co_await IndexWAL::open(path);
    MemTable recovered;
    co_await reader.replay(recovered);
    EXPECT_EQ(recovered.get("seed"), "durable");
    EXPECT_EQ(recovered.get("next"), "also durable");
    co_await wal.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, FailedWalWriteCanRetryEveryBufferedRecord) {
    auto wal = co_await IndexWAL::open("shard_0/native_index/retry_test");
    IndexWriteBatch seed;
    seed.put("seed", "already durable");
    co_await wal.append(seed);
    co_await wal.sync();
    co_await NativeIndexTestAccess::setWalReadOnly(wal, true);
    IndexWriteBatch batch;
    batch.put("new", std::string(8192, 'x'));
    co_await wal.append(batch);
    bool failed = false;
    try {
        co_await wal.sync();
    } catch (const std::exception&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    co_await NativeIndexTestAccess::setWalReadOnly(wal, false);
    co_await wal.sync();
    auto reader = co_await IndexWAL::open("shard_0/native_index/retry_test");
    MemTable recovered;
    co_await reader.replay(recovered);
    EXPECT_EQ(recovered.get("seed"), "already durable");
    EXPECT_EQ(recovered.get("new"), std::string(8192, 'x'));
    co_await wal.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, FlushingNewRecordsMustNotTruncatePreviouslySyncedTail) {
    const std::string path = "shard_0/native_index/tail_test";
    auto wal = co_await IndexWAL::open(path);
    IndexWriteBatch seed;
    seed.put("seed", "already durable");
    co_await wal.append(seed);
    co_await wal.sync();
    IndexWriteBatch next;
    next.put("next", "pending");
    co_await wal.append(next);
    // Pause the next sync after full-block flushing, before the tail write.
    co_await NativeIndexTestAccess::flushWalBlocks(wal);
    auto reader = co_await IndexWAL::open(path);
    MemTable recovered;
    co_await reader.replay(recovered);
    EXPECT_EQ(recovered.get("seed"), "already durable");
    co_await wal.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, ExistingDayMembershipDoesNotRewriteTheIndexWal) {
    NativeIndex index(0);
    co_await index.open();
    NativeIndexTestAccess::cancelTimers(index);
    TimeStarInsert<double> insert("steady", "v");
    insert.addValue(2000 * ke::NS_PER_DAY, 1.0);
    co_await index.indexInsert(insert);
    co_await index.sync();
    const auto before = NativeIndexTestAccess::walSequence(index);
    co_await index.indexInsert(insert);
    co_await index.sync();
    EXPECT_EQ(NativeIndexTestAccess::walSequence(index), before);
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, ClampMarkerMustNotShrinkAfterRestart) {
    {
        NativeIndex index(0);
        co_await index.open();
        auto id = co_await index.getOrCreateSeriesId("m", {{"host", "original"}}, "v");
        co_await index.recordDaySpan("m", id, 100 * ke::NS_PER_DAY, 1000 * ke::NS_PER_DAY);
        auto result = co_await index.findSeriesWithMetadataTimeScoped(
            "m", {{"host", "original"}}, {}, 600 * ke::NS_PER_DAY, 601 * ke::NS_PER_DAY - 1, 0);
        EXPECT_EQ(result->size(), 1u);
        co_await index.close();
    }
    {
        NativeIndex index(0);
        co_await index.open();
        auto id = co_await index.getOrCreateSeriesId("m", {{"host", "backfill"}}, "v");
        co_await index.recordDaySpan("m", id, ke::NS_PER_DAY, 500 * ke::NS_PER_DAY);
        auto result = co_await index.findSeriesWithMetadataTimeScoped(
            "m", {{"host", "original"}}, {}, 600 * ke::NS_PER_DAY, 601 * ke::NS_PER_DAY - 1, 0);
        EXPECT_EQ(result->size(), 1u) << "a smaller new clamp must preserve the persisted refused-through day";
        co_await index.close();
    }
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, LoadingPostingsMustNotAdvanceRepairWatermarkPastSkippedIds) {
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.getOrCreateSeriesId("m", {{"host", "shared"}}, "a");
        co_await index.compact();
        auto second = co_await index.getOrCreateSeriesId("m", {{"host", "shared"}}, "b");
        NativeIndexTestAccess::holdPostingsLoad(index, "m", "host", "shared", second);
        co_await index.compact();
        // The flushed index is now durable, but the loading bitmap is not.
        // A crash drops that RAM state before the loader resumes.
        NativeIndexTestAccess::discardVolatilePostings(index);
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    auto all = co_await index.getAllSeriesForMeasurement("m", 0);
    EXPECT_EQ(all->size(), 2u);
    auto scoped = co_await index.findSeriesByTag("m", "host", "shared", 0);
    EXPECT_EQ(scoped.size(), 2u) << "restart must repair the membership that the flush skipped";
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, UpgradeRepairsMembershipBelowLegacyWatermark) {
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.getOrCreateSeriesId("m", {{"host", "shared"}}, "a");
        co_await index.compact();
        auto second = co_await index.getOrCreateSeriesId("m", {{"host", "shared"}}, "b");
        NativeIndexTestAccess::discardVolatilePostings(index);
        co_await index.compact();
        co_await NativeIndexTestAccess::plantLegacyPostingsCheckpoint(index);
        EXPECT_EQ((co_await index.findSeriesByTag("m", "host", "shared", 0)).size(), 1u);
        EXPECT_TRUE((co_await index.getSeriesMetadata(second)).has_value());
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_EQ((co_await index.findSeriesByTag("m", "host", "shared", 0)).size(), 2u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, RecentRepairMustNotCertifyUnrepairedHistoricalMembership) {
    NativeIndex index(0);
    co_await index.open();
    auto id = co_await index.getOrCreateSeriesId("m", {{"host", "h"}}, "v");
    std::vector<uint64_t> timestamps{1950 * ke::NS_PER_DAY, 2000 * ke::NS_PER_DAY};
    co_await index.recordInsertDays("m", id, timestamps);
    co_await index.flushDayBitmapsNow();
    // A crash loses a newly backfilled membership older than the 32-day
    // repair window, while newer membership is already durable.
    co_await NativeIndexTestAccess::dropDayBitmapsInRange(index, "m", 1950, 1950);
    std::vector<NativeIndex::SeriesTimeBounds> bounds{{id, timestamps.front(), timestamps.back()}};
    co_await index.rebuildDayBitmapsFromBounds(bounds, 1969, 2000);
    auto result = co_await index.findSeriesWithMetadataTimeScoped("m", {}, {}, 1950 * ke::NS_PER_DAY,
                                                                  1951 * ke::NS_PER_DAY - 1, 0);
    EXPECT_EQ(result->size(), 1u) << "known-incomplete older history requires repair or conservative discovery";
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, UnrepairedCoverageSurvivesCleanRestartAndInvalidatesCachedSubsets) {
    {
        NativeIndex index(0);
        co_await index.open();
        auto hidden = co_await index.getOrCreateSeriesId("m", {{"host", "hidden"}}, "v");
        auto healthy = co_await index.getOrCreateSeriesId("m", {{"host", "healthy"}}, "v");
        std::vector<uint64_t> days{1950 * ke::NS_PER_DAY, 2000 * ke::NS_PER_DAY};
        co_await index.recordInsertDays("m", hidden, days);
        co_await index.flushDayBitmapsNow();
        co_await NativeIndexTestAccess::dropDayBitmapsInRange(index, "m", 1950, 1950);
        co_await index.recordInsertDays("m", healthy, days);
        auto partial = co_await index.findSeriesWithMetadataTimeScopedCached("m", {}, {}, days.front(),
                                                                             days.front() + ke::NS_PER_DAY - 1, 0);
        EXPECT_EQ((*partial)->size(), 1u);
        std::vector<NativeIndex::SeriesTimeBounds> bounds{{hidden, days.front(), days.back()},
                                                          {healthy, days.front(), days.back()}};
        co_await index.rebuildDayBitmapsFromBounds(bounds, 1969, 2000);
        auto full = co_await index.findSeriesWithMetadataTimeScopedCached("m", {}, {}, days.front(),
                                                                          days.front() + ke::NS_PER_DAY - 1, 0);
        EXPECT_EQ((*full)->size(), 2u);
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_TRUE(index.openedCleanly());
    auto full = co_await index.findSeriesWithMetadataTimeScoped("m", {{"host", "hidden"}}, {"v"}, 1950 * ke::NS_PER_DAY,
                                                                1951 * ke::NS_PER_DAY - 1, 0);
    EXPECT_EQ(full->size(), 1u);
    auto quiet = co_await index.findSeriesWithMetadataTimeScoped("m", {}, {}, 2001 * ke::NS_PER_DAY,
                                                                 2002 * ke::NS_PER_DAY - 1, 0);
    EXPECT_TRUE(quiet->empty()) << "verified future days should retain normal pruning";
    co_await index.close();
}

SEASTAR_TEST_F(IndexRecoveryRegressionTest, FailedBloomReadMustRestoreDirtyMembershipForRetry) {
    {
        NativeIndex index(0);
        co_await index.open();
        auto first = co_await index.getOrCreateSeriesId("m", {{"host", "shared"}}, "a");
        std::vector<uint64_t> timestamps{100 * ke::NS_PER_DAY};
        co_await index.recordInsertDays("m", first, timestamps);
        co_await index.compact();
        auto second = co_await index.getOrCreateSeriesId("m", {{"host", "shared"}}, "b");
        co_await index.recordInsertDays("m", second, timestamps);
        // A read error during bloom preparation, before the WAL append.
        NativeIndexTestAccess::flipSstableDataByte(index);
        bool failed = false;
        try {
            co_await index.compact();
        } catch (const std::exception&) {
            failed = true;
        }
        EXPECT_TRUE(failed);
        NativeIndexTestAccess::flipSstableDataByte(index);
        co_await index.compact();
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    auto all = co_await index.getAllSeriesForMeasurement("m", 0);
    EXPECT_EQ(all->size(), 2u);
    auto scoped = co_await index.findSeriesByTag("m", "host", "shared", 0);
    EXPECT_EQ(scoped.size(), 2u) << "a retried flush must include membership cleared before the read failure";
    auto day =
        co_await index.findSeriesWithMetadataTimeScoped("m", {}, {}, 100 * ke::NS_PER_DAY, 101 * ke::NS_PER_DAY - 1, 0);
    EXPECT_EQ(day->size(), 2u);
    co_await index.close();
}
