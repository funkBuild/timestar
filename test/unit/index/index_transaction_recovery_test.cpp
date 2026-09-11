#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers/native_index_test_access.hpp"
#include "series_key.hpp"

#include <filesystem>
#include <seastar/util/defer.hh>

using namespace timestar::index;
namespace ke = timestar::index::keys;

class IndexTransactionRecoveryTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

SEASTAR_TEST_F(IndexTransactionRecoveryTest, SuccessfulRetryPersistsSchemaAfterAppendFailure) {
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        co_await NativeIndexTestAccess::fillIndexWalNearThreshold(index);
        co_await NativeIndexTestAccess::setIndexWalReadOnly(index, true);
        bool failed = false;
        try {
            co_await index.getOrCreateSeriesId("retry", {{"host", "h"}}, "v");
        } catch (const std::exception&) {
            failed = true;
        }
        EXPECT_TRUE(failed);
        co_await NativeIndexTestAccess::setIndexWalReadOnly(index, false);
        MetadataOp op;
        op.measurement = "retry";
        op.tags = {{"host", "h"}};
        op.fieldName = "v";
        op.valueType = TSMValueType::Float;
        std::vector<MetadataOp> ops{op};
        auto delta = co_await index.indexMetadataBatchWithSchema(ops);
        EXPECT_EQ(delta.newFields["retry"].count("v"), 1u);
        EXPECT_EQ(delta.newTags["retry"].count("host"), 1u);
        co_await index.sync();
        co_await index.compact();
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_TRUE((co_await index.getSeriesId("retry", {{"host", "h"}}, "v")).has_value());
    EXPECT_EQ((co_await index.getFields("retry")).count("v"), 1u);
    EXPECT_EQ((co_await index.getTags("retry")).count("host"), 1u);
    EXPECT_EQ((co_await index.getTagValues("retry", "host")).count("h"), 1u);
    EXPECT_EQ((co_await index.getAllMeasurements()).count("retry"), 1u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, RetriedSstFlushRetainsPriorMemtableAndNewerSchema) {
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        co_await index.getOrCreateSeriesId("original", {}, "v");
        co_await index.sync();
        auto blocked = NativeIndexTestAccess::nextSstablePath(index);
        std::filesystem::create_directory(blocked);
        bool failed = false;
        try {
            co_await index.compact();
        } catch (const std::exception&) {
            failed = true;
        }
        EXPECT_TRUE(failed);
        std::filesystem::remove(blocked);
        EXPECT_EQ((co_await index.getAllSeriesForMeasurement("original", 0))->size(), 1u);
        co_await index.getOrCreateSeriesId("original", {}, "later");
        co_await index.compact();
        EXPECT_EQ((co_await index.getAllSeriesForMeasurement("original", 0))->size(), 2u);
        co_await index.close();
    }
    NativeIndex recovered(0);
    co_await recovered.open();
    EXPECT_EQ((co_await recovered.getAllSeriesForMeasurement("original", 0))->size(), 2u);
    EXPECT_EQ((co_await recovered.getFields("original")).count("later"), 1u);
    co_await recovered.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, RepairRestoresMissingMappingBeforeCertifyingCoverage) {
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        auto hidden = co_await index.getOrCreateSeriesId("m", {{"host", "hidden"}}, "hidden");
        auto healthy = co_await index.getOrCreateSeriesId("m", {{"host", "healthy"}}, "healthy");
        co_await NativeIndexTestAccess::dropLocalIdMapping(index, hidden);
        std::vector<NativeIndex::SeriesTimeBounds> bounds{{hidden, 100 * ke::NS_PER_DAY, 100 * ke::NS_PER_DAY},
                                                          {healthy, 100 * ke::NS_PER_DAY, 100 * ke::NS_PER_DAY}};
        co_await index.rebuildDayBitmapsFromBounds(bounds, 100, 100);
        EXPECT_EQ((co_await index.findSeriesByTag("m", "host", "hidden", 0)).size(), 1u);
        EXPECT_EQ((co_await index.findSeriesWithMetadata("m", {}, {}, 0))->size(), 2u);
        EXPECT_EQ((co_await index.findSeriesWithMetadataTimeScoped("m", {}, {}, 100 * ke::NS_PER_DAY,
                                                                   101 * ke::NS_PER_DAY - 1, 0))
                      ->size(),
                  2u);
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_EQ((co_await index.findSeriesByTag("m", "host", "hidden", 0)).size(), 1u);
    EXPECT_EQ((co_await index.findSeriesWithMetadataTimeScoped("m", {}, {}, 100 * ke::NS_PER_DAY,
                                                               101 * ke::NS_PER_DAY - 1, 0))
                  ->size(),
              2u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, MissingSourceMetadataLeavesPruningDisabledAcrossRestart) {
    {
        NativeIndex index(0);
        co_await index.open();
        auto healthy = co_await index.getOrCreateSeriesId("m", {}, "v");
        const auto unknown = SeriesId128::fromSeriesKey("lost source metadata");
        std::vector<NativeIndex::SeriesTimeBounds> bounds{{healthy, 100 * ke::NS_PER_DAY, 100 * ke::NS_PER_DAY},
                                                          {unknown, 100 * ke::NS_PER_DAY, 100 * ke::NS_PER_DAY}};
        co_await index.rebuildDayBitmapsFromBounds(bounds, 100, 100);
        EXPECT_TRUE(NativeIndexTestAccess::dayPruningDisabled(index));
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_TRUE(NativeIndexTestAccess::dayPruningDisabled(index));
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, FailedBackgroundFlushCanBeRetriedWithoutRestart) {
    const auto previous = timestar::config();
    auto restore = seastar::defer([&previous] { timestar::setGlobalConfig(previous); });
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        co_await index.getOrCreateSeriesId("m", {}, "old");
        co_await index.sync();
        const auto blocked = NativeIndexTestAccess::nextSstablePath(index);
        std::filesystem::create_directory(blocked);
        auto small = previous;
        small.index.write_buffer_size = 1;
        timestar::setGlobalConfig(small);
        co_await NativeIndexTestAccess::triggerBackgroundFlush(index);
        bool failed = false;
        try {
            co_await NativeIndexTestAccess::waitForBackgroundResult(index);
        } catch (const std::exception&) {
            failed = true;
        }
        EXPECT_TRUE(failed);
        timestar::setGlobalConfig(previous);
        std::filesystem::remove(blocked);
        co_await index.getOrCreateSeriesId("m", {}, "new");
        co_await index.compact();
        EXPECT_EQ((co_await index.getAllSeriesForMeasurement("m", 0))->size(), 2u);
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_EQ((co_await index.getFields("m")).size(), 2u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, RotationFailureDoesNotSwapAwayActiveMemtable) {
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        co_await index.getOrCreateSeriesId("m", {}, "old");
        co_await index.sync();
        co_await NativeIndexTestAccess::setIndexWalReadOnly(index, true);
        bool failed = false;
        try {
            co_await index.compact();
        } catch (const std::exception&) {
            failed = true;
        }
        EXPECT_TRUE(failed);
        EXPECT_EQ((co_await index.getAllSeriesForMeasurement("m", 0))->size(), 1u);
        co_await NativeIndexTestAccess::setIndexWalReadOnly(index, false);
        co_await index.getOrCreateSeriesId("m", {}, "new");
        co_await index.sync();
        co_await index.compact();
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_EQ((co_await index.getAllSeriesForMeasurement("m", 0))->size(), 2u);
    EXPECT_EQ((co_await index.getFields("m")).size(), 2u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, RetriedCreationBelowCheckpointRepairsPostingsAfterRestart) {
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.getOrCreateSeriesId("m", {{"host", "healthy"}}, "v");
        auto id = SeriesId128::fromSeriesKey(timestar::buildSeriesKey("m", {{"host", "retry"}}, "v"));
        // A failed creation can assign an ID before its metadata is committed.
        NativeIndexTestAccess::reserveUncommittedLocalId(index, id);
        co_await index.compact();
        co_await index.getOrCreateSeriesId("m", {{"host", "retry"}}, "v");
        co_await index.sync();
        // A crash loses all unflushed derived caches; metadata is durable.
        NativeIndexTestAccess::discardAllVolatileDerivedState(index);
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_EQ((co_await index.findSeriesByTag("m", "host", "retry", 0)).size(), 1u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, ConcurrentCreationCannotOverwriteNewerFieldSchema) {
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.getOrCreateSeriesId("m", {{"z", "old"}}, "seed");
        co_await index.getOrCreateSeriesId("m", {{"a", "x"}}, "seed");
        co_await index.close();
    }
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        co_await index.getFields("m");
        co_await index.getTags("m");
        co_await index.getTagValues("m", "a");
        co_await index.findSeriesByTag("m", "z", "old", 0);
        co_await index.findSeriesByTag("m", "a", "x", 0);
        co_await index.estimateMeasurementCardinality("m");
        co_await NativeIndexTestAccess::stagePostingsCheckpoint(index);
        NativeIndexTestAccess::clearBlockCache(index);
        auto first = index.getOrCreateSeriesId("m", {{"z", "old"}}, "first");
        EXPECT_FALSE(first.available()) << "first writer suspends in its cold tag-values scan";
        EXPECT_EQ((co_await index.getFields("m")).count("first"), 1u)
            << "the first writer has already captured its field-schema snapshot";
        auto second = index.getOrCreateSeriesId("m", {{"a", "x"}}, "second");
        co_await std::move(second);
        co_await std::move(first);
        co_await index.sync();
        co_await index.compact();
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_TRUE((co_await index.getSeriesId("m", {{"a", "x"}}, "second")).has_value());
    EXPECT_EQ((co_await index.getFields("m")).count("first"), 1u);
    EXPECT_EQ((co_await index.getFields("m")).count("second"), 1u);
    co_await index.close();
}

SEASTAR_TEST_F(IndexTransactionRecoveryTest, SchemaBroadcastAndLocalCreationPreserveBothFields) {
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.getOrCreateSeriesId("m", {{"z", "old"}}, "seed");
        co_await index.close();
    }
    {
        NativeIndex index(0);
        co_await index.open();
        NativeIndexTestAccess::cancelTimers(index);
        co_await index.getFields("m");
        co_await index.getTags("m");
        co_await index.findSeriesByTag("m", "z", "old", 0);
        co_await index.estimateMeasurementCardinality("m");
        co_await NativeIndexTestAccess::stagePostingsCheckpoint(index);
        NativeIndexTestAccess::clearBlockCache(index);
        auto local = index.getOrCreateSeriesId("m", {{"z", "old"}}, "local");
        EXPECT_FALSE(local.available());
        EXPECT_EQ((co_await index.getFields("m")).count("local"), 1u);
        SchemaUpdate update;
        update.newFields["m"].insert("remote");
        auto broadcast = index.applySchemaUpdate(std::move(update));
        co_await std::move(broadcast);
        co_await std::move(local);
        co_await index.close();
    }
    NativeIndex index(0);
    co_await index.open();
    EXPECT_EQ((co_await index.getFields("m")).count("local"), 1u);
    EXPECT_EQ((co_await index.getFields("m")).count("remote"), 1u);
    co_await index.close();
}
