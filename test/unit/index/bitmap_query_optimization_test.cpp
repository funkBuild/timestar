#include "../../seastar_gtest.hpp"
#include "../../test_helpers/native_index_test_access.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/preempt.hh>

using namespace timestar::index;

class BitmapQueryOptimizationTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

SEASTAR_TEST_F(BitmapQueryOptimizationTest, DayMembershipSurvivesPreemptionAndCacheRehash) {
    // A cached entry needs no open index or disk I/O. Force the ready-future
    // await inside addDayMembership to yield, then move the cache entries
    // before the coroutine resumes. Returning a bitmap pointer across that
    // await used to access the freed bucket array and lose the acknowledged ID.
    NativeIndex index(0);
    std::string key = "cached-day";
    NativeIndexTestAccess::plantBitmap(index, key, true, 17);
    auto pending = [&] {
        struct ForcePreemption {
            seastar::internal::preemption_monitor monitor;
            const seastar::internal::preemption_monitor* previous = seastar::internal::get_need_preempt_var();
            ForcePreemption() {
                monitor.head.store(1);
                monitor.tail.store(0);
                seastar::internal::set_need_preempt_var(&monitor);
            }
            ~ForcePreemption() { seastar::internal::set_need_preempt_var(previous); }
        } forced;
        return NativeIndexTestAccess::addDayMembership(index, key, 29);
    }();
    EXPECT_FALSE(pending.available());
    NativeIndexTestAccess::rehashBitmaps(index);
    EXPECT_TRUE(co_await std::move(pending));
    co_await NativeIndexTestAccess::visitBitmap(index, key, true, [](const roaring::Roaring* bitmap) {
        ASSERT_NE(bitmap, nullptr);
        EXPECT_EQ(bitmap->cardinality(), 2u);
        EXPECT_TRUE(bitmap->contains(17));
        EXPECT_TRUE(bitmap->contains(29));
    });
    EXPECT_FALSE(co_await NativeIndexTestAccess::addDayMembership(index, key, 29));
}

SEASTAR_TEST_F(BitmapQueryOptimizationTest, BitmapConsumptionPrecedesFutureResumption) {
    NativeIndex index(0);
    co_await index.open();
    NativeIndexTestAccess::cancelTimers(index);
    for (bool day : {false, true}) {
        std::string key = "cached";
        NativeIndexTestAccess::plantBitmap(index, key, day, 17);
        auto write = NativeIndexTestAccess::updateBitmap(index, key, day, 29);
        EXPECT_TRUE(write.available());
        NativeIndexTestAccess::rehashBitmaps(index);
        co_await std::move(write);

        roaring::Roaring snapshot;
        auto read = NativeIndexTestAccess::visitBitmap(index, key, day, [&](const roaring::Roaring* bitmap) {
            EXPECT_NE(bitmap, nullptr);
            if (bitmap)
                snapshot = *bitmap;
        });
        EXPECT_TRUE(read.available());
        // Model another task running after completion, before caller resumption.
        NativeIndexTestAccess::clearBitmaps(index);
        co_await std::move(read);
        EXPECT_EQ(snapshot.cardinality(), 2u);
        EXPECT_TRUE(snapshot.contains(17));
        EXPECT_TRUE(snapshot.contains(29));
    }
    co_await index.close();
}

SEASTAR_TEST_F(BitmapQueryOptimizationTest, CachedCardinalityAndColdReloadRemainExact) {
    NativeIndex index(0);
    co_await index.open();
    const std::map<std::string, std::string> tags{{"host", "a"}};
    for (size_t i = 0; i < 16; ++i)
        co_await index.getOrCreateSeriesId("cpu", tags, "v" + std::to_string(i));
    EXPECT_EQ(co_await index.estimateTagCardinality("cpu", "host", "a"), 16.0);
    // sync() persists day membership only. Close/open flushes tag postings too.
    co_await index.close();
    NativeIndex reopened(0);
    co_await reopened.open();
    NativeIndexTestAccess::clearBitmaps(reopened);
    EXPECT_EQ(co_await reopened.estimateTagCardinality("cpu", "host", "a"), 16.0);
    co_await reopened.getOrCreateSeriesId("cpu", tags, "extra");
    EXPECT_EQ(co_await reopened.estimateTagCardinality("cpu", "host", "a"), 17.0);
    EXPECT_EQ(co_await reopened.estimateTagCardinality("cpu", "host", "missing"), 0.0);
    co_await reopened.close();
}

SEASTAR_TEST_F(BitmapQueryOptimizationTest, TagSketchSeedsAllPriorIdsAtThresholdAndAfterReopen) {
    std::map<std::string, std::string> tags{{"region", "all"}, {"host", ""}};
    const std::string sketchKey("cpu\0region\0all", 14);
    {
        NativeIndex index(0);
        co_await index.open();
        // Below the threshold there is deliberately no sketch. The first sketch
        // must include earlier IDs, not just the ID that triggered its creation.
        for (size_t i = 0; i < 9999; ++i) {
            tags["host"] = std::to_string(i);
            co_await index.getOrCreateSeriesId("cpu", tags, "v");
        }
        EXPECT_EQ(NativeIndexTestAccess::cachedSketchEstimate(index, sketchKey), 0.0);
        tags["host"] = "threshold";
        co_await index.getOrCreateSeriesId("cpu", tags, "v");
        EXPECT_NEAR(NativeIndexTestAccess::cachedSketchEstimate(index, sketchKey), 10000.0, 500.0);
        co_await index.close();
    }
    NativeIndex reopened(0);
    co_await reopened.open();
    NativeIndexTestAccess::clearBitmaps(reopened);
    tags["host"] = "after_reopen";
    co_await reopened.getOrCreateSeriesId("cpu", tags, "v");
    EXPECT_NEAR(NativeIndexTestAccess::cachedSketchEstimate(reopened, sketchKey), 10001.0, 500.0);
    co_await reopened.close();
}
