/*
 * Tests for bounded indexedSeriesCache in LevelDBIndex.
 *
 * The indexedSeriesCache is a "have I already indexed this series?" cache
 * that avoids redundant LevelDB Gets on the insert path. Without bounding,
 * it grows without limit (~120 bytes per entry). With 10M series, that's
 * ~1.2GB just for the cache.
 *
 * These tests verify:
 * - The cache has a configurable maximum size
 * - When the cache exceeds the limit, it is cleared (not unbounded growth)
 * - After eviction, subsequent lookups still work (go to LevelDB)
 * - The default max size is reasonable (1M entries)
 * - The smaller metadata caches (fieldsCache, tagsCache) are not affected
 */

#include <gtest/gtest.h>
#include "../../seastar_gtest.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>
#include <string>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/core/series_id.hpp"

class LevelDBIndexCacheBoundsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any leftover shard directories from previous runs
        std::filesystem::remove_all("shard_0");
    }

    void TearDown() override {
        std::filesystem::remove_all("shard_0");
    }
};

// Verify the default max cache size is 1,000,000
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, DefaultMaxCacheSize) {
    LevelDBIndex index(0);
    co_await index.open();

    // Default should be 1M entries
    EXPECT_EQ(index.getMaxSeriesCacheSize(), 1'000'000u);

    co_await index.close();
    co_return;
}

// Verify that setMaxSeriesCacheSize works
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, SetMaxCacheSize) {
    LevelDBIndex index(0);
    co_await index.open();

    index.setMaxSeriesCacheSize(500);
    EXPECT_EQ(index.getMaxSeriesCacheSize(), 500u);

    index.setMaxSeriesCacheSize(10'000'000);
    EXPECT_EQ(index.getMaxSeriesCacheSize(), 10'000'000u);

    co_await index.close();
    co_return;
}

// Verify getSeriesCacheSize reports the current cache size
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, CacheSizeGrows) {
    LevelDBIndex index(0);
    co_await index.open();

    EXPECT_EQ(index.getSeriesCacheSize(), 0u);

    // Insert a few series
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    EXPECT_GE(index.getSeriesCacheSize(), 1u);

    co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "temperature");
    EXPECT_GE(index.getSeriesCacheSize(), 2u);

    co_await index.close();
    co_return;
}

// Core test: verify that the cache is cleared when it exceeds maxSeriesCacheSize
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, CacheClearedWhenExceedingMax) {
    LevelDBIndex index(0);
    co_await index.open();

    // Set a very small cache limit
    index.setMaxSeriesCacheSize(5);

    // Insert 6 series to exceed the limit
    for (int i = 0; i < 6; ++i) {
        co_await index.getOrCreateSeriesId(
            "measurement",
            {{"host", "server-" + std::to_string(i)}},
            "value"
        );
    }

    // After exceeding the limit, the cache should have been cleared and
    // only contain the most recent entry (re-inserted after clear)
    // The cache size should be small (1 entry for the re-inserted key)
    EXPECT_LE(index.getSeriesCacheSize(), 5u);

    co_await index.close();
    co_return;
}

// Verify that after cache eviction, lookups still work correctly
// (they just go to LevelDB instead of the cache)
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, LookupsWorkAfterEviction) {
    LevelDBIndex index(0);
    co_await index.open();

    // Set a tiny cache limit
    index.setMaxSeriesCacheSize(3);

    // Insert 4 series (triggers eviction on the 4th)
    std::vector<SeriesId128> seriesIds;
    for (int i = 0; i < 4; ++i) {
        auto id = co_await index.getOrCreateSeriesId(
            "cpu",
            {{"host", "h" + std::to_string(i)}},
            "usage"
        );
        seriesIds.push_back(id);
    }

    // Now re-query the first series (was evicted from cache, should still
    // work via LevelDB lookup)
    auto id_again = co_await index.getOrCreateSeriesId(
        "cpu",
        {{"host", "h0"}},
        "usage"
    );

    // Should get the same deterministic SeriesId128
    EXPECT_EQ(seriesIds[0], id_again);

    co_await index.close();
    co_return;
}

// Verify that repeated inserts of the same series (cache hits) don't
// cause unbounded growth
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, RepeatedInsertsDoNotGrow) {
    LevelDBIndex index(0);
    co_await index.open();

    index.setMaxSeriesCacheSize(10);

    // Insert the same series many times
    for (int i = 0; i < 100; ++i) {
        co_await index.getOrCreateSeriesId("temp", {{"loc", "a"}}, "val");
    }

    // Cache should have exactly 1 entry (the one series)
    EXPECT_EQ(index.getSeriesCacheSize(), 1u);

    co_await index.close();
    co_return;
}

// Verify the cache never exceeds maxSeriesCacheSize by more than 1
// (the eviction should trigger on insert that exceeds the limit)
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, CacheNeverExceedsMaxByMoreThanOne) {
    LevelDBIndex index(0);
    co_await index.open();

    const size_t maxSize = 10;
    index.setMaxSeriesCacheSize(maxSize);

    // Insert many series and check after each insert
    for (int i = 0; i < 50; ++i) {
        co_await index.getOrCreateSeriesId(
            "sensor",
            {{"id", std::to_string(i)}},
            "reading"
        );
        // After each insert, the cache should not exceed maxSize
        // (it may be 1 right after a clear, or up to maxSize)
        EXPECT_LE(index.getSeriesCacheSize(), maxSize);
    }

    co_await index.close();
    co_return;
}

// Verify that the metadata caches (fieldsCache, tagsCache) are NOT
// affected by the series cache bound -- they grow independently
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, MetadataCachesNotAffected) {
    LevelDBIndex index(0);
    co_await index.open();

    // Set a tiny series cache limit
    index.setMaxSeriesCacheSize(2);

    // Insert series across multiple measurements with different fields/tags
    // This populates fieldsCache and tagsCache
    co_await index.getOrCreateSeriesId("m1", {{"t1", "v1"}}, "f1");
    co_await index.getOrCreateSeriesId("m1", {{"t1", "v1"}}, "f2");
    co_await index.getOrCreateSeriesId("m2", {{"t2", "v2"}}, "f3");

    // The series cache should have been evicted (3 > 2)
    // But metadata should still be queryable (it's in LevelDB regardless)
    auto fields_m1 = co_await index.getFields("m1");
    EXPECT_EQ(fields_m1.size(), 2u);
    EXPECT_TRUE(fields_m1.count("f1") > 0);
    EXPECT_TRUE(fields_m1.count("f2") > 0);

    auto fields_m2 = co_await index.getFields("m2");
    EXPECT_EQ(fields_m2.size(), 1u);
    EXPECT_TRUE(fields_m2.count("f3") > 0);

    auto tags_m1 = co_await index.getTags("m1");
    EXPECT_TRUE(tags_m1.count("t1") > 0);

    co_await index.close();
    co_return;
}

// Verify that setting maxSeriesCacheSize to 0 means the cache is always
// cleared (effectively disabled)
SEASTAR_TEST_F(LevelDBIndexCacheBoundsTest, ZeroMaxSizeDisablesCache) {
    LevelDBIndex index(0);
    co_await index.open();

    // Setting to 0 means every insert triggers a clear
    index.setMaxSeriesCacheSize(0);

    co_await index.getOrCreateSeriesId("m", {{"k", "v"}}, "f");

    // Cache should be cleared after each insert (only the re-inserted
    // entry remains, but next insert will clear again)
    // With maxSize=0, the cache has at most 1 entry (the re-inserted one)
    EXPECT_LE(index.getSeriesCacheSize(), 1u);

    // Multiple inserts should still work
    for (int i = 0; i < 10; ++i) {
        co_await index.getOrCreateSeriesId(
            "m", {{"k", "v" + std::to_string(i)}}, "f"
        );
        EXPECT_LE(index.getSeriesCacheSize(), 1u);
    }

    co_await index.close();
    co_return;
}
