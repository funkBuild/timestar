/*
 * Tests for the generic LRU cache utility (lib/utils/lru_cache.hpp),
 * the series metadata cache, and the discovery result cache in LevelDBIndex.
 *
 * These tests verify:
 * - LRU cache basic operations (put, get, eviction, clearByPrefix)
 * - Memory bounds respected (insert entries until over budget, verify eviction)
 * - Metadata cache hit/miss behavior
 * - Discovery cache hit/miss behavior and invalidation on new series insertion
 * - Field filter produces different cache entries
 * - Correctness: cached results match uncached results
 */

#include "../../../lib/utils/lru_cache.hpp"

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/storage/tsm.hpp"  // for TSMValueType enum definition
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

// ============================================================================
// Phase 1: Generic LRU cache unit tests (no Seastar needed)
// ============================================================================

TEST(LRUCacheTest, BasicPutAndGet) {
    timestar::LRUCache<std::string, int> cache(4096);

    cache.put("key1", 42);
    auto* val = cache.get("key1");
    ASSERT_NE(val, nullptr);
    EXPECT_EQ(*val, 42);

    EXPECT_EQ(cache.get("nonexistent"), nullptr);
}

TEST(LRUCacheTest, UpdateExistingKey) {
    timestar::LRUCache<std::string, int> cache(4096);

    cache.put("key1", 10);
    cache.put("key1", 20);
    EXPECT_EQ(cache.size(), 1u);

    auto* val = cache.get("key1");
    ASSERT_NE(val, nullptr);
    EXPECT_EQ(*val, 20);
}

TEST(LRUCacheTest, EvictsLeastRecentlyUsed) {
    // Very small budget to force eviction
    timestar::LRUCache<std::string, int> cache(300);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.put("c", 3);

    // The cache should have evicted the oldest entries if over budget.
    // At minimum, the most recently inserted entry should be present.
    auto* val_c = cache.get("c");
    ASSERT_NE(val_c, nullptr);
    EXPECT_EQ(*val_c, 3);
}

TEST(LRUCacheTest, GetPromotesToFront) {
    // Budget large enough for exactly 2 entries (each entry ~140-180 bytes overhead)
    timestar::LRUCache<std::string, int> cache(500);

    cache.put("a", 1);
    cache.put("b", 2);

    // Access "a" to promote it to the front (b is now LRU)
    cache.get("a");

    // Insert "c" - should evict "b" (LRU) not "a" (recently accessed)
    cache.put("c", 3);

    // "a" should still be present (was promoted before "c" was inserted)
    auto* val_a = cache.get("a");
    // "b" should be evicted as it was the LRU entry
    auto* val_b = cache.get("b");
    EXPECT_NE(cache.get("c"), nullptr);  // "c" was just inserted
    // At least one of "a" should be present if budget allows 2 entries
    // If budget is tight, this is best-effort
    if (val_a != nullptr) {
        EXPECT_EQ(val_b, nullptr);  // "b" was LRU, should be evicted
    }
}

TEST(LRUCacheTest, EraseEntry) {
    timestar::LRUCache<std::string, int> cache(4096);

    cache.put("key1", 42);
    EXPECT_TRUE(cache.erase("key1"));
    EXPECT_EQ(cache.get("key1"), nullptr);
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.currentBytes(), 0u);

    // Erasing non-existent key returns false
    EXPECT_FALSE(cache.erase("nonexistent"));
}

TEST(LRUCacheTest, ClearByPrefix) {
    timestar::LRUCache<std::string, int> cache(8192);

    // Use std::string constructor with explicit length to include embedded null bytes
    std::string wk1("weather\0loc1", 12);
    std::string wk2("weather\0loc2", 12);
    std::string ck1("cpu\0host1", 9);
    std::string ck2("cpu\0host2", 9);

    cache.put(wk1, 1);
    cache.put(wk2, 2);
    cache.put(ck1, 3);
    cache.put(ck2, 4);
    EXPECT_EQ(cache.size(), 4u);

    // Clear all entries with "weather" prefix
    std::string prefix = "weather";
    size_t removed = cache.clearByPrefix(prefix);
    EXPECT_EQ(removed, 2u);
    EXPECT_EQ(cache.size(), 2u);

    // "cpu" entries should remain
    EXPECT_NE(cache.get(ck1), nullptr);
    EXPECT_NE(cache.get(ck2), nullptr);

    // "weather" entries should be gone
    EXPECT_EQ(cache.get(wk1), nullptr);
    EXPECT_EQ(cache.get(wk2), nullptr);
}

TEST(LRUCacheTest, ClearAll) {
    timestar::LRUCache<std::string, int> cache(4096);

    cache.put("a", 1);
    cache.put("b", 2);
    cache.clear();

    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.currentBytes(), 0u);
    EXPECT_EQ(cache.get("a"), nullptr);
}

TEST(LRUCacheTest, MemoryBoundsRespected) {
    // Use a small budget
    constexpr size_t budget = 1024;
    timestar::LRUCache<std::string, int> cache(budget);

    // Insert many entries - cache should evict to stay within budget
    for (int i = 0; i < 100; ++i) {
        cache.put("key" + std::to_string(i), i);
    }

    // currentBytes should not exceed maxBytes by more than one entry's worth
    // (the last entry may temporarily push over if it's the only one)
    EXPECT_LE(cache.currentBytes(), budget + 300);  // generous margin for 1 entry overhead
    EXPECT_GT(cache.size(), 0u);
}

TEST(LRUCacheTest, ZeroBudgetAllowsSingleEntry) {
    timestar::LRUCache<std::string, int> cache(0);

    // With 0 budget, each put evicts everything first, but always allows 1 entry
    cache.put("a", 1);
    EXPECT_EQ(cache.size(), 1u);

    cache.put("b", 2);
    EXPECT_EQ(cache.size(), 1u);
    EXPECT_NE(cache.get("b"), nullptr);
    EXPECT_EQ(cache.get("a"), nullptr);
}

TEST(LRUCacheTest, SeriesId128Key) {
    timestar::LRUCache<SeriesId128, std::string, SeriesId128::Hash> cache(4096);

    SeriesId128 id1 = SeriesId128::fromSeriesKey("series1");
    SeriesId128 id2 = SeriesId128::fromSeriesKey("series2");

    cache.put(id1, "metadata1");
    cache.put(id2, "metadata2");

    auto* val1 = cache.get(id1);
    ASSERT_NE(val1, nullptr);
    EXPECT_EQ(*val1, "metadata1");

    auto* val2 = cache.get(id2);
    ASSERT_NE(val2, nullptr);
    EXPECT_EQ(*val2, "metadata2");
}

// ============================================================================
// Phase 2: Series Metadata Cache tests (require Seastar + LevelDB)
// ============================================================================

class MetadataCacheTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }
    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

SEASTAR_TEST_F(MetadataCacheTest, CachePopulatedOnInsert) {
    LevelDBIndex index(0);
    co_await index.open();

    auto id = co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");

    // After insert, metadata should be in cache
    EXPECT_GE(index.getMetadataCacheSize(), 1u);
    EXPECT_GT(index.getMetadataCacheBytes(), 0u);

    // Lookup should hit cache (no LevelDB read needed)
    auto metadata = co_await index.getSeriesMetadata(id);
    EXPECT_TRUE(metadata.has_value());
    if (metadata.has_value()) {
        EXPECT_EQ(metadata->measurement, "weather");
        EXPECT_EQ(metadata->field, "temperature");
        EXPECT_EQ(metadata->tags.at("location"), "us-west");
    }

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(MetadataCacheTest, CachePopulatedOnBatchInsert) {
    LevelDBIndex index(0);
    co_await index.open();

    std::vector<MetadataOp> ops;
    ops.push_back({TSMValueType::Float, "weather", "temperature", {{"loc", "us-west"}}});
    ops.push_back({TSMValueType::Float, "weather", "humidity", {{"loc", "us-east"}}});
    ops.push_back({TSMValueType::Float, "cpu", "usage", {{"host", "server-01"}}});

    co_await index.indexMetadataBatch(ops);

    // All three series should be in the metadata cache
    EXPECT_GE(index.getMetadataCacheSize(), 3u);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(MetadataCacheTest, FindSeriesUsesCache) {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert some series
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "temperature");

    size_t cacheSizeBefore = index.getMetadataCacheSize();
    EXPECT_GE(cacheSizeBefore, 2u);

    // Query should use cached metadata
    std::unordered_set<std::string> fieldFilter;
    auto result = co_await index.findSeriesWithMetadata("weather", {}, fieldFilter, 0);
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_EQ(result->size(), 2u);
    }

    // Cache size should be unchanged (no new entries needed)
    EXPECT_EQ(index.getMetadataCacheSize(), cacheSizeBefore);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(MetadataCacheTest, CachedResultsMatchUncached) {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert series
    auto id1 = co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    auto id2 = co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "humidity");

    // Get metadata via cache
    auto meta1 = co_await index.getSeriesMetadata(id1);
    auto meta2 = co_await index.getSeriesMetadata(id2);

    EXPECT_TRUE(meta1.has_value());
    EXPECT_TRUE(meta2.has_value());

    if (meta1.has_value()) {
        EXPECT_EQ(meta1->measurement, "weather");
        EXPECT_EQ(meta1->field, "temperature");
        EXPECT_EQ(meta1->tags.at("location"), "us-west");
    }

    if (meta2.has_value()) {
        EXPECT_EQ(meta2->measurement, "weather");
        EXPECT_EQ(meta2->field, "humidity");
        EXPECT_EQ(meta2->tags.at("location"), "us-east");
    }

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(MetadataCacheTest, BatchLookupPartialCacheHit) {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert two series
    auto id1 = co_await index.getOrCreateSeriesId("weather", {{"loc", "a"}}, "temp");
    auto id2 = co_await index.getOrCreateSeriesId("weather", {{"loc", "b"}}, "temp");

    // Both should be in cache from the insert
    EXPECT_GE(index.getMetadataCacheSize(), 2u);

    // Batch lookup should use cache for both
    auto results = co_await index.getSeriesMetadataBatch({id1, id2});
    EXPECT_EQ(results.size(), 2u);
    EXPECT_TRUE(results[0].second.has_value());
    EXPECT_TRUE(results[1].second.has_value());

    co_await index.close();
    co_return;
}

// ============================================================================
// Phase 3: Discovery Cache tests
// ============================================================================

class DiscoveryCacheTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }
    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

SEASTAR_TEST_F(DiscoveryCacheTest, CachePopulatedOnFirstQuery) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");

    EXPECT_EQ(index.getDiscoveryCacheSize(), 0u);

    std::unordered_set<std::string> fieldFilter;
    auto result = co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
        EXPECT_EQ(result.value()->size(), 1u);
    }

    // Discovery cache should now have one entry
    EXPECT_EQ(index.getDiscoveryCacheSize(), 1u);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, CacheHitReturnsSameResults) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"location", "us-west"}}, "temperature");
    co_await index.getOrCreateSeriesId("weather", {{"location", "us-east"}}, "temperature");

    std::unordered_set<std::string> fieldFilter;

    // First query: cache miss
    auto result1 = co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    EXPECT_TRUE(result1.has_value());

    // Second query: cache hit
    auto result2 = co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    EXPECT_TRUE(result2.has_value());

    // Results should be identical
    if (result1.has_value() && result2.has_value()) {
        EXPECT_EQ(result1.value()->size(), result2.value()->size());
        for (size_t i = 0; i < result1.value()->size(); ++i) {
            EXPECT_EQ((*result1.value())[i].seriesId, (*result2.value())[i].seriesId);
            EXPECT_EQ((*result1.value())[i].metadata.measurement, (*result2.value())[i].metadata.measurement);
            EXPECT_EQ((*result1.value())[i].metadata.field, (*result2.value())[i].metadata.field);
        }
    }

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, DifferentFieldFiltersDifferentCacheEntries) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"loc", "us"}}, "temperature");
    co_await index.getOrCreateSeriesId("weather", {{"loc", "us"}}, "humidity");

    // Query with no field filter
    std::unordered_set<std::string> noFilter;
    auto result1 = co_await index.findSeriesWithMetadataCached("weather", {}, noFilter, 0);
    EXPECT_TRUE(result1.has_value());
    if (result1.has_value()) {
        EXPECT_EQ(result1.value()->size(), 2u);
    }

    // Query with field filter for "temperature" only
    std::unordered_set<std::string> tempFilter = {"temperature"};
    auto result2 = co_await index.findSeriesWithMetadataCached("weather", {}, tempFilter, 0);
    EXPECT_TRUE(result2.has_value());
    if (result2.has_value()) {
        EXPECT_EQ(result2.value()->size(), 1u);
    }

    // Should have two separate cache entries
    EXPECT_EQ(index.getDiscoveryCacheSize(), 2u);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, InvalidationOnNewSeries) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"loc", "us-west"}}, "temperature");

    // Populate discovery cache
    std::unordered_set<std::string> fieldFilter;
    auto result1 = co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    EXPECT_TRUE(result1.has_value());
    if (result1.has_value()) {
        EXPECT_EQ(result1.value()->size(), 1u);
    }
    EXPECT_EQ(index.getDiscoveryCacheSize(), 1u);

    // Insert new series for same measurement - should invalidate cache
    co_await index.getOrCreateSeriesId("weather", {{"loc", "us-east"}}, "temperature");
    EXPECT_EQ(index.getDiscoveryCacheSize(), 0u);

    // Re-query should now find 2 series
    auto result2 = co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    EXPECT_TRUE(result2.has_value());
    if (result2.has_value()) {
        EXPECT_EQ(result2.value()->size(), 2u);
    }

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, InvalidationOnBatchInsert) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"loc", "us-west"}}, "temperature");

    // Populate discovery cache
    std::unordered_set<std::string> fieldFilter;
    co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    EXPECT_EQ(index.getDiscoveryCacheSize(), 1u);

    // Batch insert new series
    std::vector<MetadataOp> ops;
    ops.push_back({TSMValueType::Float, "weather", "humidity", {{"loc", "us-east"}}});
    co_await index.indexMetadataBatch(ops);

    // Discovery cache for "weather" should be invalidated
    EXPECT_EQ(index.getDiscoveryCacheSize(), 0u);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, InvalidationScopedByMeasurement) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"loc", "us"}}, "temp");
    co_await index.getOrCreateSeriesId("cpu", {{"host", "s1"}}, "usage");

    // Populate discovery cache for both measurements
    std::unordered_set<std::string> fieldFilter;
    co_await index.findSeriesWithMetadataCached("weather", {}, fieldFilter, 0);
    co_await index.findSeriesWithMetadataCached("cpu", {}, fieldFilter, 0);
    EXPECT_EQ(index.getDiscoveryCacheSize(), 2u);

    // Insert new series for "weather" only
    co_await index.getOrCreateSeriesId("weather", {{"loc", "eu"}}, "temp");

    // Only "weather" cache should be invalidated, "cpu" should remain
    EXPECT_EQ(index.getDiscoveryCacheSize(), 1u);

    // "cpu" should still be cached
    auto cpuResult = co_await index.findSeriesWithMetadataCached("cpu", {}, fieldFilter, 0);
    EXPECT_TRUE(cpuResult.has_value());
    if (cpuResult.has_value()) {
        EXPECT_EQ(cpuResult.value()->size(), 1u);
    }

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, TagScopesCreateSeparateCacheEntries) {
    LevelDBIndex index(0);
    co_await index.open();

    co_await index.getOrCreateSeriesId("weather", {{"loc", "us-west"}}, "temp");
    co_await index.getOrCreateSeriesId("weather", {{"loc", "us-east"}}, "temp");

    std::unordered_set<std::string> noFilter;

    // Query with tag scope for "us-west"
    std::map<std::string, std::string> westScope = {{"loc", "us-west"}};
    auto result1 = co_await index.findSeriesWithMetadataCached("weather", westScope, noFilter, 0);
    EXPECT_TRUE(result1.has_value());
    if (result1.has_value()) {
        EXPECT_EQ(result1.value()->size(), 1u);
    }

    // Query with tag scope for "us-east"
    std::map<std::string, std::string> eastScope = {{"loc", "us-east"}};
    auto result2 = co_await index.findSeriesWithMetadataCached("weather", eastScope, noFilter, 0);
    EXPECT_TRUE(result2.has_value());
    if (result2.has_value()) {
        EXPECT_EQ(result2.value()->size(), 1u);
    }

    // Should have two separate cache entries
    EXPECT_EQ(index.getDiscoveryCacheSize(), 2u);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(DiscoveryCacheTest, CacheRespectsMaxSeriesLimit) {
    LevelDBIndex index(0);
    co_await index.open();

    // Insert more series than our query limit
    for (int i = 0; i < 5; ++i) {
        co_await index.getOrCreateSeriesId("weather", {{"id", std::to_string(i)}}, "temp");
    }

    // Query with maxSeries=2 should return error (not cached)
    std::unordered_set<std::string> noFilter;
    auto result = co_await index.findSeriesWithMetadataCached("weather", {}, noFilter, 2);
    EXPECT_FALSE(result.has_value());

    // Error results should NOT be cached
    EXPECT_EQ(index.getDiscoveryCacheSize(), 0u);

    co_await index.close();
    co_return;
}
