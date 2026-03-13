/*
 * Tests for null pointer dereference protection on non-zero shards.
 *
 * LevelDB is only opened on shard 0 (centralized metadata model).
 * Non-zero shards have db == nullptr.  All methods that access db->
 * must guard against this.  These tests create a LevelDBIndex with
 * shard ID 1 (so db remains null) and verify each method handles it
 * gracefully instead of crashing.
 */

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/index/leveldb_index.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

class LevelDBIndexNullCheckTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any leftover shard directories from previous runs
        std::filesystem::remove_all("shard_1");
    }

    void TearDown() override { std::filesystem::remove_all("shard_1"); }
};

// Test the primary bug: getSeriesMetadata on non-zero shard should return
// std::nullopt instead of crashing with a null pointer dereference.
SEASTAR_TEST_F(LevelDBIndexNullCheckTest, GetSeriesMetadataOnNonZeroShard) {
    LevelDBIndex index(1);  // Shard 1 -- db will be null

    // open() on non-zero shard simply skips LevelDB initialization
    co_await index.open();

    // Create a deterministic series ID to query
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test_measurement,tag=value field");

    // Before the fix this would crash with SIGSEGV (null db pointer).
    // After the fix it should return std::nullopt.
    auto metadata = co_await index.getSeriesMetadata(seriesId);
    EXPECT_FALSE(metadata.has_value());

    co_await index.close();
    co_return;
}

// findSeriesByTag on non-zero shard should return empty vector
SEASTAR_TEST_F(LevelDBIndexNullCheckTest, FindSeriesByTagOnNonZeroShard) {
    LevelDBIndex index(1);
    co_await index.open();

    auto results = co_await index.findSeriesByTag("measurement", "tag", "value");
    EXPECT_TRUE(results.empty());

    co_await index.close();
    co_return;
}

// getSeriesGroupedByTag on non-zero shard should return empty map
SEASTAR_TEST_F(LevelDBIndexNullCheckTest, GetSeriesGroupedByTagOnNonZeroShard) {
    LevelDBIndex index(1);
    co_await index.open();

    auto grouped = co_await index.getSeriesGroupedByTag("measurement", "tagKey");
    EXPECT_TRUE(grouped.empty());

    co_await index.close();
    co_return;
}

// updateFieldStats on non-zero shard should be a no-op (no crash)
SEASTAR_TEST_F(LevelDBIndexNullCheckTest, UpdateFieldStatsOnNonZeroShard) {
    LevelDBIndex index(1);
    co_await index.open();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.series");
    LevelDBIndex::FieldStats stats{"float", 1000, 2000, 100};

    // Should not crash -- just silently return on non-zero shard
    co_await index.updateFieldStats(seriesId, "value", stats);

    co_await index.close();
    co_return;
}

// getFieldStats on non-zero shard should return std::nullopt
SEASTAR_TEST_F(LevelDBIndexNullCheckTest, GetFieldStatsOnNonZeroShard) {
    LevelDBIndex index(1);
    co_await index.open();

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.series");

    auto stats = co_await index.getFieldStats(seriesId, "value");
    EXPECT_FALSE(stats.has_value());

    co_await index.close();
    co_return;
}
