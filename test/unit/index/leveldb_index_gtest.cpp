/*
 * Google Test + Seastar integration for LevelDBIndex tests
 *
 * This test file uses SEASTAR_TEST_F macro to test async LevelDB index operations
 */

#include <gtest/gtest.h>
#include "../../seastar_gtest.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>

// Need to include these after filesystem to avoid conflicts
#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

class LevelDBIndexAsyncTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test index
        std::filesystem::remove_all("shard_0");
        std::filesystem::remove_all("shard_0_b");
    }

    void TearDown() override {
        // Clean up test index
        std::filesystem::remove_all("shard_0");
        std::filesystem::remove_all("shard_0_b");
    }
};

SEASTAR_TEST_F(LevelDBIndexAsyncTest, BasicIndexOperations) {
    LevelDBIndex index(0); // Use shard 999 for testing

    co_await index.open();

    // Test 1: Create a series and get ID
    TSDBInsert<double> tempInsert("weather", "temperature");
    tempInsert.addTag("location", "us-midwest");
    tempInsert.addTag("host", "server-01");

    SeriesId128 seriesId1 = co_await index.indexInsert(tempInsert);
    EXPECT_FALSE(seriesId1.isZero());

    // Test 2: Same series should return same ID
    SeriesId128 seriesId2 = co_await index.indexInsert(tempInsert);
    EXPECT_EQ(seriesId1, seriesId2);

    // Test 3: Different field should get different ID
    TSDBInsert<double> humidityInsert("weather", "humidity");
    humidityInsert.addTag("location", "us-midwest");
    humidityInsert.addTag("host", "server-01");

    SeriesId128 seriesId3 = co_await index.indexInsert(humidityInsert);
    EXPECT_FALSE(seriesId3.isZero());
    EXPECT_NE(seriesId1, seriesId3);

    // Test 4: Check measurement fields
    auto fields = co_await index.getFields("weather");
    EXPECT_EQ(fields.size(), 2);
    EXPECT_TRUE(fields.find("temperature") != fields.end());
    EXPECT_TRUE(fields.find("humidity") != fields.end());

    // Test 5: Check measurement tags
    auto tags = co_await index.getTags("weather");
    EXPECT_EQ(tags.size(), 2);
    EXPECT_TRUE(tags.find("location") != tags.end());
    EXPECT_TRUE(tags.find("host") != tags.end());

    // Test 6: Check tag values
    auto locations = co_await index.getTagValues("weather", "location");
    EXPECT_EQ(locations.size(), 1);
    EXPECT_TRUE(locations.find("us-midwest") != locations.end());

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexAsyncTest, SeriesIdGeneration) {
    LevelDBIndex index(0); // Use shard 998 for this test

    co_await index.open();

    // Test multiple series with same measurement, different tags
    std::string measurement = "cpu_usage";

    std::map<std::string, std::string> tags1 = {{"host", "server-01"}, {"cpu", "cpu0"}};
    std::map<std::string, std::string> tags2 = {{"host", "server-01"}, {"cpu", "cpu1"}};
    std::map<std::string, std::string> tags3 = {{"host", "server-02"}, {"cpu", "cpu0"}};

    SeriesId128 id1 = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
    SeriesId128 id2 = co_await index.getOrCreateSeriesId(measurement, tags2, "idle");
    SeriesId128 id3 = co_await index.getOrCreateSeriesId(measurement, tags3, "idle");

    // All should have different IDs
    EXPECT_NE(id1, id2);
    EXPECT_NE(id1, id3);
    EXPECT_NE(id2, id3);

    // Test same series returns same ID
    SeriesId128 id1_again = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
    EXPECT_EQ(id1, id1_again);

    // Test different field gets different ID
    SeriesId128 id4 = co_await index.getOrCreateSeriesId(measurement, tags1, "user");
    EXPECT_NE(id1, id4);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexAsyncTest, Persistence) {
    std::string measurement = "test_measurement";
    std::map<std::string, std::string> tags = {{"host", "test-host"}};
    std::string field = "test_field";
    SeriesId128 originalId;

    // Phase 1: Create series and close
    {
        auto index = std::make_unique<LevelDBIndex>(0);
        co_await index->open();
        originalId = co_await index->getOrCreateSeriesId(measurement, tags, field);
        EXPECT_FALSE(originalId.isZero());
        co_await index->close();
    }

    // Phase 2: Reopen and verify
    {
        auto index = std::make_unique<LevelDBIndex>(0);
        co_await index->open();
        SeriesId128 newId = co_await index->getOrCreateSeriesId(measurement, tags, field);
        EXPECT_EQ(originalId, newId);

        auto fields = co_await index->getFields(measurement);
        EXPECT_EQ(fields.size(), 1);
        EXPECT_TRUE(fields.find(field) != fields.end());
        co_await index->close();
    }

    co_return;
}
