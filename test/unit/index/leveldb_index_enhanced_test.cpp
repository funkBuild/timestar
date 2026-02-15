/*
 * Tests for enhanced LevelDB index functionality
 * Tests group-by support, field statistics, and improved series discovery
 */

#include <gtest/gtest.h>
#include "../../seastar_gtest.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

class LevelDBIndexEnhancedTest : public ::testing::Test {
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

SEASTAR_TEST_F(LevelDBIndexEnhancedTest, FindSeriesByTag) {
    LevelDBIndex index(0);

    co_await index.open();

    // Create multiple series with same tag value
    std::string measurement = "temperature";

    std::map<std::string, std::string> tags1 = {{"location", "room1"}, {"sensor", "A"}};
    std::map<std::string, std::string> tags2 = {{"location", "room1"}, {"sensor", "B"}};
    std::map<std::string, std::string> tags3 = {{"location", "room2"}, {"sensor", "A"}};

    SeriesId128 id1 = co_await index.getOrCreateSeriesId(measurement, tags1, "value");
    SeriesId128 id2 = co_await index.getOrCreateSeriesId(measurement, tags2, "value");
    SeriesId128 id3 = co_await index.getOrCreateSeriesId(measurement, tags3, "value");

    // Find all series with location=room1
    auto seriesIds = co_await index.findSeriesByTag(measurement, "location", "room1");

    EXPECT_EQ(seriesIds.size(), 2);
    std::set<SeriesId128> foundIds(seriesIds.begin(), seriesIds.end());
    EXPECT_TRUE(foundIds.count(id1) > 0);
    EXPECT_TRUE(foundIds.count(id2) > 0);
    EXPECT_FALSE(foundIds.count(id3) > 0);

    // Find all series with sensor=A
    seriesIds = co_await index.findSeriesByTag(measurement, "sensor", "A");

    EXPECT_EQ(seriesIds.size(), 2);
    foundIds = std::set<SeriesId128>(seriesIds.begin(), seriesIds.end());
    EXPECT_TRUE(foundIds.count(id1) > 0);
    EXPECT_FALSE(foundIds.count(id2) > 0);
    EXPECT_TRUE(foundIds.count(id3) > 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexEnhancedTest, GetSeriesGroupedByTag) {
    LevelDBIndex index(0);

    co_await index.open();

    std::string measurement = "cpu_usage";

    // Create series with different host values
    std::map<std::string, std::string> tags1 = {{"host", "server1"}, {"cpu", "0"}};
    std::map<std::string, std::string> tags2 = {{"host", "server1"}, {"cpu", "1"}};
    std::map<std::string, std::string> tags3 = {{"host", "server2"}, {"cpu", "0"}};
    std::map<std::string, std::string> tags4 = {{"host", "server3"}, {"cpu", "0"}};

    SeriesId128 id1 = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
    SeriesId128 id2 = co_await index.getOrCreateSeriesId(measurement, tags2, "idle");
    SeriesId128 id3 = co_await index.getOrCreateSeriesId(measurement, tags3, "idle");
    SeriesId128 id4 = co_await index.getOrCreateSeriesId(measurement, tags4, "idle");

    // Get series grouped by host
    auto grouped = co_await index.getSeriesGroupedByTag(measurement, "host");

    EXPECT_EQ(grouped.size(), 3); // 3 unique hosts

    // Check server1 has 2 series
    EXPECT_EQ(grouped["server1"].size(), 2);
    std::set<SeriesId128> server1Ids(grouped["server1"].begin(), grouped["server1"].end());
    EXPECT_TRUE(server1Ids.count(id1) > 0);
    EXPECT_TRUE(server1Ids.count(id2) > 0);

    // Check server2 has 1 series
    EXPECT_EQ(grouped["server2"].size(), 1);
    EXPECT_EQ(grouped["server2"][0], id3);

    // Check server3 has 1 series
    EXPECT_EQ(grouped["server3"].size(), 1);
    EXPECT_EQ(grouped["server3"][0], id4);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexEnhancedTest, FieldStatistics) {
    LevelDBIndex index(0);

    co_await index.open();

    // Create a series
    std::string measurement = "temperature";
    std::map<std::string, std::string> tags = {{"location", "room1"}};
    SeriesId128 seriesId = co_await index.getOrCreateSeriesId(measurement, tags, "value");

    // Update field statistics
    LevelDBIndex::FieldStats stats;
    stats.dataType = "float";
    stats.minTime = 1000000000;
    stats.maxTime = 2000000000;
    stats.pointCount = 100;

    co_await index.updateFieldStats(seriesId, "value", stats);

    // Retrieve field statistics
    auto retrievedStats = co_await index.getFieldStats(seriesId, "value");

    EXPECT_TRUE(retrievedStats.has_value());
    EXPECT_EQ(retrievedStats->dataType, "float");
    EXPECT_EQ(retrievedStats->minTime, 1000000000);
    EXPECT_EQ(retrievedStats->maxTime, 2000000000);
    EXPECT_EQ(retrievedStats->pointCount, 100);

    // Non-existent stats should return nullopt
    auto noStats = co_await index.getFieldStats(seriesId, "nonexistent");
    EXPECT_FALSE(noStats.has_value());

    // Update stats with new values
    stats.maxTime = 3000000000;
    stats.pointCount = 200;
    co_await index.updateFieldStats(seriesId, "value", stats);

    retrievedStats = co_await index.getFieldStats(seriesId, "value");
    EXPECT_TRUE(retrievedStats.has_value());
    EXPECT_EQ(retrievedStats->maxTime, 3000000000);
    EXPECT_EQ(retrievedStats->pointCount, 200);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexEnhancedTest, ComplexTagQueries) {
    LevelDBIndex index(0);

    co_await index.open();

    std::string measurement = "metrics";

    // Create a matrix of series with different tag combinations
    std::vector<SeriesId128> allIds;
    for (int dc = 1; dc <= 2; dc++) {
        for (int host = 1; host <= 3; host++) {
            for (int cpu = 0; cpu <= 1; cpu++) {
                std::map<std::string, std::string> tags = {
                    {"datacenter", "dc" + std::to_string(dc)},
                    {"host", "host" + std::to_string(host)},
                    {"cpu", std::to_string(cpu)}
                };

                SeriesId128 id = co_await index.getOrCreateSeriesId(measurement, tags, "usage");
                allIds.push_back(id);
            }
        }
    }

    // Total: 2 datacenters * 3 hosts * 2 cpus = 12 series
    EXPECT_EQ(allIds.size(), 12);

    // Find all series in dc1
    auto dc1Series = co_await index.findSeriesByTag(measurement, "datacenter", "dc1");
    EXPECT_EQ(dc1Series.size(), 6); // 3 hosts * 2 cpus

    // Find all series on host2
    auto host2Series = co_await index.findSeriesByTag(measurement, "host", "host2");
    EXPECT_EQ(host2Series.size(), 4); // 2 datacenters * 2 cpus

    // Group by datacenter
    auto byDatacenter = co_await index.getSeriesGroupedByTag(measurement, "datacenter");
    EXPECT_EQ(byDatacenter.size(), 2);
    EXPECT_EQ(byDatacenter["dc1"].size(), 6);
    EXPECT_EQ(byDatacenter["dc2"].size(), 6);

    // Group by CPU
    auto byCpu = co_await index.getSeriesGroupedByTag(measurement, "cpu");
    EXPECT_EQ(byCpu.size(), 2);
    EXPECT_EQ(byCpu["0"].size(), 6); // 2 datacenters * 3 hosts
    EXPECT_EQ(byCpu["1"].size(), 6);

    co_await index.close();
    co_return;
}
