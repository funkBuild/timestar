/*
 * Tests for wildcard/regex tag filtering in LevelDB index.
 * Tests findSeriesByTagPattern() and the updated findSeries() dispatch.
 */

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/index/leveldb_index.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>

class LevelDBIndexPatternTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }

    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

SEASTAR_TEST_F(LevelDBIndexPatternTest, WildcardStarMatch) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    // Insert series with various host tag values
    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-02"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-10"}}, "usage");
    auto id4 = co_await index.getOrCreateSeriesId(measurement, {{"host", "client-01"}}, "usage");

    // Wildcard server-* should match server-01, server-02, server-10
    auto results = co_await index.findSeriesByTagPattern(measurement, "host", "server-*");

    EXPECT_EQ(results.size(), 3);
    std::set<SeriesId128> found(results.begin(), results.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);
    EXPECT_TRUE(found.count(id3) > 0);
    EXPECT_FALSE(found.count(id4) > 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, WildcardQuestionMarkMatch) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-02"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-10"}}, "usage");

    // server-0? should match server-01 and server-02 but not server-10
    auto results = co_await index.findSeriesByTagPattern(measurement, "host", "server-0?");

    EXPECT_EQ(results.size(), 2);
    std::set<SeriesId128> found(results.begin(), results.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);
    EXPECT_FALSE(found.count(id3) > 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, TildeRegexMatch) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-02"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-10"}}, "usage");
    auto id4 = co_await index.getOrCreateSeriesId(measurement, {{"host", "client-01"}}, "usage");

    // ~server-0[1-2] should match server-01 and server-02 only
    auto results = co_await index.findSeriesByTagPattern(measurement, "host", "~server-0[1-2]");

    EXPECT_EQ(results.size(), 2);
    std::set<SeriesId128> found(results.begin(), results.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);
    EXPECT_FALSE(found.count(id3) > 0);
    EXPECT_FALSE(found.count(id4) > 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, ExactMatchBackwardCompat) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");
    co_await index.getOrCreateSeriesId(measurement, {{"host", "server-02"}}, "usage");

    // Exact match should still work through findSeriesByTag
    auto results = co_await index.findSeriesByTag(measurement, "host", "server-01");

    EXPECT_EQ(results.size(), 1);
    EXPECT_EQ(results[0], id1);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, FindSeriesDispatchesPattern) {
    // Test that findSeries() correctly dispatches to findSeriesByTagPattern
    // when a scope value contains a pattern
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}, {"dc", "dc1"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-02"}, {"dc", "dc1"}}, "usage");
    auto id3 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-03"}, {"dc", "dc2"}}, "usage");
    auto id4 = co_await index.getOrCreateSeriesId(measurement, {{"host", "client-01"}, {"dc", "dc1"}}, "usage");

    // Pattern filter: host:server-* AND dc:dc1
    // Should match server-01 and server-02 (server-03 is dc2, client-01 doesn't match host)
    std::map<std::string, std::string> tagFilters = {{"host", "server-*"}, {"dc", "dc1"}};
    auto findResult = co_await index.findSeries(measurement, tagFilters);
    EXPECT_TRUE(findResult.has_value());
    if (!findResult.has_value()) {
        co_await index.close();
        co_return;
    }
    auto& results = findResult.value();

    EXPECT_EQ(results.size(), 2);
    std::set<SeriesId128> found(results.begin(), results.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);
    EXPECT_FALSE(found.count(id3) > 0);
    EXPECT_FALSE(found.count(id4) > 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, MatchAllWithStar) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId(measurement, {{"host", "client-01"}}, "usage");

    // * should match everything
    auto results = co_await index.findSeriesByTagPattern(measurement, "host", "*");

    EXPECT_EQ(results.size(), 2);
    std::set<SeriesId128> found(results.begin(), results.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, NoMatchReturnsEmpty) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");

    // Pattern that matches nothing
    auto results = co_await index.findSeriesByTagPattern(measurement, "host", "nonexistent-*");

    EXPECT_EQ(results.size(), 0);

    co_await index.close();
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexPatternTest, SlashRegexMatch) {
    LevelDBIndex index(0);
    co_await index.open();

    std::string measurement = "cpu";

    auto id1 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-01"}}, "usage");
    auto id2 = co_await index.getOrCreateSeriesId(measurement, {{"host", "server-99"}}, "usage");
    co_await index.getOrCreateSeriesId(measurement, {{"host", "client-01"}}, "usage");

    // /regex/ pattern
    auto results = co_await index.findSeriesByTagPattern(measurement, "host", "/server-[0-9]+/");

    EXPECT_EQ(results.size(), 2);
    std::set<SeriesId128> found(results.begin(), results.end());
    EXPECT_TRUE(found.count(id1) > 0);
    EXPECT_TRUE(found.count(id2) > 0);

    co_await index.close();
    co_return;
}
