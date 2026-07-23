/*
 * Postings bitmap tests — Phase 2 roaring bitmap integration.
 *
 * Verifies:
 * - Local ID assignment and persistence
 * - findSeriesByTag returns correct results via bitmap lookup
 * - Multi-tag intersection via findSeries returns correct results
 * - Mutable postings (unflushed) visible in queries
 * - After flush + compact, queries still correct
 * - Migration from Phase 1 data (TAG_INDEX → bitmap)
 * - getSeriesGroupedByTag via bitmap prefix scan
 */

#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <string>

using namespace timestar::index;

class PostingsBitmapTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

// Helper: create series and return its ID
static seastar::future<SeriesId128> createSeries(NativeIndex& index, const std::string& measurement,
                                                   std::map<std::string, std::string> tags,
                                                   const std::string& field) {
    co_return co_await index.getOrCreateSeriesId(measurement, std::move(tags), field);
}

SEASTAR_TEST_F(PostingsBitmapTest, SingleTagLookup) {
    NativeIndex index(0);
    co_await index.open();

    // Create 3 series with region=us-west, 2 with region=us-east
    auto id1 = co_await createSeries(index, "cpu", {{"region", "us-west"}, {"host", "h1"}}, "usage");
    auto id2 = co_await createSeries(index, "cpu", {{"region", "us-west"}, {"host", "h2"}}, "usage");
    auto id3 = co_await createSeries(index, "cpu", {{"region", "us-west"}, {"host", "h3"}}, "usage");
    auto id4 = co_await createSeries(index, "cpu", {{"region", "us-east"}, {"host", "h4"}}, "usage");
    auto id5 = co_await createSeries(index, "cpu", {{"region", "us-east"}, {"host", "h5"}}, "usage");

    auto west = co_await index.findSeriesByTag("cpu", "region", "us-west");
    EXPECT_EQ(west.size(), 3u);

    auto east = co_await index.findSeriesByTag("cpu", "region", "us-east");
    EXPECT_EQ(east.size(), 2u);

    // Verify exact IDs (order may vary, use a set)
    std::set<SeriesId128> westSet(west.begin(), west.end());
    EXPECT_TRUE(westSet.count(id1));
    EXPECT_TRUE(westSet.count(id2));
    EXPECT_TRUE(westSet.count(id3));

    std::set<SeriesId128> eastSet(east.begin(), east.end());
    EXPECT_TRUE(eastSet.count(id4));
    EXPECT_TRUE(eastSet.count(id5));

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, MultiTagIntersection) {
    NativeIndex index(0);
    co_await index.open();

    // Create series with varying tags
    auto id1 = co_await createSeries(index, "cpu", {{"region", "us-west"}, {"host", "h1"}, {"env", "prod"}}, "usage");
    auto id2 = co_await createSeries(index, "cpu", {{"region", "us-west"}, {"host", "h2"}, {"env", "staging"}}, "usage");
    auto id3 = co_await createSeries(index, "cpu", {{"region", "us-east"}, {"host", "h3"}, {"env", "prod"}}, "usage");
    auto id4 = co_await createSeries(index, "cpu", {{"region", "us-west"}, {"host", "h4"}, {"env", "prod"}}, "usage");

    // 2-tag intersection: region=us-west AND env=prod → id1, id4
    auto result = co_await index.findSeries("cpu", {{"region", "us-west"}, {"env", "prod"}});
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 2u);
    std::set<SeriesId128> resultSet(result->begin(), result->end());
    EXPECT_TRUE(resultSet.count(id1));
    EXPECT_TRUE(resultSet.count(id4));

    // 3-tag intersection: region=us-west AND env=prod AND host=h1 → id1 only
    auto narrow = co_await index.findSeries("cpu", {{"region", "us-west"}, {"env", "prod"}, {"host", "h1"}});
    EXPECT_TRUE(narrow.has_value());
    EXPECT_EQ(narrow->size(), 1u);
    EXPECT_EQ((*narrow)[0], id1);

    // Intersection with no results
    auto empty = co_await index.findSeries("cpu", {{"region", "us-east"}, {"env", "staging"}});
    EXPECT_TRUE(empty.has_value());
    EXPECT_EQ(empty->size(), 0u);

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, MutablePostingsVisibleBeforeFlush) {
    NativeIndex index(0);
    co_await index.open();

    // Insert series — mutable postings should be visible immediately
    auto id = co_await createSeries(index, "temp", {{"location", "office"}}, "celsius");
    auto result = co_await index.findSeriesByTag("temp", "location", "office");
    EXPECT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], id);

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, PersistsAfterFlushAndCompact) {
    NativeIndex index(0);
    co_await index.open();

    auto id1 = co_await createSeries(index, "disk", {{"host", "server-1"}, {"dc", "dc1"}}, "iops");
    auto id2 = co_await createSeries(index, "disk", {{"host", "server-2"}, {"dc", "dc1"}}, "iops");
    auto id3 = co_await createSeries(index, "disk", {{"host", "server-3"}, {"dc", "dc2"}}, "iops");

    // Force flush + compaction to persist bitmaps to SSTables
    co_await index.compact();

    // Queries should still work after compaction
    auto dc1 = co_await index.findSeriesByTag("disk", "dc", "dc1");
    EXPECT_EQ(dc1.size(), 2u);

    auto dc2 = co_await index.findSeriesByTag("disk", "dc", "dc2");
    EXPECT_EQ(dc2.size(), 1u);
    EXPECT_EQ(dc2[0], id3);

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, PersistsAcrossReopen) {
    std::vector<SeriesId128> ids;

    {
        NativeIndex index(0);
        co_await index.open();
        ids.push_back(co_await createSeries(index, "mem", {{"host", "a"}}, "used"));
        ids.push_back(co_await createSeries(index, "mem", {{"host", "b"}}, "used"));
        ids.push_back(co_await createSeries(index, "mem", {{"host", "c"}}, "used"));
        co_await index.close();
    }

    {
        NativeIndex index(0);
        co_await index.open();

        auto result = co_await index.findSeriesByTag("mem", "host", "a");
        EXPECT_EQ(result.size(), 1u);
        EXPECT_EQ(result[0], ids[0]);

        auto all = co_await index.findSeriesByTag("mem", "host", "b");
        EXPECT_EQ(all.size(), 1u);

        co_await index.close();
    }
}

SEASTAR_TEST_F(PostingsBitmapTest, GroupByTagViaBitmap) {
    NativeIndex index(0);
    co_await index.open();

    co_await createSeries(index, "net", {{"dc", "dc1"}, {"host", "h1"}}, "bytes");
    co_await createSeries(index, "net", {{"dc", "dc1"}, {"host", "h2"}}, "bytes");
    co_await createSeries(index, "net", {{"dc", "dc2"}, {"host", "h3"}}, "bytes");
    co_await createSeries(index, "net", {{"dc", "dc2"}, {"host", "h4"}}, "bytes");
    co_await createSeries(index, "net", {{"dc", "dc3"}, {"host", "h5"}}, "bytes");

    auto groups = co_await index.getSeriesGroupedByTag("net", "dc");
    EXPECT_EQ(groups.size(), 3u);
    EXPECT_EQ(groups["dc1"].size(), 2u);
    EXPECT_EQ(groups["dc2"].size(), 2u);
    EXPECT_EQ(groups["dc3"].size(), 1u);

    // After compaction
    co_await index.compact();
    auto groups2 = co_await index.getSeriesGroupedByTag("net", "dc");
    EXPECT_EQ(groups2.size(), 3u);
    EXPECT_EQ(groups2["dc1"].size(), 2u);

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, FindSeriesWithMetadataUsesBitmaps) {
    NativeIndex index(0);
    co_await index.open();

    co_await createSeries(index, "cpu", {{"host", "h1"}, {"region", "west"}}, "p50");
    co_await createSeries(index, "cpu", {{"host", "h1"}, {"region", "west"}}, "p99");
    co_await createSeries(index, "cpu", {{"host", "h2"}, {"region", "east"}}, "p50");

    // findSeriesWithMetadata should use bitmap-based findSeries
    auto result = co_await index.findSeriesWithMetadata("cpu", {{"host", "h1"}}, {"p99"});
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.field, "p99");

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, EmptyTagLookup) {
    NativeIndex index(0);
    co_await index.open();

    co_await createSeries(index, "test", {{"key", "val"}}, "field");

    // Non-existent tag value
    auto result = co_await index.findSeriesByTag("test", "key", "nonexistent");
    EXPECT_EQ(result.size(), 0u);

    // Non-existent measurement
    auto result2 = co_await index.findSeriesByTag("nonexistent", "key", "val");
    EXPECT_EQ(result2.size(), 0u);

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, LargeScaleBitmaps) {
    NativeIndex index(0);
    co_await index.open();

    // Create 200 series across 4 regions
    std::map<std::string, std::vector<SeriesId128>> expectedByRegion;
    std::array<std::string, 4> regions = {"us-east", "us-west", "eu-west", "ap-south"};

    for (int i = 0; i < 200; ++i) {
        std::string host = "host-" + std::to_string(i);
        std::string region = regions[i % 4];
        auto id = co_await createSeries(index, "metric", {{"host", host}, {"region", region}}, "value");
        expectedByRegion[region].push_back(id);
    }

    // Verify cardinality per region
    for (auto& [region, expected] : expectedByRegion) {
        auto result = co_await index.findSeriesByTag("metric", "region", region);
        EXPECT_EQ(result.size(), expected.size()) << "Region: " << region;
    }

    // Compact and verify again
    co_await index.compact();
    for (auto& [region, expected] : expectedByRegion) {
        auto result = co_await index.findSeriesByTag("metric", "region", region);
        EXPECT_EQ(result.size(), expected.size()) << "Region after compact: " << region;
    }

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, BulkInsertWithMultipleFlushes) {
    NativeIndex index(0);
    co_await index.open();

    // Insert 4000 series across 10 measurements (same scale as benchmark)
    // This will trigger memtable flushes at 16MB
    int eastCount = 0, westCount = 0;
    for (int m = 0; m < 10; ++m) {
        std::string meas = "bulk_test_" + std::to_string(m);
        for (int i = 0; i < 50; ++i) {
            std::string host = "host-" + std::to_string(i);
            std::string region = (i < 25) ? "us-east" : "us-west";
            for (int f = 0; f < 8; ++f) {
                std::string field = "f" + std::to_string(f);
                co_await index.getOrCreateSeriesId(meas,
                    {{"host", host}, {"region", region}, {"rack", "rack-" + std::to_string(i % 5)},
                     {"env", (i < 10) ? "staging" : "production"}}, field);
            }
        }
    }
    // Count for first measurement only
    eastCount = 25 * 8;  // 25 hosts × 8 fields
    westCount = 25 * 8;

    // Before compact — check via bitmap (may have unflushed mutable postings)
    auto eastBefore = co_await index.findSeriesByTag("bulk_test_0", "region", "us-east");
    EXPECT_EQ(eastBefore.size(), static_cast<size_t>(eastCount)) << "Before compact: us-east";

    auto westBefore = co_await index.findSeriesByTag("bulk_test_0", "region", "us-west");
    EXPECT_EQ(westBefore.size(), static_cast<size_t>(westCount)) << "Before compact: us-west";

    // After compact
    co_await index.compact();

    auto eastAfter = co_await index.findSeriesByTag("bulk_test_0", "region", "us-east");
    EXPECT_EQ(eastAfter.size(), static_cast<size_t>(eastCount)) << "After compact: us-east";

    auto westAfter = co_await index.findSeriesByTag("bulk_test_0", "region", "us-west");
    EXPECT_EQ(westAfter.size(), static_cast<size_t>(westCount)) << "After compact: us-west";

    co_await index.close();
}

SEASTAR_TEST_F(PostingsBitmapTest, MigrationFromPhase1) {
    // Simulate Phase 1 data: create index, close, reopen (triggers migration)
    std::vector<SeriesId128> ids;

    {
        NativeIndex index(0);
        co_await index.open();
        ids.push_back(co_await createSeries(index, "weather", {{"city", "nyc"}}, "temp"));
        ids.push_back(co_await createSeries(index, "weather", {{"city", "sf"}}, "temp"));
        ids.push_back(co_await createSeries(index, "weather", {{"city", "nyc"}}, "humidity"));
        co_await index.close();
    }

    // Reopen — should restore LocalIdMap from persisted data
    {
        NativeIndex index(0);
        co_await index.open();

        auto nyc = co_await index.findSeriesByTag("weather", "city", "nyc");
        EXPECT_EQ(nyc.size(), 2u);

        auto sf = co_await index.findSeriesByTag("weather", "city", "sf");
        EXPECT_EQ(sf.size(), 1u);
        EXPECT_EQ(sf[0], ids[1]);

        // Add new series — should work with restored local ID map
        auto newId = co_await createSeries(index, "weather", {{"city", "la"}}, "temp");
        auto la = co_await index.findSeriesByTag("weather", "city", "la");
        EXPECT_EQ(la.size(), 1u);
        EXPECT_EQ(la[0], newId);

        co_await index.close();
    }
}
