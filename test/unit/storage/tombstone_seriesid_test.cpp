#include <gtest/gtest.h>
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm_tombstone.hpp"

using namespace tsdb;

// Test that TombstoneEntry uses SeriesId128 instead of uint64_t
TEST(TombstoneSeriesIdTest, EntrySize) {
    // With SeriesId128 (16 bytes) + startTime (8) + endTime (8) + checksum (4) = 36
    EXPECT_EQ(TombstoneEntry::SIZE, 36u);
}

// Test that TombstoneEntry correctly stores SeriesId128
TEST(TombstoneSeriesIdTest, EntryStoresSeriesId128) {
    SeriesId128 id1("measurement1,host=server1#field1");
    SeriesId128 id2("measurement2,host=server2#field2");

    TombstoneEntry entry1;
    entry1.seriesId = id1;
    entry1.startTime = 1000;
    entry1.endTime = 2000;
    entry1.checksum = 0;

    TombstoneEntry entry2;
    entry2.seriesId = id2;
    entry2.startTime = 1000;
    entry2.endTime = 2000;
    entry2.checksum = 0;

    // Different series IDs should make entries not equal
    EXPECT_NE(entry1.seriesId, entry2.seriesId);
    EXPECT_FALSE(entry1 == entry2);
}

// Test that SeriesId128 works as map key (needed for seriesRanges)
TEST(TombstoneSeriesIdTest, SeriesId128AsMapKey) {
    SeriesId128 id1("series_key_alpha");
    SeriesId128 id2("series_key_beta");
    SeriesId128 id3("series_key_gamma");

    std::map<SeriesId128, std::vector<std::pair<uint64_t, uint64_t>>> ranges;
    ranges[id1].push_back({100, 200});
    ranges[id2].push_back({300, 400});
    ranges[id3].push_back({500, 600});

    EXPECT_EQ(ranges.size(), 3u);
    EXPECT_EQ(ranges[id1].size(), 1u);
    EXPECT_EQ(ranges[id1][0].first, 100u);
    EXPECT_EQ(ranges[id1][0].second, 200u);

    // Looking up a non-existent key should not find anything
    SeriesId128 id_missing("completely_different_key");
    EXPECT_EQ(ranges.find(id_missing), ranges.end());
}

// Test that SeriesId128 works in std::set (needed for getTombstonedSeries)
TEST(TombstoneSeriesIdTest, SeriesId128InSet) {
    SeriesId128 id1("series_a");
    SeriesId128 id2("series_b");
    SeriesId128 id3("series_c");

    std::set<SeriesId128> series;
    series.insert(id1);
    series.insert(id2);
    series.insert(id3);
    series.insert(id1);  // Duplicate

    EXPECT_EQ(series.size(), 3u);
    EXPECT_TRUE(series.count(id1) == 1);
    EXPECT_TRUE(series.count(id2) == 1);
    EXPECT_TRUE(series.count(id3) == 1);
}

// Test that two different SeriesId128 values that previously would have
// collided via std::hash are now distinct in the tombstone system
TEST(TombstoneSeriesIdTest, NoHashCollisionRisk) {
    // Create two different SeriesId128 values
    SeriesId128 id1("measurement,host=server01#temperature");
    SeriesId128 id2("measurement,host=server02#temperature");

    // These are definitively different series IDs
    EXPECT_NE(id1, id2);

    // Even if they happen to hash to the same uint64_t, the tombstone system
    // now uses the full 128-bit ID, so they will never collide
    // Verify ordering works correctly (needed for map/set)
    bool less_than = id1 < id2;
    bool greater_than = id2 < id1;
    // Exactly one of these must be true since id1 != id2
    EXPECT_TRUE(less_than || greater_than);
    EXPECT_FALSE(less_than && greater_than);
}

// Test TombstoneEntry comparison operators with SeriesId128
TEST(TombstoneSeriesIdTest, EntryComparisonOperators) {
    SeriesId128 id1("series_alpha");
    SeriesId128 id2("series_beta");

    TombstoneEntry e1;
    e1.seriesId = id1;
    e1.startTime = 1000;
    e1.endTime = 2000;

    TombstoneEntry e2;
    e2.seriesId = id2;
    e2.startTime = 1000;
    e2.endTime = 2000;

    TombstoneEntry e3;
    e3.seriesId = id1;
    e3.startTime = 3000;
    e3.endTime = 4000;

    // Same series, different times: should compare by startTime
    EXPECT_TRUE(e1 < e3 || e3 < e1);  // Different, so one must be less
    EXPECT_FALSE(e1 == e3);

    // Different series: should compare by seriesId first
    EXPECT_FALSE(e1 == e2);
    EXPECT_TRUE(e1 < e2 || e2 < e1);

    // Self-equality
    EXPECT_TRUE(e1 == e1);
}

// Test the TOMBSTONE_VERSION was bumped to 2
TEST(TombstoneSeriesIdTest, VersionBumped) {
    EXPECT_EQ(TOMBSTONE_VERSION, 2u);
}
