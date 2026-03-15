/*
 * Phase 3: Time-Scoped Postings Tests
 *
 * Verifies per-day bitmap operations:
 * - Day bitmap creation and lookup during inserts
 * - Time-scoped discovery prunes inactive series
 * - Interaction with tag filter bitmaps
 * - Persistence across flush + compact + reopen
 * - Fallback for pre-Phase-3 data (no day bitmaps)
 * - Retention cleanup of old day bitmaps
 * - Large-scale narrow vs wide query pruning
 */

#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <string>

using namespace timestar::index;
namespace ke = timestar::index::keys;

class TimeScopedPostingsTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

// Helper: create a TimeStarInsert with specific timestamps
template <class T>
static TimeStarInsert<T> makeInsert(const std::string& measurement, const std::string& field,
                                     std::map<std::string, std::string> tags,
                                     std::vector<uint64_t> timestamps, std::vector<T> values) {
    TimeStarInsert<T> insert(measurement, field);
    insert.tags = std::move(tags);
    insert.timestamps = std::move(timestamps);
    insert.values = std::move(values);
    return insert;
}

// ── Key Encoding Tests ──

TEST(DayBitmapKeyEncoding, DayBucketFromNs) {
    // Day 0: timestamp 0
    EXPECT_EQ(ke::dayBucketFromNs(0), 0u);
    // Day 0: just before midnight
    EXPECT_EQ(ke::dayBucketFromNs(ke::NS_PER_DAY - 1), 0u);
    // Day 1: exactly midnight
    EXPECT_EQ(ke::dayBucketFromNs(ke::NS_PER_DAY), 1u);
    // Realistic timestamp: 2024-01-15 00:00:00 UTC (day 19737)
    uint64_t jan15 = 1705276800ULL * 1'000'000'000ULL;
    uint32_t day = ke::dayBucketFromNs(jan15);
    EXPECT_EQ(day, 19737u);
}

TEST(DayBitmapKeyEncoding, EncodeDecode) {
    auto key = ke::encodeDayBitmapKey("cpu", 12345);
    EXPECT_EQ(key[0], static_cast<char>(TIME_SERIES_DAY));
    EXPECT_EQ(key.substr(1, 3), "cpu");
    EXPECT_EQ(key[4], '\0');
    EXPECT_EQ(key.size(), 1 + 3 + 1 + 4u);

    uint32_t decoded = ke::decodeDayFromDayBitmapKey(key);
    EXPECT_EQ(decoded, 12345u);
}

TEST(DayBitmapKeyEncoding, PrefixScan) {
    auto prefix = ke::encodeDayBitmapPrefix("weather");
    EXPECT_EQ(prefix[0], static_cast<char>(TIME_SERIES_DAY));
    EXPECT_EQ(prefix.substr(1, 7), "weather");
    EXPECT_EQ(prefix[8], '\0');
    EXPECT_EQ(prefix.size(), 1 + 7 + 1u);

    // Key for day 100 should start with this prefix
    auto key = ke::encodeDayBitmapKey("weather", 100);
    EXPECT_EQ(key.substr(0, prefix.size()), prefix);
}

// ── Single-Day Insert and Query ──

SEASTAR_TEST_F(TimeScopedPostingsTest, SingleDayInsertAndQuery) {
    NativeIndex index(0);
    co_await index.open();

    // Insert data for day 20000 (some arbitrary day)
    uint64_t day20000_start = 20000ULL * ke::NS_PER_DAY;
    auto insert = makeInsert<double>("cpu", "usage",
                                      {{"host", "server-01"}},
                                      {day20000_start, day20000_start + 1'000'000'000ULL},
                                      {42.0, 43.0});
    co_await index.indexInsert(insert);

    // Query within the same day — should find the series
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "cpu", {{"host", "server-01"}}, {}, day20000_start, day20000_start + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.measurement, "cpu");

    // Query a completely different day — should return empty
    uint64_t day19000_start = 19000ULL * ke::NS_PER_DAY;
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped(
        "cpu", {{"host", "server-01"}}, {}, day19000_start, day19000_start + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 0u);

    co_await index.close();
}

// ── Multi-Day Insert — Narrow Range Returns Only Active ──

SEASTAR_TEST_F(TimeScopedPostingsTest, MultiDayNarrowQuery) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t baseDay = 20000ULL * ke::NS_PER_DAY;

    // Series A: active on days 20000-20002
    auto insertA = makeInsert<double>("temp", "value",
                                       {{"location", "us-west"}},
                                       {baseDay, baseDay + ke::NS_PER_DAY, baseDay + 2 * ke::NS_PER_DAY},
                                       {10.0, 11.0, 12.0});
    co_await index.indexInsert(insertA);

    // Series B: active only on day 20005
    auto insertB = makeInsert<double>("temp", "value",
                                       {{"location", "us-east"}},
                                       {baseDay + 5 * ke::NS_PER_DAY},
                                       {20.0});
    co_await index.indexInsert(insertB);

    // Query days 20000-20002 — should only find series A
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "temp", {}, {}, baseDay, baseDay + 2 * ke::NS_PER_DAY + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.tags.at("location"), "us-west");

    // Query day 20005 — should only find series B
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped(
        "temp", {}, {}, baseDay + 5 * ke::NS_PER_DAY, baseDay + 5 * ke::NS_PER_DAY + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 1u);
    EXPECT_EQ((*result2)[0].metadata.tags.at("location"), "us-east");

    // Query all days — should find both
    auto result3 = co_await index.findSeriesWithMetadataTimeScoped(
        "temp", {}, {}, baseDay, baseDay + 6 * ke::NS_PER_DAY);
    EXPECT_TRUE(result3.has_value());
    EXPECT_EQ(result3->size(), 2u);

    co_await index.close();
}

// ── Inactive Day Pruning ──

SEASTAR_TEST_F(TimeScopedPostingsTest, InactiveDayReturnsEmpty) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day20000 = 20000ULL * ke::NS_PER_DAY;

    // Insert data on day 20000
    auto insert = makeInsert<double>("mem", "used",
                                      {{"host", "h1"}},
                                      {day20000 + 100},
                                      {1024.0});
    co_await index.indexInsert(insert);

    // Query day 20001 (no data) — should return empty (not fallback, since day bitmaps exist)
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "mem", {}, {}, (20001ULL) * ke::NS_PER_DAY, (20002ULL) * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 0u);

    co_await index.close();
}

// ── Time-Scoped with Tag Filter Intersection ──

SEASTAR_TEST_F(TimeScopedPostingsTest, TimeScopedWithTagFilter) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day100 = 100ULL * ke::NS_PER_DAY;
    uint64_t day200 = 200ULL * ke::NS_PER_DAY;

    // Series 1: region=west, active day 100
    auto i1 = makeInsert<double>("net", "bytes", {{"region", "west"}}, {day100 + 1}, {100.0});
    co_await index.indexInsert(i1);

    // Series 2: region=east, active day 100
    auto i2 = makeInsert<double>("net", "bytes", {{"region", "east"}}, {day100 + 2}, {200.0});
    co_await index.indexInsert(i2);

    // Series 3: region=west, active day 200
    auto i3 = makeInsert<double>("net", "bytes", {{"region", "west"}, {"host", "h2"}}, {day200 + 1}, {300.0});
    co_await index.indexInsert(i3);

    // Query day 100, region=west — should only find series 1
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "net", {{"region", "west"}}, {}, day100, day100 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    // Query day 200, region=west — should only find series 3
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped(
        "net", {{"region", "west"}}, {}, day200, day200 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 1u);

    // Query day 100-200, region=west — should find both west series
    auto result3 = co_await index.findSeriesWithMetadataTimeScoped(
        "net", {{"region", "west"}}, {}, day100, day200 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result3.has_value());
    EXPECT_EQ(result3->size(), 2u);

    co_await index.close();
}

// ── Persistence After Flush + Compact + Reopen ──

SEASTAR_TEST_F(TimeScopedPostingsTest, PersistenceAfterFlushCompactReopen) {
    uint64_t day500 = 500ULL * ke::NS_PER_DAY;
    uint64_t day501 = 501ULL * ke::NS_PER_DAY;

    {
        NativeIndex index(0);
        co_await index.open();

        auto i1 = makeInsert<double>("disk", "iops", {{"host", "a"}}, {day500 + 1}, {100.0});
        co_await index.indexInsert(i1);
        auto i2 = makeInsert<double>("disk", "iops", {{"host", "b"}}, {day501 + 1}, {200.0});
        co_await index.indexInsert(i2);

        co_await index.compact();
        co_await index.close();
    }

    // Reopen and verify
    {
        NativeIndex index(0);
        co_await index.open();

        // Query day 500 — should find only host a
        auto result = co_await index.findSeriesWithMetadataTimeScoped(
            "disk", {}, {}, day500, day500 + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result->size(), 1u);
        EXPECT_EQ((*result)[0].metadata.tags.at("host"), "a");

        // Query day 501 — should find only host b
        auto result2 = co_await index.findSeriesWithMetadataTimeScoped(
            "disk", {}, {}, day501, day501 + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(result2.has_value());
        EXPECT_EQ(result2->size(), 1u);
        EXPECT_EQ((*result2)[0].metadata.tags.at("host"), "b");

        co_await index.close();
    }
}

// ── Backfill Marks Historical Days ──

SEASTAR_TEST_F(TimeScopedPostingsTest, BackfillMarksHistoricalDays) {
    NativeIndex index(0);
    co_await index.open();

    // Simulate backfill: insert data spanning 30 days in the past
    uint64_t startDay = 10000ULL * ke::NS_PER_DAY;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    for (int d = 0; d < 30; ++d) {
        timestamps.push_back(startDay + static_cast<uint64_t>(d) * ke::NS_PER_DAY + 1);
        values.push_back(static_cast<double>(d));
    }

    auto insert = makeInsert<double>("backfill_metric", "value",
                                      {{"source", "historical"}},
                                      std::move(timestamps), std::move(values));
    co_await index.indexInsert(insert);

    // Should be found in day range 10000-10029
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "backfill_metric", {}, {}, startDay, startDay + 30 * ke::NS_PER_DAY);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    // Should NOT be found in day range 11000-11001
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped(
        "backfill_metric", {}, {}, 11000ULL * ke::NS_PER_DAY, 11001ULL * ke::NS_PER_DAY);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 0u);

    co_await index.close();
}

// ── Retention Cleanup Removes Old Day Bitmaps ──

SEASTAR_TEST_F(TimeScopedPostingsTest, RetentionCleanupRemovesOldDayBitmaps) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day100 = 100ULL * ke::NS_PER_DAY;
    uint64_t day200 = 200ULL * ke::NS_PER_DAY;

    auto i1 = makeInsert<double>("metrics", "val", {{"a", "b"}}, {day100 + 1}, {1.0});
    co_await index.indexInsert(i1);
    auto i2 = makeInsert<double>("metrics", "val", {{"a", "c"}}, {day200 + 1}, {2.0});
    co_await index.indexInsert(i2);

    // Verify both days are queryable
    auto before = co_await index.findSeriesWithMetadataTimeScoped(
        "metrics", {}, {}, day100, day200 + ke::NS_PER_DAY);
    EXPECT_TRUE(before.has_value());
    EXPECT_EQ(before->size(), 2u);

    // Remove day bitmaps before day 150
    co_await index.removeExpiredDayBitmaps("metrics", 150);

    // Day 100 should now be empty
    auto after100 = co_await index.findSeriesWithMetadataTimeScoped(
        "metrics", {}, {}, day100, day100 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(after100.has_value());
    EXPECT_EQ(after100->size(), 0u);

    // Day 200 should still work
    auto after200 = co_await index.findSeriesWithMetadataTimeScoped(
        "metrics", {}, {}, day200, day200 + ke::NS_PER_DAY);
    EXPECT_TRUE(after200.has_value());
    EXPECT_EQ(after200->size(), 1u);

    co_await index.close();
}

// ── Fallback When No Day Bitmaps Exist (Pre-Phase-3 Data) ──

SEASTAR_TEST_F(TimeScopedPostingsTest, FallbackWhenNoDayBitmaps) {
    NativeIndex index(0);
    co_await index.open();

    // Create series via getOrCreateSeriesId only (no indexInsert, no day bitmaps)
    co_await index.getOrCreateSeriesId("legacy", {{"host", "old-server"}}, "cpu");

    // Time-scoped query should fall back to non-time-scoped path and still find the series
    uint64_t someDay = 15000ULL * ke::NS_PER_DAY;
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "legacy", {}, {}, someDay, someDay + ke::NS_PER_DAY);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.measurement, "legacy");

    co_await index.close();
}

// ── Large-Scale: Narrow vs Wide Query Pruning ──

SEASTAR_TEST_F(TimeScopedPostingsTest, LargeScaleNarrowVsWide) {
    NativeIndex index(0);
    co_await index.open();

    constexpr int NUM_SERIES = 200;
    constexpr int NUM_DAYS = 30;
    uint64_t baseDay = 20000ULL * ke::NS_PER_DAY;

    // Create 200 series, each active on different day ranges:
    // - Series 0-49: active on days 0-4 only
    // - Series 50-99: active on days 5-14
    // - Series 100-149: active on days 15-24
    // - Series 150-199: active on days 25-29
    for (int s = 0; s < NUM_SERIES; ++s) {
        int dayStart, dayEnd;
        if (s < 50) { dayStart = 0; dayEnd = 4; }
        else if (s < 100) { dayStart = 5; dayEnd = 14; }
        else if (s < 150) { dayStart = 15; dayEnd = 24; }
        else { dayStart = 25; dayEnd = 29; }

        std::string host = "host-" + std::to_string(s);
        auto insert = makeInsert<double>("cluster", "load",
                                          {{"host", host}},
                                          {baseDay + static_cast<uint64_t>(dayStart) * ke::NS_PER_DAY + 1},
                                          {static_cast<double>(s)});
        co_await index.indexInsert(insert);
    }

    // Narrow query: days 0-4 should only return 50 series
    auto narrow = co_await index.findSeriesWithMetadataTimeScoped(
        "cluster", {}, {}, baseDay, baseDay + 5 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(narrow.has_value());
    EXPECT_EQ(narrow->size(), 50u);

    // Medium query: days 5-14 should return 50 series
    auto medium = co_await index.findSeriesWithMetadataTimeScoped(
        "cluster", {}, {}, baseDay + 5 * ke::NS_PER_DAY, baseDay + 15 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(medium.has_value());
    EXPECT_EQ(medium->size(), 50u);

    // Wide query: all 30 days should return all 200 series
    auto wide = co_await index.findSeriesWithMetadataTimeScoped(
        "cluster", {}, {}, baseDay, baseDay + NUM_DAYS * ke::NS_PER_DAY);
    EXPECT_TRUE(wide.has_value());
    EXPECT_EQ(wide->size(), static_cast<size_t>(NUM_SERIES));

    co_await index.close();
}

// ── Field Filter with Time Scoping ──

SEASTAR_TEST_F(TimeScopedPostingsTest, FieldFilterWithTimeScope) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day1 = 1000ULL * ke::NS_PER_DAY;

    // Two fields for the same measurement/tags on day 1000
    auto i1 = makeInsert<double>("sys", "cpu_pct", {{"host", "h1"}}, {day1 + 1}, {50.0});
    co_await index.indexInsert(i1);
    auto i2 = makeInsert<double>("sys", "mem_pct", {{"host", "h1"}}, {day1 + 2}, {75.0});
    co_await index.indexInsert(i2);

    // Query with field filter — only cpu_pct
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "sys", {}, {"cpu_pct"}, day1, day1 + ke::NS_PER_DAY);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.field, "cpu_pct");

    co_await index.close();
}

// ── Multi-Tag Intersection with Time Scoping ──

SEASTAR_TEST_F(TimeScopedPostingsTest, MultiTagIntersectionWithTimeScope) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day300 = 300ULL * ke::NS_PER_DAY;
    uint64_t day400 = 400ULL * ke::NS_PER_DAY;

    // Series: region=west, env=prod, active day 300
    auto i1 = makeInsert<double>("api", "latency",
                                  {{"region", "west"}, {"env", "prod"}},
                                  {day300 + 1}, {10.0});
    co_await index.indexInsert(i1);

    // Series: region=west, env=staging, active day 300
    auto i2 = makeInsert<double>("api", "latency",
                                  {{"region", "west"}, {"env", "staging"}},
                                  {day300 + 2}, {20.0});
    co_await index.indexInsert(i2);

    // Series: region=east, env=prod, active day 400
    auto i3 = makeInsert<double>("api", "latency",
                                  {{"region", "east"}, {"env", "prod"}},
                                  {day400 + 1}, {30.0});
    co_await index.indexInsert(i3);

    // Query day 300, region=west AND env=prod — should find exactly 1
    auto result = co_await index.findSeriesWithMetadataTimeScoped(
        "api", {{"region", "west"}, {"env", "prod"}}, {},
        day300, day300 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    // Query day 300, region=west (no env filter) — should find 2
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped(
        "api", {{"region", "west"}}, {},
        day300, day300 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 2u);

    co_await index.close();
}
