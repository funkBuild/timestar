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

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers/native_index_test_access.hpp"

#include <endian.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <string>

using namespace timestar::index;

// Test helper: construct a day bitmap KV key (matching the format used by NativeIndex)
static std::string testEncodeDayBitmapKey(const std::string& measurement, uint32_t day) {
    std::string key;
    key.push_back(static_cast<char>(TIME_SERIES_DAY));
    key += measurement;
    key.push_back('\0');
    uint32_t dayBE = htobe32(day);
    key.append(reinterpret_cast<const char*>(&dayBE), 4);
    return key;
}
namespace ke = timestar::index::keys;

class TimeScopedPostingsTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

// Helper: create a TimeStarInsert with specific timestamps
template <class T>
static TimeStarInsert<T> makeInsert(const std::string& measurement, const std::string& field,
                                    std::map<std::string, std::string> tags, std::vector<uint64_t> timestamps,
                                    std::vector<T> values) {
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
    auto key = testEncodeDayBitmapKey("cpu", 12345);
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
    auto key = testEncodeDayBitmapKey("weather", 100);
    EXPECT_EQ(key.substr(0, prefix.size()), prefix);
}

TEST(DayBitmapKeyEncoding, DecodeTooShortKeyThrows) {
    // Keys shorter than 7 bytes (prefix + measurement + null + day) should throw
    EXPECT_THROW(ke::decodeDayFromDayBitmapKey(""), std::runtime_error);
    EXPECT_THROW(ke::decodeDayFromDayBitmapKey("abc"), std::runtime_error);
    EXPECT_THROW(ke::decodeDayFromDayBitmapKey("abcdef"), std::runtime_error);
    // Exactly 7 bytes should succeed (minimal: prefix(1) + m(1) + null(1) + day(4))
    std::string minimal(7, '\0');
    EXPECT_NO_THROW(ke::decodeDayFromDayBitmapKey(minimal));
}

// ── Single-Day Insert and Query ──

SEASTAR_TEST_F(TimeScopedPostingsTest, SingleDayInsertAndQuery) {
    NativeIndex index(0);
    co_await index.open();

    // Insert data for day 20000 (some arbitrary day)
    uint64_t day20000_start = 20000ULL * ke::NS_PER_DAY;
    auto insert = makeInsert<double>("cpu", "usage", {{"host", "server-01"}},
                                     {day20000_start, day20000_start + 1'000'000'000ULL}, {42.0, 43.0});
    co_await index.indexInsert(insert);

    // Query within the same day — should find the series
    auto result = co_await index.findSeriesWithMetadataTimeScoped("cpu", {{"host", "server-01"}}, {}, day20000_start,
                                                                  day20000_start + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.measurement, "cpu");

    // Query a completely different day — should return empty
    uint64_t day19000_start = 19000ULL * ke::NS_PER_DAY;
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped("cpu", {{"host", "server-01"}}, {}, day19000_start,
                                                                   day19000_start + ke::NS_PER_DAY - 1);
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
    auto insertA =
        makeInsert<double>("temp", "value", {{"location", "us-west"}},
                           {baseDay, baseDay + ke::NS_PER_DAY, baseDay + 2 * ke::NS_PER_DAY}, {10.0, 11.0, 12.0});
    co_await index.indexInsert(insertA);

    // Series B: active only on day 20005
    auto insertB =
        makeInsert<double>("temp", "value", {{"location", "us-east"}}, {baseDay + 5 * ke::NS_PER_DAY}, {20.0});
    co_await index.indexInsert(insertB);

    // Query days 20000-20002 — should only find series A
    auto result = co_await index.findSeriesWithMetadataTimeScoped("temp", {}, {}, baseDay,
                                                                  baseDay + 2 * ke::NS_PER_DAY + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].metadata.tags.at("location"), "us-west");

    // Query day 20005 — should only find series B
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped("temp", {}, {}, baseDay + 5 * ke::NS_PER_DAY,
                                                                   baseDay + 5 * ke::NS_PER_DAY + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 1u);
    EXPECT_EQ((*result2)[0].metadata.tags.at("location"), "us-east");

    // Query all days — should find both
    auto result3 =
        co_await index.findSeriesWithMetadataTimeScoped("temp", {}, {}, baseDay, baseDay + 6 * ke::NS_PER_DAY);
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
    auto insert = makeInsert<double>("mem", "used", {{"host", "h1"}}, {day20000 + 100}, {1024.0});
    co_await index.indexInsert(insert);

    // Query day 20001 (no data) — should return empty (not fallback, since day bitmaps exist)
    auto result = co_await index.findSeriesWithMetadataTimeScoped("mem", {}, {}, (20001ULL) * ke::NS_PER_DAY,
                                                                  (20002ULL) * ke::NS_PER_DAY - 1);
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
    auto result = co_await index.findSeriesWithMetadataTimeScoped("net", {{"region", "west"}}, {}, day100,
                                                                  day100 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    // Query day 200, region=west — should only find series 3
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped("net", {{"region", "west"}}, {}, day200,
                                                                   day200 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 1u);

    // Query day 100-200, region=west — should find both west series
    auto result3 = co_await index.findSeriesWithMetadataTimeScoped("net", {{"region", "west"}}, {}, day100,
                                                                   day200 + ke::NS_PER_DAY - 1);
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
        auto result =
            co_await index.findSeriesWithMetadataTimeScoped("disk", {}, {}, day500, day500 + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result->size(), 1u);
        EXPECT_EQ((*result)[0].metadata.tags.at("host"), "a");

        // Query day 501 — should find only host b
        auto result2 =
            co_await index.findSeriesWithMetadataTimeScoped("disk", {}, {}, day501, day501 + ke::NS_PER_DAY - 1);
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

    auto insert = makeInsert<double>("backfill_metric", "value", {{"source", "historical"}}, std::move(timestamps),
                                     std::move(values));
    co_await index.indexInsert(insert);

    // Should be found in day range 10000-10029
    auto result = co_await index.findSeriesWithMetadataTimeScoped("backfill_metric", {}, {}, startDay,
                                                                  startDay + 30 * ke::NS_PER_DAY);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    // Should NOT be found in day range 11000-11001
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped("backfill_metric", {}, {}, 11000ULL * ke::NS_PER_DAY,
                                                                   11001ULL * ke::NS_PER_DAY);
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
    auto before = co_await index.findSeriesWithMetadataTimeScoped("metrics", {}, {}, day100, day200 + ke::NS_PER_DAY);
    EXPECT_TRUE(before.has_value());
    EXPECT_EQ(before->size(), 2u);

    // Remove day bitmaps before day 150
    co_await index.removeExpiredDayBitmaps("metrics", 150);

    // Day 100 should now be empty
    auto after100 =
        co_await index.findSeriesWithMetadataTimeScoped("metrics", {}, {}, day100, day100 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(after100.has_value());
    EXPECT_EQ(after100->size(), 0u);

    // Day 200 should still work
    auto after200 = co_await index.findSeriesWithMetadataTimeScoped("metrics", {}, {}, day200, day200 + ke::NS_PER_DAY);
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
    auto result = co_await index.findSeriesWithMetadataTimeScoped("legacy", {}, {}, someDay, someDay + ke::NS_PER_DAY);
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
        if (s < 50) {
            dayStart = 0;
            dayEnd = 4;
        } else if (s < 100) {
            dayStart = 5;
            dayEnd = 14;
        } else if (s < 150) {
            dayStart = 15;
            dayEnd = 24;
        } else {
            dayStart = 25;
            dayEnd = 29;
        }

        std::string host = "host-" + std::to_string(s);
        auto insert = makeInsert<double>("cluster", "load", {{"host", host}},
                                         {baseDay + static_cast<uint64_t>(dayStart) * ke::NS_PER_DAY + 1},
                                         {static_cast<double>(s)});
        co_await index.indexInsert(insert);
    }

    // Narrow query: days 0-4 should only return 50 series
    auto narrow =
        co_await index.findSeriesWithMetadataTimeScoped("cluster", {}, {}, baseDay, baseDay + 5 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(narrow.has_value());
    EXPECT_EQ(narrow->size(), 50u);

    // Medium query: days 5-14 should return 50 series
    auto medium = co_await index.findSeriesWithMetadataTimeScoped("cluster", {}, {}, baseDay + 5 * ke::NS_PER_DAY,
                                                                  baseDay + 15 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(medium.has_value());
    EXPECT_EQ(medium->size(), 50u);

    // Wide query: all 30 days should return all 200 series
    auto wide = co_await index.findSeriesWithMetadataTimeScoped("cluster", {}, {}, baseDay,
                                                                baseDay + NUM_DAYS * ke::NS_PER_DAY);
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
    auto result = co_await index.findSeriesWithMetadataTimeScoped("sys", {}, {"cpu_pct"}, day1, day1 + ke::NS_PER_DAY);
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
    auto i1 = makeInsert<double>("api", "latency", {{"region", "west"}, {"env", "prod"}}, {day300 + 1}, {10.0});
    co_await index.indexInsert(i1);

    // Series: region=west, env=staging, active day 300
    auto i2 = makeInsert<double>("api", "latency", {{"region", "west"}, {"env", "staging"}}, {day300 + 2}, {20.0});
    co_await index.indexInsert(i2);

    // Series: region=east, env=prod, active day 400
    auto i3 = makeInsert<double>("api", "latency", {{"region", "east"}, {"env", "prod"}}, {day400 + 1}, {30.0});
    co_await index.indexInsert(i3);

    // Query day 300, region=west AND env=prod — should find exactly 1
    auto result = co_await index.findSeriesWithMetadataTimeScoped("api", {{"region", "west"}, {"env", "prod"}}, {},
                                                                  day300, day300 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 1u);

    // Query day 300, region=west (no env filter) — should find 2
    auto result2 = co_await index.findSeriesWithMetadataTimeScoped("api", {{"region", "west"}}, {}, day300,
                                                                   day300 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2->size(), 2u);

    co_await index.close();
}

// ── Batch path day-bitmap recording (regression: batch-only series were
// never added to day bitmaps, so once ANY day bitmap existed for a
// measurement they were wrongly pruned from time-scoped discovery) ──

SEASTAR_TEST_F(TimeScopedPostingsTest, MetadataOpDaySpanCoversFirstBatch) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day21000 = 21000ULL * ke::NS_PER_DAY;

    // Simulate the HTTP batch path: metadata arrives via indexMetadataBatch
    // (with the insert's timestamp range), NOT via indexInsert.
    MetadataOp op;
    op.valueType = TSMValueType::Float;
    op.measurement = "batchmeas";
    op.fieldName = "v";
    op.tags = {{"host", "h1"}};
    op.minTs = day21000;
    op.maxTs = day21000 + 2 * ke::NS_PER_DAY;  // spans 3 days
    co_await index.indexMetadataBatch({op});

    // Series must be discoverable on every day of the span.
    for (int d = 0; d < 3; ++d) {
        auto r = co_await index.findSeriesWithMetadataTimeScoped("batchmeas", {{"host", "h1"}}, {},
                                                                 day21000 + d * ke::NS_PER_DAY,
                                                                 day21000 + d * ke::NS_PER_DAY + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(r.has_value());
        EXPECT_EQ(r->size(), 1u) << "day offset " << d;
    }
    // And absent outside it.
    auto rOut = co_await index.findSeriesWithMetadataTimeScoped(
        "batchmeas", {{"host", "h1"}}, {}, day21000 + 5 * ke::NS_PER_DAY, day21000 + 6 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(rOut.has_value());
    EXPECT_EQ(rOut->size(), 0u);
    co_await index.close();
}

SEASTAR_TEST_F(TimeScopedPostingsTest, RecordInsertDaysAddsLaterDays) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day22000 = 22000ULL * ke::NS_PER_DAY;

    // First batch: metadata op establishes the series + day 22000.
    MetadataOp op;
    op.valueType = TSMValueType::Float;
    op.measurement = "bm2";
    op.fieldName = "v";
    op.tags = {{"host", "h1"}};
    op.minTs = day22000;
    op.maxTs = day22000;
    co_await index.indexMetadataBatch({op});

    // Later batch for a KNOWN series (no metadata op emitted): the data-shard
    // path records its days via recordInsertDays.
    auto seriesId = SeriesId128::fromSeriesKey(timestar::buildSeriesKey("bm2", {{"host", "h1"}}, "v"));
    std::vector<uint64_t> ts = {day22000 + 3 * ke::NS_PER_DAY, day22000 + 3 * ke::NS_PER_DAY + 1'000'000'000ULL};
    co_await index.recordInsertDays("bm2", seriesId, ts);

    auto r = co_await index.findSeriesWithMetadataTimeScoped("bm2", {{"host", "h1"}}, {}, day22000 + 3 * ke::NS_PER_DAY,
                                                             day22000 + 4 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r->size(), 1u);
    co_await index.close();
}

// ── TASK C: cached time-scoped discovery ──

// Two identical day-scoped discovery calls must be served from the discovery
// cache (same shared vector) with consistent results.
SEASTAR_TEST_F(TimeScopedPostingsTest, TimeScopedCachedServesSharedVector) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day23000 = 23000ULL * ke::NS_PER_DAY;
    auto insert =
        makeInsert<double>("tsc_cache", "usage", {{"host", "h1"}}, {day23000, day23000 + 1'000'000'000ULL}, {1.0, 2.0});
    co_await index.indexInsert(insert);

    auto r1 = co_await index.findSeriesWithMetadataTimeScopedCached("tsc_cache", {{"host", "h1"}}, {}, day23000,
                                                                    day23000 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(r1.has_value());
    EXPECT_EQ((*r1)->size(), 1u);
    EXPECT_EQ((*(*r1))[0].metadata.measurement, "tsc_cache");

    auto r2 = co_await index.findSeriesWithMetadataTimeScopedCached("tsc_cache", {{"host", "h1"}}, {}, day23000,
                                                                    day23000 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(r2.has_value());
    EXPECT_EQ(r1.value().get(), r2.value().get()) << "identical day-scoped queries must share the cached vector";

    // A different day range must NOT alias to the same cache entry.
    auto r3 = co_await index.findSeriesWithMetadataTimeScopedCached(
        "tsc_cache", {{"host", "h1"}}, {}, day23000 + ke::NS_PER_DAY, day23000 + 2 * ke::NS_PER_DAY - 1);
    EXPECT_TRUE(r3.has_value());
    EXPECT_NE(r1.value().get(), r3.value().get());
    EXPECT_EQ((*r3)->size(), 0u);

    co_await index.close();
}

// When an EXISTING series first writes into a day inside a cached range, the
// cached day-scoped result must be invalidated (addChecked → generation bump).
SEASTAR_TEST_F(TimeScopedPostingsTest, TimeScopedCachedInvalidatedByNewDayMembership) {
    NativeIndex index(0);
    co_await index.open();

    uint64_t day24000 = 24000ULL * ke::NS_PER_DAY;
    auto insert = makeInsert<double>("tsc_inval", "usage", {{"host", "h1"}}, {day24000}, {1.0});
    auto seriesId = co_await index.indexInsert(insert);

    // Cache a result for a later day the series has NOT written into: empty.
    uint64_t day24002 = day24000 + 2 * ke::NS_PER_DAY;
    auto before = co_await index.findSeriesWithMetadataTimeScopedCached("tsc_inval", {}, {}, day24002,
                                                                        day24002 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(before.has_value());
    EXPECT_EQ((*before)->size(), 0u);

    // Existing series now writes into that day (data-shard batch path).
    co_await index.recordInsertDays("tsc_inval", seriesId, {day24002 + 1'000'000'000ULL});

    // The next call must see the new day membership, not the stale cached {}.
    auto after = co_await index.findSeriesWithMetadataTimeScopedCached("tsc_inval", {}, {}, day24002,
                                                                       day24002 + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(after.has_value());
    EXPECT_EQ((*after)->size(), 1u) << "new day membership must invalidate cached day-scoped discovery";
    EXPECT_EQ((*(*after))[0].seriesId, seriesId);

    co_await index.close();
}

// ── Regression: day bitmaps lost to an unclean shutdown ──
//
// Production 2026-09-01 (1.4.2), project 99e7bc58 `builtIn.node.stats`: for a
// given series a query over [T, now] returned ZERO points while [T-2d, now]
// returned every point after T. The cutoff was per-series (2026-08-30T00:00Z on
// one series, 2026-08-31T03:57Z on another) and fell on data that was written,
// stored and readable — only DISCOVERY dropped it.
//
// Mechanism: findSeriesWithMetadataTimeScoped unions the per-day bitmaps of the
// days the query spans and treats an empty union as "no active series". Day
// membership lives in dayBitmapCache_ and is persisted ONLY when the index
// memtable crosses write_buffer_size (flushDirtyDayBitmaps, called from
// maybeFlushMemTable/flushMemTable). In steady state that memtable is driven by
// NEW series metadata, so a fleet writing to long-established series can go days
// without a flush. Postings bitmaps get a crash-window repair at open() and the
// measurement blooms have been rebuilt at open() since 1.4.2 — day bitmaps get
// neither (native_index.cpp:285-289 concedes this and assumes the gap is
// "bounded to the crash window").
//
// So an unclean exit — a health-check restart, an OOM kill, a task replacement —
// permanently erases day membership for every day since the last flush, and the
// series silently disappears from any query that STARTS in that region. Widening
// the start picks up an older day, the union is non-empty, and the same points
// come back: exactly the asymmetry observed.
//
// Note what the fix must NOT be: making an empty union fall back to
// non-time-scoped discovery. InactiveDayReturnsEmpty above pins the opposite —
// a day with genuinely no data must return empty. The recovery has to restore
// the membership (rebuild at open from the TSM per-series [minTime,maxTime]
// bounds plus the memory stores), not weaken the pruning.

SEASTAR_TEST_F(TimeScopedPostingsTest, DayMembershipSurvivesUncleanShutdown) {
    // Day 20100 is flushed and durable; 20101-20103 are the "crash window".
    constexpr uint32_t kFlushedDay = 20100;
    constexpr uint32_t kCrashDay = 20101;
    const uint64_t flushedDayNs = static_cast<uint64_t>(kFlushedDay) * ke::NS_PER_DAY;

    {
        NativeIndex index(0);
        co_await index.open();

        auto insert =
            makeInsert<double>("stats", "batteryPercent", {{"deviceId", "dev-1c4e"}}, {flushedDayNs + 1}, {94.0});
        co_await index.indexInsert(insert);
        co_await index.compact();  // day 20100 is now on disk
        co_await index.close();
    }

    {
        NativeIndex index(0);
        co_await index.open();

        // Three more days of writes to the SAME, already-established series —
        // no new series metadata, so nothing forces a memtable flush.
        auto seriesId =
            SeriesId128::fromSeriesKey(timestar::buildSeriesKey("stats", {{"deviceId", "dev-1c4e"}}, "batteryPercent"));
        for (uint32_t d = kCrashDay; d <= kCrashDay + 2; ++d) {
            co_await index.recordInsertDays("stats", seriesId,
                                            {static_cast<uint64_t>(d) * ke::NS_PER_DAY + 1'000'000'000ULL});
        }

        // Precondition: those three days are RAM-only. If a future change makes
        // recordInsertDays durable immediately, this test stops modelling a
        // crash — say so loudly rather than passing for the wrong reason.
        EXPECT_FALSE(co_await NativeIndexTestAccess::hasPersistedDayBitmap(index, "stats", kCrashDay))
            << "test precondition: day membership was expected to be RAM-only until a memtable flush";

        // The process dies here: everything dirty is lost, metadata and LocalIds
        // (written with the series-creation batch) are not.
        co_await NativeIndexTestAccess::dropDayBitmapsFrom(index, "stats", kCrashDay);
        co_await index.close();
    }

    {
        NativeIndex index(0);
        co_await index.open();

        // What Engine::rebuildDayBitmaps() does at startup, with the time bounds
        // the TSM sparse index supplies (Engine drives this because the index
        // itself has no access to storage-layer timestamps — see engine.cpp).
        auto seriesId =
            SeriesId128::fromSeriesKey(timestar::buildSeriesKey("stats", {{"deviceId", "dev-1c4e"}}, "batteryPercent"));
        std::vector<NativeIndex::SeriesTimeBounds> bounds{
            {seriesId, flushedDayNs, static_cast<uint64_t>(kCrashDay + 2) * ke::NS_PER_DAY + 1'000'000'000ULL}};
        const uint32_t watermark = index.dayBitmapWatermark().value_or(0);
        EXPECT_EQ(watermark, kCrashDay - 1) << "watermark must name the last durable day";
        co_await index.rebuildDayBitmapsFromBounds(bounds, watermark, kCrashDay + 2);

        // The console's narrow window: a range that STARTS inside the crash
        // window. This is the query that returned nothing in production.
        const uint64_t narrowStart = static_cast<uint64_t>(kCrashDay + 2) * ke::NS_PER_DAY;
        auto narrow = co_await index.findSeriesWithMetadataTimeScoped("stats", {{"deviceId", "dev-1c4e"}}, {},
                                                                      narrowStart, narrowStart + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(narrow.has_value());
        EXPECT_EQ(narrow->size(), 1u) << "series lost from a range starting inside the crash window";

        // Widening the start to before the last durable day must not change the
        // answer. In production it did — that asymmetry IS the bug.
        auto wide = co_await index.findSeriesWithMetadataTimeScoped("stats", {{"deviceId", "dev-1c4e"}}, {},
                                                                    flushedDayNs, narrowStart + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(wide.has_value());
        EXPECT_EQ(wide->size(), 1u);

        // A day the series never wrote to must still prune — the repair may not
        // degrade into "always discoverable".
        const uint64_t quietStart = static_cast<uint64_t>(kCrashDay + 10) * ke::NS_PER_DAY;
        auto quiet = co_await index.findSeriesWithMetadataTimeScoped("stats", {{"deviceId", "dev-1c4e"}}, {},
                                                                     quietStart, quietStart + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(quiet.has_value());
        EXPECT_EQ(quiet->size(), 0u) << "pruning must survive the repair";

        co_await index.close();
    }
}

// The same defect seen through the aggregate symptom rather than one series: a
// measurement keeps ingesting, and every device's recent day membership is lost.
// A per-day walk over the affected window must not report a hole that the
// whole-range query contradicts.
SEASTAR_TEST_F(TimeScopedPostingsTest, EveryDayInTheCrashWindowStaysDiscoverable) {
    constexpr uint32_t kFlushedDay = 20500;
    constexpr int kDevices = 5;
    constexpr int kCrashDays = 4;
    const uint64_t flushedDayNs = static_cast<uint64_t>(kFlushedDay) * ke::NS_PER_DAY;

    {
        NativeIndex index(0);
        co_await index.open();
        for (int d = 0; d < kDevices; ++d) {
            auto insert = makeInsert<double>("fleet", "uptimeSeconds", {{"deviceId", "dev-" + std::to_string(d)}},
                                             {flushedDayNs + 1}, {1.0});
            co_await index.indexInsert(insert);
        }
        co_await index.compact();
        co_await index.close();
    }

    {
        NativeIndex index(0);
        co_await index.open();
        for (int d = 0; d < kDevices; ++d) {
            auto seriesId = SeriesId128::fromSeriesKey(
                timestar::buildSeriesKey("fleet", {{"deviceId", "dev-" + std::to_string(d)}}, "uptimeSeconds"));
            for (int day = 1; day <= kCrashDays; ++day) {
                co_await index.recordInsertDays(
                    "fleet", seriesId,
                    {(static_cast<uint64_t>(kFlushedDay) + day) * ke::NS_PER_DAY + 1'000'000'000ULL});
            }
        }
        co_await NativeIndexTestAccess::dropDayBitmapsFrom(index, "fleet", kFlushedDay + 1);
        co_await index.close();
    }

    {
        NativeIndex index(0);
        co_await index.open();

        std::vector<NativeIndex::SeriesTimeBounds> bounds;
        for (int d = 0; d < kDevices; ++d) {
            bounds.push_back({SeriesId128::fromSeriesKey(timestar::buildSeriesKey(
                                  "fleet", {{"deviceId", "dev-" + std::to_string(d)}}, "uptimeSeconds")),
                              flushedDayNs,
                              (static_cast<uint64_t>(kFlushedDay) + kCrashDays) * ke::NS_PER_DAY + 1'000'000'000ULL});
        }
        co_await index.rebuildDayBitmapsFromBounds(bounds, index.dayBitmapWatermark().value_or(0),
                                                   kFlushedDay + kCrashDays);

        for (int day = 1; day <= kCrashDays; ++day) {
            const uint64_t start = (static_cast<uint64_t>(kFlushedDay) + day) * ke::NS_PER_DAY;
            auto r =
                co_await index.findSeriesWithMetadataTimeScoped("fleet", {}, {}, start, start + ke::NS_PER_DAY - 1);
            EXPECT_TRUE(r.has_value());
            EXPECT_EQ(r->size(), static_cast<size_t>(kDevices)) << "day " << (kFlushedDay + day);
        }
        co_await index.close();
    }
}

// Phase 1: the watermark names the last day whose membership is durable, and it
// survives a reopen. Without it the startup repair has no bound and would have
// to choose between rescanning all history or guessing.
SEASTAR_TEST_F(TimeScopedPostingsTest, DayBitmapWatermarkPersistsLastDurableDay) {
    constexpr uint32_t kDay = 20800;
    {
        NativeIndex index(0);
        co_await index.open();
        EXPECT_FALSE(index.dayBitmapWatermark().has_value()) << "nothing flushed yet";

        auto insert =
            makeInsert<double>("wm", "v", {{"host", "h1"}}, {static_cast<uint64_t>(kDay) * ke::NS_PER_DAY + 1}, {1.0});
        co_await index.indexInsert(insert);
        co_await index.close();  // flushes day bitmaps + watermark
    }

    NativeIndex index(0);
    co_await index.open();
    ASSERT_TRUE(index.dayBitmapWatermark().has_value());
    EXPECT_EQ(*index.dayBitmapWatermark(), kDay);
    co_await index.close();
}

// Phase 3: membership becomes durable on the timed flush path, with no memtable
// crossing. This is what shrinks the crash window from days to one interval —
// the memtable threshold is fed by NEW series metadata, so a shard writing only
// to established series may never cross it.
SEASTAR_TEST_F(TimeScopedPostingsTest, TimedFlushPersistsDayBitmapsWithoutMemtableCrossing) {
    constexpr uint32_t kDay = 20900;
    NativeIndex index(0);
    co_await index.open();

    auto insert =
        makeInsert<double>("timed", "v", {{"host", "h1"}}, {static_cast<uint64_t>(kDay) * ke::NS_PER_DAY + 1}, {1.0});
    auto seriesId = co_await index.indexInsert(insert);

    // A later day on an ESTABLISHED series: no new metadata, so nothing pushes
    // the memtable past write_buffer_size.
    const uint32_t laterDay = kDay + 1;
    co_await index.recordInsertDays("timed", seriesId,
                                    {static_cast<uint64_t>(laterDay) * ke::NS_PER_DAY + 1'000'000'000ULL});
    EXPECT_FALSE(co_await NativeIndexTestAccess::hasPersistedDayBitmap(index, "timed", laterDay))
        << "precondition: not yet durable";

    co_await index.flushDayBitmapsNow();

    EXPECT_TRUE(co_await NativeIndexTestAccess::hasPersistedDayBitmap(index, "timed", laterDay))
        << "the timed flush must make day membership durable";
    ASSERT_TRUE(index.dayBitmapWatermark().has_value());
    EXPECT_EQ(*index.dayBitmapWatermark(), laterDay);

    co_await index.close();
}

// The repair is a superset, never a resurrection of everything: it must respect
// the window it is given, so a clamped window (and the retention deletes below
// it) is not undone. Bounds spanning far more days than the window are handed
// in deliberately.
SEASTAR_TEST_F(TimeScopedPostingsTest, RebuildRespectsWindowAndDoesNotResurrectExpiredDays) {
    constexpr uint32_t kOldDay = 21100;
    constexpr uint32_t kRecentDay = 21140;

    NativeIndex index(0);
    co_await index.open();

    auto insert = makeInsert<double>(
        "ret", "v", {{"host", "h1"}},
        {static_cast<uint64_t>(kOldDay) * ke::NS_PER_DAY + 1, static_cast<uint64_t>(kRecentDay) * ke::NS_PER_DAY + 1},
        {1.0, 2.0});
    auto seriesId = co_await index.indexInsert(insert);

    // Retention drops everything before the recent day.
    co_await index.removeExpiredDayBitmaps("ret", kRecentDay);
    co_await NativeIndexTestAccess::dropDayBitmapsFrom(index, "ret", kRecentDay);

    // Repair only the recent window, exactly as Engine clamps it — even though
    // the series' bounds reach back to kOldDay.
    std::vector<NativeIndex::SeriesTimeBounds> bounds{{seriesId, static_cast<uint64_t>(kOldDay) * ke::NS_PER_DAY + 1,
                                                       static_cast<uint64_t>(kRecentDay) * ke::NS_PER_DAY + 1}};
    co_await index.rebuildDayBitmapsFromBounds(bounds, kRecentDay, kRecentDay);

    const uint64_t recentStart = static_cast<uint64_t>(kRecentDay) * ke::NS_PER_DAY;
    auto recent =
        co_await index.findSeriesWithMetadataTimeScoped("ret", {}, {}, recentStart, recentStart + ke::NS_PER_DAY - 1);
    EXPECT_TRUE(recent.has_value());
    EXPECT_EQ(recent->size(), 1u) << "the repaired window must be discoverable";

    EXPECT_FALSE(co_await NativeIndexTestAccess::hasPersistedDayBitmap(index, "ret", kOldDay))
        << "a day outside the window must stay deleted — the repair must not undo retention";

    co_await index.close();
}

// The watermark must never run ahead of what has actually been repaired.
//
// A rebuild flushes intermediately to bound memory (dirty day bitmaps cannot be
// evicted). If those flushes also advanced the watermark, a rebuild killed
// halfway would tell the NEXT startup that days it never got to were already
// durable — converting a recoverable gap into a permanent one, which is the
// exact failure this change exists to remove. So the watermark moves once, at
// the end, and a rebuild wide enough to force intermediate flushes must still
// end with every day discoverable.
SEASTAR_TEST_F(TimeScopedPostingsTest, RebuildAdvancesWatermarkOnlyOnCompletion) {
    // Wide enough to cross kRebuildFlushEveryDirtyKeys (4096 dirty day keys).
    constexpr uint32_t kFirstDay = 12000;
    constexpr uint32_t kSpanDays = 5000;
    const uint64_t firstNs = static_cast<uint64_t>(kFirstDay) * ke::NS_PER_DAY;
    const uint64_t lastNs = static_cast<uint64_t>(kFirstDay + kSpanDays) * ke::NS_PER_DAY;

    NativeIndex index(0);
    co_await index.open();

    auto seriesId = co_await index.getOrCreateSeriesId("wide", {{"host", "h1"}}, "v");
    const uint32_t before = index.dayBitmapWatermark().value_or(0);

    std::vector<NativeIndex::SeriesTimeBounds> bounds{{seriesId, firstNs, lastNs}};
    auto added = co_await index.rebuildDayBitmapsFromBounds(bounds, kFirstDay, kFirstDay + kSpanDays);
    EXPECT_EQ(added, static_cast<uint64_t>(kSpanDays) + 1);

    ASSERT_TRUE(index.dayBitmapWatermark().has_value());
    EXPECT_GT(*index.dayBitmapWatermark(), before);
    EXPECT_EQ(*index.dayBitmapWatermark(), kFirstDay + kSpanDays);

    // Every day is discoverable, including ones written before an intermediate
    // flush and ones written after it.
    for (uint32_t day : {kFirstDay, kFirstDay + kSpanDays / 2, kFirstDay + kSpanDays}) {
        const uint64_t start = static_cast<uint64_t>(day) * ke::NS_PER_DAY;
        auto r = co_await index.findSeriesWithMetadataTimeScoped("wide", {{"host", "h1"}}, {}, start,
                                                                 start + ke::NS_PER_DAY - 1);
        EXPECT_TRUE(r.has_value());
        EXPECT_EQ(r->size(), 1u) << "day " << day;
    }

    co_await index.close();
}

// A rebuild that finds everything already intact must still move the watermark
// forward — otherwise the repair window never advances and every restart pays
// for the same scan.
SEASTAR_TEST_F(TimeScopedPostingsTest, RebuildAdvancesWatermarkWhenNothingWasMissing) {
    constexpr uint32_t kDay = 21700;
    const uint64_t dayNs = static_cast<uint64_t>(kDay) * ke::NS_PER_DAY;

    NativeIndex index(0);
    co_await index.open();

    auto insert = makeInsert<double>("intact", "v", {{"host", "h1"}}, {dayNs + 1}, {1.0});
    auto seriesId = co_await index.indexInsert(insert);
    co_await index.flushDayBitmapsNow();
    ASSERT_TRUE(index.dayBitmapWatermark().has_value());
    EXPECT_EQ(*index.dayBitmapWatermark(), kDay);

    // Nothing to add: every day in the window is already recorded.
    std::vector<NativeIndex::SeriesTimeBounds> bounds{{seriesId, dayNs + 1, dayNs + 2}};
    auto added = co_await index.rebuildDayBitmapsFromBounds(bounds, kDay, kDay + 3);
    EXPECT_EQ(added, 0u);

    // The window examined reached kDay+3, so that is now known durable.
    EXPECT_EQ(*index.dayBitmapWatermark(), kDay + 3);

    {
        NativeIndex reopened(0);
        co_await index.close();
        co_await reopened.open();
        ASSERT_TRUE(reopened.dayBitmapWatermark().has_value());
        EXPECT_EQ(*reopened.dayBitmapWatermark(), kDay + 3) << "the advance must be durable, not just in RAM";
        co_await reopened.close();
    }
}
