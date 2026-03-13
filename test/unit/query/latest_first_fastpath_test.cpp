// Tests for the LATEST/FIRST fast path in queryTsmAggregated.
//
// The fast path skips Gate 2 (the expensive overlap check that DMA-reads full
// index entries from every TSM file) and instead uses getSeriesType() (bloom
// filter + sparse index, pure in-memory) to filter candidate files. Files are
// sorted by seqNum and read selectively.
//
// Tests:
//   1. LATEST returns correct value with multiple TSM files
//   2. FIRST returns correct value with multiple TSM files
//   3. LATEST with tombstoned newest data falls through correctly
//   4. LATEST with bucketed aggregation works across multiple files

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/query/query_result.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace fs = std::filesystem;

class LatestFirstFastPathTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// Helper: run a block of async test code with Engine lifecycle management.
#define WITH_ENGINE(engine_var, body)        \
    do {                                     \
        Engine engine_var;                   \
        std::exception_ptr __ex;             \
        try {                                \
            co_await engine_var.init();      \
            body                             \
        } catch (...) {                      \
            __ex = std::current_exception(); \
        }                                    \
        co_await engine_var.stop();          \
        if (__ex)                            \
            std::rethrow_exception(__ex);    \
    } while (0)

// ---------------------------------------------------------------------------
// Helper: insert points for a single series and flush to TSM.
// Each call creates a separate TSM file (via rollover + sleep).
// ---------------------------------------------------------------------------
static seastar::future<> insertAndFlushBatch(Engine& engine, const std::string& measurement, const std::string& field,
                                             const std::string& tagKey, const std::string& tagVal, uint64_t startTs,
                                             int count, uint64_t step, double baseVal) {
    TimeStarInsert<double> insert(measurement, field);
    insert.addTag(tagKey, tagVal);
    for (int i = 0; i < count; i++) {
        insert.addValue(startTs + static_cast<uint64_t>(i) * step, baseVal + i);
    }
    co_await engine.insert(std::move(insert));
    co_await engine.rolloverMemoryStore();
    // Allow background conversion to complete
    co_await seastar::sleep(std::chrono::milliseconds(200));
}

// ===========================================================================
// Test 1: LATEST returns the correct (most recent) value when data is
// spread across multiple TSM files.
// ===========================================================================
SEASTAR_TEST_F(LatestFirstFastPathTest, LatestAcrossMultipleTsmFiles) {
    WITH_ENGINE(engine, {
        std::string seriesKey = "sensor,host=h1 temp";
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        // File 1: timestamps 1000..1090 (10 points), values 1.0..10.0
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h1", 1000, 10, 10, 1.0);

        // File 2: timestamps 2000..2090 (10 points), values 11.0..20.0
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h1", 2000, 10, 10, 11.0);

        // File 3: timestamps 3000..3090 (10 points), values 21.0..30.0
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h1", 3000, 10, 10, 21.0);

        // Pushdown LATEST with interval=0 should return the single latest point
        auto result =
            co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::LATEST);

        EXPECT_TRUE(result.has_value()) << "LATEST pushdown should succeed for TSM-only float data";
        if (!result.has_value())
            co_return;
        EXPECT_EQ(result->totalPoints, 1u);
        EXPECT_EQ(result->sortedTimestamps.size(), 1u);
        if (result->sortedTimestamps.empty())
            co_return;
        // Latest point: t=3090, val=30.0
        EXPECT_EQ(result->sortedTimestamps[0], 3090u);
        EXPECT_DOUBLE_EQ(result->sortedValues[0], 30.0);
    });
    co_return;
}

// ===========================================================================
// Test 2: FIRST returns the correct (oldest) value when data is spread
// across multiple TSM files.
// ===========================================================================
SEASTAR_TEST_F(LatestFirstFastPathTest, FirstAcrossMultipleTsmFiles) {
    WITH_ENGINE(engine, {
        std::string seriesKey = "sensor,host=h2 temp";
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        // File 1: timestamps 1000..1090
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h2", 1000, 10, 10, 1.0);

        // File 2: timestamps 2000..2090
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h2", 2000, 10, 10, 11.0);

        // Pushdown FIRST with interval=0
        auto result =
            co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::FIRST);

        EXPECT_TRUE(result.has_value()) << "FIRST pushdown should succeed for TSM-only float data";
        if (!result.has_value())
            co_return;
        EXPECT_EQ(result->totalPoints, 1u);
        EXPECT_EQ(result->sortedTimestamps.size(), 1u);
        if (result->sortedTimestamps.empty())
            co_return;
        // First point: t=1000, val=1.0
        EXPECT_EQ(result->sortedTimestamps[0], 1000u);
        EXPECT_DOUBLE_EQ(result->sortedValues[0], 1.0);
    });
    co_return;
}

// ===========================================================================
// Test 3: LATEST with tombstoned newest data falls through to next file.
// The newest file's data is deleted, so LATEST should return the newest
// non-deleted point from the previous file.
// ===========================================================================
SEASTAR_TEST_F(LatestFirstFastPathTest, LatestSkipsTombstonedNewestFile) {
    WITH_ENGINE(engine, {
        std::string seriesKey = "sensor,host=h3 temp";
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        // File 1: timestamps 1000..1090
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h3", 1000, 10, 10, 1.0);

        // File 2: timestamps 2000..2090
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h3", 2000, 10, 10, 11.0);

        // Delete all data in the newer range [2000, 2090]
        co_await engine.deleteRange(seriesKey, 2000, 2090);

        // LATEST should skip tombstoned data and return from file 1
        auto result =
            co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0, timestar::AggregationMethod::LATEST);

        EXPECT_TRUE(result.has_value()) << "LATEST pushdown should succeed even with tombstones";
        if (!result.has_value())
            co_return;
        EXPECT_EQ(result->totalPoints, 1u);
        EXPECT_EQ(result->sortedTimestamps.size(), 1u);
        if (result->sortedTimestamps.empty())
            co_return;
        // Latest non-tombstoned: t=1090, val=10.0
        EXPECT_EQ(result->sortedTimestamps[0], 1090u);
        EXPECT_DOUBLE_EQ(result->sortedValues[0], 10.0);
    });
    co_return;
}

// ===========================================================================
// Test 4: LATEST with bucketed aggregation across multiple files.
// Verifies that the bucketed path fills buckets correctly when data spans
// multiple TSM files.
// ===========================================================================
SEASTAR_TEST_F(LatestFirstFastPathTest, LatestBucketedAcrossMultipleFiles) {
    WITH_ENGINE(engine, {
        std::string seriesKey = "sensor,host=h4 temp";
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        // File 1: timestamps 1000..1490 (50 points, step=10)
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h4", 1000, 50, 10, 1.0);

        // File 2: timestamps 2000..2490 (50 points, step=10)
        co_await insertAndFlushBatch(engine, "sensor", "temp", "host", "h4", 2000, 50, 10, 51.0);

        // Bucketed LATEST with interval=1000
        uint64_t interval = 1000;
        auto result = co_await engine.queryAggregated(seriesKey, seriesId, 1000, 2490, interval,
                                                      timestar::AggregationMethod::LATEST);

        EXPECT_TRUE(result.has_value()) << "Bucketed LATEST pushdown should succeed";
        if (!result.has_value())
            co_return;
        EXPECT_GT(result->totalPoints, 0u);

        // Verify we got bucket states
        EXPECT_FALSE(result->bucketStates.empty());

        // Each bucket should have data
        for (const auto& [bucketKey, state] : result->bucketStates) {
            EXPECT_EQ(bucketKey, (bucketKey / interval) * interval) << "Bucket key should be interval-aligned";
            EXPECT_GT(state.count, 0u);
        }
    });
    co_return;
}
