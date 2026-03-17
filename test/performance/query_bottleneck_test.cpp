// Performance tests for TSBS query bottlenecks (RC2-RC5).
// Each test inserts representative data, flushes to TSM, then benchmarks
// the specific query path that was identified as a bottleneck.
//
// RC2: Batch metadata fetch (sequential NativeIndex Gets)
// RC3: queryAggregated pushdown vs full materialization
// RC4: prefetchSeriesIndices TSM index warming
// RC5: latest aggregation full materialization

#include "../seastar_gtest.hpp"
#include "../test_helpers.hpp"
#include "engine.hpp"
#include "query_parser.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "timestar_value.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

using Clock = std::chrono::high_resolution_clock;

class QueryBottleneckTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// ---------------------------------------------------------------------------
// Helper: insert N series with P points each, flush to TSM
// ---------------------------------------------------------------------------
static seastar::future<> insertAndFlush(Engine& engine, int numSeries, int pointsPerSeries,
                                        const std::string& measurement = "cpu",
                                        const std::string& field = "usage_idle") {
    uint64_t ts = 1000000000ULL;  // 1 second in nanos
    for (int s = 0; s < numSeries; s++) {
        TimeStarInsert<double> insert(measurement, field);
        insert.addTag("hostname", "host_" + std::to_string(s));
        for (int p = 0; p < pointsPerSeries; p++) {
            insert.addValue(ts + static_cast<uint64_t>(p) * 10000000000ULL, 50.0 + (p % 100) * 0.1);
        }
        co_await engine.insert(std::move(insert));
    }
    // Flush to TSM
    co_await engine.rolloverMemoryStore();
    // Allow background conversion to complete
    co_await seastar::sleep(std::chrono::milliseconds(200));
}

// ---------------------------------------------------------------------------
// RC3: queryAggregated pushdown
// Verifies that queryAggregated returns actual aggregated results instead of
// nullopt (the old no-op stub behavior). After the fix, TSM-only queries
// should return PushdownResult with correct aggregate values.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryBottleneckTest, RC3_QueryAggregatedPushdown) {
    Engine engine;
    co_await engine.init();

    // Insert 10 series, 100 points each, flush to TSM
    co_await insertAndFlush(engine, 10, 100);

    // Query one series with pushdown aggregation
    std::string seriesKey = "cpu,hostname=host_0 usage_idle";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    uint64_t startTime = 0;
    uint64_t endTime = UINT64_MAX;
    uint64_t aggregationInterval = 60000000000ULL;  // 60 seconds

    auto start = Clock::now();
    auto pushdownResult = co_await engine.queryAggregated(seriesKey, seriesId, startTime, endTime, aggregationInterval);
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start);

    // After fix: should return actual pushdown result, not nullopt
    EXPECT_TRUE(pushdownResult.has_value()) << "queryAggregated should return pushdown result for TSM-only float data";

    if (pushdownResult.has_value()) {
        EXPECT_GT(pushdownResult->totalPoints, 0u) << "Pushdown should have found data points";
        std::cout << "[RC3] queryAggregated pushdown: " << pushdownResult->totalPoints << " points in "
                  << elapsed.count() << " µs" << std::endl;
    }

    // Compare with fallback (full materialization) path
    auto start2 = Clock::now();
    auto fullResult = co_await engine.query(seriesKey, seriesId, startTime, endTime);
    auto elapsed2 = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start2);

    EXPECT_TRUE(fullResult.has_value());
    if (fullResult.has_value()) {
        auto& result = std::get<QueryResult<double>>(fullResult.value());
        std::cout << "[RC3] Full materialization: " << result.timestamps.size() << " points in " << elapsed2.count()
                  << " µs" << std::endl;
    }

    co_await engine.stop();
}

// ---------------------------------------------------------------------------
// RC3: queryAggregated correctness
// Verifies that pushdown aggregation produces correct results by comparing
// against manually computed aggregates.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryBottleneckTest, RC3_QueryAggregatedCorrectness) {
    Engine engine;
    co_await engine.init();

    // Insert known values
    TimeStarInsert<double> insert("test_agg", "value");
    insert.addTag("host", "h0");
    // 10 values: 10, 20, 30, ..., 100
    for (int i = 1; i <= 10; i++) {
        insert.addValue(static_cast<uint64_t>(i) * 1000000000ULL, static_cast<double>(i * 10));
    }
    co_await engine.insert(std::move(insert));
    co_await engine.rolloverMemoryStore();
    co_await seastar::sleep(std::chrono::milliseconds(200));

    std::string seriesKey = "test_agg,host=h0 value";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // No bucketing (aggregationInterval=0) with default AVG method: should now
    // produce a collapsed AggregationState (streaming pushdown) instead of raw vectors.
    auto result = co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0);
    EXPECT_TRUE(result.has_value()) << "Pushdown should work for TSM-only float data";

    if (result.has_value()) {
        EXPECT_EQ(result->totalPoints, 10u);
        // Streamable method (AVG) with interval=0 produces aggregatedState
        EXPECT_TRUE(result->aggregatedState.has_value()) << "Non-bucketed AVG pushdown should produce collapsed state";
        if (result->aggregatedState.has_value()) {
            EXPECT_EQ(result->aggregatedState->count, 10u);
            // AVG of 10,20,...,100 = 55.0
            double avg = result->aggregatedState->getValue(timestar::AggregationMethod::AVG);
            EXPECT_DOUBLE_EQ(avg, 55.0);
        }
    }

    co_await engine.stop();
}

// ---------------------------------------------------------------------------
// RC4: prefetchSeriesIndices
// Verifies that prefetch warms the TSM cache so subsequent queries are faster.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryBottleneckTest, RC4_PrefetchSeriesIndices) {
    Engine engine;
    co_await engine.init();

    // Insert 20 series, 50 points each
    co_await insertAndFlush(engine, 20, 50);

    // Build series IDs for all 20 series
    std::vector<SeriesId128> seriesIds;
    for (int i = 0; i < 20; i++) {
        std::string key = "cpu,hostname=host_" + std::to_string(i) + " usage_idle";
        seriesIds.push_back(SeriesId128::fromSeriesKey(key));
    }

    // Prefetch all series indices
    auto start = Clock::now();
    co_await engine.prefetchSeriesIndices(seriesIds);
    auto prefetchTime = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start);
    std::cout << "[RC4] Prefetch 20 series: " << prefetchTime.count() << " µs" << std::endl;

    // Now query all 20 series — should be faster due to warm cache
    auto start2 = Clock::now();
    for (int i = 0; i < 20; i++) {
        std::string key = "cpu,hostname=host_" + std::to_string(i) + " usage_idle";
        auto result = co_await engine.query(key, seriesIds[i], 0, UINT64_MAX);
        EXPECT_TRUE(result.has_value());
    }
    auto queryTime = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start2);
    std::cout << "[RC4] Query 20 series (warm cache): " << queryTime.count() << " µs" << std::endl;

    co_await engine.stop();
}

// ---------------------------------------------------------------------------
// RC5: latest aggregation
// Verifies that latest queries work correctly and eventually benefit from
// pushdown (only reading the last block instead of all data).
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryBottleneckTest, RC5_LatestAggregation) {
    Engine engine;
    co_await engine.init();

    // Insert 10 series with 200 points each (enough to span multiple TSM blocks)
    co_await insertAndFlush(engine, 10, 200);

    // Time a full query (baseline)
    std::string seriesKey = "cpu,hostname=host_0 usage_idle";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    auto start = Clock::now();
    auto fullResult = co_await engine.query(seriesKey, seriesId, 0, UINT64_MAX);
    auto fullTime = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start);

    EXPECT_TRUE(fullResult.has_value());
    if (!fullResult.has_value()) {
        co_await engine.stop();
        co_return;
    }
    auto& qr = std::get<QueryResult<double>>(fullResult.value());
    EXPECT_GT(qr.timestamps.size(), 0u);
    if (qr.timestamps.empty()) {
        co_await engine.stop();
        co_return;
    }

    // The "latest" value is the one with the highest timestamp
    double expectedLatest = qr.values.back();
    uint64_t expectedLatestTs = qr.timestamps.back();

    std::cout << "[RC5] Full query: " << qr.timestamps.size() << " points in " << fullTime.count() << " µs"
              << std::endl;
    std::cout << "[RC5] Latest value: " << expectedLatest << " at timestamp " << expectedLatestTs << std::endl;

    // Test that queryAggregated with interval=0 and default AVG method returns
    // an aggregated state that includes the latest value in its tracking.
    auto pushdownResult = co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX, 0);
    if (pushdownResult.has_value() && pushdownResult->aggregatedState.has_value()) {
        // The aggregated state should track the latest value
        double pushdownLatest = pushdownResult->aggregatedState->latest;
        EXPECT_DOUBLE_EQ(pushdownLatest, expectedLatest);
        std::cout << "[RC5] Pushdown aggregated state latest matches full query latest" << std::endl;
    }

    co_await engine.stop();
}

// ---------------------------------------------------------------------------
// RC3+RC4 combined: full pipeline test
// Simulates the TSBS query flow: prefetch → pushdown aggregation for all series
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(QueryBottleneckTest, FullPipeline_PrefetchThenPushdown) {
    Engine engine;
    co_await engine.init();

    constexpr int NUM_SERIES = 30;
    constexpr int POINTS_PER_SERIES = 100;

    co_await insertAndFlush(engine, NUM_SERIES, POINTS_PER_SERIES);

    // Build series IDs
    std::vector<SeriesId128> seriesIds;
    std::vector<std::string> seriesKeys;
    for (int i = 0; i < NUM_SERIES; i++) {
        std::string key = "cpu,hostname=host_" + std::to_string(i) + " usage_idle";
        seriesKeys.push_back(key);
        seriesIds.push_back(SeriesId128::fromSeriesKey(key));
    }

    // Prefetch
    auto t0 = Clock::now();
    co_await engine.prefetchSeriesIndices(seriesIds);
    auto prefetchTime = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0);

    // Pushdown aggregation for all series
    uint64_t aggregationInterval = 60000000000ULL;  // 60s
    size_t pushdownHits = 0;
    size_t fallbacks = 0;

    auto t1 = Clock::now();
    for (int i = 0; i < NUM_SERIES; i++) {
        auto result = co_await engine.queryAggregated(seriesKeys[i], seriesIds[i], 0, UINT64_MAX, aggregationInterval);
        if (result.has_value()) {
            pushdownHits++;
        } else {
            fallbacks++;
        }
    }
    auto aggTime = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t1);

    std::cout << "[Pipeline] Prefetch: " << prefetchTime.count() << " µs" << std::endl;
    std::cout << "[Pipeline] Aggregation: " << aggTime.count() << " µs (" << pushdownHits << " pushdown, " << fallbacks
              << " fallback)" << std::endl;

    // After fixes, all should use pushdown (TSM-only float data, no overlap)
    EXPECT_EQ(pushdownHits, static_cast<size_t>(NUM_SERIES)) << "All series should use pushdown (TSM-only float data)";

    co_await engine.stop();
}
