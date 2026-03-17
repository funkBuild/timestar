/*
 * TSM Read Path Optimization Benchmarks
 *
 * Targeted benchmarks for TSM read path improvements:
 *   R1 — Sparse index time-range pre-filter in queryTsm<T>()
 *   R2 — Integer ZigZag decode range optimization
 *   R3 — Skip redundant block sort (already-sorted detection)
 *   R4 — Shared TSM file map snapshot
 *   R5 — Non-pushdown narrow-range query (end-to-end)
 *
 * Run with: --gtest_filter='TsmReadPathBenchmark*'
 */

#include "../seastar_gtest.hpp"
#include "../test_helpers.hpp"

#include "engine.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "timestar_value.hpp"
#include "tsm.hpp"
#include "tsm_writer.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <chrono>
#include <filesystem>
#include <fmt/core.h>
#include <random>
#include <string>
#include <vector>

using clk = std::chrono::high_resolution_clock;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
//  Helpers
// ---------------------------------------------------------------------------

struct BenchStats {
    std::vector<double> samples_us;

    void add(clk::duration d) {
        samples_us.push_back(std::chrono::duration<double, std::micro>(d).count());
    }

    void print(const char* label) const {
        if (samples_us.empty()) {
            fmt::print("  {:50s}  (no samples)\n", label);
            return;
        }
        auto sorted = samples_us;
        std::sort(sorted.begin(), sorted.end());
        double sum = 0;
        for (double v : sorted) sum += v;
        double avg = sum / static_cast<double>(sorted.size());
        auto pct = [&](double p) {
            return sorted[static_cast<size_t>(p * static_cast<double>(sorted.size() - 1))];
        };
        fmt::print("  {:50s}  min={:8.1f}  avg={:8.1f}  p50={:8.1f}  p95={:8.1f}  max={:8.1f} µs  (n={})\n",
                   label, sorted.front(), avg, pct(0.50), pct(0.95), sorted.back(), sorted.size());
    }

    double avg() const {
        if (samples_us.empty()) return 0;
        double s = 0;
        for (double v : samples_us) s += v;
        return s / static_cast<double>(samples_us.size());
    }
};

// ---------------------------------------------------------------------------
//  Fixture
// ---------------------------------------------------------------------------

class TsmReadPathBenchmark : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// ---------------------------------------------------------------------------
// Helper: insert data across multiple TSM files to simulate time-partitioned
// storage.  Creates numFiles TSM files, each containing numSeries series with
// pointsPerFile points.  Time ranges are non-overlapping across files.
// ---------------------------------------------------------------------------
static seastar::future<> insertMultiFileTsm(Engine& engine, int numFiles, int numSeries,
                                             int pointsPerFile,
                                             const std::string& measurement = "cpu",
                                             const std::string& field = "usage_idle") {
    uint64_t ts = 1000000000ULL;  // 1s in nanos
    uint64_t interval = 10000000000ULL;  // 10s between points

    for (int f = 0; f < numFiles; f++) {
        for (int s = 0; s < numSeries; s++) {
            TimeStarInsert<double> insert(measurement, field);
            insert.addTag("hostname", "host_" + std::to_string(s));
            for (int p = 0; p < pointsPerFile; p++) {
                uint64_t t = ts + static_cast<uint64_t>(f * pointsPerFile + p) * interval;
                insert.addValue(t, 50.0 + (p % 100) * 0.1);
            }
            co_await engine.insert(std::move(insert));
        }
        // Flush each batch to a separate TSM file
        co_await engine.rolloverMemoryStore();
        co_await seastar::sleep(std::chrono::milliseconds(200));
    }
}

// Same but for integer series
static seastar::future<> insertMultiFileTsmInt(Engine& engine, int numFiles, int numSeries,
                                                int pointsPerFile,
                                                const std::string& measurement = "sensors",
                                                const std::string& field = "count") {
    uint64_t ts = 1000000000ULL;
    uint64_t interval = 10000000000ULL;

    for (int f = 0; f < numFiles; f++) {
        for (int s = 0; s < numSeries; s++) {
            TimeStarInsert<int64_t> insert(measurement, field);
            insert.addTag("sensor", "sensor_" + std::to_string(s));
            for (int p = 0; p < pointsPerFile; p++) {
                uint64_t t = ts + static_cast<uint64_t>(f * pointsPerFile + p) * interval;
                insert.addValue(t, static_cast<int64_t>(100 + p));
            }
            co_await engine.insert(std::move(insert));
        }
        co_await engine.rolloverMemoryStore();
        co_await seastar::sleep(std::chrono::milliseconds(200));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  R1: Narrow-range query over many TSM files (sparse time pre-filter)
//
//  Inserts data across 10 TSM files with non-overlapping time ranges.
//  Then queries a narrow window that only overlaps 1-2 files.
//  Pre-filter should skip DMA reads for the ~8 irrelevant files.
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(TsmReadPathBenchmark, R1_NarrowRangeQuery_ManyFiles) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  R1: Narrow-Range Query Over Many TSM Files                    ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    Engine engine;
    co_await engine.init();

    constexpr int NUM_FILES = 10;
    constexpr int NUM_SERIES = 20;
    constexpr int POINTS_PER_FILE = 50;

    co_await insertMultiFileTsm(engine, NUM_FILES, NUM_SERIES, POINTS_PER_FILE);

    // Query window: only the 3rd file's time range
    // Each file has POINTS_PER_FILE points at 10s intervals starting from file offset
    uint64_t fileInterval = static_cast<uint64_t>(POINTS_PER_FILE) * 10000000000ULL;
    uint64_t queryStart = 1000000000ULL + 2 * fileInterval;  // Start of file 2
    uint64_t queryEnd = queryStart + fileInterval;  // End of file 2

    std::string seriesKey = "cpu,hostname=host_0 usage_idle";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Warm up
    for (int i = 0; i < 3; i++) {
        auto r = co_await engine.query(seriesKey, seriesId, queryStart, queryEnd);
        EXPECT_TRUE(r.has_value());
    }

    // Benchmark narrow-range query
    BenchStats narrowStats;
    for (int i = 0; i < 20; i++) {
        auto t0 = clk::now();
        auto result = co_await engine.query(seriesKey, seriesId, queryStart, queryEnd);
        narrowStats.add(clk::now() - t0);
        EXPECT_TRUE(result.has_value());
        if (result.has_value()) {
            auto& qr = std::get<QueryResult<double>>(*result);
            EXPECT_GE(qr.timestamps.size(), static_cast<size_t>(POINTS_PER_FILE));
        }
    }
    narrowStats.print("Narrow range (1/10 files)");

    // Benchmark wide-range query (all files) for comparison
    BenchStats wideStats;
    for (int i = 0; i < 20; i++) {
        auto t0 = clk::now();
        auto result = co_await engine.query(seriesKey, seriesId, 0, UINT64_MAX);
        wideStats.add(clk::now() - t0);
        EXPECT_TRUE(result.has_value());
        if (result.has_value()) {
            auto& qr = std::get<QueryResult<double>>(*result);
            EXPECT_EQ(qr.timestamps.size(), static_cast<size_t>(NUM_FILES * POINTS_PER_FILE));
        }
    }
    wideStats.print("Wide range (all files)");

    double ratio = narrowStats.avg() / wideStats.avg();
    fmt::print("\n  Narrow/Wide ratio: {:.2f}x (ideal ≈ 0.1x for 1/10 files)\n", ratio);

    co_await engine.stop();
}

// ═══════════════════════════════════════════════════════════════════════════
//  R2: Integer series narrow-range query
//
//  Tests integer decode path with narrow time range. ZigZag decodes only
//  the filtered range rather than the full block.
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(TsmReadPathBenchmark, R2_IntegerNarrowRange) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  R2: Integer Series Narrow-Range Query                         ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    Engine engine;
    co_await engine.init();

    constexpr int NUM_FILES = 5;
    constexpr int NUM_SERIES = 10;
    constexpr int POINTS_PER_FILE = 200;

    co_await insertMultiFileTsmInt(engine, NUM_FILES, NUM_SERIES, POINTS_PER_FILE);

    uint64_t fileInterval = static_cast<uint64_t>(POINTS_PER_FILE) * 10000000000ULL;

    // Narrow range: 10% of total
    uint64_t totalEnd = 1000000000ULL + static_cast<uint64_t>(NUM_FILES * POINTS_PER_FILE) * 10000000000ULL;
    uint64_t totalRange = totalEnd - 1000000000ULL;
    uint64_t queryStart = 1000000000ULL + totalRange / 2;
    uint64_t queryEnd = queryStart + totalRange / 10;

    std::string seriesKey = "sensors,sensor=sensor_0 count";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Warm up
    for (int i = 0; i < 3; i++) {
        auto r = co_await engine.query(seriesKey, seriesId, queryStart, queryEnd);
        EXPECT_TRUE(r.has_value());
    }

    // Benchmark narrow query
    BenchStats narrowStats;
    for (int i = 0; i < 20; i++) {
        auto t0 = clk::now();
        auto result = co_await engine.query(seriesKey, seriesId, queryStart, queryEnd);
        narrowStats.add(clk::now() - t0);
        EXPECT_TRUE(result.has_value());
    }
    narrowStats.print("Integer narrow range (10% of data)");

    // Wide-range baseline
    BenchStats wideStats;
    for (int i = 0; i < 20; i++) {
        auto t0 = clk::now();
        auto result = co_await engine.query(seriesKey, seriesId, 0, UINT64_MAX);
        wideStats.add(clk::now() - t0);
        EXPECT_TRUE(result.has_value());
        if (result.has_value()) {
            auto& qr = std::get<QueryResult<int64_t>>(*result);
            EXPECT_EQ(qr.timestamps.size(), static_cast<size_t>(NUM_FILES * POINTS_PER_FILE));
        }
    }
    wideStats.print("Integer wide range (all data)");

    double ratio = narrowStats.avg() / wideStats.avg();
    fmt::print("\n  Narrow/Wide ratio: {:.2f}x (ideal ≈ 0.1x for 10%% data)\n", ratio);

    co_await engine.stop();
}

// ═══════════════════════════════════════════════════════════════════════════
//  R5: Multi-series narrow-range query (full pipeline benchmark)
//
//  Simulates a realistic query: 20 series, 10 TSM files, narrow time range.
//  Measures the complete per-shard query pipeline including snapshot, prefetch,
//  read, merge.
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(TsmReadPathBenchmark, R5_MultiSeriesNarrowRange) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  R5: Multi-Series Narrow-Range Query Pipeline                  ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    Engine engine;
    co_await engine.init();

    constexpr int NUM_FILES = 10;
    constexpr int NUM_SERIES = 20;
    constexpr int POINTS_PER_FILE = 100;

    co_await insertMultiFileTsm(engine, NUM_FILES, NUM_SERIES, POINTS_PER_FILE);

    // Narrow range: 2 files worth of data
    uint64_t fileInterval = static_cast<uint64_t>(POINTS_PER_FILE) * 10000000000ULL;
    uint64_t queryStart = 1000000000ULL + 4 * fileInterval;
    uint64_t queryEnd = queryStart + 2 * fileInterval;

    // Build all series keys
    std::vector<std::string> seriesKeys;
    std::vector<SeriesId128> seriesIds;
    for (int s = 0; s < NUM_SERIES; s++) {
        std::string key = "cpu,hostname=host_" + std::to_string(s) + " usage_idle";
        seriesKeys.push_back(key);
        seriesIds.push_back(SeriesId128::fromSeriesKey(key));
    }

    // Warm up
    for (int s = 0; s < NUM_SERIES; s++) {
        auto r = co_await engine.query(seriesKeys[s], seriesIds[s], queryStart, queryEnd);
    }

    // Benchmark: query all series sequentially (per-shard behavior)
    BenchStats totalStats;
    for (int iter = 0; iter < 10; iter++) {
        auto t0 = clk::now();
        co_await engine.prefetchSeriesIndices(seriesIds);
        for (int s = 0; s < NUM_SERIES; s++) {
            auto result = co_await engine.query(seriesKeys[s], seriesIds[s], queryStart, queryEnd);
            EXPECT_TRUE(result.has_value());
        }
        totalStats.add(clk::now() - t0);
    }
    totalStats.print("20 series narrow range (prefetch + query)");

    // Wide-range comparison
    BenchStats wideStats;
    for (int iter = 0; iter < 10; iter++) {
        auto t0 = clk::now();
        co_await engine.prefetchSeriesIndices(seriesIds);
        for (int s = 0; s < NUM_SERIES; s++) {
            auto result = co_await engine.query(seriesKeys[s], seriesIds[s], 0, UINT64_MAX);
        }
        wideStats.add(clk::now() - t0);
    }
    wideStats.print("20 series wide range (prefetch + query)");

    double ratio = totalStats.avg() / wideStats.avg();
    fmt::print("\n  Narrow/Wide ratio: {:.2f}x (ideal ≈ 0.2x for 2/10 files)\n", ratio);

    co_await engine.stop();
}

// ═══════════════════════════════════════════════════════════════════════════
//  R3: Integer pushdown aggregation vs full materialization
//
//  Tests whether integer series can use pushdown aggregation (AVG/SUM/COUNT)
//  instead of the full decode → merge → aggregate pipeline.
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(TsmReadPathBenchmark, R3_IntegerPushdownAggregation) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  R3: Integer Pushdown Aggregation vs Full Materialization       ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    Engine engine;
    co_await engine.init();

    constexpr int NUM_FILES = 5;
    constexpr int NUM_SERIES = 10;
    constexpr int POINTS_PER_FILE = 500;

    co_await insertMultiFileTsmInt(engine, NUM_FILES, NUM_SERIES, POINTS_PER_FILE);

    std::string seriesKey = "sensors,sensor=sensor_0 count";
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    uint64_t aggregationInterval = 60000000000ULL;  // 60s buckets

    // Warm up
    for (int i = 0; i < 3; i++) {
        co_await engine.query(seriesKey, seriesId, 0, UINT64_MAX);
    }

    // Test pushdown path
    BenchStats pushdownStats;
    size_t pushdownHits = 0;
    for (int i = 0; i < 20; i++) {
        auto t0 = clk::now();
        auto result = co_await engine.queryAggregated(seriesKey, seriesId, 0, UINT64_MAX,
                                                       aggregationInterval, timestar::AggregationMethod::AVG);
        pushdownStats.add(clk::now() - t0);
        if (result.has_value()) pushdownHits++;
    }
    pushdownStats.print("Integer AVG pushdown (if supported)");
    fmt::print("  Pushdown hits: {}/20\n", pushdownHits);

    // Full materialization baseline
    BenchStats fullStats;
    for (int i = 0; i < 20; i++) {
        auto t0 = clk::now();
        auto result = co_await engine.query(seriesKey, seriesId, 0, UINT64_MAX);
        fullStats.add(clk::now() - t0);
        EXPECT_TRUE(result.has_value());
    }
    fullStats.print("Integer full materialization");

    if (pushdownHits > 0) {
        double speedup = fullStats.avg() / pushdownStats.avg();
        fmt::print("\n  Pushdown speedup: {:.2f}x vs full materialization\n", speedup);
    } else {
        fmt::print("\n  Pushdown not yet supported for integer series\n");
    }

    co_await engine.stop();
}
