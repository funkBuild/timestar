/*
 * Native Index Benchmark (gtest)
 *
 * High-cardinality dataset producing many compacted SSTable files.
 * Simulates a monitoring platform:
 *
 *   10 measurements  x  500 hosts  x  8 fields  =  40 000 unique series
 *   4 tags per series (host, rack, region, env)
 *
 * Benchmarks inserts then every query type, printing latency stats.
 * Run with:  --gtest_filter='NativeIndexBench*'
 */

#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fmt/core.h>
#include <random>
#include <string>
#include <vector>

using clk = std::chrono::high_resolution_clock;
using namespace timestar::index;

// ---------------------------------------------------------------------------
//  Configuration
// ---------------------------------------------------------------------------

static constexpr int NUM_MEASUREMENTS = 10;
static constexpr int NUM_HOSTS = 500;
static constexpr int NUM_RACKS = 5;
static constexpr int FIELDS_PER_MEASUREMENT = 8;
static constexpr int TOTAL_SERIES = NUM_MEASUREMENTS * NUM_HOSTS * FIELDS_PER_MEASUREMENT;

static const std::array<std::string, NUM_MEASUREMENTS> MEASUREMENTS = {
    "api.latency",     "api.throughput",  "db.queries",      "db.connections", "cache.hits",
    "cache.evictions", "queue.depth",     "queue.wait_time", "disk.iops",     "disk.utilization",
};

static const std::array<std::string, FIELDS_PER_MEASUREMENT> FIELDS = {
    "p50", "p90", "p99", "max", "count", "error_rate", "avg", "stddev",
};

// ---------------------------------------------------------------------------
//  Helpers
// ---------------------------------------------------------------------------

static std::string hostName(int id) { return fmt::format("host-{:04d}", id); }
static std::string rackName(int id) { return fmt::format("rack-{:02d}", id); }

struct LatencyStats {
    std::vector<double> samples_ms;

    void add(clk::duration d) { samples_ms.push_back(std::chrono::duration<double, std::milli>(d).count()); }

    void print(const char* label) const {
        if (samples_ms.empty()) {
            fmt::print("  {:44s}  (no samples)\n", label);
            return;
        }
        auto sorted = samples_ms;
        std::sort(sorted.begin(), sorted.end());
        double sum = 0;
        for (double v : sorted) sum += v;
        double avg = sum / static_cast<double>(sorted.size());
        auto pct = [&](double p) {
            return sorted[static_cast<size_t>(p * static_cast<double>(sorted.size() - 1))];
        };
        fmt::print("  {:44s}  min={:8.3f}  avg={:8.3f}  p50={:8.3f}  p95={:8.3f}  p99={:8.3f}  max={:8.3f} ms  "
                   "(n={})\n",
                   label, sorted.front(), avg, pct(0.50), pct(0.95), pct(0.99), sorted.back(), sorted.size());
    }
};

// ---------------------------------------------------------------------------
//  Fixture — creates 40k series, compacts, then runs query benchmarks
// ---------------------------------------------------------------------------

class NativeIndexBench : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

// Single test that runs all phases sequentially
SEASTAR_TEST_F(NativeIndexBench, FullBenchmark) {
    NativeIndex index(0);
    co_await index.open();

    fmt::print("\n╔══════════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  Native Index Benchmark: {} meas x {} hosts x {} fields = {} series  ║\n",
               NUM_MEASUREMENTS, NUM_HOSTS, FIELDS_PER_MEASUREMENT, TOTAL_SERIES);
    fmt::print("╚══════════════════════════════════════════════════════════════════════╝\n");

    // ── Phase 1: Bulk Insert ──────────────────────────────────────────────
    fmt::print("\n── Phase 1: Bulk Insert ({} series) ──\n", TOTAL_SERIES);
    std::vector<SeriesId128> allIds;
    allIds.reserve(TOTAL_SERIES);

    LatencyStats insertStats;
    auto insertStart = clk::now();

    for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
        for (int h = 0; h < NUM_HOSTS; ++h) {
            int rack = h % NUM_RACKS;
            std::map<std::string, std::string> tags = {
                {"host", hostName(h)},
                {"rack", rackName(rack)},
                {"region", (h < NUM_HOSTS / 2) ? "us-east" : "us-west"},
                {"env", (h < NUM_HOSTS / 5) ? "staging" : "production"},
            };
            for (int f = 0; f < FIELDS_PER_MEASUREMENT; ++f) {
                auto t0 = clk::now();
                auto id =
                    co_await index.getOrCreateSeriesId(std::string(MEASUREMENTS[m]), tags, std::string(FIELDS[f]));
                insertStats.add(clk::now() - t0);
                allIds.push_back(id);
            }
        }
    }

    double insertSec = std::chrono::duration<double>(clk::now() - insertStart).count();
    fmt::print("  Total: {:.3f}s  ({:.0f} series/sec)\n", insertSec, static_cast<double>(TOTAL_SERIES) / insertSec);
    insertStats.print("getOrCreateSeriesId");
    EXPECT_EQ(allIds.size(), static_cast<size_t>(TOTAL_SERIES));

    // ── Phase 2: Compaction ───────────────────────────────────────────────
    fmt::print("\n── Phase 2: Compaction ──\n");
    {
        auto t0 = clk::now();
        co_await index.compact();
        fmt::print("  compact(): {:.3f} ms\n",
                   std::chrono::duration<double, std::milli>(clk::now() - t0).count());
    }

    // ── Phase 3: Point Lookups (cold then warm) ──────────────────────────
    fmt::print("\n── Phase 3: Point Lookups (getSeriesMetadata, 5000 random) ──\n");
    {
        std::mt19937_64 rng(42);
        std::uniform_int_distribution<size_t> dist(0, allIds.size() - 1);
        LatencyStats cold;
        for (int i = 0; i < 5000; ++i) {
            auto t0 = clk::now();
            auto meta = co_await index.getSeriesMetadata(allIds[dist(rng)]);
            cold.add(clk::now() - t0);
            EXPECT_TRUE(meta.has_value());
        }
        cold.print("getSeriesMetadata (cold)");

        // Warm pass — same seed, same IDs, should hit cache
        rng.seed(42);
        LatencyStats warm;
        for (int i = 0; i < 5000; ++i) {
            auto t0 = clk::now();
            co_await index.getSeriesMetadata(allIds[dist(rng)]);
            warm.add(clk::now() - t0);
        }
        warm.print("getSeriesMetadata (warm cache)");
    }

    // ── Phase 4: Tag Scans ───────────────────────────────────────────────
    fmt::print("\n── Phase 4: Tag Scans (findSeriesByTag) ──\n");
    {
        LatencyStats hostScan;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesByTag(std::string(MEASUREMENTS[m]), "host", hostName(0));
            hostScan.add(clk::now() - t0);
            EXPECT_EQ(res.size(), static_cast<size_t>(FIELDS_PER_MEASUREMENT));
        }
        hostScan.print("findSeriesByTag (single host)");

        LatencyStats regionScan;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesByTag(std::string(MEASUREMENTS[m]), "region", "us-east");
            regionScan.add(clk::now() - t0);
            // us-east = first half of hosts * 8 fields = 250 * 8 = 2000
            EXPECT_EQ(res.size(), static_cast<size_t>(NUM_HOSTS / 2 * FIELDS_PER_MEASUREMENT));
        }
        regionScan.print("findSeriesByTag (region, 2000 results)");
    }

    // ── Phase 5: getAllSeriesForMeasurement ───────────────────────────────
    fmt::print("\n── Phase 5: getAllSeriesForMeasurement ──\n");
    {
        LatencyStats stats;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.getAllSeriesForMeasurement(std::string(MEASUREMENTS[m]));
            stats.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            EXPECT_EQ(res->size(), static_cast<size_t>(NUM_HOSTS * FIELDS_PER_MEASUREMENT));
        }
        stats.print("getAllSeriesForMeasurement (4000 each)");
    }

    // ── Phase 6: Group-by ────────────────────────────────────────────────
    fmt::print("\n── Phase 6: getSeriesGroupedByTag ──\n");
    {
        LatencyStats hostGroup;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto groups = co_await index.getSeriesGroupedByTag(std::string(MEASUREMENTS[m]), "host");
            hostGroup.add(clk::now() - t0);
            EXPECT_EQ(groups.size(), static_cast<size_t>(NUM_HOSTS));
        }
        hostGroup.print("groupByTag (host, 500 groups)");

        LatencyStats rackGroup;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto groups = co_await index.getSeriesGroupedByTag(std::string(MEASUREMENTS[m]), "rack");
            rackGroup.add(clk::now() - t0);
            EXPECT_EQ(groups.size(), static_cast<size_t>(NUM_RACKS));
        }
        rackGroup.print("groupByTag (rack, 5 groups)");
    }

    // ── Phase 7: findSeriesWithMetadata ──────────────────────────────────
    fmt::print("\n── Phase 7: findSeriesWithMetadata (filtered) ──\n");
    {
        LatencyStats narrow;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesWithMetadata(std::string(MEASUREMENTS[m]),
                                                              {{"host", hostName(42)}}, {"p99"});
            narrow.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            EXPECT_EQ(res->size(), 1u);
        }
        narrow.print("findSeriesWithMetadata (host+field, 1 result)");

        LatencyStats wide;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesWithMetadata(std::string(MEASUREMENTS[m]),
                                                              {{"region", "us-west"}});
            wide.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
        }
        wide.print("findSeriesWithMetadata (region, 2000 results)");
    }

    // ── Phase 8: Schema Metadata ─────────────────────────────────────────
    fmt::print("\n── Phase 8: Schema Metadata ──\n");
    {
        auto t0 = clk::now();
        auto meas = co_await index.getAllMeasurements();
        fmt::print("  getAllMeasurements:    {:.3f} ms  ({} measurements)\n",
                   std::chrono::duration<double, std::milli>(clk::now() - t0).count(), meas.size());
        EXPECT_EQ(meas.size(), static_cast<size_t>(NUM_MEASUREMENTS));

        LatencyStats fields, tags, tagVals;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            { auto t = clk::now(); co_await index.getFields(std::string(MEASUREMENTS[m])); fields.add(clk::now() - t); }
            { auto t = clk::now(); co_await index.getTags(std::string(MEASUREMENTS[m])); tags.add(clk::now() - t); }
            { auto t = clk::now(); co_await index.getTagValues(std::string(MEASUREMENTS[m]), "host"); tagVals.add(clk::now() - t); }
        }
        fields.print("getFields");
        tags.print("getTags");
        tagVals.print("getTagValues (host, 500 values)");
    }

    // ── Phase 9: Batch Metadata ──────────────────────────────────────────
    fmt::print("\n── Phase 9: Batch Metadata (100 IDs x 50 batches) ──\n");
    {
        std::mt19937_64 rng(99);
        std::uniform_int_distribution<size_t> dist(0, allIds.size() - 1);
        LatencyStats stats;
        for (int b = 0; b < 50; ++b) {
            std::vector<SeriesId128> batch;
            batch.reserve(100);
            for (int i = 0; i < 100; ++i) batch.push_back(allIds[dist(rng)]);
            auto t0 = clk::now();
            co_await index.getSeriesMetadataBatch(batch);
            stats.add(clk::now() - t0);
        }
        stats.print("getSeriesMetadataBatch (100 IDs)");
    }

    // ── Phase 10: Cached Discovery ───────────────────────────────────────
    fmt::print("\n── Phase 10: Cached Discovery (repeat phase 7 via cache) ──\n");
    {
        LatencyStats stats;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            co_await index.findSeriesWithMetadataCached(std::string(MEASUREMENTS[m]),
                                                        {{"host", hostName(42)}}, {"p99"});
            stats.add(clk::now() - t0);
        }
        stats.print("findSeriesWithMetadataCached (warm)");
    }

    // ── Summary ──────────────────────────────────────────────────────────
    fmt::print("\n── Index Statistics ──\n");
    auto count = co_await index.getSeriesCount();
    fmt::print("  Total series:       {}\n", count);
    fmt::print("  Metadata cache:     {} entries ({:.1f} MB)\n", index.getMetadataCacheSize(),
               static_cast<double>(index.getMetadataCacheBytes()) / (1024.0 * 1024.0));
    fmt::print("  Discovery cache:    {} entries ({:.1f} MB)\n", index.getDiscoveryCacheSize(),
               static_cast<double>(index.getDiscoveryCacheBytes()) / (1024.0 * 1024.0));
    fmt::print("  Series dedup cache: {} entries\n", index.getSeriesCacheSize());
    EXPECT_EQ(count, static_cast<size_t>(TOTAL_SERIES));

    co_await index.close();
    fmt::print("\n── Benchmark Complete ──\n\n");
}
