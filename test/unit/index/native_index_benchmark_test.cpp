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

    // ── Phase 4b: Multi-Tag Intersection (findSeries with tag filters) ──
    fmt::print("\n── Phase 4b: Multi-Tag Intersection (findSeries) ──\n");
    {
        // 2-tag intersection: host + region
        LatencyStats twoTagHostRegion;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeries(std::string(MEASUREMENTS[m]),
                                                  {{"host", hostName(42)}, {"region", "us-east"}});
            twoTagHostRegion.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            // host-0042 is in us-east (index < 250), so expect 8 fields
            EXPECT_EQ(res->size(), static_cast<size_t>(FIELDS_PER_MEASUREMENT));
        }
        twoTagHostRegion.print("findSeries (host+region, 8 results)");

        // 2-tag intersection: rack + env
        LatencyStats twoTagRackEnv;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeries(std::string(MEASUREMENTS[m]),
                                                  {{"rack", rackName(0)}, {"env", "production"}});
            twoTagRackEnv.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            // rack-00: hosts 0,5,10,...495 (100 hosts), production: hosts >= 100 (400 hosts)
            // Intersection: rack-00 hosts that are production = hosts 100,105,...495 = 80 hosts * 8 fields = 640
            EXPECT_GT(res->size(), 0u);
        }
        twoTagRackEnv.print("findSeries (rack+env, ~640 results)");

        // 3-tag intersection: host + rack + region
        LatencyStats threeTag;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeries(std::string(MEASUREMENTS[m]),
                                                  {{"host", hostName(42)}, {"rack", rackName(42 % NUM_RACKS)}, {"region", "us-east"}});
            threeTag.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            // host-0042 is rack-02, us-east — consistent, expect 8
            EXPECT_EQ(res->size(), static_cast<size_t>(FIELDS_PER_MEASUREMENT));
        }
        threeTag.print("findSeries (host+rack+region, 8 results)");

        // Single-tag via findSeries API (same as findSeriesByTag but through intersection path)
        LatencyStats singleTagViaFindSeries;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeries(std::string(MEASUREMENTS[m]),
                                                  {{"host", hostName(0)}});
            singleTagViaFindSeries.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            EXPECT_EQ(res->size(), static_cast<size_t>(FIELDS_PER_MEASUREMENT));
        }
        singleTagViaFindSeries.print("findSeries (single host tag, 8 results)");
    }

    // ── Phase 4c: findSeriesByTagPattern (baseline) ─────────────────────
    fmt::print("\n── Phase 4c: findSeriesByTagPattern (baseline) ──\n");
    {
        LatencyStats patternStats;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesByTagPattern(std::string(MEASUREMENTS[m]),
                                                              "host", hostName(42));
            patternStats.add(clk::now() - t0);
            EXPECT_EQ(res.size(), static_cast<size_t>(FIELDS_PER_MEASUREMENT));
        }
        patternStats.print("findSeriesByTagPattern (single host)");
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

    // ── Phase 11: Time-Scoped Discovery ────────────────────────────────
    fmt::print("\n── Phase 11: Time-Scoped Discovery ──\n");
    {
        // Simulate time-scoped activity: re-index all series with timestamps
        // spread across 30 days. Only the first 50 hosts are active on the last day.
        constexpr uint32_t NUM_DAYS = 30;
        uint64_t baseDay = 20000ULL * 86400ULL * 1'000'000'000ULL;  // NS_PER_DAY

        fmt::print("  Indexing {} series with timestamps across {} days...\n", TOTAL_SERIES, NUM_DAYS);
        auto timeScopeStart = clk::now();
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
                    // Determine which day this series is active on
                    int activeDay = h % NUM_DAYS;  // Spread across days
                    uint64_t ts = baseDay + static_cast<uint64_t>(activeDay) * 86400ULL * 1'000'000'000ULL + 1;
                    TimeStarInsert<double> insert(std::string(MEASUREMENTS[m]), std::string(FIELDS[f]));
                    insert.tags = tags;
                    insert.timestamps = {ts};
                    insert.values = {42.0};
                    co_await index.indexInsert(insert);
                }
            }
        }
        double timeScopeSec = std::chrono::duration<double>(clk::now() - timeScopeStart).count();
        fmt::print("  Time-scoped indexing: {:.3f}s\n", timeScopeSec);

        // Benchmark: narrow query (last 1 day) vs wide query (all 30 days)
        uint64_t ns_per_day = 86400ULL * 1'000'000'000ULL;

        LatencyStats narrowDay;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            // Day 0: hosts 0, 30, 60, ... (about NUM_HOSTS/NUM_DAYS hosts * 8 fields)
            auto t0 = clk::now();
            auto res = co_await index.findSeriesWithMetadataTimeScoped(
                std::string(MEASUREMENTS[m]), {}, {},
                baseDay, baseDay + ns_per_day - 1);
            narrowDay.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            // ~17 hosts per day * 8 fields = ~136
            size_t expected = (NUM_HOSTS / NUM_DAYS) * FIELDS_PER_MEASUREMENT;
            // Allow some rounding: hosts h where h%30==0
            EXPECT_GT(res->size(), 0u);
            fmt::print("    {} day-0 series: {}\n", MEASUREMENTS[m], res->size());
        }
        narrowDay.print("findSeriesWithMetadataTimeScoped (1 day)");

        LatencyStats wideAll;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesWithMetadataTimeScoped(
                std::string(MEASUREMENTS[m]), {}, {},
                baseDay, baseDay + static_cast<uint64_t>(NUM_DAYS) * ns_per_day);
            wideAll.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
            EXPECT_EQ(res->size(), static_cast<size_t>(NUM_HOSTS * FIELDS_PER_MEASUREMENT));
        }
        wideAll.print("findSeriesWithMetadataTimeScoped (30 days, all)");

        LatencyStats nonTimeScoped;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesWithMetadata(std::string(MEASUREMENTS[m]), {}, {});
            nonTimeScoped.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
        }
        nonTimeScoped.print("findSeriesWithMetadata (non-time-scoped)");

        LatencyStats narrowWithTags;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            auto res = co_await index.findSeriesWithMetadataTimeScoped(
                std::string(MEASUREMENTS[m]), {{"region", "us-east"}}, {},
                baseDay, baseDay + ns_per_day - 1);
            narrowWithTags.add(clk::now() - t0);
            EXPECT_TRUE(res.has_value());
        }
        narrowWithTags.print("findSeriesWithMetadataTimeScoped (1 day + region tag)");
    }

    // ── Phase 12: Cardinality Estimation (Phase 4) ────────────────────
    fmt::print("\n── Phase 12: Cardinality Estimation ──\n");
    {
        LatencyStats measCard;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            double est = index.estimateMeasurementCardinality(std::string(MEASUREMENTS[m]));
            measCard.add(clk::now() - t0);
            double actual = static_cast<double>(NUM_HOSTS * FIELDS_PER_MEASUREMENT);
            double error = std::abs(est - actual) / actual;
            fmt::print("    {} estimated={:.0f}  actual={:.0f}  error={:.2f}%\n",
                       MEASUREMENTS[m], est, actual, error * 100.0);
            EXPECT_LT(error, 0.05) << "Cardinality estimate too far off for " << MEASUREMENTS[m];
        }
        measCard.print("estimateMeasurementCardinality");

        LatencyStats tagCard;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            double est = index.estimateTagCardinality(std::string(MEASUREMENTS[m]), "region", "us-east");
            tagCard.add(clk::now() - t0);
            double actual = static_cast<double>(NUM_HOSTS / 2 * FIELDS_PER_MEASUREMENT);
            double error = std::abs(est - actual) / actual;
            EXPECT_LT(error, 0.05) << "Tag cardinality estimate too far off for " << MEASUREMENTS[m];
        }
        tagCard.print("estimateTagCardinality (region=us-east)");

        LatencyStats hostCard;
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            auto t0 = clk::now();
            double est = index.estimateTagCardinality(std::string(MEASUREMENTS[m]), "host", hostName(42));
            hostCard.add(clk::now() - t0);
            // Single host should have FIELDS_PER_MEASUREMENT series
            EXPECT_GT(est, 0.0);
        }
        hostCard.print("estimateTagCardinality (single host)");
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
