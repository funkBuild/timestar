/*
 * Optimization Baseline Benchmark
 *
 * Covers performance gaps identified in the optimization audit.
 * Run with: --gtest_filter='OptimizationBaseline*'
 *
 * Benchmarks are grouped by optimization area:
 *   B1  — Aggregator merge scaling (P3-QUERY-1)
 *   B2  — SSTable read memory & latency (P1-MEM-1)
 *   B3  — NativeIndex insert cache overhead (P4-INSERT-3, P1-MEM-3, P1-MEM-4)
 *   B4  — TSM fullIndexCache scaling (P1-MEM-2)
 *   B5  — Multi-series query I/O (P3-QUERY-2, P3-QUERY-3)
 *   B6  — Compactor dedup algorithm (P6-LOW-3)
 *   B7  — NativeIndex high-cardinality (P1-MEM-3, P4-INSERT-4)
 *   B8  — Engine insert path (P4-INSERT-1, P4-INSERT-2)
 *   B9  — Query parser allocation (P6-LOW-1)
 *   B10 — TSM writer flush latency (P5-IO-5)
 */

#include "../test_helpers.hpp"
#include "../seastar_gtest.hpp"

#include "engine.hpp"
#include "query_result.hpp"
#include "timestar_value.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/index/native/sstable.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/query/query_parser.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fmt/core.h>
#include <random>
#include <string>
#include <vector>

using clk = std::chrono::high_resolution_clock;
using namespace timestar::index;
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
//  Shared helpers
// ---------------------------------------------------------------------------

struct LatStats {
    std::vector<double> samples_ms;

    void add(clk::duration d) {
        samples_ms.push_back(std::chrono::duration<double, std::milli>(d).count());
    }

    void print(const char* label) const {
        if (samples_ms.empty()) {
            fmt::print("  {:50s}  (no samples)\n", label);
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
        fmt::print("  {:50s}  min={:8.3f}  avg={:8.3f}  p50={:8.3f}  p95={:8.3f}  max={:8.3f} ms  (n={})\n",
                   label, sorted.front(), avg, pct(0.50), pct(0.95), sorted.back(), sorted.size());
    }

    double avg() const {
        if (samples_ms.empty()) return 0;
        double s = 0;
        for (double v : samples_ms) s += v;
        return s / static_cast<double>(samples_ms.size());
    }

    double total() const {
        double s = 0;
        for (double v : samples_ms) s += v;
        return s;
    }
};

static std::string hostName(int id) { return fmt::format("host-{:04d}", id); }

// ---------------------------------------------------------------------------
//  Fixture
// ---------------------------------------------------------------------------

class OptimizationBaseline : public ::testing::Test {
public:
    void SetUp() override {
        fs::remove_all("shard_0");
        fs::create_directories("shard_0/tsm");
        fs::create_directories("shard_0/native_index");
    }
    void TearDown() override {
        fs::remove_all("shard_0");
    }
};

// ═══════════════════════════════════════════════════════════════════════════
//  B1: Aggregator merge scaling — measures O(K²N) pairwise merge
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B1_AggregatorMergeScaling) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B1: Aggregator Merge Scaling                                  ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    constexpr size_t POINTS_PER_SERIES = 50000;
    constexpr uint64_t START_NS = 1704067200000000000ULL;
    constexpr uint64_t INTERVAL_NS = 60000000000ULL;  // 1 minute

    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(20.0, 80.0);

    // Pre-generate one set of timestamps
    std::vector<uint64_t> timestamps;
    timestamps.reserve(POINTS_PER_SERIES);
    for (size_t i = 0; i < POINTS_PER_SERIES; ++i) {
        timestamps.push_back(START_NS + i * INTERVAL_NS);
    }

    // ── Same-timestamps test (common case: multiple series, same time range) ──
    fmt::print("\n  ── Same-timestamps (element-wise fold, O(N)) ──\n");
    for (int K : {1, 5, 10, 25, 50, 100}) {
        // Build K value arrays (all share timestamps)
        std::vector<std::vector<double>> allVals(K);
        for (int s = 0; s < K; ++s) {
            allVals[s].reserve(POINTS_PER_SERIES);
            for (size_t i = 0; i < POINTS_PER_SERIES; ++i) {
                allVals[s].push_back(dist(rng));
            }
        }

        auto t0 = clk::now();

        // Element-wise fold — the new fast path for same-timestamp partials
        std::vector<double> result = std::move(allVals[0]);
        for (int s = 1; s < K; ++s) {
            const double* src = allVals[s].data();
            double* dst = result.data();
            for (size_t i = 0; i < POINTS_PER_SERIES; ++i) {
                dst[i] += src[i];
            }
        }

        double elapsed = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
        size_t totalPoints = static_cast<size_t>(K) * POINTS_PER_SERIES;
        double ptsPerSec = (totalPoints * 1000.0) / elapsed;

        fmt::print("  K={:4d}  {:8.2f} ms  ({:.0f} pts/sec)\n", K, elapsed, ptsPerSec);
    }

    // ── Different-timestamps test (N-way heap merge) ──
    fmt::print("\n  ── Different-timestamps (N-way heap merge, O(KN log K)) ──\n");
    for (int K : {1, 5, 10, 25, 50, 100}) {
        std::vector<std::vector<uint64_t>> partialTs(K);
        std::vector<std::vector<double>> partialVals(K);

        for (int s = 0; s < K; ++s) {
            partialTs[s].reserve(POINTS_PER_SERIES);
            partialVals[s].reserve(POINTS_PER_SERIES);
            for (size_t i = 0; i < POINTS_PER_SERIES; ++i) {
                // Offset each series by s nanoseconds so timestamps differ
                partialTs[s].push_back(timestamps[i] + static_cast<uint64_t>(s));
                partialVals[s].push_back(dist(rng));
            }
        }

        auto t0 = clk::now();

        // N-way heap merge
        struct Entry {
            uint64_t ts;
            size_t idx;
            size_t pos;
            bool operator>(const Entry& o) const { return ts > o.ts; }
        };
        std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap;
        for (int s = 0; s < K; ++s) {
            if (!partialTs[s].empty()) heap.push({partialTs[s][0], static_cast<size_t>(s), 0});
        }

        size_t totalPoints = static_cast<size_t>(K) * POINTS_PER_SERIES;
        std::vector<uint64_t> resultTs;
        std::vector<double> resultVals;
        resultTs.reserve(totalPoints);
        resultVals.reserve(totalPoints);

        while (!heap.empty()) {
            auto [ts, idx, pos] = heap.top();
            heap.pop();
            resultTs.push_back(ts);
            resultVals.push_back(partialVals[idx][pos]);
            if (pos + 1 < partialTs[idx].size()) {
                heap.push({partialTs[idx][pos + 1], idx, pos + 1});
            }
        }

        double elapsed = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
        double ptsPerSec = (totalPoints * 1000.0) / elapsed;

        fmt::print("  K={:4d}  {:8.2f} ms  ({:.0f} pts/sec)\n", K, elapsed, ptsPerSec);
        EXPECT_EQ(resultTs.size(), totalPoints);
    }

    // ── Old pairwise merge (reference) ──
    fmt::print("\n  ── Old pairwise merge O(K²N) (reference) ──\n");
    for (int K : {1, 5, 10, 25, 50, 100}) {
        std::vector<std::vector<uint64_t>> partialTs(K);
        std::vector<std::vector<double>> partialVals(K);

        for (int s = 0; s < K; ++s) {
            partialTs[s].reserve(POINTS_PER_SERIES);
            partialVals[s].reserve(POINTS_PER_SERIES);
            for (size_t i = 0; i < POINTS_PER_SERIES; ++i) {
                partialTs[s].push_back(timestamps[i] + static_cast<uint64_t>(s));
                partialVals[s].push_back(dist(rng));
            }
        }

        auto t0 = clk::now();

        std::vector<uint64_t> resultTs = std::move(partialTs[0]);
        std::vector<double> resultVals = std::move(partialVals[0]);

        for (int s = 1; s < K; ++s) {
            std::vector<uint64_t> mergedTs;
            std::vector<double> mergedVals;
            mergedTs.reserve(resultTs.size() + partialTs[s].size());
            mergedVals.reserve(resultVals.size() + partialVals[s].size());

            size_t a = 0, b = 0;
            while (a < resultTs.size() && b < partialTs[s].size()) {
                if (resultTs[a] <= partialTs[s][b]) {
                    mergedTs.push_back(resultTs[a]);
                    mergedVals.push_back(resultVals[a]);
                    ++a;
                } else {
                    mergedTs.push_back(partialTs[s][b]);
                    mergedVals.push_back(partialVals[s][b]);
                    ++b;
                }
            }
            while (a < resultTs.size()) {
                mergedTs.push_back(resultTs[a]);
                mergedVals.push_back(resultVals[a++]);
            }
            while (b < partialTs[s].size()) {
                mergedTs.push_back(partialTs[s][b]);
                mergedVals.push_back(partialVals[s][b++]);
            }

            resultTs = std::move(mergedTs);
            resultVals = std::move(mergedVals);
        }

        double elapsed = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
        size_t totalPoints = static_cast<size_t>(K) * POINTS_PER_SERIES;
        double ptsPerSec = (totalPoints * 1000.0) / elapsed;

        fmt::print("  K={:4d}  {:8.2f} ms  ({:.0f} pts/sec)\n", K, elapsed, ptsPerSec);
    }

    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B2: SSTable read — full file load vs block-level access
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B2_SSTableReadMemory) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B2: SSTable Read Memory & Latency                             ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    std::string dir = "shard_0/native_index";
    fs::create_directories(dir);

    // Create SSTables of increasing size
    for (int numKeys : {1000, 10000, 50000}) {
        std::string path = dir + fmt::format("/bench_{}.sst", numKeys);

        // Write SSTable
        {
            auto t0 = clk::now();
            auto writer = co_await SSTableWriter::create(path, 4096, 10);
            for (int i = 0; i < numKeys; ++i) {
                std::string key = fmt::format("key_{:08d}", i);
                std::string val = fmt::format("value_{:08d}_padding_to_make_it_bigger_{}", i, i * 31);
                writer.add(key, val);
                co_await writer.flushPending();
            }
            co_await writer.finish();
            double writeMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();

            size_t fileSize = fs::file_size(path);
            fmt::print("  Write {:6d} keys:  {:8.2f} ms  file={:.1f} KB\n",
                       numKeys, writeMs, fileSize / 1024.0);
        }

        // Read SSTable — measure open latency (includes full file load)
        {
            LatStats openStats;
            for (int trial = 0; trial < 5; ++trial) {
                auto t0 = clk::now();
                auto reader = co_await SSTableReader::open(path);
                openStats.add(clk::now() - t0);
            }
            openStats.print(fmt::format("Open SSTable ({} keys)", numKeys).c_str());
        }

        // Point lookups — cold then warm
        {
            auto reader = co_await SSTableReader::open(path);

            LatStats cold;
            std::mt19937 rng2(99);
            std::uniform_int_distribution<int> dist2(0, numKeys - 1);
            int lookups = std::min(numKeys, 1000);
            for (int i = 0; i < lookups; ++i) {
                std::string key = fmt::format("key_{:08d}", dist2(rng2));
                auto t0 = clk::now();
                auto val = reader->get(key);
                cold.add(clk::now() - t0);
                EXPECT_TRUE(val.has_value());
            }
            cold.print(fmt::format("Point lookup ({} keys, {} lookups)", numKeys, lookups).c_str());
        }

        fs::remove(path);
    }

    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B3: NativeIndex insert cache overhead
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B3_NativeIndexInsertCacheOverhead) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B3: NativeIndex Insert Cache & Allocation Overhead            ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    NativeIndex index(0);
    co_await index.open();

    // Phase A: Measure getOrCreateSeriesId for increasing cardinality
    constexpr int NUM_MEASUREMENTS = 5;
    constexpr int FIELDS = 4;
    std::vector<int> hostCounts = {10, 50, 100, 500, 1000};

    for (int numHosts : hostCounts) {
        LatStats insertStats;
        int totalSeries = 0;

        auto t0 = clk::now();
        for (int m = 0; m < NUM_MEASUREMENTS; ++m) {
            std::string meas = fmt::format("meas_{}", m);
            for (int h = 0; h < numHosts; ++h) {
                std::map<std::string, std::string> tags = {
                    {"host", hostName(h)},
                    {"region", (h < numHosts / 2) ? "us-east" : "us-west"},
                };
                for (int f = 0; f < FIELDS; ++f) {
                    std::string field = fmt::format("field_{}", f);
                    auto ti = clk::now();
                    co_await index.getOrCreateSeriesId(meas, tags, field);
                    insertStats.add(clk::now() - ti);
                    ++totalSeries;
                }
            }
        }
        double totalMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
        double seriesPerSec = (totalSeries * 1000.0) / totalMs;

        fmt::print("  {:6d} hosts → {:6d} series:  {:8.2f} ms total  ({:.0f} series/sec)\n",
                   numHosts, totalSeries, totalMs, seriesPerSec);
        insertStats.print(fmt::format("  getOrCreateSeriesId ({} hosts)", numHosts).c_str());
    }

    // Report cache sizes
    fmt::print("\n  Cache sizes after all inserts:\n");
    fmt::print("    Series dedup cache:  {} entries\n", index.getSeriesCacheSize());
    fmt::print("    Metadata cache:      {} entries ({:.1f} MB)\n",
               index.getMetadataCacheSize(),
               static_cast<double>(index.getMetadataCacheBytes()) / (1024.0 * 1024.0));
    fmt::print("    Discovery cache:     {} entries ({:.1f} MB)\n",
               index.getDiscoveryCacheSize(),
               static_cast<double>(index.getDiscoveryCacheBytes()) / (1024.0 * 1024.0));

    co_await index.close();
    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B4: TSM fullIndexCache scaling
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B4_TSMFullIndexCacheScaling) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B4: TSM fullIndexCache Scaling (entry-count vs bytes)         ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    std::string tsmDir = "shard_0/tsm";

    // Create TSM files with increasing series count and blocks-per-series
    for (auto [numSeries, blocksPerSeries] : std::vector<std::pair<int,int>>{{100, 1}, {500, 2}, {1000, 3}}) {
        std::string filename = tsmDir + fmt::format("/0_{}.tsm", numSeries);
        size_t totalPoints = 0;

        // Write TSM file
        {
            TSMWriter writer(filename);
            std::mt19937 rng3(42);
            std::uniform_real_distribution<double> dist3(0.0, 100.0);

            for (int s = 0; s < numSeries; ++s) {
                size_t pointsPerSeries = static_cast<size_t>(blocksPerSeries) * 10000;
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                timestamps.reserve(pointsPerSeries);
                values.reserve(pointsPerSeries);

                uint64_t baseTs = 1700000000000000000ULL + static_cast<uint64_t>(s) * 1000000000ULL;
                for (size_t i = 0; i < pointsPerSeries; ++i) {
                    timestamps.push_back(baseTs + i * 1000000ULL);
                    values.push_back(dist3(rng3));
                }

                std::string key = fmt::format("metrics,host=server_{:04d} value", s);
                SeriesId128 sid = SeriesId128::fromSeriesKey(key);
                writer.writeSeries(TSMValueType::Float, sid, timestamps, values);
                totalPoints += pointsPerSeries;
            }
            writer.writeIndex();
            writer.close();
        }

        size_t fileSize = fs::file_size(filename);
        fmt::print("  TSM file: {} series x {} blocks = {} pts ({:.1f} MB)\n",
                   numSeries, blocksPerSeries, totalPoints, fileSize / (1024.0 * 1024.0));

        // Benchmark: open + random getFullIndexEntry lookups
        {
            TSM tsm(filename);
            co_await tsm.open();

            auto seriesIds = tsm.getSeriesIds();
            std::mt19937 rng4(77);
            std::uniform_int_distribution<size_t> dist4(0, seriesIds.size() - 1);

            // Cold lookups
            LatStats cold;
            int lookups = std::min(static_cast<int>(seriesIds.size()), 500);
            for (int i = 0; i < lookups; ++i) {
                auto t0 = clk::now();
                auto* entry = co_await tsm.getFullIndexEntry(seriesIds[dist4(rng4)]);
                cold.add(clk::now() - t0);
                EXPECT_NE(entry, nullptr);
            }
            cold.print(fmt::format("getFullIndexEntry cold ({} series)", numSeries).c_str());

            // Warm lookups (same seed → same IDs)
            rng4.seed(77);
            LatStats warm;
            for (int i = 0; i < lookups; ++i) {
                auto t0 = clk::now();
                co_await tsm.getFullIndexEntry(seriesIds[dist4(rng4)]);
                warm.add(clk::now() - t0);
            }
            warm.print(fmt::format("getFullIndexEntry warm ({} series)", numSeries).c_str());

            co_await tsm.close();
        }

        fs::remove(filename);
    }

    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B5: Multi-series query I/O (per-series DMA reads)
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B5_MultiSeriesQueryIO) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B5: Multi-Series Query I/O (per-series DMA reads)             ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    std::string tsmDir = "shard_0/tsm";

    // Create TSM with many series
    constexpr int SERIES_COUNT = 500;
    constexpr size_t PTS_PER_SERIES = 10000;
    std::string filename = tsmDir + "/0_1.tsm";

    {
        TSMWriter writer(filename);
        std::mt19937 rng5(42);
        std::uniform_real_distribution<double> dist5(0.0, 100.0);

        for (int s = 0; s < SERIES_COUNT; ++s) {
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            timestamps.reserve(PTS_PER_SERIES);
            values.reserve(PTS_PER_SERIES);

            uint64_t baseTs = 1700000000000000000ULL;
            for (size_t i = 0; i < PTS_PER_SERIES; ++i) {
                timestamps.push_back(baseTs + i * 1000000000ULL);
                values.push_back(dist5(rng5));
            }

            std::string key = fmt::format("query_bench,host=server_{:04d} value", s);
            SeriesId128 sid = SeriesId128::fromSeriesKey(key);
            writer.writeSeries(TSMValueType::Float, sid, timestamps, values);
        }
        writer.writeIndex();
        writer.close();
    }

    fmt::print("  Created TSM: {} series x {} pts\n", SERIES_COUNT, PTS_PER_SERIES);

    TSM tsm(filename);
    co_await tsm.open();
    auto allIds = tsm.getSeriesIds();

    // Benchmark: sequential getFullIndexEntry for N series (simulates scatter-gather query)
    for (int queryWidth : {1, 10, 50, 100, 250, 500}) {
        int n = std::min(queryWidth, static_cast<int>(allIds.size()));

        LatStats stats;
        auto t0 = clk::now();
        for (int i = 0; i < n; ++i) {
            auto ti = clk::now();
            co_await tsm.getFullIndexEntry(allIds[i]);
            stats.add(clk::now() - ti);
        }
        double totalMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();

        fmt::print("  {:4d}-series index load: {:8.2f} ms total  ({:.2f} ms/series)\n",
                   n, totalMs, totalMs / n);
    }

    // Benchmark: readSeries for N series (full data read)
    for (int queryWidth : {1, 10, 50, 100}) {
        int n = std::min(queryWidth, static_cast<int>(allIds.size()));

        auto t0 = clk::now();
        size_t totalPts = 0;
        for (int i = 0; i < n; ++i) {
            TSMResult<double> result(0);
            co_await tsm.readSeries(allIds[i], 0, UINT64_MAX, result);
            for (size_t b = 0; b < result.blocks.size(); ++b) {
                totalPts += result.blocks[b]->size();
            }
        }
        double totalMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();

        fmt::print("  {:4d}-series data read:  {:8.2f} ms total  ({} pts, {:.0f} pts/ms)\n",
                   n, totalMs, totalPts, totalPts / totalMs);
    }

    co_await tsm.close();
    fs::remove(filename);
    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B6: Compactor dedup algorithm (set vs unordered_set)
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B6_CompactorDedupAlgorithm) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B6: Compactor Dedup Algorithm (std::set vs unordered_set)     ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    // Simulate getAllSeriesIds with overlap across files
    std::mt19937_64 rng6(42);

    for (int totalUnique : {1000, 10000, 50000, 100000}) {
        // Generate series IDs (some overlap to simulate multi-file dedup)
        std::vector<SeriesId128> allIds;
        allIds.reserve(totalUnique * 2);  // ~2x with duplicates

        for (int i = 0; i < totalUnique; ++i) {
            std::string key = fmt::format("metrics,host=server_{:06d},region=us-east value", i);
            allIds.push_back(SeriesId128::fromSeriesKey(key));
        }
        // Add 50% duplicates (simulating 2 overlapping files)
        for (int i = 0; i < totalUnique / 2; ++i) {
            allIds.push_back(allIds[i]);
        }

        std::shuffle(allIds.begin(), allIds.end(), rng6);

        // Benchmark: std::set (current implementation)
        {
            auto t0 = clk::now();
            std::set<SeriesId128> uniqueSet(allIds.begin(), allIds.end());
            double ms = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
            fmt::print("  {:7d} IDs → std::set:          {:8.2f} ms  ({} unique)\n",
                       static_cast<int>(allIds.size()), ms, uniqueSet.size());
            EXPECT_EQ(uniqueSet.size(), static_cast<size_t>(totalUnique));
        }

        // Benchmark: std::unordered_set (proposed fix)
        {
            auto t0 = clk::now();
            std::unordered_set<SeriesId128, SeriesId128::Hash> uniqueSet(allIds.begin(), allIds.end());
            double ms = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
            fmt::print("  {:7d} IDs → unordered_set:     {:8.2f} ms  ({} unique)\n",
                       static_cast<int>(allIds.size()), ms, uniqueSet.size());
            EXPECT_EQ(uniqueSet.size(), static_cast<size_t>(totalUnique));
        }
    }

    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B7: NativeIndex high-cardinality cache pressure
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B7_NativeIndexHighCardinality) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B7: NativeIndex High-Cardinality Cache Pressure               ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    NativeIndex index(0);
    co_await index.open();

    // Insert many measurements with many unique tag values to stress caches
    constexpr int MEASUREMENTS = 50;
    constexpr int HOSTS_PER_MEAS = 100;
    constexpr int FIELDS = 4;
    int totalSeries = 0;

    auto insertStart = clk::now();
    for (int m = 0; m < MEASUREMENTS; ++m) {
        std::string meas = fmt::format("hc_meas_{:03d}", m);
        for (int h = 0; h < HOSTS_PER_MEAS; ++h) {
            std::map<std::string, std::string> tags = {
                {"host", fmt::format("hchost-{:04d}", h)},
                {"dc", fmt::format("dc-{:02d}", h % 10)},
            };
            for (int f = 0; f < FIELDS; ++f) {
                co_await index.getOrCreateSeriesId(meas, tags, fmt::format("f_{}", f));
                ++totalSeries;
            }
        }
    }
    double insertMs = std::chrono::duration<double, std::milli>(clk::now() - insertStart).count();
    fmt::print("  Inserted {} series across {} measurements: {:.2f} ms ({:.0f} series/sec)\n",
               totalSeries, MEASUREMENTS, insertMs, (totalSeries * 1000.0) / insertMs);

    // Measure schema metadata lookups (tests fieldsCache_, tagsCache_)
    {
        LatStats fieldStats, tagStats, tagValStats;
        for (int m = 0; m < MEASUREMENTS; ++m) {
            std::string meas = fmt::format("hc_meas_{:03d}", m);

            auto t0 = clk::now();
            co_await index.getFields(meas);
            fieldStats.add(clk::now() - t0);

            t0 = clk::now();
            co_await index.getTags(meas);
            tagStats.add(clk::now() - t0);

            t0 = clk::now();
            co_await index.getTagValues(meas, "host");
            tagValStats.add(clk::now() - t0);
        }
        fieldStats.print("getFields (50 measurements)");
        tagStats.print("getTags (50 measurements)");
        tagValStats.print("getTagValues host (50 measurements, 100 vals each)");
    }

    // Measure discovery with many measurements
    {
        LatStats discoveryStats;
        for (int m = 0; m < MEASUREMENTS; ++m) {
            std::string meas = fmt::format("hc_meas_{:03d}", m);
            auto t0 = clk::now();
            co_await index.findSeriesWithMetadata(meas, {{"dc", "dc-00"}}, {});
            discoveryStats.add(clk::now() - t0);
        }
        discoveryStats.print("findSeriesWithMetadata (dc filter, 50 meas)");
    }

    fmt::print("  Cache sizes:\n");
    fmt::print("    Series dedup:   {} entries\n", index.getSeriesCacheSize());
    fmt::print("    Metadata cache: {} entries ({:.1f} MB)\n",
               index.getMetadataCacheSize(),
               static_cast<double>(index.getMetadataCacheBytes()) / (1024.0 * 1024.0));

    co_await index.close();
    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B8: Engine insert path (SeriesId roundtrip + timestamp copies)
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B8_EngineInsertPath) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B8: Engine Insert Path (SeriesId roundtrip + alloc overhead)  ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    seastar::thread([] {
        cleanTestShardDirectories();
        ScopedEngine eng;
        eng.init();

        constexpr int SERIES = 100;
        constexpr int PTS_PER_INSERT = 100;
        constexpr int INSERTS = 500;

        std::mt19937 rng7(42);
        std::uniform_real_distribution<double> vdist(0.0, 100.0);

        // Pre-build inserts
        std::vector<TimeStarInsert<double>> inserts;
        inserts.reserve(INSERTS);
        for (int i = 0; i < INSERTS; ++i) {
            int seriesIdx = i % SERIES;
            TimeStarInsert<double> ins(fmt::format("engine_bench_{}", seriesIdx / 4), fmt::format("f_{}", seriesIdx % 4));
            ins.tags = {
                {"host", hostName(seriesIdx)},
                {"region", (seriesIdx < SERIES / 2) ? "us-east" : "us-west"}
            };
            uint64_t baseTs = 1700000000000000000ULL + static_cast<uint64_t>(i) * 1000000000ULL;
            ins.timestamps.reserve(PTS_PER_INSERT);
            ins.values.reserve(PTS_PER_INSERT);
            for (int p = 0; p < PTS_PER_INSERT; ++p) {
                ins.timestamps.push_back(baseTs + static_cast<uint64_t>(p) * 1000000ULL);
                ins.values.push_back(vdist(rng7));
            }
            inserts.push_back(std::move(ins));
        }

        // Benchmark
        std::vector<double> latencies;
        latencies.reserve(INSERTS);

        auto totalStart = clk::now();
        for (int i = 0; i < INSERTS; ++i) {
            auto t0 = clk::now();
            eng->insert(inserts[i]).get();
            auto t1 = clk::now();
            latencies.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
        }
        double totalMs = std::chrono::duration<double, std::milli>(clk::now() - totalStart).count();

        std::sort(latencies.begin(), latencies.end());
        size_t totalPts = static_cast<size_t>(INSERTS) * PTS_PER_INSERT;

        fmt::print("  {} inserts x {} pts = {} total points\n", INSERTS, PTS_PER_INSERT, totalPts);
        fmt::print("  Total:   {:8.2f} ms  ({:.0f} pts/sec)\n", totalMs, (totalPts * 1000.0) / totalMs);
        fmt::print("  Latency: min={:.3f}  avg={:.3f}  p50={:.3f}  p95={:.3f}  p99={:.3f}  max={:.3f} ms\n",
                   latencies.front(),
                   totalMs / INSERTS,
                   latencies[latencies.size() / 2],
                   latencies[static_cast<size_t>(latencies.size() * 0.95)],
                   latencies[static_cast<size_t>(latencies.size() * 0.99)],
                   latencies.back());
    }).join().get();

    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B9: Query parser allocation overhead
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B9_QueryParserAllocation) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B9: Query Parser Allocation Overhead                          ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    // Test queries of increasing complexity
    std::vector<std::pair<std::string, std::string>> queries = {
        {"simple",   "avg:temperature()"},
        {"1-field",  "max:cpu(usage_percent)"},
        {"1-scope",  "avg:cpu(usage){host:server-01}"},
        {"3-scopes", "avg:cpu(usage,load,latency){host:server-01,dc:us-east,env:prod}"},
        {"group-by", "avg:cpu(usage){host:server-01,dc:us-east} by {host,dc}"},
        {"complex",  "stddev:metrics(p50,p90,p99,max,count){host:server-01,rack:rack-01,region:us-east,env:production} by {host,rack,region}"},
    };

    constexpr int ITERATIONS = 10000;

    for (const auto& [label, queryStr] : queries) {
        auto t0 = clk::now();
        for (int i = 0; i < ITERATIONS; ++i) {
            auto result = timestar::QueryParser::parseQueryString(queryStr);
        }
        double totalMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
        double perParse = totalMs / ITERATIONS;

        fmt::print("  {:12s}  {:8.2f} ms total  ({:.4f} ms/parse, {:.0f} parses/sec)\n",
                   label, totalMs, perParse, (ITERATIONS * 1000.0) / totalMs);
    }

    fmt::print("\n");
    co_return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  B10: TSM writer flush latency (reactor stall risk)
// ═══════════════════════════════════════════════════════════════════════════

SEASTAR_TEST_F(OptimizationBaseline, B10_TSMWriterFlushLatency) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  B10: TSM Writer Flush Latency (reactor stall measurement)     ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    std::string tsmDir = "shard_0/tsm";
    std::mt19937 rng8(42);
    std::uniform_real_distribution<double> dist8(0.0, 100.0);

    // Measure write latency for increasing series counts
    for (auto [numSeries, ptsPerSeries] : std::vector<std::pair<int,int>>{{10, 10000}, {50, 10000}, {100, 10000}, {500, 10000}}) {
        std::string filename = tsmDir + fmt::format("/flush_{}.tsm", numSeries);

        auto t0 = clk::now();

        TSMWriter writer(filename);

        LatStats perSeriesStats;
        for (int s = 0; s < numSeries; ++s) {
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            timestamps.reserve(ptsPerSeries);
            values.reserve(ptsPerSeries);

            uint64_t baseTs = 1700000000000000000ULL + static_cast<uint64_t>(s) * 100000000000ULL;
            for (int i = 0; i < ptsPerSeries; ++i) {
                timestamps.push_back(baseTs + static_cast<uint64_t>(i) * 1000000ULL);
                values.push_back(dist8(rng8));
            }

            std::string key = fmt::format("flush_bench,host=server_{:04d} value", s);
            SeriesId128 sid = SeriesId128::fromSeriesKey(key);

            auto ts = clk::now();
            writer.writeSeries(TSMValueType::Float, sid, timestamps, values);
            perSeriesStats.add(clk::now() - ts);
        }

        auto indexStart = clk::now();
        writer.writeIndex();
        double indexMs = std::chrono::duration<double, std::milli>(clk::now() - indexStart).count();

        auto closeStart = clk::now();
        writer.close();
        double closeMs = std::chrono::duration<double, std::milli>(clk::now() - closeStart).count();

        double totalMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
        size_t fileSize = fs::file_size(filename);

        fmt::print("  {:4d} series x {:5d} pts:  total={:8.2f} ms  index={:.2f} ms  close={:.2f} ms  ({:.1f} MB)\n",
                   numSeries, ptsPerSeries, totalMs, indexMs, closeMs, fileSize / (1024.0 * 1024.0));
        perSeriesStats.print(fmt::format("  writeSeries ({} series)", numSeries).c_str());

        fs::remove(filename);
    }

    fmt::print("  (Large per-series max latency indicates reactor stall risk)\n\n");
    co_return;
}
