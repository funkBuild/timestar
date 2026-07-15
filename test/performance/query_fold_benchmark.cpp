/*
 * Query fold / SIMD kernel micro-benchmarks
 *
 * Gates for the July 2026 query-path performance review.
 * Run with: --gtest_filter='QueryFoldBench*'
 *
 *   Q1 — BlockAggregator::addPointsRange multi-bucket fold (memory-store path)
 *   Q2 — BlockAggregator::addPoints scalar fold, AVG (per-point divide)
 *   Q3 — SimdAggregator min/max on NaN-free data (pre-pass cost + unroll)
 *   Q4 — BlockAggregator STDDEV fold (scalar Welford vs SIMD two-pass)
 *   Q5 — BlockAggregator::addTimestampsOnly multi-bucket (COUNT path)
 *
 * These are pure CPU benchmarks (no I/O, no Seastar reactor required).
 */

#include "../../lib/query/block_aggregator.hpp"
#include "../../lib/query/simd_aggregator.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <cstdint>
#include <fmt/core.h>
#include <random>
#include <vector>

using clk = std::chrono::high_resolution_clock;
using timestar::AggregationMethod;
using timestar::AggregationState;
using timestar::BlockAggregator;

namespace {

// Deterministic test data: N points, 1s spacing, sinusoid-ish values.
struct TestData {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    explicit TestData(size_t n, uint64_t startNs = 1'000'000'000'000'000'000ULL,
                      uint64_t stepNs = 1'000'000'000ULL) {
        timestamps.resize(n);
        values.resize(n);
        std::mt19937_64 rng(42);
        std::uniform_real_distribution<double> jitter(-1.0, 1.0);
        for (size_t i = 0; i < n; ++i) {
            timestamps[i] = startNs + i * stepNs;
            values[i] = 50.0 + 20.0 * std::sin(static_cast<double>(i) * 0.001) + jitter(rng);
        }
    }
};

double toMs(clk::duration d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

// Best-of-R timing to suppress scheduler noise.
template <typename F>
double bestOfMs(int reps, F&& fn) {
    double best = 1e100;
    for (int r = 0; r < reps; ++r) {
        auto t0 = clk::now();
        fn();
        best = std::min(best, toMs(clk::now() - t0));
    }
    return best;
}

}  // namespace

// Q1 — multi-bucket fold through addPointsRange (the MemoryStore pushdown path).
// 1M points, 1s spacing, 5m buckets => ~3334 buckets, ~300 points per bucket.
TEST(QueryFoldBench, Q1_AddPointsRangeMultiBucket) {
    constexpr size_t N = 1'000'000;
    constexpr uint64_t INTERVAL = 300'000'000'000ULL;  // 5 minutes
    TestData data(N);
    const uint64_t start = data.timestamps.front();
    const uint64_t end = data.timestamps.back() + 1;

    double sumCheck = 0.0;
    double ms = bestOfMs(7, [&] {
        BlockAggregator agg(INTERVAL, start, end, AggregationMethod::AVG, true);
        agg.addPointsRange(data.timestamps, data.values, 0, N);
        auto states = agg.takeBucketStates();
        double s = 0.0;
        for (auto& [k, st] : states)
            s += st.sum;
        sumCheck = s;
    });
    fmt::print("[Q1] addPointsRange multi-bucket AVG: {:.3f} ms for {} pts ({:.1f} Mpts/s), checksum={:.3f}\n", ms, N,
               N / ms / 1000.0, sumCheck);
    SUCCEED();
}

// Q2 — single-bucket scalar fold path via addPoint (per-point divide visibility).
// Uses methodAware AVG with points added one at a time (scalar addValueForMethod).
TEST(QueryFoldBench, Q2_ScalarFoldAvgPerPoint) {
    constexpr size_t N = 1'000'000;
    TestData data(N);

    double sumCheck = 0.0;
    double ms = bestOfMs(7, [&] {
        BlockAggregator agg(0, AggregationMethod::AVG);
        agg.enableFoldToSingleState();
        for (size_t i = 0; i < N; ++i) {
            agg.addPoint(data.timestamps[i], data.values[i]);
        }
        sumCheck = agg.takeSingleState().sum;
    });
    fmt::print("[Q2] scalar per-point AVG fold: {:.3f} ms for {} pts ({:.1f} Mpts/s), checksum={:.3f}\n", ms, N,
               N / ms / 1000.0, sumCheck);
    SUCCEED();
}

// Q3 — SIMD min/max kernels on NaN-free data (measures the containsNaN pre-pass
// plus the reduction itself), and sum for reference.
TEST(QueryFoldBench, Q3_SimdMinMaxSum) {
    constexpr size_t N = 4'000'000;
    TestData data(N);
    const double* v = data.values.data();

    volatile double sink;
    double msSum = bestOfMs(9, [&] { sink = timestar::simd::SimdAggregator::calculateSum(v, N); });
    double msMin = bestOfMs(9, [&] { sink = timestar::simd::SimdAggregator::calculateMin(v, N); });
    double msMax = bestOfMs(9, [&] { sink = timestar::simd::SimdAggregator::calculateMax(v, N); });
    (void)sink;
    fmt::print("[Q3] SIMD kernels, {} doubles: sum={:.3f} ms ({:.1f} GB/s)  min={:.3f} ms ({:.1f} GB/s)  max={:.3f} "
               "ms ({:.1f} GB/s)\n",
               N, msSum, N * 8.0 / msSum / 1e6, msMin, N * 8.0 / msMin / 1e6, msMax, N * 8.0 / msMax / 1e6);
    SUCCEED();
}

// Q4 — STDDEV fold through the BlockAggregator batch API (currently scalar
// Welford per point; candidate for SIMD two-pass).
TEST(QueryFoldBench, Q4_StddevBatchFold) {
    constexpr size_t N = 1'000'000;
    constexpr size_t BLOCK = 1000;  // fold in TSM-block-sized batches
    TestData data(N);

    double stddevCheck = 0.0;
    double ms = bestOfMs(7, [&] {
        BlockAggregator agg(0, AggregationMethod::STDDEV);
        agg.enableFoldToSingleState();
        for (size_t off = 0; off < N; off += BLOCK) {
            size_t len = std::min(BLOCK, N - off);
            agg.addPointsRange(data.timestamps, data.values, off, off + len);
        }
        auto st = agg.takeSingleState();
        stddevCheck = st.getValue(AggregationMethod::STDDEV);
    });
    fmt::print("[Q4] STDDEV batch fold: {:.3f} ms for {} pts ({:.1f} Mpts/s), stddev={:.6f}\n", ms, N, N / ms / 1000.0,
               stddevCheck);
    SUCCEED();
}

// Q5 — COUNT-only timestamps into multi-bucket states.
TEST(QueryFoldBench, Q5_AddTimestampsOnlyMultiBucket) {
    constexpr size_t N = 1'000'000;
    constexpr uint64_t INTERVAL = 300'000'000'000ULL;  // 5 minutes
    TestData data(N);
    const uint64_t start = data.timestamps.front();
    const uint64_t end = data.timestamps.back() + 1;

    uint64_t countCheck = 0;
    double ms = bestOfMs(7, [&] {
        BlockAggregator agg(INTERVAL, start, end, AggregationMethod::COUNT, true);
        agg.addTimestampsOnly(data.timestamps);
        auto states = agg.takeBucketStates();
        uint64_t c = 0;
        for (auto& [k, st] : states)
            c += st.count;
        countCheck = c;
    });
    fmt::print("[Q5] addTimestampsOnly multi-bucket COUNT: {:.3f} ms for {} pts ({:.1f} Mpts/s), count={}\n", ms, N,
               N / ms / 1000.0, countCheck);
    EXPECT_EQ(countCheck, N);
}
