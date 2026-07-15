/*
 * AnomalyExecutor micro-benchmarks
 *
 * Gates for the July 2026 anomaly-path performance review (Scope B):
 *   B1 — AnomalyExecutor::execute end-to-end copies (input spans, output moves)
 *   B2 — STL decomposition dead-x / per-iteration copies, robust scalar-scale bounds
 *
 * Times AnomalyExecutor::execute (basic + robust detectors) on 1M points.
 * Pure CPU benchmarks (no I/O, no Seastar reactor required).
 *
 * Run with: --gtest_filter='AnomalyExecBench*'
 */

#include "../../lib/query/anomaly/anomaly_executor.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <cstdint>
#include <fmt/core.h>
#include <random>
#include <vector>

using clk = std::chrono::high_resolution_clock;
using namespace timestar::anomaly;

namespace {

constexpr size_t kPoints = 1'000'000;

struct BenchData {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::vector<std::string> groupTags;

    explicit BenchData(size_t n) {
        timestamps.resize(n);
        values.resize(n);
        const uint64_t startNs = 1704067200000000000ULL;  // Jan 1, 2024
        const uint64_t stepNs = 60000000000ULL;           // 1 minute
        std::mt19937_64 rng(42);
        std::normal_distribution<double> noise(0.0, 5.0);
        for (size_t i = 0; i < n; ++i) {
            timestamps[i] = startNs + i * stepNs;
            values[i] = 100.0 + 20.0 * std::sin(2.0 * M_PI * static_cast<double>(i) / 60.0) + noise(rng);
        }
        // Sprinkle anomalies
        for (size_t i = n / 10; i < n; i += n / 10) {
            values[i] = 400.0;
        }
        groupTags = {"host:bench-01"};
    }
};

double toMs(clk::duration d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

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

TEST(AnomalyExecBench, BasicDetector1M) {
    BenchData data(kPoints);
    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;
    config.windowSize = 60;
    config.minDataPoints = 20;

    AnomalyExecutor executor;
    size_t pieces = 0;
    double ms = bestOfMs(5, [&] {
        auto result = executor.execute(data.timestamps, data.values, data.groupTags, config);
        ASSERT_TRUE(result.success);
        pieces = result.series.size();
    });
    fmt::print("[AnomalyExecBench] basic  1M points: {:.2f} ms ({} pieces), {:.1f} Mpts/s\n", ms, pieces,
               static_cast<double>(kPoints) / (ms * 1000.0));
}

TEST(AnomalyExecBench, RobustDetector1M) {
    BenchData data(kPoints);
    AnomalyConfig config;
    config.algorithm = Algorithm::ROBUST;
    config.bounds = 2.0;
    config.seasonality = Seasonality::HOURLY;

    AnomalyExecutor executor;
    size_t pieces = 0;
    double ms = bestOfMs(3, [&] {
        auto result = executor.execute(data.timestamps, data.values, data.groupTags, config);
        ASSERT_TRUE(result.success);
        pieces = result.series.size();
    });
    fmt::print("[AnomalyExecBench] robust 1M points: {:.2f} ms ({} pieces), {:.1f} Mpts/s\n", ms, pieces,
               static_cast<double>(kPoints) / (ms * 1000.0));
}
