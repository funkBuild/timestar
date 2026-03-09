/**
 * Anomaly Detection Benchmark
 *
 * Measures performance of anomaly detection algorithms with and without SIMD.
 * Run with SIMD: ./anomaly_detection_benchmark
 * Run without SIMD: Rebuild with -DTIMESTAR_ANOMALY_DISABLE_SIMD=1
 */

#include <benchmark/benchmark.h>
#include "anomaly_result.hpp"
#include "anomaly_detector.hpp"
#include "basic_detector.hpp"
#include "robust_detector.hpp"
#include "agile_detector.hpp"
#include "stl_decomposition.hpp"
#include "simd_anomaly.hpp"

#include <vector>
#include <random>
#include <cmath>

using namespace timestar::anomaly;

// ==================== Data Generators ====================

static std::vector<uint64_t> generateTimestamps(size_t count) {
    std::vector<uint64_t> timestamps(count);
    uint64_t startNs = 1704067200000000000ULL;  // Jan 1, 2024
    uint64_t intervalNs = 60000000000ULL;       // 1 minute
    for (size_t i = 0; i < count; ++i) {
        timestamps[i] = startNs + i * intervalNs;
    }
    return timestamps;
}

static std::vector<double> generateConstantWithNoise(size_t count, double value, double noise) {
    std::vector<double> values(count);
    std::mt19937 gen(42);
    std::normal_distribution<> dist(0.0, noise);
    for (size_t i = 0; i < count; ++i) {
        values[i] = value + dist(gen);
    }
    return values;
}

static std::vector<double> generateSinusoidal(size_t count, double baseline, double amplitude, size_t period) {
    std::vector<double> values(count);
    std::mt19937 gen(42);
    std::normal_distribution<> dist(0.0, amplitude * 0.1);
    for (size_t i = 0; i < count; ++i) {
        values[i] = baseline + amplitude * std::sin(2.0 * M_PI * i / period) + dist(gen);
    }
    return values;
}

static std::vector<double> generateLevelShift(size_t count, double level1, double level2, double noise) {
    std::vector<double> values(count);
    std::mt19937 gen(42);
    std::normal_distribution<> dist(0.0, noise);
    for (size_t i = 0; i < count; ++i) {
        double level = (i < count / 2) ? level1 : level2;
        values[i] = level + dist(gen);
    }
    return values;
}

// ==================== SIMD Primitives Benchmarks ====================

static void BM_VectorSum_Scalar(benchmark::State& state) {
    size_t count = state.range(0);
    auto values = generateConstantWithNoise(count, 100.0, 10.0);

    for (auto _ : state) {
        double sum = 0.0;
        for (size_t i = 0; i < count; ++i) {
            sum += values[i];
        }
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * count);
}

static void BM_VectorSum_SIMD(benchmark::State& state) {
    size_t count = state.range(0);
    auto values = generateConstantWithNoise(count, 100.0, 10.0);

    for (auto _ : state) {
        double sum = simd::vectorSum(values.data(), count);
        benchmark::DoNotOptimize(sum);
    }
    state.SetItemsProcessed(state.iterations() * count);
}

static void BM_VectorVariance_Scalar(benchmark::State& state) {
    size_t count = state.range(0);
    auto values = generateConstantWithNoise(count, 100.0, 10.0);
    double mean = simd::vectorMean(values.data(), count);

    for (auto _ : state) {
        double sumSq = 0.0;
        for (size_t i = 0; i < count; ++i) {
            double diff = values[i] - mean;
            sumSq += diff * diff;
        }
        double variance = sumSq / (count - 1);
        benchmark::DoNotOptimize(variance);
    }
    state.SetItemsProcessed(state.iterations() * count);
}

static void BM_VectorVariance_SIMD(benchmark::State& state) {
    size_t count = state.range(0);
    auto values = generateConstantWithNoise(count, 100.0, 10.0);
    double mean = simd::vectorMean(values.data(), count);

    for (auto _ : state) {
        double variance = simd::vectorVariance(values.data(), count, mean);
        benchmark::DoNotOptimize(variance);
    }
    state.SetItemsProcessed(state.iterations() * count);
}

static void BM_ComputeBounds_Scalar(benchmark::State& state) {
    size_t count = state.range(0);
    auto predictions = generateConstantWithNoise(count, 100.0, 5.0);
    auto scale = generateConstantWithNoise(count, 10.0, 1.0);
    std::vector<double> upper(count), lower(count);
    double bounds = 2.0;

    for (auto _ : state) {
        for (size_t i = 0; i < count; ++i) {
            double margin = bounds * scale[i];
            upper[i] = predictions[i] + margin;
            lower[i] = predictions[i] - margin;
        }
        benchmark::DoNotOptimize(upper.data());
        benchmark::DoNotOptimize(lower.data());
    }
    state.SetItemsProcessed(state.iterations() * count);
}

static void BM_ComputeBounds_SIMD(benchmark::State& state) {
    size_t count = state.range(0);
    auto predictions = generateConstantWithNoise(count, 100.0, 5.0);
    auto scale = generateConstantWithNoise(count, 10.0, 1.0);
    std::vector<double> upper(count), lower(count);
    double bounds = 2.0;

    for (auto _ : state) {
        simd::computeBounds(predictions.data(), scale.data(), bounds,
                           upper.data(), lower.data(), count);
        benchmark::DoNotOptimize(upper.data());
        benchmark::DoNotOptimize(lower.data());
    }
    state.SetItemsProcessed(state.iterations() * count);
}

// ==================== Algorithm Benchmarks ====================

static void BM_BasicDetector(benchmark::State& state) {
    size_t count = state.range(0);
    auto timestamps = generateTimestamps(count);
    auto values = generateConstantWithNoise(count, 100.0, 10.0);

    // Insert some anomalies
    values[count / 4] = 200.0;
    values[count / 2] = 0.0;
    values[3 * count / 4] = 250.0;

    AnomalyInput input;
    input.timestamps = timestamps;
    input.values = values;

    AnomalyConfig config;
    config.bounds = 2.0;
    config.windowSize = 60;
    config.minDataPoints = 20;

    BasicDetector detector;

    for (auto _ : state) {
        auto output = detector.detect(input, config);
        benchmark::DoNotOptimize(output);
    }

    state.SetItemsProcessed(state.iterations() * count);
    state.counters["points"] = count;
}

static void BM_AgileDetector(benchmark::State& state) {
    size_t count = state.range(0);
    auto timestamps = generateTimestamps(count);
    auto values = generateLevelShift(count, 100.0, 150.0, 5.0);

    AnomalyInput input;
    input.timestamps = timestamps;
    input.values = values;

    AnomalyConfig config;
    config.bounds = 2.0;
    config.windowSize = 60;
    config.minDataPoints = 20;

    AgileDetector detector;

    for (auto _ : state) {
        auto output = detector.detect(input, config);
        benchmark::DoNotOptimize(output);
    }

    state.SetItemsProcessed(state.iterations() * count);
    state.counters["points"] = count;
}

static void BM_RobustDetector(benchmark::State& state) {
    size_t count = state.range(0);
    auto timestamps = generateTimestamps(count);
    auto values = generateSinusoidal(count, 100.0, 20.0, 60);

    AnomalyInput input;
    input.timestamps = timestamps;
    input.values = values;

    AnomalyConfig config;
    config.bounds = 2.0;
    config.seasonality = Seasonality::HOURLY;

    RobustDetector detector;

    for (auto _ : state) {
        auto output = detector.detect(input, config);
        benchmark::DoNotOptimize(output);
    }

    state.SetItemsProcessed(state.iterations() * count);
    state.counters["points"] = count;
}

static void BM_STLDecomposition(benchmark::State& state) {
    size_t count = state.range(0);
    auto values = generateSinusoidal(count, 100.0, 20.0, 60);

    STLConfig config;
    config.seasonalPeriod = 60;
    config.seasonalWindow = 7;
    config.robust = true;

    for (auto _ : state) {
        auto components = STLDecomposition::decompose(values, config);
        benchmark::DoNotOptimize(components);
    }

    state.SetItemsProcessed(state.iterations() * count);
    state.counters["points"] = count;
}

// ==================== Incremental vs Batch Rolling Stats ====================

static void BM_RollingStats_Batch(benchmark::State& state) {
    size_t count = state.range(0);
    size_t windowSize = 60;
    auto values = generateConstantWithNoise(count, 100.0, 10.0);

    for (auto _ : state) {
        for (size_t i = windowSize; i < count; ++i) {
            // Recalculate from scratch each time (old O(N*W) approach)
            size_t start = i - windowSize;
            double sum = 0.0;
            for (size_t j = start; j < i; ++j) {
                sum += values[j];
            }
            double mean = sum / windowSize;

            double sumSq = 0.0;
            for (size_t j = start; j < i; ++j) {
                double diff = values[j] - mean;
                sumSq += diff * diff;
            }
            double stddev = std::sqrt(sumSq / (windowSize - 1));
            benchmark::DoNotOptimize(mean);
            benchmark::DoNotOptimize(stddev);
        }
    }

    state.SetItemsProcessed(state.iterations() * count);
}

static void BM_RollingStats_Incremental(benchmark::State& state) {
    size_t count = state.range(0);
    size_t windowSize = 60;
    auto values = generateConstantWithNoise(count, 100.0, 10.0);

    for (auto _ : state) {
        simd::IncrementalRollingStats stats(windowSize);
        for (size_t i = 0; i < count; ++i) {
            stats.update(values[i]);
            if (i >= windowSize) {
                double mean = stats.mean();
                double stddev = stats.stddev();
                benchmark::DoNotOptimize(mean);
                benchmark::DoNotOptimize(stddev);
            }
        }
    }

    state.SetItemsProcessed(state.iterations() * count);
}

// ==================== Register Benchmarks ====================

// SIMD primitives
BENCHMARK(BM_VectorSum_Scalar)->Range(64, 65536);
BENCHMARK(BM_VectorSum_SIMD)->Range(64, 65536);
BENCHMARK(BM_VectorVariance_Scalar)->Range(64, 65536);
BENCHMARK(BM_VectorVariance_SIMD)->Range(64, 65536);
BENCHMARK(BM_ComputeBounds_Scalar)->Range(64, 65536);
BENCHMARK(BM_ComputeBounds_SIMD)->Range(64, 65536);

// Rolling stats comparison
BENCHMARK(BM_RollingStats_Batch)->Range(1000, 100000);
BENCHMARK(BM_RollingStats_Incremental)->Range(1000, 100000);

// Full algorithm benchmarks
BENCHMARK(BM_BasicDetector)->Range(100, 100000);
BENCHMARK(BM_AgileDetector)->Range(100, 100000);
BENCHMARK(BM_RobustDetector)->Range(100, 10000);  // Smaller range as STL is slower
BENCHMARK(BM_STLDecomposition)->Range(100, 10000);

BENCHMARK_MAIN();
