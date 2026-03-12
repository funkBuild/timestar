// forecast_benchmark.cpp
// Micro-benchmark for all forecast code paths using Google Benchmark.
//
// Build: add_executable(forecast_benchmark forecast_benchmark.cpp)
//        target_link_libraries(forecast_benchmark libtimestar benchmark::benchmark)
//
// Run:   ./bin/forecast_benchmark \
//            --benchmark_repetitions=5 \
//            --benchmark_report_aggregates_only=true \
//            --benchmark_display_aggregates_only=true

#include "forecast/forecast_executor.hpp"
#include "forecast/forecast_result.hpp"
#include "forecast/linear_forecaster.hpp"
#include "forecast/periodicity_detector.hpp"
#include "forecast/seasonal_forecaster.hpp"
#include "forecast/stl_decomposition.hpp"

#include <benchmark/benchmark.h>

#include <cmath>
#include <cstdlib>
#include <numeric>
#include <vector>

using namespace timestar::forecast;

// ─────────────────────────────── constants ───────────────────────────────────

// 1 year of 5-minute data ≈ 105,120 points
static constexpr size_t N_YEAR = 105'120;
// 1 month of 5-minute data ≈ 8,640 points
static constexpr size_t N_MONTH = 8'640;
// 1 week of 5-minute data ≈ 2,016 points
static constexpr size_t N_WEEK = 2'016;
// Interval: 5 minutes in nanoseconds
static constexpr uint64_t INTERVAL = 300'000'000'000ULL;

// ──────────────────────────── global test data ───────────────────────────────

// Realistic time series: trend + daily seasonality + weekly seasonality + noise
static std::vector<uint64_t> g_timestamps_year;
static std::vector<double> g_values_year;
static std::vector<uint64_t> g_timestamps_month;
static std::vector<double> g_values_month;
static std::vector<uint64_t> g_timestamps_week;
static std::vector<double> g_values_week;

struct GlobalSetup {
    GlobalSetup() {
        auto generate = [](size_t n, std::vector<uint64_t>& ts, std::vector<double>& vs) {
            ts.resize(n);
            vs.resize(n);
            std::srand(42);
            constexpr double PI2 = 2.0 * 3.14159265358979323846;
            // Points per day at 5-min intervals = 288
            // Points per week = 2016
            for (size_t i = 0; i < n; ++i) {
                ts[i] = static_cast<uint64_t>(i) * INTERVAL;
                double trend = 50.0 + 0.001 * static_cast<double>(i);
                double daily = 10.0 * std::sin(PI2 * static_cast<double>(i) / 288.0);
                double weekly = 5.0 * std::sin(PI2 * static_cast<double>(i) / 2016.0);
                double noise = 0.5 * (static_cast<double>(std::rand() % 1000) / 1000.0 - 0.5);
                vs[i] = trend + daily + weekly + noise;
            }
        };
        generate(N_YEAR, g_timestamps_year, g_values_year);
        generate(N_MONTH, g_timestamps_month, g_values_month);
        generate(N_WEEK, g_timestamps_week, g_values_week);
    }
};
static GlobalSetup g_setup;

// ─────────────────── helpers ──────────────────────────────────────────────

static ForecastInput makeInput(const std::vector<uint64_t>& ts, const std::vector<double>& vs) {
    ForecastInput input;
    input.timestamps = ts;
    input.values = vs;
    return input;
}

// ═══════════════════════════════════════════════════════════════════════════
// STL Decomposition benchmarks  (the dominant cost centre)
// ═══════════════════════════════════════════════════════════════════════════

static void BM_STL_Decompose_Week(benchmark::State& state) {
    STLDecomposer stl;
    for (auto _ : state) {
        auto r = stl.decompose(g_values_week, 288, 7, 0, true, 2, 2);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_WEEK);
}
BENCHMARK(BM_STL_Decompose_Week);

static void BM_STL_Decompose_Month(benchmark::State& state) {
    STLDecomposer stl;
    for (auto _ : state) {
        auto r = stl.decompose(g_values_month, 288, 7, 0, true, 2, 2);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_MONTH);
}
BENCHMARK(BM_STL_Decompose_Month);

static void BM_STL_Decompose_Year(benchmark::State& state) {
    STLDecomposer stl;
    for (auto _ : state) {
        auto r = stl.decompose(g_values_year, 288, 7, 0, true, 2, 2);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_STL_Decompose_Year);

static void BM_STL_MSTL_Month(benchmark::State& state) {
    STLDecomposer stl;
    std::vector<size_t> periods = {288, 2016};  // daily + weekly
    for (auto _ : state) {
        auto r = stl.decomposeMultiple(g_values_month, periods);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_MONTH);
}
BENCHMARK(BM_STL_MSTL_Month);

static void BM_STL_MSTL_Year(benchmark::State& state) {
    STLDecomposer stl;
    std::vector<size_t> periods = {288, 2016};
    for (auto _ : state) {
        auto r = stl.decomposeMultiple(g_values_year, periods);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_STL_MSTL_Year);

// ═══════════════════════════════════════════════════════════════════════════
// LOESS (isolated — the innermost hot loop of STL)
// ═══════════════════════════════════════════════════════════════════════════

static void BM_LoessEvenlySpaced_Month(benchmark::State& state) {
    STLDecomposer stl;
    double span = 0.05;
    for (auto _ : state) {
        auto r = stl.loessEvenlySpaced(g_values_month, span);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_MONTH);
}
BENCHMARK(BM_LoessEvenlySpaced_Month);

static void BM_LoessEvenlySpaced_Year(benchmark::State& state) {
    STLDecomposer stl;
    double span = 0.05;
    for (auto _ : state) {
        auto r = stl.loessEvenlySpaced(g_values_year, span);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_LoessEvenlySpaced_Year);

// ═══════════════════════════════════════════════════════════════════════════
// Periodicity Detection (FFT + ACF)
// ═══════════════════════════════════════════════════════════════════════════

static void BM_PeriodicityDetect_Month(benchmark::State& state) {
    PeriodicityDetector detector;
    for (auto _ : state) {
        auto r = detector.detectPeriods(g_values_month, 4, N_MONTH / 2, 3, 0.15);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_MONTH);
}
BENCHMARK(BM_PeriodicityDetect_Month);

static void BM_PeriodicityDetect_Year(benchmark::State& state) {
    PeriodicityDetector detector;
    for (auto _ : state) {
        auto r = detector.detectPeriods(g_values_year, 4, N_YEAR / 2, 3, 0.15);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_PeriodicityDetect_Year);

static void BM_Periodogram_Year(benchmark::State& state) {
    PeriodicityDetector detector;
    for (auto _ : state) {
        auto r = detector.computePeriodogram(g_values_year);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_Periodogram_Year);

static void BM_AutoCorrelation_Year(benchmark::State& state) {
    PeriodicityDetector detector;
    // Detrend once (the ACF operates on detrended data)
    auto detrended = detector.detrend(g_values_year);
    for (auto _ : state) {
        double r = detector.autoCorrelation(detrended, 288);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_AutoCorrelation_Year);

// ═══════════════════════════════════════════════════════════════════════════
// Seasonal Forecaster (SARIMA path)
// ═══════════════════════════════════════════════════════════════════════════

static void BM_SeasonalForecast_Month(benchmark::State& state) {
    SeasonalForecaster forecaster;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::DAILY;
    config.deviations = 2.0;
    auto input = makeInput(g_timestamps_month, g_values_month);
    auto fts = ForecastExecutor::generateForecastTimestamps(input.timestamps, 500);
    for (auto _ : state) {
        auto r = forecaster.forecast(input, config, fts);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_MONTH);
}
BENCHMARK(BM_SeasonalForecast_Month);

static void BM_SeasonalForecast_Year(benchmark::State& state) {
    SeasonalForecaster forecaster;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::DAILY;
    config.deviations = 2.0;
    auto input = makeInput(g_timestamps_year, g_values_year);
    auto fts = ForecastExecutor::generateForecastTimestamps(input.timestamps, 2000);
    for (auto _ : state) {
        auto r = forecaster.forecast(input, config, fts);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_SeasonalForecast_Year);

static void BM_SeasonalForecast_MSTL_Month(benchmark::State& state) {
    SeasonalForecaster forecaster;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::MULTI;
    config.deviations = 2.0;
    auto input = makeInput(g_timestamps_month, g_values_month);
    auto fts = ForecastExecutor::generateForecastTimestamps(input.timestamps, 500);
    for (auto _ : state) {
        auto r = forecaster.forecast(input, config, fts);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_MONTH);
}
BENCHMARK(BM_SeasonalForecast_MSTL_Month);

// ═══════════════════════════════════════════════════════════════════════════
// Linear Forecaster
// ═══════════════════════════════════════════════════════════════════════════

static void BM_LinearForecast_Year(benchmark::State& state) {
    LinearForecaster forecaster;
    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;
    auto input = makeInput(g_timestamps_year, g_values_year);
    auto fts = ForecastExecutor::generateForecastTimestamps(input.timestamps, 2000);
    for (auto _ : state) {
        auto r = forecaster.forecast(input, config, fts);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_LinearForecast_Year);

// ═══════════════════════════════════════════════════════════════════════════
// Full Forecast Executor (end-to-end)
// ═══════════════════════════════════════════════════════════════════════════

static void BM_ForecastExecutor_Linear_Year(benchmark::State& state) {
    ForecastExecutor executor;
    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;
    config.disableAutoWindow = true;  // Baseline: no windowing
    auto input = makeInput(g_timestamps_year, g_values_year);
    for (auto _ : state) {
        auto r = executor.execute(input, config);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_ForecastExecutor_Linear_Year);

static void BM_ForecastExecutor_Seasonal_Year(benchmark::State& state) {
    ForecastExecutor executor;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::DAILY;
    config.deviations = 2.0;
    config.disableAutoWindow = true;  // Baseline: no windowing
    auto input = makeInput(g_timestamps_year, g_values_year);
    for (auto _ : state) {
        auto r = executor.execute(input, config);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_ForecastExecutor_Seasonal_Year);

static void BM_ForecastExecutor_Multi_Year(benchmark::State& state) {
    ForecastExecutor executor;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::MULTI;
    config.deviations = 2.0;
    config.disableAutoWindow = true;  // Baseline: no windowing
    auto input = makeInput(g_timestamps_year, g_values_year);
    for (auto _ : state) {
        auto r = executor.execute(input, config);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_ForecastExecutor_Multi_Year);

static void BM_ForecastExecutor_Multi_Year_Windowed(benchmark::State& state) {
    ForecastExecutor executor;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::MULTI;
    config.deviations = 2.0;
    // Auto-windowing is on by default (disableAutoWindow = false)
    auto input = makeInput(g_timestamps_year, g_values_year);
    for (auto _ : state) {
        auto r = executor.execute(input, config);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_ForecastExecutor_Multi_Year_Windowed);

static void BM_ForecastExecutor_Seasonal_Year_Windowed(benchmark::State& state) {
    ForecastExecutor executor;
    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::DAILY;
    config.deviations = 2.0;
    auto input = makeInput(g_timestamps_year, g_values_year);
    for (auto _ : state) {
        auto r = executor.execute(input, config);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_ForecastExecutor_Seasonal_Year_Windowed);

// ═══════════════════════════════════════════════════════════════════════════
// Isolated hot functions
// ═══════════════════════════════════════════════════════════════════════════

static void BM_SeasonalAutoCorrelation(benchmark::State& state) {
    SeasonalForecaster forecaster;
    double mean = std::accumulate(g_values_year.begin(), g_values_year.end(), 0.0) / static_cast<double>(N_YEAR);
    double variance = 0.0;
    for (double v : g_values_year)
        variance += (v - mean) * (v - mean);
    variance /= N_YEAR;
    for (auto _ : state) {
        double r = forecaster.autoCorrelation(g_values_year, mean, variance, 288);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_SeasonalAutoCorrelation);

static void BM_RobustnessWeights_Year(benchmark::State& state) {
    STLDecomposer stl;
    // Use values as pseudo-residuals
    for (auto _ : state) {
        auto r = stl.computeRobustnessWeights(g_values_year);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_RobustnessWeights_Year);

static void BM_CycleSubseries_Year(benchmark::State& state) {
    STLDecomposer stl;
    for (auto _ : state) {
        auto r = stl.smoothCycleSubseries(g_values_year, 288, 7);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_CycleSubseries_Year);

static void BM_LowPassFilter_Year(benchmark::State& state) {
    STLDecomposer stl;
    for (auto _ : state) {
        auto r = stl.lowPassFilter(g_values_year, 288);
        benchmark::DoNotOptimize(r);
    }
    state.SetItemsProcessed(state.iterations() * N_YEAR);
}
BENCHMARK(BM_LowPassFilter_Year);

// ═══════════════════════════════════════════════════════════════════════════

BENCHMARK_MAIN();
