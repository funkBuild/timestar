// expression_benchmark.cpp
// Comprehensive benchmark for all AlignedSeries expression functions.
//
// Build: add_executable(expression_benchmark expression_benchmark.cpp)
//        target_link_libraries(expression_benchmark libtsdb benchmark::benchmark)
//
// Run:   ./bin/expression_benchmark \
//            --benchmark_repetitions=10 \
//            --benchmark_report_aggregates_only=true \
//            --benchmark_display_aggregates_only=true

#include <benchmark/benchmark.h>
#include "expression_evaluator.hpp"
#include "topk_filter.hpp"

#include <cmath>
#include <cstdlib>
#include <limits>
#include <vector>

// ─────────────────────────────── constants ───────────────────────────────────

static constexpr size_t N          = 1'000'000;
static constexpr size_t GROUP_N    = 250'000;   // per cross-series group
static constexpr size_t NUM_GROUPS = 4;

// ──────────────────────────── global test data ───────────────────────────────

static tsdb::AlignedSeries g_series;   // 1 M points — primary signal
static tsdb::AlignedSeries g_series2;  // 1 M points — secondary signal (for binary ops)

static std::vector<tsdb::GroupedSeries> g_groups;   // 4 × 250 K for cross-series

// Build everything once at static-init time.
struct GlobalSetup {
    GlobalSetup() {
        // ── primary series ────────────────────────────────────────────────
        std::vector<uint64_t> ts(N);
        std::vector<double>   vs(N);
        std::srand(42);
        for (size_t i = 0; i < N; ++i) {
            ts[i] = static_cast<uint64_t>(i) * 1'000'000'000ULL;
            vs[i] = 100.0
                    + 50.0 * std::sin(static_cast<double>(i) * 0.01)
                    + 0.1  * static_cast<double>(i % 1000)
                    + static_cast<double>(std::rand() % 10);
            if (i % 10000 == 0) {
                vs[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        g_series = tsdb::AlignedSeries(ts, vs);

        // ── secondary series (slightly different phase) ───────────────────
        std::vector<double> vs2(N);
        std::srand(99);
        for (size_t i = 0; i < N; ++i) {
            vs2[i] = 80.0
                     + 40.0 * std::sin(static_cast<double>(i) * 0.01 + 1.0)
                     + 0.05 * static_cast<double>(i % 1000)
                     + static_cast<double>(std::rand() % 8);
            if (i % 10000 == 0) {
                vs2[i] = std::numeric_limits<double>::quiet_NaN();
            }
        }
        g_series2 = tsdb::AlignedSeries(ts, vs2);

        // ── cross-series groups (4 × 250 K) ──────────────────────────────
        g_groups.resize(NUM_GROUPS);
        for (size_t g = 0; g < NUM_GROUPS; ++g) {
            std::vector<uint64_t> gts(GROUP_N);
            std::vector<double>   gvs(GROUP_N);
            std::srand(static_cast<unsigned>(g * 1000 + 7));
            for (size_t i = 0; i < GROUP_N; ++i) {
                gts[i] = static_cast<uint64_t>(i) * 1'000'000'000ULL;
                gvs[i] = static_cast<double>(g * 20 + 50)
                         + 30.0 * std::sin(static_cast<double>(i) * 0.02
                                           + static_cast<double>(g) * 0.5)
                         + static_cast<double>(std::rand() % 15);
                if (i % 5000 == 0) {
                    gvs[i] = std::numeric_limits<double>::quiet_NaN();
                }
            }
            g_groups[g].group_tags["group"] = "g" + std::to_string(g);
            g_groups[g].series = tsdb::AlignedSeries(std::move(gts), std::move(gvs));
        }
    }
} g_setup;

// ─────────────────────────── helper macros ───────────────────────────────────

// Convenience: copy groups before passing to functions that consume by value.
static std::vector<tsdb::GroupedSeries> copyGroups() {
    return g_groups;   // vector of GroupedSeries with copy-constructible AlignedSeries
}

// ═════════════════════════ unary function benchmarks ═════════════════════════

#define UNARY_BM(name, expr)                                          \
static void BM_##name(benchmark::State& state) {                      \
    for (auto _ : state) {                                            \
        auto result = (expr);                                         \
        benchmark::DoNotOptimize(result);                             \
    }                                                                 \
    state.SetItemsProcessed(                                          \
        static_cast<int64_t>(state.iterations()) *                    \
        static_cast<int64_t>(g_series.size()));                       \
}                                                                     \
BENCHMARK(BM_##name)

UNARY_BM(abs,            g_series.abs());
UNARY_BM(log,            g_series.abs().log());   // abs() first to avoid log(neg)
UNARY_BM(log10,          g_series.abs().log10());
UNARY_BM(sqrt,           g_series.abs().sqrt());
UNARY_BM(ceil,           g_series.ceil());
UNARY_BM(floor,          g_series.floor());
UNARY_BM(diff,           g_series.diff());
UNARY_BM(monotonic_diff, g_series.monotonic_diff());
UNARY_BM(default_zero,   g_series.default_zero());
UNARY_BM(fill_forward,   g_series.fill_forward());
UNARY_BM(fill_backward,  g_series.fill_backward());
UNARY_BM(fill_linear,    g_series.fill_linear());
UNARY_BM(cumsum,         g_series.cumsum());
UNARY_BM(integral,       g_series.integral());
UNARY_BM(normalize,      g_series.normalize());
UNARY_BM(rate,           g_series.rate());
UNARY_BM(irate,          g_series.irate());
UNARY_BM(increase,       g_series.increase());

// ═══════════════════════ scalar-parameter benchmarks ═════════════════════════

#define SCALAR_BM(name, expr)                                         \
static void BM_##name(benchmark::State& state) {                      \
    for (auto _ : state) {                                            \
        auto result = (expr);                                         \
        benchmark::DoNotOptimize(result);                             \
    }                                                                 \
    state.SetItemsProcessed(                                          \
        static_cast<int64_t>(state.iterations()) *                    \
        static_cast<int64_t>(g_series.size()));                       \
}                                                                     \
BENCHMARK(BM_##name)

SCALAR_BM(rolling_avg_100,    g_series.rolling_avg(100));
SCALAR_BM(rolling_min_100,    g_series.rolling_min(100));
SCALAR_BM(rolling_max_100,    g_series.rolling_max(100));
SCALAR_BM(rolling_stddev_100, g_series.rolling_stddev(100));
SCALAR_BM(ema_0_1,            g_series.ema(0.1));
SCALAR_BM(zscore_100,         g_series.zscore(100));
SCALAR_BM(holt_winters,       g_series.holt_winters(0.3, 0.1));
SCALAR_BM(fill_value,         g_series.fill_value(0.0));
SCALAR_BM(clamp_min,          g_series.clamp_min(90.0));
SCALAR_BM(clamp_max,          g_series.clamp_max(150.0));
SCALAR_BM(per_minute,         g_series.per_minute(1.0));
SCALAR_BM(per_hour,           g_series.per_hour(1.0));

// ═══════════════════════ binary series benchmarks ════════════════════════════

#define BINARY_BM(name, expr)                                         \
static void BM_##name(benchmark::State& state) {                      \
    for (auto _ : state) {                                            \
        auto result = (expr);                                         \
        benchmark::DoNotOptimize(result);                             \
    }                                                                 \
    state.SetItemsProcessed(                                          \
        static_cast<int64_t>(state.iterations()) *                    \
        static_cast<int64_t>(g_series.size()));                       \
}                                                                     \
BENCHMARK(BM_##name)

BINARY_BM(add,      g_series + g_series2);
BINARY_BM(subtract, g_series - g_series2);
BINARY_BM(multiply, g_series * g_series2);
BINARY_BM(divide,   g_series / g_series2);

// ══════════════════════ cross-series benchmarks ══════════════════════════════

#define CROSS_BM(name, expr)                                          \
static void BM_##name(benchmark::State& state) {                      \
    for (auto _ : state) {                                            \
        auto grps = copyGroups();                                     \
        auto result = (expr);                                         \
        benchmark::DoNotOptimize(result);                             \
    }                                                                 \
    /* total points processed = NUM_GROUPS * GROUP_N */               \
    state.SetItemsProcessed(                                          \
        static_cast<int64_t>(state.iterations()) *                    \
        static_cast<int64_t>(NUM_GROUPS * GROUP_N));                  \
}                                                                     \
BENCHMARK(BM_##name)

CROSS_BM(avg_of_series,           tsdb::avg_of_series(std::move(grps)));
CROSS_BM(sum_of_series,           tsdb::sum_of_series(std::move(grps)));
CROSS_BM(min_of_series,           tsdb::min_of_series(std::move(grps)));
CROSS_BM(max_of_series,           tsdb::max_of_series(std::move(grps)));
CROSS_BM(percentile_of_series_p50, tsdb::percentile_of_series(50.0, std::move(grps)));
CROSS_BM(percentile_of_series_p95, tsdb::percentile_of_series(95.0, std::move(grps)));

// topk / bottomk — copy groups explicitly (they are consumed by value)
static void BM_topk(benchmark::State& state) {
    for (auto _ : state) {
        auto grps = copyGroups();
        auto result = tsdb::topk(2, std::move(grps));
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(
        static_cast<int64_t>(state.iterations()) *
        static_cast<int64_t>(NUM_GROUPS * GROUP_N));
}
BENCHMARK(BM_topk);

static void BM_bottomk(benchmark::State& state) {
    for (auto _ : state) {
        auto grps = copyGroups();
        auto result = tsdb::bottomk(2, std::move(grps));
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(
        static_cast<int64_t>(state.iterations()) *
        static_cast<int64_t>(NUM_GROUPS * GROUP_N));
}
BENCHMARK(BM_bottomk);

// ─────────────────────────────── main ────────────────────────────────────────

BENCHMARK_MAIN();
