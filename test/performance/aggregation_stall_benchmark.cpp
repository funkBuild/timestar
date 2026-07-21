/*
 * Aggregation Stall Benchmark
 *
 * Pins the Jul 21 2026 v1.2.4 production incident shape: reactor stalls up to
 * 980 ms inside the interval==0 fallback aggregation path for a query touching
 * 195 series / ~3.5M points in a single group (no `by` clause).
 *
 * Measures the two synchronous phases exactly as production composes them:
 *   S1 — shard side:      Aggregator::createPartialAggregations
 *   S2 — coordinator:     Aggregator::mergePartialAggregationsGrouped
 *   S3 — response:        timestar::proto::formatQueryResponse (protobuf client)
 *
 * Each phase reports wall time AND the longest reactor monopolization
 * (max gap observed by a concurrently scheduled yield-loop probe).  Before the
 * yield work the gap equals the wall time; after it the gap must collapse to
 * roughly the scheduler task quota.
 *
 * Run with: --gtest_filter='AggregationStallBench*'
 */

#include "../../lib/http/http_query_handler.hpp"
#include "../../lib/http/proto_converters.hpp"
#include "../../lib/query/aggregator.hpp"
#include "../seastar_gtest.hpp"

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <chrono>
#include <random>
#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>
#include <string>
#include <vector>

using clk = std::chrono::steady_clock;
using timestar::AggregationMethod;
using timestar::Aggregator;
using timestar::PartialAggregationResult;
using timestar::http::SeriesResult;

namespace {

constexpr uint64_t START_NS = 1753056000000000000ULL;  // 2025-07-21T00:00:00Z
constexpr uint64_t STEP_NS = 1'000'000'000ULL;         // 1s cadence per device

// Incident shape: 195 devices x 18k points = 3.51M points, all timestamps
// distinct (devices report on their own clocks), one group per field.
constexpr int INCIDENT_SERIES = 195;
constexpr size_t INCIDENT_POINTS = 18000;

// Build K device series with globally distinct, per-series sorted timestamps.
std::vector<SeriesResult> buildSeries(int firstDevice, int deviceCount, size_t pointsPerSeries, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> dist(20.0, 80.0);

    std::vector<SeriesResult> out;
    out.reserve(static_cast<size_t>(deviceCount));
    for (int d = 0; d < deviceCount; ++d) {
        const int device = firstDevice + d;
        SeriesResult sr;
        sr.measurement = "deviceData";
        sr.tags = {{"device", fmt::format("dev-{:04d}", device)}};

        std::vector<uint64_t> ts;
        std::vector<double> vals;
        ts.reserve(pointsPerSeries);
        vals.reserve(pointsPerSeries);
        for (size_t i = 0; i < pointsPerSeries; ++i) {
            // Offset each device by a few microseconds so timestamps never
            // collide across series — the worst case for the merge, and what
            // the incident data looked like (3.5M final points returned).
            ts.push_back(START_NS + i * STEP_NS + static_cast<uint64_t>(device) * 3000ULL);
            vals.push_back(dist(rng));
        }
        sr.fields["value"] = {std::move(ts), timestar::FieldValues(std::move(vals))};
        out.push_back(std::move(sr));
    }
    return out;
}

// Reactor-monopolization probe: a competing task that yields in a loop and
// records the longest gap between two of its own resumptions.  While a
// synchronous function hogs the reactor the probe cannot run, so
// maxGapMs ~= the longest stall the benchmarked code would inflict.
struct GapProbe {
    bool stop = false;
    double maxGapMs = 0.0;
    seastar::future<> loop() {
        auto last = clk::now();
        while (!stop) {
            co_await seastar::yield();
            auto now = clk::now();
            double gap = std::chrono::duration<double, std::milli>(now - last).count();
            if (gap > maxGapMs) {
                maxGapMs = gap;
            }
            last = now;
        }
    }
};

struct PhaseResult {
    double wallMs = 0.0;
    double maxGapMs = 0.0;
};

// Run `fn` (a future factory) with a GapProbe alongside it.
template <typename Fn>
seastar::future<PhaseResult> measure(Fn fn) {
    GapProbe probe;
    auto probeFut = probe.loop();
    auto t0 = clk::now();
    co_await fn();
    PhaseResult r;
    r.wallMs = std::chrono::duration<double, std::milli>(clk::now() - t0).count();
    probe.stop = true;
    co_await std::move(probeFut);
    r.maxGapMs = probe.maxGapMs;
    co_return r;
}

const char* methodName(AggregationMethod m) {
    switch (m) {
        case AggregationMethod::AVG:
            return "AVG";
        case AggregationMethod::STDDEV:
            return "STDDEV";
        default:
            return "?";
    }
}

}  // namespace

class AggregationStallBench : public ::testing::Test {};

// ═══════════════════════════════════════════════════════════════════════════
//  S1: createPartialAggregations — K-scaling at the incident point count.
//  The v1.2.4 code re-merged the accumulated group per series: O(K²·N).
// ═══════════════════════════════════════════════════════════════════════════
SEASTAR_TEST_F(AggregationStallBench, S1_CreatePartials_KScaling) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  S1: createPartialAggregations (interval==0, one group)        ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    for (AggregationMethod method : {AggregationMethod::AVG, AggregationMethod::STDDEV}) {
        fmt::print("\n  ── method={} ──\n", methodName(method));
        for (int K : {10, 50, 100, INCIDENT_SERIES}) {
            auto series = buildSeries(0, K, INCIDENT_POINTS, 42);

            std::vector<PartialAggregationResult> partials;
            auto r = co_await measure([&]() -> seastar::future<> {
                partials = co_await Aggregator::createPartialAggregations(series, method, /*interval=*/0,
                                                                          /*groupByTags=*/{});
            });

            size_t totalPoints = 0;
            for (const auto& p : partials) {
                totalPoints += p.totalPoints;
            }
            EXPECT_EQ(totalPoints, static_cast<size_t>(K) * INCIDENT_POINTS);

            fmt::print("  K={:4d}  N={}  wall={:9.2f} ms   maxReactorGap={:9.2f} ms   ({:.2f}M pts/s)\n", K,
                       INCIDENT_POINTS, r.wallMs, r.maxGapMs,
                       (static_cast<double>(K) * INCIDENT_POINTS / 1e6) / (r.wallMs / 1e3));
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  S2: full incident pipeline — 2 shards' createPartialAggregations feeding
//  the coordinator's mergePartialAggregationsGrouped.
// ═══════════════════════════════════════════════════════════════════════════
SEASTAR_TEST_F(AggregationStallBench, S2_IncidentPipeline_TwoShards) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  S2: 2-shard pipeline, 195 series x 18k pts (incident shape)   ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    for (AggregationMethod method : {AggregationMethod::AVG, AggregationMethod::STDDEV}) {
        // Shard split as production routes it: roughly half the devices each.
        auto shardA = buildSeries(0, 98, INCIDENT_POINTS, 7);
        auto shardB = buildSeries(98, 97, INCIDENT_POINTS, 11);

        std::vector<PartialAggregationResult> allPartials;
        auto shardPhase = co_await measure([&]() -> seastar::future<> {
            auto pa = co_await Aggregator::createPartialAggregations(shardA, method, 0, {});
            auto pb = co_await Aggregator::createPartialAggregations(shardB, method, 0, {});
            allPartials.reserve(pa.size() + pb.size());
            std::move(pa.begin(), pa.end(), std::back_inserter(allPartials));
            std::move(pb.begin(), pb.end(), std::back_inserter(allPartials));
        });

        std::vector<timestar::GroupedAggregationResult> grouped;
        auto mergePhase = co_await measure([&]() -> seastar::future<> {
            grouped = co_await Aggregator::mergePartialAggregationsGrouped(allPartials, method);
        });

        // Correctness pins: one group ("value" field, no group-by), and with
        // all-distinct timestamps every input point must survive per-timestamp
        // cross-series aggregation.
        EXPECT_EQ(grouped.size(), 1u);
        const size_t expectPoints = static_cast<size_t>(INCIDENT_SERIES) * INCIDENT_POINTS;
        const size_t gotPoints =
            !grouped[0].rawTimestamps.empty() ? grouped[0].rawTimestamps.size() : grouped[0].points.size();
        EXPECT_EQ(gotPoints, expectPoints);

        fmt::print(
            "\n  method={:6s}  shardPhase: wall={:9.2f} ms  maxGap={:9.2f} ms\n"
            "                 mergePhase: wall={:9.2f} ms  maxGap={:9.2f} ms\n"
            "                 total wall={:9.2f} ms\n",
            methodName(method), shardPhase.wallMs, shardPhase.maxGapMs, mergePhase.wallMs, mergePhase.maxGapMs,
            shardPhase.wallMs + mergePhase.wallMs);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  S3: protobuf response build+encode of the incident-sized result.
// ═══════════════════════════════════════════════════════════════════════════
SEASTAR_TEST_F(AggregationStallBench, S3_ProtoResponseEncode) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  S3: protobuf QueryResponse encode, 195 series x 18k pts       ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    auto series = buildSeries(0, INCIDENT_SERIES, INCIDENT_POINTS, 99);

    timestar::proto::QueryResponseData resp;
    resp.success = true;
    resp.statistics.seriesCount = series.size();
    for (auto& sr : series) {
        timestar::proto::SeriesResultData srd;
        srd.measurement = sr.measurement;
        srd.tags = sr.tags;
        for (auto& [fieldName, fieldData] : sr.fields) {
            srd.fields[fieldName] = fieldData;
            resp.statistics.pointCount += fieldData.first.size();
        }
        resp.series.push_back(std::move(srd));
    }

    std::string encoded;
    auto r = co_await measure(
        [&]() -> seastar::future<> { encoded = co_await timestar::proto::formatQueryResponseYielding(resp); });

    EXPECT_GT(encoded.size(), 0u);
    fmt::print("\n  encode: wall={:9.2f} ms  maxGap={:9.2f} ms  bytes={} ({:.1f} MB)\n", r.wallMs, r.maxGapMs,
               encoded.size(), static_cast<double>(encoded.size()) / 1e6);
}
