// Regression tests for two state-dependent query bugs:
//
// BUG A — interval queries collapsed to ONE bucket on a warmed server:
//   BlockAggregator's constructor computed its bucket count as
//   ceil((endTime - startTime) / interval) instead of the number of
//   epoch-aligned buckets the (inclusive) range overlaps, and
//   queryTsmAggregated constructed the aggregator over the TSM-only
//   sub-range [startTime, memMinTime - 1] while feeding memory-store
//   fallback data up to endTime into it.  Once the shard had ANY TSM file
//   (a "warmed" server), the memory-split path engaged the single-bucket
//   optimisation and folded every point into one bucket stamped
//   floor(startTime / interval).  Bucketing must be a pure function of
//   (data, query): epoch-aligned floor(ts / interval) * interval buckets
//   regardless of startTime alignment, point count, or data placement.
//
// BUG B — the no-interval response shape depended on server state:
//   memstore-resident data with startTime == first point returned N raw
//   points, while TSM-resident data (or a bucket-misaligned startTime on a
//   warmed server) returned 1 collapsed aggregate.  Canonical semantics
//   (see CLAUDE.md "No-Interval Aggregation Semantics"): without an
//   aggregationInterval and without group-by, non-LATEST/FIRST aggregations
//   return per-timestamp values (N points) on every path; LATEST/FIRST and
//   group-by queries collapse to one value per series/group on every path.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

using namespace timestar;

namespace {

// 1.7e18 ns is a multiple of 10s (and of 1s), so expected epoch bucket
// starts are easy to express.
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;
constexpr uint64_t SEC = 1'000'000'000ULL;

class IntervalEpochBucketingTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

void insertFloatSeries(seastar::sharded<Engine>& eng, const std::string& measurement, const std::string& field,
                       const std::map<std::string, std::string>& tags,
                       const std::vector<std::pair<uint64_t, double>>& points) {
    TimeStarInsert<double> insert(measurement, field);
    for (const auto& [k, v] : tags) {
        insert.addTag(k, v);
    }
    for (const auto& [ts, val] : points) {
        insert.addValue(ts, val);
    }
    shardedInsert(eng, std::move(insert));
}

// The canonical 5-point repro series from the nodejs correctness suite:
// t = BASE + 0/5/10/15/20s, v = 10/20/30/40/50.
void insertFivePointRepro(seastar::sharded<Engine>& eng, const std::string& measurement) {
    insertFloatSeries(eng, measurement, "v", {{"t", "a"}},
                      {{BASE, 10.0},
                       {BASE + 5 * SEC, 20.0},
                       {BASE + 10 * SEC, 30.0},
                       {BASE + 15 * SEC, 40.0},
                       {BASE + 20 * SEC, 50.0}});
}

// Roll all memory stores over and wait until the total TSM file count across
// shards reaches at least minTotalFiles (background conversion is async).
void flushToTsm(seastar::sharded<Engine>& eng, size_t minTotalFiles) {
    eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
    for (int attempt = 0; attempt < 100; ++attempt) {
        size_t files =
            eng.map_reduce0([](Engine& engine) { return engine.getTSMFileCount(); }, size_t{0}, std::plus<size_t>())
                .get();
        if (files >= minTotalFiles) {
            return;
        }
        seastar::sleep(std::chrono::milliseconds(100)).get();
    }
    FAIL() << "TSM conversion did not produce " << minTotalFiles << " file(s) within 10s";
}

struct FieldSeries {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
};

// Run a query through the full HTTP query pipeline and extract the single
// expected field result.
FieldSeries runQuery(seastar::sharded<Engine>& eng, const std::string& measurement, uint64_t startTime,
                     uint64_t endTime, uint64_t interval, AggregationMethod method) {
    http::HttpQueryHandler handler(&eng);
    QueryRequest request;
    request.aggregation = method;
    request.measurement = measurement;
    request.fields = {"v"};
    request.startTime = startTime;
    request.endTime = endTime;
    request.aggregationInterval = interval;

    auto response = handler.executeQuery(request).get();
    EXPECT_TRUE(response.success) << response.errorMessage;

    FieldSeries out;
    if (response.series.size() != 1u) {
        ADD_FAILURE() << "expected exactly 1 series, got " << response.series.size();
        return out;
    }
    auto it = response.series[0].fields.find("v");
    if (it == response.series[0].fields.end()) {
        ADD_FAILURE() << "field 'v' missing from response";
        return out;
    }
    out.timestamps = it->second.first;
    auto* vals = std::get_if<std::vector<double>>(&it->second.second);
    if (!vals) {
        ADD_FAILURE() << "field 'v' is not a float series";
        return out;
    }
    out.values = *vals;
    return out;
}

void expectSeries(const FieldSeries& got, const std::vector<uint64_t>& wantTs, const std::vector<double>& wantVals,
                  const char* what) {
    EXPECT_EQ(got.timestamps, wantTs) << what;
    ASSERT_EQ(got.values.size(), wantVals.size()) << what;
    for (size_t i = 0; i < wantVals.size(); ++i) {
        EXPECT_DOUBLE_EQ(got.values[i], wantVals[i]) << what << " value[" << i << "]";
    }
}

// Expected result of the misaligned 10s-bucket query over the 5-point repro
// series, range [BASE+3s, BASE+21s]: epoch-aligned buckets, endTime inclusive.
//   bucket BASE      : {+5s → 20}          → 20
//   bucket BASE+10s  : {+10s → 30, +15s → 40} → 35
//   bucket BASE+20s  : {+20s → 50}         → 50
void expectMisalignedReproBuckets(const FieldSeries& got, const char* what) {
    expectSeries(got, {BASE, BASE + 10 * SEC, BASE + 20 * SEC}, {20.0, 35.0, 50.0}, what);
}

}  // namespace

// ===========================================================================
// BUG A (1) — 5-point misaligned repro, COLD path: data only in the memory
// store, shard has no TSM files at all (Gate 0 in queryTsmAggregated).
// ===========================================================================
TEST_F(IntervalEpochBucketingTest, MisalignedSmallWindowColdMemstore) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        insertFivePointRepro(eng.eng, "collapse_cold");

        auto got =
            runQuery(eng.eng, "collapse_cold", BASE + 3 * SEC, BASE + 21 * SEC, 10 * SEC, AggregationMethod::AVG);
        expectMisalignedReproBuckets(got, "cold memstore");
    })
        .join()
        .get();
}

// ===========================================================================
// BUG A (1) — 5-point misaligned repro, WARMED path.  Warm the engine the
// same way a live server warms: earlier data of the series has been rolled
// over to TSM (so the shard has TSM files and Gate 0 is skipped), the repro
// points sit in the memory store, and the same query has already been run
// with an aligned range.  The misaligned query then takes the memory-split
// path (memMinTime = BASE+5s > startTime = BASE+3s) that used to collapse
// every point into one bucket stamped floor(startTime / interval) → [35].
// ===========================================================================
TEST_F(IntervalEpochBucketingTest, MisalignedSmallWindowWarmedTsmPlusMemstore) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Old data for the SAME series, flushed to TSM: guarantees the TSM
        // file lands on the shard that owns the repro series.
        insertFloatSeries(eng.eng, "collapse_warm", "v", {{"t", "a"}},
                          {{BASE - 1000 * SEC, 1.0}, {BASE - 900 * SEC, 2.0}});
        flushToTsm(eng.eng, 1);

        // Fresh repro points stay in the memory store.
        insertFivePointRepro(eng.eng, "collapse_warm");

        // Warm-up queries, including the aligned variant of the same query.
        (void)runQuery(eng.eng, "collapse_warm", BASE, BASE + 21 * SEC, 10 * SEC, AggregationMethod::AVG);
        (void)runQuery(eng.eng, "collapse_warm", BASE, BASE + 60 * SEC, 10 * SEC, AggregationMethod::AVG);

        auto got =
            runQuery(eng.eng, "collapse_warm", BASE + 3 * SEC, BASE + 21 * SEC, 10 * SEC, AggregationMethod::AVG);
        expectMisalignedReproBuckets(got, "warmed TSM+memstore");
    })
        .join()
        .get();
}

// ===========================================================================
// BUG A (1) — 5-point misaligned repro with the repro points themselves
// flushed to TSM (pure TSM pushdown path).
// ===========================================================================
TEST_F(IntervalEpochBucketingTest, MisalignedSmallWindowTsmResident) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        insertFivePointRepro(eng.eng, "collapse_tsm");
        flushToTsm(eng.eng, 1);

        auto got = runQuery(eng.eng, "collapse_tsm", BASE + 3 * SEC, BASE + 21 * SEC, 10 * SEC, AggregationMethod::AVG);
        expectMisalignedReproBuckets(got, "TSM resident");
    })
        .join()
        .get();
}

// ===========================================================================
// BUG A (2) — range <= 1 interval must still return every epoch bucket it
// overlaps.  [BASE, BASE+1s] with a 1s interval and inclusive endTime covers
// TWO epoch buckets (BASE and BASE+1s); this used to collapse into a single
// bucket averaging both points.
// ===========================================================================
TEST_F(IntervalEpochBucketingTest, RangeEqualToIntervalCrossingEpochBoundary) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        insertFloatSeries(eng.eng, "collapse2", "v", {{"t", "a"}},
                          {{BASE, 0.0}, {BASE + SEC, 1.0}, {BASE + 2 * SEC, 2.0}, {BASE + 3 * SEC, 3.0}});

        auto memGot = runQuery(eng.eng, "collapse2", BASE, BASE + SEC, SEC, AggregationMethod::AVG);
        expectSeries(memGot, {BASE, BASE + SEC}, {0.0, 1.0}, "memstore");

        flushToTsm(eng.eng, 1);
        auto tsmGot = runQuery(eng.eng, "collapse2", BASE, BASE + SEC, SEC, AggregationMethod::AVG);
        expectSeries(tsmGot, {BASE, BASE + SEC}, {0.0, 1.0}, "TSM");
    })
        .join()
        .get();
}

// ===========================================================================
// BUG A (2) — a misaligned range genuinely inside ONE epoch bucket returns
// that single bucket, stamped with the epoch-aligned bucket start (not the
// query startTime).
// ===========================================================================
TEST_F(IntervalEpochBucketingTest, MisalignedRangeWithinOneEpochBucket) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        insertFivePointRepro(eng.eng, "onebucket");

        // [BASE+13s, BASE+17s] lies inside epoch bucket BASE+10s; only the
        // +15s point (v=40) is in range.
        auto got = runQuery(eng.eng, "onebucket", BASE + 13 * SEC, BASE + 17 * SEC, 10 * SEC, AggregationMethod::AVG);
        expectSeries(got, {BASE + 10 * SEC}, {40.0}, "single epoch bucket");
    })
        .join()
        .get();
}

// ===========================================================================
// BUG B — no-interval AVG returns per-timestamp raw points (N points) on
// EVERY path: memstore-resident, TSM-resident, and split TSM+memstore, with
// aligned, misaligned, and before-first-point startTimes.
// ===========================================================================
TEST_F(IntervalEpochBucketingTest, NoIntervalAvgShapeIsPlacementAndAlignmentIndependent) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        insertFivePointRepro(eng.eng, "rawshape");

        const std::vector<uint64_t> allTs = {BASE, BASE + 5 * SEC, BASE + 10 * SEC, BASE + 15 * SEC, BASE + 20 * SEC};
        const std::vector<double> allVals = {10.0, 20.0, 30.0, 40.0, 50.0};

        // -- memstore-resident --
        auto exact = runQuery(eng.eng, "rawshape", BASE, BASE + 21 * SEC, 0, AggregationMethod::AVG);
        expectSeries(exact, allTs, allVals, "memstore, startTime == first point");

        auto before = runQuery(eng.eng, "rawshape", BASE - SEC, BASE + 21 * SEC, 0, AggregationMethod::AVG);
        expectSeries(before, allTs, allVals, "memstore, startTime before first point");

        auto misaligned = runQuery(eng.eng, "rawshape", BASE + 3 * SEC, BASE + 21 * SEC, 0, AggregationMethod::AVG);
        expectSeries(misaligned, {BASE + 5 * SEC, BASE + 10 * SEC, BASE + 15 * SEC, BASE + 20 * SEC},
                     {20.0, 30.0, 40.0, 50.0}, "memstore, startTime inside data");

        // -- TSM-resident --
        flushToTsm(eng.eng, 1);
        auto tsmExact = runQuery(eng.eng, "rawshape", BASE, BASE + 21 * SEC, 0, AggregationMethod::AVG);
        expectSeries(tsmExact, allTs, allVals, "TSM, startTime == first point");

        auto tsmBefore = runQuery(eng.eng, "rawshape", BASE - SEC, BASE + 21 * SEC, 0, AggregationMethod::AVG);
        expectSeries(tsmBefore, allTs, allVals, "TSM, startTime before first point");

        // -- split: first points in TSM, later points fresh in the memstore --
        insertFloatSeries(eng.eng, "rawshape", "v", {{"t", "a"}}, {{BASE + 25 * SEC, 60.0}, {BASE + 30 * SEC, 70.0}});
        auto split = runQuery(eng.eng, "rawshape", BASE - SEC, BASE + 31 * SEC, 0, AggregationMethod::AVG);
        std::vector<uint64_t> splitTs = allTs;
        splitTs.push_back(BASE + 25 * SEC);
        splitTs.push_back(BASE + 30 * SEC);
        std::vector<double> splitVals = allVals;
        splitVals.push_back(60.0);
        splitVals.push_back(70.0);
        expectSeries(split, splitTs, splitVals, "split TSM+memstore");
    })
        .join()
        .get();
}

// ===========================================================================
// BUG B — no-interval LATEST collapses to exactly 1 point per series on
// every path (memstore and TSM).
// ===========================================================================
// LATEST is not special without an interval: no method aggregates over time
// unless the caller asks (CLAUDE.md "Aggregation Result Shape").  `latest` is a
// cross-series tie-break at each timestamp, so for a single series it is raw
// passthrough — identical in shape to AVG above, on every placement.
//
// It used to collapse to one point, which is what forced callers to send a
// throwaway tiny aggregationInterval just to keep their time axis.
TEST_F(IntervalEpochBucketingTest, NoIntervalLatestKeepsEveryTimestampOnEveryPath) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        insertFivePointRepro(eng.eng, "latestshape");

        const std::vector<uint64_t> allTs = {BASE, BASE + 5 * SEC, BASE + 10 * SEC, BASE + 15 * SEC, BASE + 20 * SEC};
        const std::vector<double> allVals = {10.0, 20.0, 30.0, 40.0, 50.0};

        auto memGot = runQuery(eng.eng, "latestshape", BASE, BASE + 21 * SEC, 0, AggregationMethod::LATEST);
        expectSeries(memGot, allTs, allVals, "memstore latest");

        flushToTsm(eng.eng, 1);
        auto tsmGot = runQuery(eng.eng, "latestshape", BASE, BASE + 21 * SEC, 0, AggregationMethod::LATEST);
        expectSeries(tsmGot, allTs, allVals, "TSM latest");

        // With an interval it still selects one point per bucket: a 10s bucket
        // over this data keeps the latest of each.
        auto bucketed = runQuery(eng.eng, "latestshape", BASE, BASE + 21 * SEC, 10 * SEC, AggregationMethod::LATEST);
        ASSERT_FALSE(bucketed.timestamps.empty());
        EXPECT_LT(bucketed.timestamps.size(), allTs.size()) << "an explicit interval must still reduce";
        EXPECT_DOUBLE_EQ(bucketed.values.back(), 50.0) << "last bucket's latest";
    })
        .join()
        .get();
}
