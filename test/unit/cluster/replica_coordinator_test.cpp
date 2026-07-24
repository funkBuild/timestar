// Phase 6 GATE (routing): replica-read selection/hedging/retry. Each VShard
// contributes EXACTLY ONCE; a hedge or retry never combines two attempts' output;
// a placement change mid-query cannot change contributions (pinned config); a
// VShard no replica can serve fails closed (or is named under allow_partial).
#include "../../../lib/cluster/data/replica_coordinator.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <seastar/core/coroutine.hh>
#include <vector>

using namespace timestar::data;

namespace {
SeriesId128 sid(const std::string& k) {
    return SeriesId128::fromSeriesKey(k);
}

class MockReplica : public ReplicaReader {
public:
    uint16_t vshard = 0;
    std::vector<DataPoint> data;
    bool fail = false;
    int reads = 0;

    seastar::future<ReplicaReadResult> read(ReplicaReadRequest req) override {
        ++reads;
        if (fail)
            return seastar::make_exception_future<ReplicaReadResult>(ReplicaReadUnavailable("mock replica down"));
        QueryPartial p;
        for (const auto& d : data)
            if (d.timestamp >= req.spec.startTime && d.timestamp <= req.spec.endTime)
                p.raw.push_back(d);
        return seastar::make_ready_future<ReplicaReadResult>(
            ReplicaReadResult{std::move(p), ReadEnvelope{vshard, 1, 1}});
    }
    ReadEnvelope envelope() const override { return {vshard, 1, 1}; }
};

ReplicaReadRequest req() {
    return {QuerySpec{0, 100000, AggMethod::Raw, {}}, ReadConsistency::Linearizable, {}, 0};
}

// Two replicas of the SAME VShard hold the SAME data (they applied the same log).
// Hedging both must yield the data ONCE, never doubled.
seastar::future<> testHedgeNoCombining() {
    MockReplica r0, r1;
    r0.vshard = r1.vshard = 5;
    r0.data = r1.data = {{sid("a"), 10, 1.0}, {sid("a"), 20, 2.0}, {sid("a"), 30, 3.0}};
    ReplicaQueryCoordinator coord({{5, {&r0, &r1}}}, /*hedgeWidth=*/2);
    auto got = co_await coord.query(req());
    EXPECT_EQ(got.partial.raw.size(), 3u);  // NOT 6 -- only the first success contributes
    EXPECT_EQ(r0.reads, 1);
    EXPECT_EQ(r1.reads, 1);  // both hedged, but only one merged
}

// Preferred replica fails; the coordinator retries the next and contributes once.
seastar::future<> testRetryOnFailureNoCombine() {
    MockReplica r0, r1;
    r0.vshard = r1.vshard = 7;
    r0.fail = true;
    r1.data = {{sid("b"), 10, 1.0}, {sid("b"), 20, 2.0}};
    ReplicaQueryCoordinator coord({{7, {&r0, &r1}}}, /*hedgeWidth=*/1);
    auto got = co_await coord.query(req());
    EXPECT_EQ(got.partial.raw.size(), 2u);
    EXPECT_EQ(r0.reads, 1);
    EXPECT_EQ(r1.reads, 1);
}

// Two VShards each contribute their own data exactly once.
seastar::future<> testExactlyOncePerVShard() {
    MockReplica a, b;
    a.vshard = 1;
    b.vshard = 2;
    a.data = {{sid("a"), 10, 1.0}};
    b.data = {{sid("b"), 20, 2.0}};
    ReplicaQueryCoordinator coord({{1, {&a}}, {2, {&b}}});
    auto got = co_await coord.query(req());
    EXPECT_EQ(got.partial.raw.size(), 2u);
}

// A VShard no replica can serve fails closed by default; allow_partial names it.
seastar::future<> testFailClosedAndAllowPartial() {
    MockReplica ok, dead;
    ok.vshard = 1;
    ok.data = {{sid("a"), 10, 1.0}};
    dead.vshard = 2;
    dead.fail = true;

    // Default fail-closed.
    ReplicaQueryCoordinator strict({{1, {&ok}}, {2, {&dead}}});
    bool incomplete = false;
    try {
        co_await strict.query(req());
    } catch (const QueryIncomplete&) {
        incomplete = true;
    }
    EXPECT_TRUE(incomplete);

    // allow_partial: serve what we can, NAME what we could not (never silent).
    ReplicaQueryCoordinator partial({{1, {&ok}}, {2, {&dead}}}, 1, /*allowPartial=*/true);
    auto got = co_await partial.query(req());
    EXPECT_EQ(got.partial.raw.size(), 1u);
    EXPECT_EQ(got.missing.size(), 1u);
    if (got.missing.size() == 1u)
        EXPECT_EQ(got.missing[0], 2u);
}

// The coordinator reads ONLY its pinned placement: a replica for a VShard not in
// the pinned set never contributes, and a "live" placement change is invisible.
seastar::future<> testPlacementPinned() {
    MockReplica a, extra;
    a.vshard = 1;
    a.data = {{sid("a"), 10, 1.0}};
    extra.vshard = 9;
    extra.data = {{sid("z"), 99, 9.0}};
    // Only VShard 1 is pinned; VShard 9's replica exists but is not in the config.
    ReplicaQueryCoordinator coord({{1, {&a}}});
    auto got = co_await coord.query(req());
    EXPECT_EQ(got.partial.raw.size(), 1u);  // only VShard 1's point; VShard 9 not consulted
    EXPECT_EQ(extra.reads, 0);
}

// Per-query selection: drops unreachable + over-stale, orders local/fresh/idle first.
seastar::future<> testReplicaSelection() {
    MockReplica local, fresh, laggy, down;
    std::vector<ReplicaHealth> cands = {
        {&down, /*reachable=*/false, 0, 0, false, 0.0},
        {&laggy, true, /*lag=*/500, 0, false, 0.0},
        {&fresh, true, /*lag=*/1, /*queue=*/9, false, 0.0},
        {&local, true, /*lag=*/2, /*queue=*/0, /*local=*/true, 0.0},
    };
    // Linearizable: unreachable dropped; local first, then by lag.
    auto lin = ReplicaSelector::select(cands, ReadConsistency::Linearizable, 0);
    EXPECT_EQ(lin.size(), 3u);  // down excluded
    if (lin.size() == 3u) {
        EXPECT_EQ(lin[0], &local);  // locality wins
        EXPECT_EQ(lin[1], &fresh);  // then lowest lag
        EXPECT_EQ(lin[2], &laggy);
    }
    // BoundedStaleness with a tight bound also drops the laggy replica.
    auto bs = ReplicaSelector::select(cands, ReadConsistency::BoundedStaleness, /*maxLag=*/10);
    EXPECT_EQ(bs.size(), 2u);  // down + laggy excluded

    // A NaN error rate (e.g. errors/0) must not break the sort's ordering (UB).
    MockReplica good, nanr;
    std::vector<ReplicaHealth> nanCands = {
        {&nanr, true, 0, 0, false, std::nan("")},
        {&good, true, 0, 0, false, 0.5},
    };
    auto ordered = ReplicaSelector::select(nanCands, ReadConsistency::Linearizable, 0);
    EXPECT_EQ(ordered.size(), 2u);
    if (ordered.size() == 2u)
        EXPECT_EQ(ordered[0], &good);  // NaN treated as worst, ordered last
    co_return;
}

}  // namespace

TEST(ReplicaCoordinatorTest, ReplicaSelection) {
    testReplicaSelection().get();
}
TEST(ReplicaCoordinatorTest, HedgeNoCombining) {
    testHedgeNoCombining().get();
}
TEST(ReplicaCoordinatorTest, RetryOnFailureNoCombine) {
    testRetryOnFailureNoCombine().get();
}
TEST(ReplicaCoordinatorTest, ExactlyOncePerVShard) {
    testExactlyOncePerVShard().get();
}
TEST(ReplicaCoordinatorTest, FailClosedAndAllowPartial) {
    testFailClosedAndAllowPartial().get();
}
TEST(ReplicaCoordinatorTest, PlacementPinned) {
    testPlacementPinned().get();
}
