// Phase 5 write/read routing: any-node writes are grouped by VShard and sent to
// each VShard's LEADER; reads fan out to leaders and merge. Tests the routing
// logic against mock VShardLeaders (the real RF=3 replication under a leader is
// proven separately in replicated_rf3_test). Mirrors how Phase 4 tested routing
// with MemStore/MemClient mocks.
#include "../../../lib/cluster/data/replicated_router.hpp"

#include "../../../lib/core/placement_table.hpp"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <vector>

using namespace timestar::data;

namespace {

SeriesId128 sid(const std::string& k) {
    return SeriesId128::fromSeriesKey(k);
}

// Records what it was asked to write and answers reads from those points.
class MockLeader : public VShardLeader {
public:
    bool amLeader = true;
    std::vector<DataPoint> points;

    seastar::future<bool> write(DataCommand cmd) override {
        if (!amLeader)
            return seastar::make_ready_future<bool>(false);
        if (const auto* wp = std::get_if<WritePoints>(&cmd))
            for (const auto& p : wp->points)
                points.push_back(p);
        return seastar::make_ready_future<bool>(true);
    }
    seastar::future<QueryPartial> linearizableRead(QuerySpec spec) override {
        QueryPartial part;
        for (const auto& p : points) {
            if (p.timestamp < spec.startTime || p.timestamp > spec.endTime)
                continue;
            if (spec.method == AggMethod::Raw)
                part.raw.push_back(p);
            else
                part.perSeries[p.series].add(p.value);
        }
        return seastar::make_ready_future<QueryPartial>(std::move(part));
    }
};

// Two series that hash to two DIFFERENT VShards, so a mixed batch must split.
struct TwoSeries {
    SeriesId128 a, b;
    uint16_t va, vb;
};
TwoSeries pickTwo() {
    SeriesId128 a = sid("m,h=A v");
    uint16_t va = timestar::virtualShard(a);
    for (int i = 0;; ++i) {
        SeriesId128 b = sid("m,h=B" + std::to_string(i) + " v");
        uint16_t vb = timestar::virtualShard(b);
        if (vb != va)
            return {a, b, va, vb};
    }
}

seastar::future<> testRoutesEachVShardToItsLeader() {
    TwoSeries t = pickTwo();
    MockLeader la, lb;
    LeaderResolver resolve = [&](uint16_t vs) -> VShardLeader* {
        if (vs == t.va)
            return &la;
        if (vs == t.vb)
            return &lb;
        return nullptr;
    };
    ReplicatedWriteRouter router(resolve);
    co_await router.write({{t.a, 10, 1.0}, {t.b, 20, 2.0}, {t.a, 30, 3.0}});
    // Each leader received exactly its VShard's points.
    EXPECT_EQ(la.points.size(), 2u);
    EXPECT_EQ(lb.points.size(), 1u);

    // A read fans out to both leaders and merges.
    ReplicatedQueryCoordinator coord(resolve, {t.va, t.vb});
    QueryPartial got = co_await coord.query(QuerySpec{0, 1000, AggMethod::Raw, {}});
    EXPECT_EQ(got.raw.size(), 3u);
    // Sorted by timestamp.
    if (got.raw.size() == 3u) {
        EXPECT_EQ(got.raw[0].timestamp, 10u);
        EXPECT_EQ(got.raw[2].timestamp, 30u);
    }

    // A filtered read only touches the needed VShard's leader.
    QueryPartial onlyA = co_await coord.query(QuerySpec{0, 1000, AggMethod::Raw, {t.a}});
    EXPECT_EQ(onlyA.raw.size(), 2u);
}

seastar::future<> testUnassignedVShardRejectsWholeBatch() {
    TwoSeries t = pickTwo();
    MockLeader la;
    // Only VShard va has a leader; vb resolves to nullptr (unassigned).
    LeaderResolver resolve = [&](uint16_t vs) -> VShardLeader* { return vs == t.va ? &la : nullptr; };
    ReplicatedWriteRouter router(resolve);
    bool threw = false;
    try {
        co_await router.write({{t.a, 10, 1.0}, {t.b, 20, 2.0}});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    EXPECT_TRUE(la.points.empty());  // rejected BEFORE any dispatch: no partial write
}

seastar::future<> testStaleLeaderRoutingSurfaces() {
    TwoSeries t = pickTwo();
    MockLeader la, lb;
    lb.amLeader = false;  // vb's resolved facade is not actually the leader
    LeaderResolver resolve = [&](uint16_t vs) -> VShardLeader* {
        if (vs == t.va)
            return &la;
        if (vs == t.vb)
            return &lb;
        return nullptr;
    };
    ReplicatedWriteRouter router(resolve);
    bool threw = false;
    try {
        co_await router.write({{t.a, 10, 1.0}, {t.b, 20, 2.0}});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);  // stale routing surfaced, not silently dropped
}

seastar::future<> testUnfilteredQueryNeedsEveryVShard() {
    TwoSeries t = pickTwo();
    MockLeader la;
    // vb has no leader; an unfiltered query spans all VShards -> QueryIncomplete.
    LeaderResolver resolve = [&](uint16_t vs) -> VShardLeader* { return vs == t.va ? &la : nullptr; };
    ReplicatedQueryCoordinator coord(resolve, {t.va, t.vb});
    bool incomplete = false;
    try {
        co_await coord.query(QuerySpec{0, 1000, AggMethod::Raw, {}});
    } catch (const QueryIncomplete&) {
        incomplete = true;
    }
    EXPECT_TRUE(incomplete);

    // But a query filtered to va's series succeeds (vb not needed).
    co_await la.write(DataCommand{WritePoints{{{t.a, 5, 1.0}}, 0}});
    QueryPartial got = co_await coord.query(QuerySpec{0, 1000, AggMethod::Raw, {t.a}});
    EXPECT_EQ(got.raw.size(), 1u);
}

}  // namespace

TEST(ReplicatedRouterTest, RoutesEachVShardToItsLeader) {
    testRoutesEachVShardToItsLeader().get();
}
TEST(ReplicatedRouterTest, UnassignedVShardRejectsWholeBatch) {
    testUnassignedVShardRejectsWholeBatch().get();
}
TEST(ReplicatedRouterTest, StaleLeaderRoutingSurfaces) {
    testStaleLeaderRoutingSurfaces().get();
}
TEST(ReplicatedRouterTest, UnfilteredQueryNeedsEveryVShard) {
    testUnfilteredQueryNeedsEveryVShard().get();
}
