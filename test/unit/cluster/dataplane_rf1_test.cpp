// Phase 4 gate: static multi-node RF=1 behaviour MATCHES single-node results.
// A set of writes is distributed across N nodes by VShard owner; a scatter-gather
// query merges the per-node partials and must equal what one node holding ALL the
// data returns -- for raw reads and every aggregation. In-memory stores/client
// (no sockets); integer values so sums are order-exact (FP-associativity is an
// aggregator concern, not a routing one).
#include "../../../lib/cluster/data/query_coordinator.hpp"
#include "../../../lib/cluster/data/write_router.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <vector>

using namespace timestar::data;
using timestar::control::ControlMap;
using timestar::raft::NodeId;
// SeriesId128 is in the global namespace.

namespace {

class MemStore : public LocalStore {
public:
    std::vector<DataPoint> points;
    seastar::future<> applyWrites(std::vector<DataPoint> pts) override {
        for (auto& p : pts)
            points.push_back(p);
        return seastar::make_ready_future<>();
    }
    seastar::future<QueryPartial> queryLocal(QuerySpec spec) override {
        QueryPartial part;
        std::set<SeriesId128> filter(spec.series.begin(), spec.series.end());
        for (const auto& p : points) {
            if (p.timestamp < spec.startTime || p.timestamp > spec.endTime)
                continue;
            if (!filter.empty() && filter.count(p.series) == 0)
                continue;
            if (spec.method == AggMethod::Raw)
                part.raw.push_back(p);
            else
                part.perSeries[p.series].add(p.value);
        }
        return seastar::make_ready_future<QueryPartial>(std::move(part));
    }
};

class MemClient : public DataPlaneClient {
public:
    std::map<NodeId, MemStore*> stores;
    seastar::future<> forwardWrites(NodeId to, std::vector<DataPoint> pts) override {
        return stores.at(to)->applyWrites(std::move(pts));
    }
    seastar::future<QueryPartial> queryRemote(NodeId to, QuerySpec spec) override {
        return stores.at(to)->queryLocal(std::move(spec));
    }
};

// RF=1 control map: VShard v -> node (v % N)+1, for all 4096 VShards.
ControlMap rf1Map(unsigned n) {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v)
        m.placement[v] = {static_cast<NodeId>((v % n) + 1)};
    return m;
}

std::vector<DataPoint> makePoints() {
    std::vector<DataPoint> pts;
    for (int s = 0; s < 60; ++s) {
        SeriesId128 id = SeriesId128::fromSeriesKey("m,host=h" + std::to_string(s) + " v");
        for (int t = 0; t < 10; ++t)
            pts.push_back({id, static_cast<uint64_t>(1000 + t * 100), static_cast<double>(s + t)});
    }
    return pts;
}

void sortRaw(std::vector<DataPoint>& v) {
    std::sort(v.begin(), v.end(), [](const DataPoint& a, const DataPoint& b) {
        if (a.timestamp != b.timestamp)
            return a.timestamp < b.timestamp;
        return a.series < b.series;
    });
}

bool rawEqual(std::vector<DataPoint> a, std::vector<DataPoint> b) {
    sortRaw(a);
    sortRaw(b);
    if (a.size() != b.size())
        return false;
    for (size_t i = 0; i < a.size(); ++i)
        if (!(a[i].series == b[i].series) || a[i].timestamp != b[i].timestamp || a[i].value != b[i].value)
            return false;
    return true;
}

seastar::future<> testRf1MatchesSingleNode() {
    const unsigned N = 3;
    const std::vector<DataPoint> points = makePoints();

    // --- Single-node reference: one store holds everything. ---
    MemStore ref;
    co_await ref.applyWrites(points);

    // --- Multi-node RF=1 cluster. ---
    ControlMap map = rf1Map(N);
    std::vector<std::unique_ptr<MemStore>> stores;
    MemClient client;
    for (NodeId id = 1; id <= N; ++id) {
        stores.push_back(std::make_unique<MemStore>());
        client.stores[id] = stores.back().get();
    }
    // Route the writes through node 1 (any node can accept and route by owner).
    VShardDirectory dir1(1, map);
    WriteRouter router(dir1, *stores[0], client);
    co_await router.write(points);

    // Writes actually spread across nodes (not all local).
    size_t nonEmpty = 0;
    for (auto& s : stores)
        if (!s->points.empty())
            ++nonEmpty;
    EXPECT_EQ(nonEmpty, N);

    // Query from node 2's coordinator (a different node than the writer).
    VShardDirectory dir2(2, map);
    QueryCoordinator coord(dir2, *stores[1], client);

    const uint64_t start = 0, end = 100000;
    // Raw read == single-node raw read.
    {
        QuerySpec spec{start, end, AggMethod::Raw, {}};
        QueryPartial got = co_await coord.query(spec);
        QueryPartial refPart = co_await ref.queryLocal(spec);
        EXPECT_EQ(got.raw.size(), points.size());
        EXPECT_TRUE(rawEqual(got.raw, refPart.raw));
    }
    // Every aggregation matches single-node per-series.
    for (AggMethod m : {AggMethod::Count, AggMethod::Sum, AggMethod::Avg, AggMethod::Min, AggMethod::Max}) {
        QuerySpec spec{start, end, m, {}};
        QueryPartial got = co_await coord.query(spec);
        QueryPartial refPart = co_await ref.queryLocal(spec);
        EXPECT_EQ(got.perSeries.size(), refPart.perSeries.size());
        EXPECT_EQ(got.perSeries, refPart.perSeries) << "method " << static_cast<int>(m);
    }
}

seastar::future<> testUnassignedVShardRejectsWrite() {
    ControlMap map;  // empty placement -> every VShard unassigned
    map.epoch = 1;
    MemStore store;
    MemClient client;
    client.stores[1] = &store;
    VShardDirectory dir(1, map);
    WriteRouter router(dir, store, client);

    std::vector<DataPoint> pts = {{SeriesId128::fromSeriesKey("m,h=1 v"), 5, 1.0}};
    bool threw = false;
    try {
        co_await router.write(pts);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    EXPECT_TRUE(store.points.empty());  // no partial write
}

seastar::future<> testQueryIncompleteOnUnassignedVShard() {
    // A partial control map (only VShard 0 assigned) -> an unfiltered query must
    // fail with QueryIncomplete rather than return a silent empty success.
    ControlMap map;
    map.epoch = 1;
    map.placement[0] = {1};
    MemStore store;
    MemClient client;
    client.stores[1] = &store;
    VShardDirectory dir(1, map);
    QueryCoordinator coord(dir, store, client);

    bool incomplete = false;
    try {
        co_await coord.query(QuerySpec{0, 100000, AggMethod::Raw, {}});
    } catch (const QueryIncomplete&) {
        incomplete = true;
    }
    EXPECT_TRUE(incomplete);

    // A query filtered to a series whose VShard IS assigned succeeds.
    SeriesId128 s0;
    for (int i = 0;; ++i) {  // find a series that hashes to VShard 0
        SeriesId128 cand = SeriesId128::fromSeriesKey("m,h=" + std::to_string(i) + " v");
        if (timestar::virtualShard(cand) == 0) {
            s0 = cand;
            break;
        }
    }
    co_await store.applyWrites({{s0, 5, 1.0}});
    QueryPartial got = co_await coord.query(QuerySpec{0, 100000, AggMethod::Raw, {s0}});
    EXPECT_EQ(got.raw.size(), 1u);
}

}  // namespace

TEST(DataPlaneRf1Test, MultiNodeMatchesSingleNode) {
    testRf1MatchesSingleNode().get();
}

TEST(DataPlaneRf1Test, QueryIncompleteOnUnassignedVShard) {
    testQueryIncompleteOnUnassignedVShard().get();
}

TEST(DataPlaneRf1Test, UnassignedVShardRejectsWrite) {
    testUnassignedVShardRejectsWrite().get();
}
