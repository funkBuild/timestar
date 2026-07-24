// Integration F.5b (stage 2b): the enriched NodeQueryCoordinator. Proves the
// CROSS-NODE property finalized SeriesResult partials could not express: two nodes
// each holding one series of region=west contribute unfinalized partials, and the
// coordinator merges them into a single `spread by {region}` = 20 -- the single-node
// answer. Also proves fail-closed: a node reporting incompleteReasons fails the
// whole query. Uses realistic partials from the REAL Aggregator::createPartialAggregations
// + a real (empty) Engine as the finalizer; the two "nodes" are in-memory doubles.
#include "../../../lib/cluster/data/node_query_coordinator.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar::data;
using timestar::AggregationMethod;
using timestar::control::ControlMap;
using timestar::FieldValues;
using timestar::PartialAggregationResult;
using timestar::QueryRequest;
using timestar::raft::NodeId;
using timestar::http::SeriesResult;

namespace {

// A NodeStore/NodeTransport double returning a fixed partial (copied per call).
class Canned : public NodeStore, public NodeTransport {
public:
    NodeQueryPartial answer;
    seastar::future<> applyWrites(WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<bool> applyDelete(std::string, uint64_t, uint64_t) override {
        return seastar::make_ready_future<bool>(true);
    }
    seastar::future<NodeQueryPartial> queryLocal(NodeQueryRequest) override {
        return seastar::make_ready_future<NodeQueryPartial>(answer);
    }
    seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_ready_future<NodeQueryPartial>(answer);
    }
};

ControlMap rf1Map(unsigned n) {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v)
        m.placement[v] = {static_cast<NodeId>((v % n) + 1)};
    return m;
}

// Build one node's realistic partials via the real producer, from a single-series
// slice (region=west, given host + value at ts=5), spread grouped by region.
std::vector<PartialAggregationResult> slicePartials(const std::string& host, double value) {
    SeriesResult s;
    s.measurement = "m";
    s.tags = {{"region", "west"}, {"host", host}};
    s.fields["v"] = {{5}, FieldValues{std::vector<double>{value}}};
    return timestar::Aggregator::createPartialAggregations({s}, AggregationMethod::SPREAD, /*interval=*/0,
                                                           /*groupByTags=*/{"region"})
        .get();
}

QueryRequest spreadByRegion() {
    QueryRequest q;
    q.aggregation = AggregationMethod::SPREAD;
    q.measurement = "m";
    q.fields = {"v"};
    q.groupByTags = {"region"};
    q.startTime = 0;
    q.endTime = 100;
    return q;
}

class NodeQueryCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};
}  // namespace

TEST_F(NodeQueryCoordinatorTest, CrossNodeGroupBySpreadMergesToSingleNodeAnswer) {
    seastar::async([] {
        ScopedShardedEngine eng;  // empty; only the finalizer
        eng.start();
        timestar::http::HttpQueryHandler finalizer(&*eng);

        Canned node1, node2;  // node1 = self (local), node2 = peer (transport)
        node1.answer.partials = slicePartials("h1", 10.0);
        node2.answer.partials = slicePartials("h2", 30.0);

        VShardDirectory dir(1, rf1Map(2));  // self=1; owners {1,2}
        NodeQueryCoordinator coord(dir, node1, node2, finalizer);

        QueryResponse resp = coord.query(spreadByRegion()).get();
        ASSERT_TRUE(resp.success) << resp.errorMessage;
        ASSERT_EQ(resp.series.size(), 1u);
        EXPECT_EQ(resp.series[0].tags.at("region"), "west");
        auto& f = resp.series[0].fields.at("v");
        auto* v = std::get_if<std::vector<double>>(&f.second);
        ASSERT_NE(v, nullptr);
        ASSERT_EQ(v->size(), 1u);
        // spread across the two NODES' series at ts=5 = 30 - 10 = 20.
        EXPECT_DOUBLE_EQ((*v)[0], 20.0);
    }).get();
}

TEST_F(NodeQueryCoordinatorTest, NodeIncompleteFailsTheWholeQuery) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        timestar::http::HttpQueryHandler finalizer(&*eng);

        Canned node1, node2;
        node1.answer.partials = slicePartials("h1", 10.0);
        node2.answer.incompleteReasons.push_back("vshard 7 unreachable");  // fail-closed

        VShardDirectory dir(1, rf1Map(2));
        NodeQueryCoordinator coord(dir, node1, node2, finalizer);

        QueryResponse resp = coord.query(spreadByRegion()).get();
        EXPECT_FALSE(resp.success);
        EXPECT_EQ(resp.errorCode, "QUERY_INCOMPLETE");
    }).get();
}

TEST_F(NodeQueryCoordinatorTest, UnionSeriesCountEnforcesMaxSeriesCount) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        timestar::http::HttpQueryHandler finalizer(&*eng);

        // Each node individually is under the limit, but their union exceeds it -- a
        // cluster must fail exactly like a single node holding all the series.
        const uint64_t limit = timestar::http::HttpQueryHandler::maxSeriesCount();
        Canned node1, node2;
        node1.answer.partials = slicePartials("h1", 10.0);
        node1.answer.seriesFound = limit;  // at the limit alone
        node2.answer.partials = slicePartials("h2", 30.0);
        node2.answer.seriesFound = limit;  // union = 2*limit > limit

        VShardDirectory dir(1, rf1Map(2));
        NodeQueryCoordinator coord(dir, node1, node2, finalizer);

        QueryResponse resp = coord.query(spreadByRegion()).get();
        EXPECT_FALSE(resp.success);
        EXPECT_EQ(resp.errorCode, "TOO_MANY_SERIES");
    }).get();
}

TEST_F(NodeQueryCoordinatorTest, UnassignedVShardIsIncomplete) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        timestar::http::HttpQueryHandler finalizer(&*eng);

        Canned node1, node2;
        ControlMap partial;  // only VShard 0 assigned -> the rest unassigned
        partial.epoch = 1;
        partial.placement[0] = {1};
        VShardDirectory dir(1, partial);
        NodeQueryCoordinator coord(dir, node1, node2, finalizer);

        QueryResponse resp = coord.query(spreadByRegion()).get();
        EXPECT_FALSE(resp.success);
        EXPECT_EQ(resp.errorCode, "QUERY_INCOMPLETE");
    }).get();
}
