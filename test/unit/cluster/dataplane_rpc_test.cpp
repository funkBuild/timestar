#include "../../../lib/cluster/data/dataplane_rpc.hpp"
#include "../../../lib/cluster/data/write_router.hpp"
#include "../../../lib/cluster/data/query_coordinator.hpp"
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/net/socket_defs.hh>
#include <vector>
using namespace timestar::data;
using timestar::control::ControlMap;
using timestar::raft::NodeId;
namespace {
seastar::socket_address loopback(uint16_t port) {
    return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", port));
}
// A LocalStore that actually stores forwarded points and answers queries from
// them, so a query fanned out over RPC returns real per-series partials.
class MemStore : public LocalStore {
public:
    std::vector<DataPoint> points;
    seastar::future<> applyWrites(std::vector<DataPoint> pts) override {
        for (auto& p : pts) points.push_back(p);
        return seastar::make_ready_future<>();
    }
    seastar::future<QueryPartial> queryLocal(QuerySpec spec) override {
        QueryPartial part;
        for (const auto& p : points) {
            if (p.timestamp < spec.startTime || p.timestamp > spec.endTime) continue;
            part.perSeries[p.series].add(p.value);
        }
        return seastar::make_ready_future<QueryPartial>(std::move(part));
    }
};
ControlMap rf1Map(unsigned n) {
    ControlMap m; m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v) m.placement[v] = {static_cast<NodeId>((v % n) + 1)};
    return m;
}
// End-to-end over real loopback sockets: write ANY-node -> routed to VShard
// owners; query fanned out from a DIFFERENT node -> merged partial == the whole
// dataset, matching a single node. Node 2 is the coordinator on purpose: it only
// received inbound forwards during the write phase, so its query fan-out opens
// fresh outbound connections concurrently -- the case eager peer-connect fixes.
seastar::future<> test3f() {
    const unsigned N = 3;
    const std::map<NodeId, uint16_t> ports = {{1, 39290}, {2, 39291}, {3, 39292}};
    const ControlMap map = rf1Map(N);
    std::vector<std::unique_ptr<MemStore>> stores;
    std::vector<std::unique_ptr<DataPlaneRpc>> rpcs;
    for (NodeId id = 1; id <= N; ++id) {
        stores.push_back(std::make_unique<MemStore>());
        rpcs.push_back(std::make_unique<DataPlaneRpc>());
    }
    for (NodeId id = 1; id <= N; ++id)
        co_await rpcs[id - 1]->start(loopback(ports.at(id)), *stores[id - 1]);
    for (NodeId id = 1; id <= N; ++id)
        for (NodeId peer = 1; peer <= N; ++peer)
            if (peer != id) rpcs[id - 1]->addPeer(peer, loopback(ports.at(peer)));
    std::vector<DataPoint> points;
    for (int s = 0; s < 30; ++s)
        points.push_back({SeriesId128::fromSeriesKey("m,h=" + std::to_string(s) + " v"), 5, 1.0});

    // WRITE from node 1: local apply for node 1's VShards, RPC-forward for the rest.
    VShardDirectory dir1(1, map);
    WriteRouter router(dir1, *stores[0], *rpcs[0]);
    co_await router.write(points);
    size_t spread = 0;
    for (auto& s : stores) if (!s->points.empty()) ++spread;
    EXPECT_EQ(spread, N);

    // QUERY from node 2: local for node 2's VShards, RPC fan-out to nodes 1 and 3.
    VShardDirectory dir2(2, map);
    QueryCoordinator coord(dir2, *stores[1], *rpcs[1]);
    QueryPartial got = co_await coord.query(QuerySpec{0, 100000, AggMethod::Sum, {}});
    EXPECT_EQ(got.perSeries.size(), 30u);

    std::vector<seastar::future<>> stops;
    for (auto& r : rpcs) stops.push_back(r->stop());
    for (auto& f : stops) co_await std::move(f);
}
}  // namespace
TEST(DataPlaneRpcTest, WriteForwardAndQueryFanoutOverRpc) { test3f().get(); }
