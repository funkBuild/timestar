// Integration F.5a: the enriched NodeWriteRouter routes a WriteBatch to each
// series' VShard owner -- local group through the NodeStore, remote groups
// forwarded as WriteBatch RPCs via NodeTransport -- with the same invariants as
// the DataPoint WriteRouter: fail-closed on an unassigned VShard (atomic reject),
// every dispatch awaited, and the union of routed series == the input (nothing
// dropped or duplicated). In-memory doubles (no sockets).
#include "../../../lib/cluster/data/node_write_router.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

using namespace timestar::data;
using timestar::buildSeriesKey;
using timestar::control::ControlMap;
using timestar::raft::NodeId;

namespace {

// Records every WriteSeries applied locally (by seriesKey).
class RecordingNodeStore : public NodeStore {
public:
    std::vector<std::string> appliedKeys;
    seastar::future<> applyWrites(WriteBatch batch) override {
        for (auto& s : batch.series)
            appliedKeys.push_back(s.seriesKey);
        return seastar::make_ready_future<>();
    }
    seastar::future<bool> applyDelete(std::string, uint64_t, uint64_t) override {
        return seastar::make_ready_future<bool>(true);
    }
    seastar::future<NodeQueryPartial> queryLocal(NodeQueryRequest) override {
        return seastar::make_ready_future<NodeQueryPartial>();
    }
};

// Records every WriteSeries forwarded to a peer (by destination node + seriesKey).
class RecordingNodeTransport : public NodeTransport {
public:
    std::map<NodeId, std::vector<std::string>> forwardedKeys;
    bool fail = false;
    seastar::future<> forwardWriteBatch(NodeId to, WriteBatch batch) override {
        if (fail)
            return seastar::make_exception_future<>(std::runtime_error("forward boom"));
        for (auto& s : batch.series)
            forwardedKeys[to].push_back(s.seriesKey);
        return seastar::make_ready_future<>();
    }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
    }
};

ControlMap rf1Map(unsigned n) {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v)
        m.placement[v] = {static_cast<NodeId>((v % n) + 1)};
    return m;
}

WriteSeries floatSeries(const std::string& key) {
    WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {1000};
    s.values = std::vector<double>{1.0};
    return s;
}

WriteBatch manySeries(int n) {
    WriteBatch b;
    for (int i = 0; i < n; ++i)
        b.series.push_back(floatSeries(buildSeriesKey("m", {{"host", "h" + std::to_string(i)}}, "v")));
    return b;
}

seastar::future<> testRoutesByOwner() {
    const unsigned N = 3;
    const ControlMap map = rf1Map(N);
    VShardDirectory dir(1, map);  // self = node 1
    RecordingNodeStore local;
    RecordingNodeTransport client;
    NodeWriteRouter router(dir, local, client);

    WriteBatch batch = manySeries(90);
    // The expected split, computed independently from the router via the directory.
    std::map<NodeId, std::vector<std::string>> expect;
    for (auto& s : batch.series)
        expect[dir.ownerOfSeries(SeriesId128::fromSeriesKey(s.seriesKey))].push_back(s.seriesKey);
    // The split actually spreads across all 3 nodes (else the test proves nothing).
    EXPECT_EQ(expect.size(), 3u);

    co_await router.write(std::move(batch));

    // Local group == node 1's expected series (order-independent).
    auto sortedLocal = local.appliedKeys;
    std::sort(sortedLocal.begin(), sortedLocal.end());
    auto expLocal = expect[1];
    std::sort(expLocal.begin(), expLocal.end());
    EXPECT_EQ(sortedLocal, expLocal);

    // Remote groups == nodes 2 and 3's expected series; node 1 never self-forwards.
    EXPECT_EQ(client.forwardedKeys.count(1), 0u);
    for (NodeId peer : {2, 3}) {
        auto got = client.forwardedKeys[peer];
        std::sort(got.begin(), got.end());
        auto exp = expect[peer];
        std::sort(exp.begin(), exp.end());
        EXPECT_EQ(got, exp) << "peer " << peer;
    }
}

seastar::future<> testUnassignedRejectsAtomically() {
    ControlMap empty;  // no placement -> every VShard unassigned
    empty.epoch = 1;
    VShardDirectory dir(1, empty);
    RecordingNodeStore local;
    RecordingNodeTransport client;
    NodeWriteRouter router(dir, local, client);

    bool threw = false;
    try {
        co_await router.write(manySeries(10));
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    // Fail-closed BEFORE dispatch: nothing applied locally or forwarded.
    EXPECT_TRUE(local.appliedKeys.empty());
    EXPECT_TRUE(client.forwardedKeys.empty());
}

seastar::future<> testForwardFailurePropagates() {
    const ControlMap map = rf1Map(2);
    VShardDirectory dir(1, map);
    RecordingNodeStore local;
    RecordingNodeTransport client;
    client.fail = true;  // remote forwards fail
    NodeWriteRouter router(dir, local, client);

    bool threw = false;
    try {
        co_await router.write(manySeries(20));
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "a failed forward must propagate";
    // The local group still applied (no cross-node atomic commit -- documented).
    EXPECT_FALSE(local.appliedKeys.empty());
}

}  // namespace

TEST(NodeWriteRouterTest, RoutesEachSeriesToItsOwner) { testRoutesByOwner().get(); }
TEST(NodeWriteRouterTest, UnassignedVShardRejectsAtomically) { testUnassignedRejectsAtomically().get(); }
TEST(NodeWriteRouterTest, ForwardFailurePropagates) { testForwardFailurePropagates().get(); }
