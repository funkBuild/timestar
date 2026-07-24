// Integration M3: ReplicatedBatchWriteRouter routes a WriteBatch to each VShard's LEADER
// (local ProposeSink vs remote proposeWrite), commits on quorum, and fails the whole
// write (for the caller to retry) if a leader is stale or a VShard is unassigned.
// In-memory doubles (no sockets/Raft).
#include "../../../lib/cluster/data/replicated_write_router.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

using namespace timestar::data;
using timestar::buildSeriesKey;
using timestar::control::ControlMap;
using timestar::raft::NodeId;

namespace {

// Records batches proposed locally (this node leads these VShards).
class LocalSink : public ProposeSink {
public:
    std::vector<std::string> keys;
    bool committed = true;
    seastar::future<bool> proposeBatch(WriteBatch batch) override {
        for (auto& s : batch.series)
            keys.push_back(s.seriesKey);
        return seastar::make_ready_future<bool>(committed);
    }
};

// Records batches forwarded to remote leaders via proposeWrite.
class RemoteTransport : public NodeTransport {
public:
    std::map<NodeId, std::vector<std::string>> keys;
    bool committed = true;
    seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
    }
    seastar::future<bool> proposeWrite(NodeId to, WriteBatch batch) override {
        for (auto& s : batch.series)
            keys[to].push_back(s.seriesKey);
        return seastar::make_ready_future<bool>(committed);
    }
};

ControlMap rf3Map(unsigned n) {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v) {
        std::vector<NodeId> reps;
        for (uint16_t k = 0; k < 3; ++k)
            reps.push_back(static_cast<NodeId>(((v + k) % n) + 1));
        m.placement[v] = std::move(reps);  // primary = reps[0] = leader hint
    }
    return m;
}

WriteBatch manySeries(int n) {
    WriteBatch b;
    for (int i = 0; i < n; ++i) {
        WriteSeries s;
        s.seriesKey = buildSeriesKey("m", {{"host", "h" + std::to_string(i)}}, "v");
        s.type = TSMValueType::Float;
        s.timestamps = {1000};
        s.values = std::vector<double>{1.0};
        b.series.push_back(std::move(s));
    }
    return b;
}

seastar::future<> testRoutesToLeaders() {
    const unsigned N = 3;
    VShardDirectory dir(1, rf3Map(N));  // self = node 1
    LocalSink local;
    RemoteTransport client;
    ReplicatedBatchWriteRouter router(dir, local, client);

    WriteBatch batch = manySeries(90);
    // Expected split by leader (primary), computed independently from the directory.
    std::map<NodeId, std::vector<std::string>> expect;
    for (auto& s : batch.series) {
        uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(s.seriesKey));
        expect[dir.ownerOf(vs)].push_back(s.seriesKey);
    }
    EXPECT_EQ(expect.size(), 3u);  // leaders spread across all 3 nodes

    co_await router.write(std::move(batch));

    // Local (node 1) group replicated through the local sink.
    auto le = expect[1];
    std::sort(le.begin(), le.end());
    std::sort(local.keys.begin(), local.keys.end());
    EXPECT_EQ(local.keys, le);
    // Remote groups forwarded to their leaders (2 and 3); node 1 never self-forwards.
    EXPECT_EQ(client.keys.count(1), 0u);
    for (NodeId peer : {2, 3}) {
        auto got = client.keys[peer];
        std::sort(got.begin(), got.end());
        auto exp = expect[peer];
        std::sort(exp.begin(), exp.end());
        EXPECT_EQ(got, exp) << "leader " << peer;
    }
}

seastar::future<> testStaleLeaderFailsWrite() {
    VShardDirectory dir(1, rf3Map(3));
    LocalSink local;
    local.committed = false;  // local leader stale (not actually leader)
    RemoteTransport client;
    ReplicatedBatchWriteRouter router(dir, local, client);
    bool threw = false;
    try {
        co_await router.write(manySeries(30));
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "a stale leader must fail the write for retry";
}

seastar::future<> testUnassignedRejects() {
    ControlMap empty;
    empty.epoch = 1;  // no placement -> every VShard unassigned
    VShardDirectory dir(1, empty);
    LocalSink local;
    RemoteTransport client;
    ReplicatedBatchWriteRouter router(dir, local, client);
    bool threw = false;
    try {
        co_await router.write(manySeries(10));
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    EXPECT_TRUE(local.keys.empty());   // nothing replicated
    EXPECT_TRUE(client.keys.empty());
}

}  // namespace

TEST(ReplicatedBatchWriteRouterTest, RoutesEachSeriesToItsVShardLeader) { testRoutesToLeaders().get(); }
TEST(ReplicatedBatchWriteRouterTest, StaleLeaderFailsWholeWrite) { testStaleLeaderFailsWrite().get(); }
TEST(ReplicatedBatchWriteRouterTest, UnassignedVShardRejects) { testUnassignedRejects().get(); }
