// Integration F.4 gate: the enriched data-plane transport (WriteBatch / queryNode)
// driven over a REAL loopback seastar::rpc socket into a REAL sharded<Engine> via
// EngineLocalStore. Proves the lossless command path survives the wire end to end:
//   - a WriteBatch (incl. a STRING series the flat DataPoint could not carry) is
//     forwarded over the socket and durably applied into the Engine, and
//   - queryNode returns the owner's NodeQueryPartial, byte-for-byte equal to the
//     in-process EngineLocalStore answer -- for a non-numeric (string) read and a
//     cross-series `spread ... by {tag}` aggregation (a method whose fold-of-one is
//     not the identity, so it must survive the round-trip through real aggregation).
#include "../../../lib/cluster/data/dataplane_rpc.hpp"
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>
#include <seastar/net/socket_defs.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class DataPlaneRpcEnrichedTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

seastar::socket_address loopback(uint16_t port) {
    return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", port));
}

data::WriteSeries series(const std::string& m, std::map<std::string, std::string> tags, const std::string& field,
                         TSMValueType type, std::vector<uint64_t> ts,
                         std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>,
                                      std::vector<std::string>>
                             vals) {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey(m, tags, field);
    s.type = type;
    s.timestamps = std::move(ts);
    s.values = std::move(vals);
    return s;
}

// A one-value extract of a field's double column (for the spread assertion).
std::vector<double> doublesOf(const timestar::http::SeriesResult& r, const std::string& field) {
    auto it = r.fields.find(field);
    if (it == r.fields.end())
        return {};
    auto* v = std::get_if<std::vector<double>>(&it->second.second);
    return v ? *v : std::vector<double>{};
}
std::vector<std::string> stringsOf(const timestar::http::SeriesResult& r, const std::string& field) {
    auto it = r.fields.find(field);
    if (it == r.fields.end())
        return {};
    auto* v = std::get_if<std::vector<std::string>>(&it->second.second);
    return v ? *v : std::vector<std::string>{};
}

// A NodeStore whose apply always fails -- to prove a failed owner-apply surfaces
// to the forwarding caller as a failed RPC (fail-closed), never a silent ack.
class ThrowingNodeStore : public data::NodeStore {
public:
    seastar::future<> applyWrites(data::WriteBatch) override {
        return seastar::make_exception_future<>(std::runtime_error("apply boom"));
    }
    seastar::future<bool> applyDelete(std::string, uint64_t, uint64_t) override {
        return seastar::make_exception_future<bool>(std::runtime_error("delete boom"));
    }
    seastar::future<data::NodeQueryPartial> queryLocal(data::NodeQueryRequest) override {
        return seastar::make_exception_future<data::NodeQueryPartial>(std::runtime_error("query boom"));
    }
};

// Minimal legacy DataPoint sink, so a node can be started on the LEGACY path and
// we can prove an enriched verb sent to it fails cleanly (unknown verb), not hangs.
class LegacyMemStore : public data::LocalStore {
public:
    seastar::future<> applyWrites(std::vector<data::DataPoint>) override { return seastar::make_ready_future<>(); }
    seastar::future<data::QueryPartial> queryLocal(data::QuerySpec) override {
        return seastar::make_ready_future<data::QueryPartial>();
    }
};

data::WriteBatch oneFloatBatch() {
    data::WriteBatch b;
    b.series.push_back(
        series("m", {{"host", "h1"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{1.0}));
    return b;
}
}  // namespace

TEST_F(DataPlaneRpcEnrichedTest, WriteBatchAndQueryNodeOverRealSocket) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        // One transport as both owner-server and self-client: addPeer(self) loops a
        // forwardWriteBatch/queryNode back over its own loopback socket -- a genuine
        // wire round-trip through a single real Engine (no second-Engine shard-dir
        // collision).
        const uint16_t port = 39310;
        const data::NodeId self = 1;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        // A batch spanning a STRING series (DataPoint could not carry it) and two
        // FLOAT series sharing region=west at the SAME timestamp, so a cross-series
        // `spread by {region}` has a non-trivial answer (max-min = 20).
        data::WriteBatch batch;
        batch.series.push_back(
            series("log", {{"host", "h1"}}, "msg", TSMValueType::String, {BASE}, std::vector<std::string>{"over the wire"}));
        batch.series.push_back(
            series("m", {{"host", "h1"}, {"region", "west"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{10.0}));
        batch.series.push_back(
            series("m", {{"host", "h2"}, {"region", "west"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{30.0}));

        // Forward over the socket; resolves only after the owner durably applied.
        rpc.forwardWriteBatch(self, batch).get();

        http::HttpQueryHandler handler(&*eng);
        // queryNode returns UNFINALIZED partials; the coordinator finalizes them. We
        // finalize here and assert the socket-shipped partials produce the same
        // answer as executeQuery would -- the real F.5b cluster query round-trip.
        auto finalizeSocket = [&](data::NodeQueryRequest req) {
            data::NodeQueryPartial viaSocket = rpc.queryNode(self, req).get();
            EXPECT_TRUE(viaSocket.incompleteReasons.empty());
            return handler
                .finalizeClusterPartials(req.request, std::move(viaSocket.partials), std::move(viaSocket.nonNumeric))
                .get();
        };

        // (1) Non-numeric read over the socket == the string written.
        {
            data::NodeQueryRequest req;
            req.request.aggregation = AggregationMethod::LATEST;
            req.request.measurement = "log";
            req.request.fields = {"msg"};
            req.request.startTime = BASE - 1'000'000'000ULL;
            req.request.endTime = BASE + 1'000'000'000ULL;

            QueryResponse got = finalizeSocket(req);
            QueryResponse direct = handler.executeQuery(req.request).get();
            ASSERT_TRUE(got.success) << got.errorMessage;
            ASSERT_EQ(got.series.size(), 1u);
            EXPECT_EQ(stringsOf(got.series[0], "msg"), (std::vector<std::string>{"over the wire"}));
            ASSERT_EQ(direct.series.size(), 1u);
            EXPECT_EQ(stringsOf(got.series[0], "msg"), stringsOf(direct.series[0], "msg"));
        }

        // (2) spread by {region}: max-min across the two series at BASE = 20, over the
        // wire -- the cross-series/cross-node fold-of-one-non-identity method that
        // finalized SeriesResult partials could NOT express.
        {
            data::NodeQueryRequest req;
            req.request.aggregation = AggregationMethod::SPREAD;
            req.request.measurement = "m";
            req.request.fields = {"v"};
            req.request.groupByTags = {"region"};
            req.request.startTime = BASE - 1'000'000'000ULL;
            req.request.endTime = BASE + 1'000'000'000ULL;

            QueryResponse got = finalizeSocket(req);
            ASSERT_TRUE(got.success) << got.errorMessage;
            ASSERT_EQ(got.series.size(), 1u);
            EXPECT_EQ(got.series[0].tags.at("region"), "west");
            ASSERT_EQ(doublesOf(got.series[0], "v").size(), 1u);
            EXPECT_DOUBLE_EQ(doublesOf(got.series[0], "v")[0], 20.0);
        }

        rpc.stop().get();
    }).get();
}

// Fail-closed: a failed owner-apply propagates to the forwarding caller as a
// failed RPC, never a silent success (the plan's "no silent partial" contract).
TEST_F(DataPlaneRpcEnrichedTest, OwnerApplyFailureSurfacesToCaller) {
    seastar::async([] {
        const uint16_t port = 39311;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), sink).get();
        rpc.addPeer(self, loopback(port));

        bool threw = false;
        try {
            rpc.forwardWriteBatch(self, oneFloatBatch()).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "a failed owner-apply must fail the forwarding RPC";
        rpc.stop().get();
    }).get();
}

// Coexistence safety: an enriched verb sent to a peer serving only the LEGACY
// path fails cleanly (unknown verb) rather than hanging forever on a waited call.
TEST_F(DataPlaneRpcEnrichedTest, EnrichedVerbToLegacyPeerFailsCleanly) {
    seastar::async([] {
        const uint16_t serverPort = 39312, clientPort = 39313;
        const data::NodeId server = 1;
        LegacyMemStore legacy;
        ThrowingNodeStore unused;  // the client's own sink (never invoked here)
        data::DataPlaneRpc srv, cli;
        srv.start(loopback(serverPort), legacy).get();   // legacy path only (verbs 1/2)
        cli.start(loopback(clientPort), unused).get();   // enriched path
        cli.addPeer(server, loopback(serverPort));

        bool threw = false;
        try {
            cli.forwardWriteBatch(server, oneFloatBatch()).get();  // verb 3 -> unknown on srv
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "enriched verb to a legacy-only peer must fail, not hang";
        cli.stop().get();
        srv.stop().get();
    }).get();
}

// start() is not idempotent -- a second call throws loudly before mutating state.
TEST_F(DataPlaneRpcEnrichedTest, StartTwiceThrows) {
    seastar::async([] {
        const uint16_t port = 39314;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), sink).get();
        bool threw = false;
        try {
            rpc.start(loopback(port), sink).get();
        } catch (const std::logic_error&) {
            threw = true;
        }
        EXPECT_TRUE(threw);
        rpc.stop().get();
    }).get();
}
