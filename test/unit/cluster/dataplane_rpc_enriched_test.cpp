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

        // (1) Non-numeric read over the socket == the string written.
        {
            data::NodeQueryRequest req;
            req.request.aggregation = AggregationMethod::LATEST;
            req.request.measurement = "log";
            req.request.fields = {"msg"};
            req.request.startTime = BASE - 1'000'000'000ULL;
            req.request.endTime = BASE + 1'000'000'000ULL;

            data::NodeQueryPartial viaSocket = rpc.queryNode(self, req).get();
            data::NodeQueryPartial inProcess = store.queryLocal(req).get();

            EXPECT_TRUE(viaSocket.incompleteReasons.empty());
            ASSERT_EQ(viaSocket.series.size(), 1u);
            ASSERT_EQ(inProcess.series.size(), 1u);
            EXPECT_EQ(stringsOf(viaSocket.series[0], "msg"), (std::vector<std::string>{"over the wire"}));
            // Socket answer is byte-for-byte the in-process answer.
            EXPECT_EQ(stringsOf(viaSocket.series[0], "msg"), stringsOf(inProcess.series[0], "msg"));
        }

        // (2) spread by {region}: max-min across the two series at BASE = 20, and the
        // socket answer equals the in-process answer (the round-trip must not change
        // a fold-of-one-non-identity method).
        {
            data::NodeQueryRequest req;
            req.request.aggregation = AggregationMethod::SPREAD;
            req.request.measurement = "m";
            req.request.fields = {"v"};
            req.request.groupByTags = {"region"};
            req.request.startTime = BASE - 1'000'000'000ULL;
            req.request.endTime = BASE + 1'000'000'000ULL;

            data::NodeQueryPartial viaSocket = rpc.queryNode(self, req).get();
            data::NodeQueryPartial inProcess = store.queryLocal(req).get();

            EXPECT_TRUE(viaSocket.incompleteReasons.empty());
            ASSERT_EQ(viaSocket.series.size(), 1u);
            EXPECT_EQ(viaSocket.series[0].tags.at("region"), "west");
            ASSERT_EQ(doublesOf(viaSocket.series[0], "v").size(), 1u);
            EXPECT_DOUBLE_EQ(doublesOf(viaSocket.series[0], "v")[0], 20.0);
            EXPECT_EQ(doublesOf(viaSocket.series[0], "v"), doublesOf(inProcess.series[0], "v"));
        }

        rpc.stop().get();
    }).get();
}
