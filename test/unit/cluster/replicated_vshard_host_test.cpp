// Integration M3: ReplicatedVShardHost manages the per-VShard Raft groups a node
// replicates, over the real Engine. A single-voter VShard group is instantiated,
// propose() commits a ReplicatedCommand on quorum (self), it applies through
// EngineDataStateMachine into the Engine, and is queryable. Proves the per-VShard
// hosting/management layer (the multi-engine RF=3 convergence is proven separately
// in engine_rf3_test).
#include "../../../lib/cluster/integration/replicated_vshard_host.hpp"
#include "../../../lib/core/vshard.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <set>

using namespace timestar;
using namespace timestar::raft;
namespace fs = std::filesystem;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class ReplicatedVShardHostTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

fs::path tmpDir() {
    static std::atomic_uint64_t seq{0};
    auto d = fs::temp_directory_path() /
             ("ts_rvhost_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(d);
    return d;
}
class NoopTransport : public RaftTransport {
public:
    seastar::future<> send(Envelope) override { return seastar::make_ready_future<>(); }
};
data::ReplicatedCommand writeCmd(const std::string& key, double value) {
    data::WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {BASE};
    s.values = std::vector<double>{value};
    data::WriteBatch b;
    b.series = {std::move(s)};
    return data::ReplicatedCommand{std::move(b)};
}
}  // namespace

TEST_F(ReplicatedVShardHostTest, HostsVShardAndReplicatesThroughRaft) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;
        host.addVShard(/*vshard=*/5, /*voters=*/{1}, opts).get();
        EXPECT_EQ(host.vshardCount(), 1u);

        // Drive the group to leadership by ticking it directly (deterministic; the
        // registry's timer-driven ticking is the production path).
        RaftGroup* g = host.group(5);
        ASSERT_NE(g, nullptr);
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());

        // Replicate a write; drive ticks until the commit+apply ack resolves.
        auto f = host.propose(5, writeCmd(buildSeriesKey("temp", {{"host", "h1"}}, "value"), 42.5));
        for (int i = 0; i < 20 && !f.available(); ++i)
            g->tick().get();
        EXPECT_TRUE(f.get());

        // Visible in the real Engine.
        http::HttpQueryHandler h(&*eng);
        QueryRequest q;
        q.aggregation = AggregationMethod::LATEST;
        q.measurement = "temp";
        q.fields = {"value"};
        q.startTime = BASE - 1'000'000'000ULL;
        q.endTime = BASE + 1'000'000'000ULL;
        auto r = h.executeQuery(q).get();
        ASSERT_TRUE(r.success) << r.errorMessage;
        ASSERT_EQ(r.series.size(), 1u);
        EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(r.series[0].fields.at("value").second)[0], 42.5);

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, ProposeBatchSplitsAcrossVShards) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, 1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        // Two series -> their (possibly distinct) VShards; host each so this node
        // leads them.
        const std::string k1 = buildSeriesKey("a", {{"host", "h1"}}, "value");
        const std::string k2 = buildSeriesKey("b", {{"host", "h2"}}, "value");
        std::set<uint16_t> vshards = {timestar::virtualShard(SeriesId128::fromSeriesKey(k1)),
                                      timestar::virtualShard(SeriesId128::fromSeriesKey(k2))};
        for (uint16_t vs : vshards) {
            host.addVShard(vs, {1}, opts).get();
            RaftGroup* g = host.group(vs);
            for (int i = 0; i < 10 && !g->isLeader(); ++i)
                g->tick().get();
            ASSERT_TRUE(g->isLeader()) << "vshard " << vs;
        }

        data::WriteBatch batch;
        auto mk = [](const std::string& key, double v) {
            data::WriteSeries s;
            s.seriesKey = key;
            s.type = TSMValueType::Float;
            s.timestamps = {BASE};
            s.values = std::vector<double>{v};
            return s;
        };
        batch.series = {mk(k1, 10.0), mk(k2, 20.0)};

        auto f = host.proposeBatch(std::move(batch));
        for (int i = 0; i < 60 && !f.available(); ++i)
            for (uint16_t vs : vshards)
                host.group(vs)->tick().get();
        EXPECT_TRUE(f.get());

        http::HttpQueryHandler h(&*eng);
        auto latest = [&](const std::string& m) {
            QueryRequest q;
            q.aggregation = AggregationMethod::LATEST;
            q.measurement = m;
            q.fields = {"value"};
            q.startTime = BASE - 1'000'000'000ULL;
            q.endTime = BASE + 1'000'000'000ULL;
            auto r = h.executeQuery(q).get();
            return (r.success && !r.series.empty())
                       ? std::get<std::vector<double>>(r.series[0].fields.at("value").second)[0]
                       : -1.0;
        };
        EXPECT_DOUBLE_EQ(latest("a"), 10.0);
        EXPECT_DOUBLE_EQ(latest("b"), 20.0);

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}
