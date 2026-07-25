// Write scale-out Phase 1: the PER-SHARD data plane (ShardRaftPlane + its own
// DataPlaneRpc listener/peer clients). These are real-socket tests -- they live in the
// cluster socket suite, not the unit suite.
//
// What they pin, and what breaks without the production code they cover:
//
//  1. PerShardListenersDistributeInboundConnections -- every shard starts a
//     DataPlaneRpc on ONE address with perShardListener=true, and inbound connections
//     must actually be served by more than one shard. This seastar hardcodes
//     posix_reuseport_available() to false, so "each shard listens" does NOT give each
//     shard a socket: shard 0 owns the only socket and hands each accepted fd to the
//     shard the listen options name. With set_fixed_cpu (perShardListener=false) that
//     is always shard 0 and the other shards' listeners serve nothing -- silently, with
//     every request still answered correctly. Only a shard-identity assertion catches it.
//
//  2. ProposeOverSocketSplitsAcrossOwningShardsAndFailsCleanlyDuringStop -- an inbound
//     peer proposeWrite arrives on an arbitrary shard and must be split across the
//     shards that OWN its VShards' Raft groups, committed, and visible in the Engine.
//     Then, with one shard's plane already torn down (sharded<>::stop() runs every
//     shard's stop() concurrently, so this is not a contrived state), a write routed to
//     that shard must raise a RETRYABLE error rather than dereference a null plane_ and
//     take the node down mid-rolling-restart.
#include "../../../lib/cluster/integration/shard_raft_plane.hpp"

#include "../../../lib/cluster/data/dataplane_rpc.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <set>
#include <string>
#include <vector>

using namespace timestar;
using timestar::control::ControlMap;
namespace fs = std::filesystem;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

fs::path tmpDir(const std::string& t) {
    static std::atomic_uint64_t seq{0};
    auto d = fs::temp_directory_path() /
             ("ts_srp_" + t + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(d);
    return d;
}

seastar::socket_address loopback(uint16_t port) {
    return seastar::socket_address(seastar::net::inet_address("127.0.0.1"), port);
}

data::WriteSeries floatSeries(const std::string& key, double v, uint64_t ts = BASE) {
    data::WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {ts};
    s.values = std::vector<double>{v};
    return s;
}

// Records WHICH shard served each inbound request -- the only way to tell a listener
// that distributes from one that quietly funnels every connection to shard 0.
class RecordingStore : public data::NodeStore {
public:
    size_t served = 0;
    seastar::future<> applyWrites(data::WriteBatch) override {
        ++served;
        return seastar::make_ready_future<>();
    }
    seastar::future<bool> applyDelete(std::string, uint64_t, uint64_t) override {
        return seastar::make_ready_future<bool>(false);
    }
    seastar::future<data::NodeQueryPartial> queryLocal(data::NodeQueryRequest) override {
        return seastar::make_ready_future<data::NodeQueryPartial>();
    }
};

// One shard's listener, so sharded<> can start one per shard on the same address.
class ShardListener {
public:
    seastar::future<> startOn(seastar::socket_address addr, bool perShard) {
        store_ = std::make_unique<RecordingStore>();
        rpc_ = std::make_unique<data::DataPlaneRpc>();
        return rpc_->start(addr, *store_, perShard);
    }
    size_t served() const { return store_ ? store_->served : 0; }
    seastar::future<> stop() {
        if (rpc_)
            co_await rpc_->stop();
        rpc_.reset();
        store_.reset();
        co_return;
    }

private:
    std::unique_ptr<RecordingStore> store_;
    std::unique_ptr<data::DataPlaneRpc> rpc_;
};

class ShardRaftPlaneTest : public ::testing::Test {};
}  // namespace

TEST_F(ShardRaftPlaneTest, PerShardListenersDistributeInboundConnections) {
    if (seastar::smp::count < 2)
        GTEST_SKIP() << "needs >= 2 shards to observe distribution";
    seastar::async([] {
        const seastar::socket_address addr = loopback(18131);

        seastar::sharded<ShardListener> listeners;
        listeners.start().get();
        listeners.invoke_on_all([addr](ShardListener& l) { return l.startOn(addr, /*perShard=*/true); }).get();

        // Each client is one connection. The server-side load balancer places each new
        // connection on the least-loaded shard, so several connections must land on
        // several shards.
        const size_t kConns = std::min<size_t>(8, seastar::smp::count);
        std::vector<std::unique_ptr<data::DataPlaneRpc>> clients;
        for (size_t i = 0; i < kConns; ++i) {
            auto c = std::make_unique<data::DataPlaneRpc>();
            c->addPeer(1, addr);
            c->startClientOnly().get();
            data::WriteBatch b;
            b.series = {floatSeries(buildSeriesKey("m", {{"i", std::to_string(i)}}, "value"), 1.0)};
            c->forwardWriteBatch(1, std::move(b)).get();  // waited: served before it returns
            clients.push_back(std::move(c));
        }

        size_t total = 0, shardsThatServed = 0;
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            const size_t n = listeners.invoke_on(s, [](ShardListener& l) { return l.served(); }).get();
            total += n;
            if (n > 0)
                ++shardsThatServed;
        }
        EXPECT_EQ(total, kConns) << "every forwarded write must be served exactly once";
        EXPECT_GT(shardsThatServed, 1u)
            << "all " << kConns << " connections were served by a single shard -- the per-shard listeners are "
            << "not distributing (reuseport is disabled here, so this needs connection_distribution, "
            << "not set_fixed_cpu)";

        for (auto& c : clients)
            c->stop().get();
        listeners.stop().get();
    }).get();
}

TEST_F(ShardRaftPlaneTest, ProposeOverSocketSplitsAcrossOwningShardsAndFailsCleanlyDuringStop) {
    if (seastar::smp::count < 2)
        GTEST_SKIP() << "needs >= 2 shards to exercise the cross-shard split";
    seastar::async([] {
        const seastar::socket_address addr = loopback(18132);
        const data::NodeId self = 1;

        // Two series whose VShards are owned by DIFFERENT shards, so one inbound batch
        // must be split across shards to commit.
        std::string k1, k2;
        uint16_t vs1 = 0, vs2 = 0;
        for (int i = 0; i < 4096 && k2.empty(); ++i) {
            const std::string key = buildSeriesKey("cpu", {{"host", "h" + std::to_string(i)}}, "value");
            const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
            if (k1.empty()) {
                k1 = key;
                vs1 = vs;
            } else if (vs != vs1 && cluster::shardForVShard(vs) != cluster::shardForVShard(vs1)) {
                k2 = key;
                vs2 = vs;
            }
        }
        ASSERT_FALSE(k2.empty()) << "could not find two series on different shards";

        fs::path edir = tmpDir("eng");
        fs::path jroot = tmpDir("journal");
        {  // the engine must be DESTROYED before its data dir is removed
            ScopedShardedEngine eng;
            eng.startAt(edir.string());

            // A reduced placement: only these two VShards, owned solely by this node, so the
            // groups are single-voter and elect themselves (no peers needed).
            ControlMap map;
            map.epoch = 1;
            map.placement[vs1] = {self};
            map.placement[vs2] = {self};
            data::VShardDirectory dir(self, map);

            seastar::sharded<cluster::ShardRaftPlane> shards;
            shards.start().get();
            shards
                .invoke_on_all([engines = &eng.eng, peers = &shards, dirp = &dir, self,
                                jroot = jroot.string()](cluster::ShardRaftPlane& p) {
                    return p.init(engines, peers, dirp, self, jroot, std::chrono::milliseconds(10));
                })
                .get();
            shards
                .invoke_on_all([addr](cluster::ShardRaftPlane& p) {
                    return p.startDataPlane(addr, std::nullopt, timestar::features::VersionRange{});
                })
                .get();

            raft::RaftOptions opts;
            opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
            opts.heartbeatTimeout = 1;
            for (uint16_t vs : {vs1, vs2})
                shards
                    .invoke_on(cluster::shardForVShard(vs),
                               [vs, opts, self](cluster::ShardRaftPlane& p) { return p.addVShard(vs, {self}, opts); })
                    .get();
            shards
                .invoke_on_all([](cluster::ShardRaftPlane& p) {
                    p.startTicking();
                    return seastar::make_ready_future<>();
                })
                .get();

            // Wait for both single-voter groups to elect themselves (timer-driven ticking).
            auto isLeader = [&](uint16_t vs) {
                return shards
                    .invoke_on(cluster::shardForVShard(vs),
                               [vs](cluster::ShardRaftPlane& p) {
                                   auto* g = p.plane().host().group(vs);
                                   return g && g->isLeader();
                               })
                    .get();
            };
            for (int i = 0; i < 200 && !(isLeader(vs1) && isLeader(vs2)); ++i)
                seastar::sleep(std::chrono::milliseconds(20)).get();
            ASSERT_TRUE(isLeader(vs1) && isLeader(vs2)) << "single-voter groups did not elect";

            // (a) A PEER's proposeWrite over the real socket: it lands on whichever shard the
            // listener gives it, must be split across the two owning shards, committed on
            // both, and applied into the real Engine.
            data::DataPlaneRpc peer;
            peer.addPeer(self, addr);
            peer.startClientOnly().get();
            data::WriteBatch batch;
            // Distinct timestamps: with no aggregationInterval the query aggregates ACROSS
            // series at EQUAL timestamps, so same-timestamp points would fold into one and
            // hide a lost slice.
            batch.series = {floatSeries(k1, 11.0, BASE), floatSeries(k2, 22.0, BASE + 1'000'000'000ULL)};
            EXPECT_TRUE(peer.proposeWrite(self, std::move(batch)).get())
                << "an inbound proposeWrite must commit on every owning shard";

            http::HttpQueryHandler h(&*eng);
            QueryRequest q;
            q.aggregation = AggregationMethod::LATEST;
            q.measurement = "cpu";
            q.fields = {"value"};
            q.startTime = BASE - 1'000'000'000ULL;
            q.endTime = BASE + 2'000'000'000ULL;
            auto r = h.executeQuery(q).get();
            ASSERT_TRUE(r.success) << r.errorMessage;
            ASSERT_EQ(r.series.size(), 1u);
            const auto& vals = std::get<std::vector<double>>(r.series[0].fields.at("value").second);
            EXPECT_EQ(vals, (std::vector<double>{11.0, 22.0}))
                << "both slices must have been applied -- one value missing means a shard's "
                << "slice never committed";

            // (a2) The PRODUCTION listener is per-shard-distributing. Several peer
            // connections must be SERVED by more than one shard: this is what pins
            // ShardRaftPlane::startDataPlane's perShardListener=true. Flipping it to
            // false pins every accepted connection onto shard 0 (reuseport is disabled
            // here, so set_fixed_cpu means "give shard 0 every fd"), and this fails --
            // whereas the rest of the test still passes, because correctness never
            // depended on WHICH shard served.
            {
                const size_t kConns = std::min<size_t>(8, seastar::smp::count * 2);
                std::vector<std::unique_ptr<data::DataPlaneRpc>> peers;
                for (size_t i = 0; i < kConns; ++i) {
                    auto c = std::make_unique<data::DataPlaneRpc>();
                    c->addPeer(self, addr);
                    c->startClientOnly().get();
                    data::WriteBatch b;
                    b.series = {floatSeries(k1, 1.0 + static_cast<double>(i), BASE + 10'000'000'000ULL)};
                    EXPECT_TRUE(c->proposeWrite(self, std::move(b)).get());
                    peers.push_back(std::move(c));
                }
                size_t served = 0, shardsThatServed = 0;
                for (unsigned s = 0; s < seastar::smp::count; ++s) {
                    const uint64_t n =
                        shards.invoke_on(s, [](cluster::ShardRaftPlane& p) { return p.inboundProposals(); }).get();
                    served += n;
                    if (n > 0)
                        ++shardsThatServed;
                }
                EXPECT_EQ(served, kConns + 1) << "every inbound proposal must be served exactly once";
                EXPECT_GT(shardsThatServed, 1u)
                    << "all " << (kConns + 1) << " inbound proposals were served by ONE shard -- the per-shard "
                    << "data-plane listeners are not distributing (perShardListener must be true)";
                for (auto& c : peers)
                    c->stop().get();
            }

            // (b) The shutdown race. Tear down ONLY the shard owning vs2 -- exactly the state
            // sharded<>::stop() produces transiently, since it stops every shard
            // concurrently -- and route a write there. It must raise a retryable error, not
            // dereference the null plane and crash the node.
            shards.invoke_on(cluster::shardForVShard(vs2), [](cluster::ShardRaftPlane& p) { return p.stop(); }).get();

            // The error must be the RETRYABLE shard-stopping one specifically -- any
            // other runtime_error here (a null deref turned into a generic failure, a
            // routing error) would be a different, non-retryable bug wearing the same
            // exception type.
            auto expectShardStopping = [](auto&& fn, const char* what) {
                try {
                    fn();
                    ADD_FAILURE() << what << " must throw while the owning shard is stopping";
                } catch (const std::runtime_error& e) {
                    EXPECT_STREQ(e.what(), cluster::kShardStoppingError) << what;
                } catch (...) {
                    ADD_FAILURE() << what << " threw a non-runtime_error";
                }
            };
            expectShardStopping(
                [&] {
                    data::WriteBatch afterStop;
                    afterStop.series = {floatSeries(k2, 33.0)};
                    cluster::writeSlicesToOwningShards(shards, std::move(afterStop)).get();
                },
                "writeSlicesToOwningShards");
            expectShardStopping(
                [&] {
                    data::WriteBatch afterStop2;
                    afterStop2.series = {floatSeries(k2, 44.0)};
                    cluster::proposeSlicesToOwningShards(shards, std::move(afterStop2)).get();
                },
                "proposeSlicesToOwningShards");

            // The still-live shard keeps working -- the failure is scoped to the stopped one.
            data::WriteBatch stillLive;
            stillLive.series = {floatSeries(k1, 55.0)};
            EXPECT_NO_THROW(cluster::writeSlicesToOwningShards(shards, std::move(stillLive)).get());

            peer.stop().get();
            shards.stop().get();
        }  // engine destroyed here
        fs::remove_all(jroot);
        fs::remove_all(edir);
    }).get();
}

