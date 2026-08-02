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

ControlMap completeMap(data::NodeId owner) {
    ControlMap map;
    map.epoch = 1;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        map.placement[vshard] = {owner};
    return map;
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

TEST_F(ShardRaftPlaneTest, DynamicJoinRegistersPeerBeforeLearnerAndRetriesAfterResolutionFailure) {
    seastar::async([] {
        constexpr data::NodeId self = 1;
        constexpr data::NodeId joining = 2;
        const ControlMap map = completeMap(self);
        fs::path edir = tmpDir("join_eng");
        fs::path jroot = tmpDir("join_journal");
        {
            ScopedShardedEngine eng;
            eng.startAt(edir.string());
            bool addressResolved = false;
            size_t registrationAttempts = 0;
            std::vector<std::pair<data::NodeId, std::string>> registrations;
            cluster::ShardRaftPlane::DynamicPeerRegistrar registrar =
                [&addressResolved, &registrationAttempts,
                 &registrations](data::NodeId id, std::string address) {
                    ++registrationAttempts;
                    registrations.emplace_back(id, std::move(address));
                    return seastar::make_ready_future<bool>(addressResolved);
                };

            seastar::sharded<cluster::ShardRaftPlane> shards;
            shards.start().get();
            shards
                .invoke_on_all([engines = &eng.eng, peers = &shards, map, self, registrar,
                                jroot = jroot.string()](cluster::ShardRaftPlane& p) {
                    return p.init(engines, peers, map, self, jroot, std::chrono::milliseconds(10),
                                  cluster::JournalIdentity::testing(), registrar);
                })
                .get();

            shards
                .invoke_on(0, [map](cluster::ShardRaftPlane& p) -> seastar::future<> {
                    raft::RaftOptions opts;
                    opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
                    opts.heartbeatTimeout = 1;
                    co_await p.addGroup0({self}, opts);
                    auto* host = p.group0();
                    if (!host || !host->group() || !host->stateMachine())
                        throw std::runtime_error("test group-0 host was not initialized");
                    co_await host->group()->campaign();
                    if (!host->group()->isLeader())
                        throw std::runtime_error("test group-0 host did not become leader");
                    control::Group0Controller controller(*host->group(), *host->stateMachine());
                    control::NodeRecord seed{self, "seed-uuid", "127.0.0.1:18140", "rack-a",
                                             control::NodeState::Active};
                    co_await controller.initCluster("cluster-dynamic-join", seed);
                    if (!co_await controller.publishInitialServingMap(map) ||
                        !co_await controller.mintJoinToken("join-token"))
                        throw std::runtime_error("test group-0 bootstrap commands did not commit");
                    p.startGroup0Ticking();
                })
                .get();

            control::ControlJoinRequest request;
            request.clusterUuid = "cluster-dynamic-join";
            request.record = control::NodeRecord{joining, "joining-uuid", "127.0.0.1:18141", "rack-b",
                                                 control::NodeState::Joining};
            request.token = "join-token";
            auto join = [&] {
                return shards
                    .invoke_on(0, [request](cluster::ShardRaftPlane& p) mutable {
                        return p.handleControlJoin(std::move(request));
                    })
                    .get();
            };

            const auto unresolved = join();
            EXPECT_EQ(unresolved.status, control::ControlJoinStatus::Joining);
            EXPECT_EQ(registrationAttempts, 1u);
            const auto beforeRetry = shards
                                         .invoke_on(0, [joining](cluster::ShardRaftPlane& p) {
                                             auto* host = p.group0();
                                             return std::pair{host->state().nodes.contains(joining),
                                                              host->group()->node().config().isLearner(joining)};
                                         })
                                         .get();
            EXPECT_TRUE(beforeRetry.first) << "the consumed-token admission must be durable and retryable";
            EXPECT_FALSE(beforeRetry.second) << "an unreachable peer must not be committed as a learner";

            addressResolved = true;
            const auto retried = join();
            EXPECT_EQ(retried.status, control::ControlJoinStatus::Joining)
                << "registration succeeded, but activation still waits for real learner catch-up";
            EXPECT_EQ(registrationAttempts, 2u);
            ASSERT_EQ(registrations.size(), 2u);
            EXPECT_EQ(registrations.back(), (std::pair<data::NodeId, std::string>{joining, request.record.address}));
            EXPECT_TRUE(shards
                            .invoke_on(0, [joining](cluster::ShardRaftPlane& p) {
                                return p.group0()->group()->node().config().isLearner(joining);
                            })
                            .get())
                << "peer registration must complete before learner membership is committed";

            shards.stop().get();
        }
        fs::remove_all(jroot);
        fs::remove_all(edir);
    }).get();
}

TEST_F(ShardRaftPlaneTest, DataPeerAddressChangeRetiresTheCachedConnection) {
    seastar::async([] {
        RecordingStore oldStore;
        RecordingStore newStore;
        data::DataPlaneRpc oldServer;
        data::DataPlaneRpc newServer;
        data::DataPlaneRpc client;
        const auto oldAddress = loopback(18142);
        const auto newAddress = loopback(18143);
        oldServer.start(oldAddress, oldStore).get();
        newServer.start(newAddress, newStore).get();
        client.startClientOnly().get();

        data::WriteBatch first;
        first.series = {floatSeries(buildSeriesKey("peer_move", {{"host", "old"}}, "value"), 1.0)};
        client.addPeer(2, oldAddress);
        client.forwardWriteBatch(2, std::move(first)).get();
        EXPECT_EQ(oldStore.served, 1u);
        EXPECT_EQ(newStore.served, 0u);

        data::WriteBatch second;
        second.series = {floatSeries(buildSeriesKey("peer_move", {{"host", "new"}}, "value"), 2.0)};
        client.addPeer(2, newAddress);
        client.forwardWriteBatch(2, std::move(second)).get();
        EXPECT_EQ(oldStore.served, 1u) << "the cached client must not keep using the retired address";
        EXPECT_EQ(newStore.served, 1u);

        client.stop().get();
        EXPECT_NO_THROW(client.stop().get()) << "failed-start/owner cleanup may stop a transport twice";
        oldServer.stop().get();
        newServer.stop().get();
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
            } else if (vs != vs1 && cluster::shardForGroup(vs) != cluster::shardForGroup(vs1)) {
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
            seastar::sharded<cluster::ShardRaftPlane> shards;
            shards.start().get();
            shards
                .invoke_on_all([engines = &eng.eng, peers = &shards, map, self,
                                jroot = jroot.string()](cluster::ShardRaftPlane& p) {
                    return p.init(engines, peers, map, self, jroot, std::chrono::milliseconds(10));
                })
                .get();
            shards
                .invoke_on_all([addr](cluster::ShardRaftPlane& p) {
                    return p.startDataPlane(addr, std::nullopt);
                })
                .get();

            raft::RaftOptions opts;
            opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
            opts.heartbeatTimeout = 1;
            for (uint16_t vs : {vs1, vs2})
                shards
                    .invoke_on(cluster::shardForGroup(vs),
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
                    .invoke_on(cluster::shardForGroup(vs),
                               [vs](cluster::ShardRaftPlane& p) {
                                   auto* g = p.plane().host().group(vs);
                                   return g && g->isLeader();
                               })
                    .get();
            };
            for (int i = 0; i < 200 && !(isLeader(vs1) && isLeader(vs2)); ++i)
                seastar::sleep(std::chrono::milliseconds(20)).get();
            ASSERT_TRUE(isLeader(vs1) && isLeader(vs2)) << "single-voter groups did not elect";

            // (a) A peer proposal over the real socket lands on whichever shard the
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
            auto groups = data::splitByVShard(std::move(batch));
            EXPECT_TRUE(peer.proposeWriteHinted(self, data::viewOf(groups), std::nullopt).get().committed)
                << "an inbound proposal must commit on every owning shard";

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

            // A Group-0 publication is copied into EACH reactor's directory, not
            // written through a shard-0 pointer. Every copy advances, and exact
            // replay is harmless (the retry path after a partial observer failure).
            ControlMap published;
            published.epoch = 2;
            for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
                published.placement[vshard] = {self};
            data::VShardDirectory coordinatorDirectory(self, map);
            // Simulate an observer failure after only shard 0 changed. The
            // production helper must accept the exact map there and still
            // advance the reactor that missed the first attempt.
            EXPECT_TRUE(shards.invoke_on(0, [published](cluster::ShardRaftPlane& p) {
                return p.updateServingMap(published);
            }).get());
            EXPECT_EQ(shards.invoke_on(0, [](cluster::ShardRaftPlane& p) { return p.directory().epoch(); }).get(),
                      2u);
            EXPECT_EQ(shards.invoke_on(1, [](cluster::ShardRaftPlane& p) { return p.directory().epoch(); }).get(),
                      1u);
            cluster::publishServingMapOnShards(shards, coordinatorDirectory, published).get();
            EXPECT_EQ(coordinatorDirectory.epoch(), 2u);
            shards.invoke_on_all([](cluster::ShardRaftPlane& p) { EXPECT_EQ(p.directory().epoch(), 2u); }).get();
            cluster::publishServingMapOnShards(shards, coordinatorDirectory, published).get();
            shards
                .invoke_on_all([published](cluster::ShardRaftPlane& p) {
                    EXPECT_EQ(p.directory().map(), published);
                })
                .get();

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
                    auto oneGroup = data::splitByVShard(std::move(b));
                    EXPECT_TRUE(c->proposeWriteHinted(self, data::viewOf(oneGroup), std::nullopt).get().committed);
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
            shards.invoke_on(cluster::shardForGroup(vs2), [](cluster::ShardRaftPlane& p) { return p.stop(); }).get();

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

// write-scaleout 3d: the WHOLE batch is validated against the placement map on the
// REQUEST shard, before a single slice is dispatched.
//
// The per-shard router also fail-closes on an unassigned VShard, but it only ever sees
// its own slice -- so a batch whose one unassigned VShard happened to land on the last
// shard could durably commit every other shard's slice and only then fail the client.
// That is a partial commit reported as an error, which the ack contract forbids.
//
// The discriminator is the exception TYPE: with the check, UnassignedVShardError before
// any dispatch; without it, the slices reach the (uninitialised, hence not-ready) planes
// and the caller sees kShardStoppingError instead.
TEST_F(ShardRaftPlaneTest, UnassignedVShardFailsTheWholeBatchBeforeAnyDispatch) {
    seastar::async([] {
        seastar::sharded<cluster::ShardRaftPlane> shards;
        shards.start().get();

        data::WriteBatch batch;
        for (int i = 0; i < 64; ++i)
            batch.series.push_back(floatSeries(buildSeriesKey("m", {{"host", "h" + std::to_string(i)}}, "v"), 1.0));

        // A full map, minus the VShard of the LAST series -- so every earlier slice is
        // routable and would commit if the check were per-shard.
        data::WriteSeries last = batch.series.back();
        const uint16_t orphan = data::vshardOf(last);
        ControlMap m;
        m.epoch = 1;
        for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v)
            if (v != orphan)
                m.placement[v] = {1, 2, 3};
        data::VShardDirectory dir(1, m);

        try {
            data::WriteBatch copy;
            copy.series = batch.series;
            cluster::writeSlicesToOwningShards(shards, std::move(copy), &dir).get();
            ADD_FAILURE() << "a batch touching an unassigned VShard must be refused";
        } catch (const data::UnassignedVShardError&) {
            // exactly right: fail-closed, nothing dispatched
        } catch (const std::exception& e) {
            ADD_FAILURE() << "expected UnassignedVShardError (pre-dispatch), got: " << e.what();
        }

        // A fully-assigned map gets past the check and fails LATER, at the (not-ready)
        // planes -- proving the check above is the thing that fired, not a broken harness.
        ControlMap full;
        full.epoch = 1;
        for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v)
            full.placement[v] = {1, 2, 3};
        data::VShardDirectory fullDir(1, full);
        try {
            cluster::writeSlicesToOwningShards(shards, std::move(batch), &fullDir).get();
            ADD_FAILURE() << "the planes are not started; this must fail";
        } catch (const data::UnassignedVShardError& e) {
            ADD_FAILURE() << "a fully-assigned map must pass the placement check: " << e.what();
        } catch (const std::runtime_error& e) {
            EXPECT_STREQ(e.what(), cluster::kShardStoppingError);
        }

        shards.stop().get();
    }).get();
}

// PEER-INGRESS ADMISSION (debt D-8): the budget that used to cover only what this node
// ORIGINATES now covers what its peers push at it too -- ~2/3 of replication traffic on a
// balanced 3-node RF=3 cluster, previously bounded only by rpc::resource_limits.
//
// The charge is taken on the SERVING shard (the one the peer's connection landed on),
// which is where the decoded batch and the whole fan-out frame live. The interesting
// property is not that it rejects -- it is HOW: on the hinted verb the rejection is
// REPORTED as per-slice `Overloaded` rejects with no leader hint, not thrown, so the
// coordinator retries against the pacing table for "something must drain" instead of
// reading a full node as an unreachable one and waking the Raft groups behind it.
TEST_F(ShardRaftPlaneTest, PeerIngressIsChargedAndRejectsWithoutWedgingOrDoubleReleasing) {
    seastar::async([] {
        seastar::sharded<cluster::ShardRaftPlane> shards;
        shards.start().get();

        data::WriteBatch batch;
        for (int i = 0; i < 32; ++i)
            batch.series.push_back(floatSeries(buildSeriesKey("m", {{"host", "h" + std::to_string(i)}}, "v"), 1.0));
        const std::set<uint16_t> asked = [&] {
            std::set<uint16_t> vs;
            for (const auto& s : batch.series) {
                data::WriteSeries copy = s;
                vs.insert(data::vshardOf(copy));
            }
            return vs;
        }();

        auto& ingress = cluster::WriteAdmission::local(cluster::AdmissionClass::PeerIngress);
        auto& originated = cluster::WriteAdmission::local(cluster::AdmissionClass::Originated);
        ASSERT_EQ(ingress.inFlight(), 0u) << "a previous test leaked an ingress charge";
        const size_t lim = cluster::WriteAdmission::limitBytes(cluster::AdmissionClass::PeerIngress);
        {
            // Fill the ingress budget on THIS shard, which is the one that will serve.
            cluster::WriteAdmissionGuard full(lim, cluster::AdmissionClass::PeerIngress);

            data::WriteBatch copy;
            copy.series = batch.series;
            data::ProposeOutcome out = cluster::proposeSlicesToOwningShardsHinted(shards, std::move(copy)).get();

            EXPECT_FALSE(out.committed) << "an admission rejection must never ack";
            EXPECT_TRUE(out.committedVShards.empty()) << "nothing was proposed, so nothing may be crossed off";
            ASSERT_EQ(out.rejects.size(), asked.size()) << "every asked-about slice must be named uncommitted";
            for (const auto& r : out.rejects) {
                EXPECT_EQ(r.kind, data::WriteFailure::Overloaded);
                EXPECT_TRUE(data::isRetryableWriteFailure(r.kind));
                EXPECT_FALSE(data::isElectionWaitFailure(r.kind))
                    << "overload must never buy the 6 s election window (debt D-14)";
                EXPECT_EQ(r.leaderHint, timestar::raft::kNoNode) << "we may still lead it; there is nowhere better";
                EXPECT_TRUE(asked.count(r.vshard)) << "a reject for a VShard nobody asked about";
            }
            // The rejected batch was NOT charged (all-or-nothing), so the budget still
            // holds exactly the test's own charge -- no double-charge, no leak.
            EXPECT_EQ(ingress.inFlight(), lim);
            EXPECT_EQ(originated.inFlight(), 0u) << "peer ingress must not spend the originated budget";
        }
        EXPECT_EQ(ingress.inFlight(), 0u) << "double release would show up here as a saturated-to-zero counter";

        // With the budget free the SAME call gets past admission and fails downstream
        // instead -- the planes are not started -- which proves the rejection above came
        // from admission and not from the harness.
        {
            data::WriteBatch copy;
            copy.series = batch.series;
            data::ProposeOutcome out = cluster::proposeSlicesToOwningShardsHinted(shards, std::move(copy)).get();
            EXPECT_FALSE(out.committed);
            ASSERT_FALSE(out.rejects.empty());
            EXPECT_EQ(out.rejects[0].kind, data::WriteFailure::ShardStopping);
            EXPECT_EQ(ingress.inFlight(), 0u) << "the charge must be released on the failure path too";
        }

        // The coarse bool convenience path has no way to say "overloaded", so it THROWS.
        // The coordinator classifies that as retryable Transport: still retryable and not
        // election-shaped. Production uses the detailed path above.
        {
            cluster::WriteAdmissionGuard full(lim, cluster::AdmissionClass::PeerIngress);
            data::WriteBatch copy;
            copy.series = batch.series;
            EXPECT_THROW(cluster::proposeSlicesToOwningShards(shards, std::move(copy)).get(),
                         data::WriteOverloadedError);
        }
        EXPECT_EQ(ingress.inFlight(), 0u);

        shards.stop().get();
    }).get();
}
