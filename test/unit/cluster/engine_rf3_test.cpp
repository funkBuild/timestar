// Integration M3 GATE: RF=3 over THREE REAL Engines. A write proposed on the leader
// commits on quorum and applies through EngineDataStateMachine into every replica's
// real Engine; all three converge. A write still commits with one node partitioned
// (2/3 quorum), and the partitioned node catches up on heal -- no acknowledged loss,
// no duplicate, no split brain. This is the Phase-5 RF=3 gate re-proven with the
// enriched command over the real Engine (not the toy store).
#include "../../../lib/cluster/integration/controller_job_driver.hpp"
#include "../../../lib/cluster/integration/engine_data_state_machine.hpp"
#include "../../../lib/cluster/integration/raft_move_executor.hpp"
#include "../../../lib/cluster/features/operator_surface.hpp"
#include "../../../lib/cluster/features/stream_subscription.hpp"
#include "../../../lib/cluster/movement/movement_throttle.hpp"
#include "../../../lib/cluster/movement/placement_balancer.hpp"
#include "../../../lib/cluster/integration/replica_engine_coordinator.hpp"
#include "../../../lib/cluster/integration/replica_engine_reader.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/storage/journal_segment.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <set>

using namespace timestar;
using namespace timestar::raft;
namespace fs = std::filesystem;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto d = fs::temp_directory_path() /
             ("ts_rf3e_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(d);
    return d;
}
JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 1;
    h.bootId.fill(0x44);
    return h;
}

class Router;
class RouterTransport : public RaftTransport {
public:
    explicit RouterTransport(Router& r) : r_(r) {}
    seastar::future<> send(Envelope env) override;

private:
    Router& r_;
};
class Router {
public:
    void setGroup(NodeId id, RaftGroup* g) { groups_[id] = g; }
    void enqueue(Envelope e) { queue_.push_back(std::move(e)); }
    void partition(NodeId id) { down_.insert(id); }
    void heal(NodeId id) { down_.erase(id); }
    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 200000) {
            Envelope e = std::move(queue_.front());
            queue_.pop_front();
            if (down_.count(e.message.to) || down_.count(e.message.from))
                continue;
            auto it = groups_.find(e.message.to);
            if (it != groups_.end() && it->second)
                co_await it->second->step(std::move(e.message));
        }
    }

private:
    std::map<NodeId, RaftGroup*> groups_;
    std::set<NodeId> down_;
    std::deque<Envelope> queue_;
};
seastar::future<> RouterTransport::send(Envelope env) {
    r_.enqueue(std::move(env));
    return seastar::make_ready_future<>();
}

RaftOptions optsFor(NodeId id, NodeId leader) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == leader ? 2 : (id == (leader % 3) + 1 ? 5 : 30));
    o.heartbeatTimeout = 1;
    return o;
}

// One replica: a real Engine + its state machine + a Raft group.
struct Replica {
    std::unique_ptr<ScopedShardedEngine> engine;
    std::unique_ptr<cluster::EngineLocalStore> store;
    std::unique_ptr<cluster::EngineDataStateMachine> sm;
    std::unique_ptr<JournalWriter> writer;
    std::unique_ptr<JournalRaftPersistence> persistence;
    std::unique_ptr<RouterTransport> transport;
    std::unique_ptr<RaftGroup> group;
};

std::string writeCmd(const std::string& key, double value) {
    data::WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {BASE};
    s.values = std::vector<double>{value};
    data::WriteBatch b;
    b.series = {std::move(s)};
    return data::encodeReplicatedCommand(data::ReplicatedCommand{std::move(b)});
}

double latestOn(ScopedShardedEngine& e, const std::string& m, const std::string& f) {
    http::HttpQueryHandler h(&*e);
    QueryRequest q;
    q.aggregation = AggregationMethod::LATEST;
    q.measurement = m;
    q.fields = {f};
    q.startTime = BASE - 1'000'000'000ULL;
    q.endTime = BASE + 1'000'000'000ULL;
    auto r = h.executeQuery(q).get();
    if (!r.success || r.series.empty())
        return -1;
    auto* v = std::get_if<std::vector<double>>(&r.series[0].fields.at(f).second);
    return (v && !v->empty()) ? (*v)[0] : -1;
}

class EngineRf3Test : public ::testing::Test {};
}  // namespace

TEST_F(EngineRf3Test, ThreeRealEnginesConvergeUnderPartition) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i) {
                for (auto& [id, r] : reps)
                    if (r.group)
                        r.group->tick().get();
                router.pump().get();
            }
        };
        auto drive = [&](seastar::future<bool>& f, int rounds) {
            for (int i = 0; i < rounds && !f.available(); ++i) {
                router.pump().get();
                for (auto& [id, r] : reps)
                    if (r.group)
                        r.group->tick().get();
                router.pump().get();
            }
        };

        for (NodeId id : voters) {
            Replica r;
            fs::path edir = tmpDir("eng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{0});
            fs::path jdir = tmpDir("j" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }

        // Elect the preferred leader.
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        // Write 1 (all healthy): commits on quorum, converges on all THREE engines.
        // Distinct measurement per write so a LATEST read is unambiguous.
        {
            const std::string key = buildSeriesKey("m1", {{"host", "h1"}}, "value");
            auto f = reps[leader].group->proposeAndAwaitApplied(writeCmd(key, 42.5));
            drive(f, 40);
            ASSERT_TRUE(f.get());
            tick(20);  // let followers apply
            for (NodeId id : voters)
                EXPECT_DOUBLE_EQ(latestOn(*reps[id].engine, "m1", "value"), 42.5) << "node " << id;
        }

        // Write 2 with node 3 partitioned: still commits (2/3 quorum).
        router.partition(3);
        {
            const std::string key = buildSeriesKey("m2", {{"host", "h2"}}, "value");
            auto f = reps[leader].group->proposeAndAwaitApplied(writeCmd(key, 99.0));
            drive(f, 60);
            ASSERT_TRUE(f.get()) << "write must commit with 2/3 quorum";
            tick(20);
            EXPECT_DOUBLE_EQ(latestOn(*reps[1].engine, "m2", "value"), 99.0);
            EXPECT_DOUBLE_EQ(latestOn(*reps[2].engine, "m2", "value"), 99.0);
            EXPECT_DOUBLE_EQ(latestOn(*reps[3].engine, "m2", "value"), -1);  // partitioned: not yet
        }

        // Heal node 3: it catches up via Raft replication (no loss, no dup).
        router.heal(3);
        tick(60);
        EXPECT_DOUBLE_EQ(latestOn(*reps[3].engine, "m2", "value"), 99.0) << "node 3 must catch up on heal";
        EXPECT_DOUBLE_EQ(latestOn(*reps[3].engine, "m1", "value"), 42.5) << "node 3 still has write 1";

        // Teardown: stop groups/persistence before engines; close writers.
        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();  // engines stopped in ScopedShardedEngine dtor
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M4 cross-node gate (in-process, the same harness style M3 was proven on): a
// FOLLOWER replica serves a linearizable read after confirming the leader's
// ReadIndex, coordinated across nodes with exactly-once contribution. Proves the
// replica-read path end to end over three real engines + Raft groups -- the multi-
// node behaviour the docker suite gates, verified here without sockets.
TEST_F(EngineRf3Test, ReplicaReadServesFromFollowerAcrossNodes) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i) {
                for (auto& [id, r] : reps)
                    if (r.group)
                        r.group->tick().get();
                router.pump().get();
            }
        };
        auto drive = [&](auto& f, int rounds) {
            for (int i = 0; i < rounds && !f.available(); ++i) {
                router.pump().get();
                for (auto& [id, r] : reps)
                    if (r.group)
                        r.group->tick().get();
                router.pump().get();
            }
        };

        for (NodeId id : voters) {
            Replica r;
            fs::path edir = tmpDir("rreng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("rrj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }

        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        // A series whose VShard is 1 (the group's VShard), so the readers' VShard-
        // restricted queries return it.
        std::string key;
        for (int i = 0;; ++i) {
            key = buildSeriesKey("rrm", {{"host", "h" + std::to_string(i)}}, "value");
            if (timestar::virtualShard(SeriesId128::fromSeriesKey(key)) == 1)
                break;
        }
        {
            auto f = reps[leader].group->proposeAndAwaitApplied(writeCmd(key, 7.25));
            drive(f, 60);
            ASSERT_TRUE(f.get());
            tick(30);  // followers apply
        }

        // Leader-reach fns confirm at the current leader (node 1's group), driven by the
        // outer tick loop. Readers are the two FOLLOWERS only, to PROVE a follower serves.
        auto leaderReadIndex = [&] { return reps[leader].group->readBarrier(); };
        auto leaderCommit = [&] {
            return seastar::make_ready_future<raft::LogIndex>(reps[leader].group->commitIndex());
        };
        cluster::ReplicaEngineReader reader2(*reps[2].group, *reps[2].store, 1, leaderReadIndex, leaderCommit);
        cluster::ReplicaEngineReader reader3(*reps[3].group, *reps[3].store, 1, leaderReadIndex, leaderCommit);

        cluster::ReplicaEngineQueryCoordinator coord(
            {{1, {&reader2, &reader3}}}, /*hedgeWidth=*/1, /*allowPartial=*/false);

        data::NodeQueryRequest nq;
        nq.request.aggregation = AggregationMethod::LATEST;
        nq.request.measurement = "rrm";
        nq.request.fields = {"value"};
        nq.request.startTime = BASE - 1'000'000'000ULL;
        nq.request.endTime = BASE + 1'000'000'000ULL;

        // Linearizable read: the follower confirms the leader ReadIndex, waits applied,
        // and serves. Drive ticks while the ReadIndex round completes.
        auto qf = coord.query(nq, data::ReadConsistency::Linearizable, {}, 0);
        drive(qf, 80);
        auto res = qf.get();
        EXPECT_TRUE(res.partial.incompleteReasons.empty());
        EXPECT_TRUE(res.missing.empty());
        EXPECT_GT(res.partial.seriesFound, 0u) << "a follower served the linearizable cross-node read";

        // Finalize to confirm the actual value round-trips from the follower.
        http::HttpQueryHandler fin(&**reps[2].engine);
        auto resp = fin.finalizeClusterPartials(nq.request, std::move(res.partial.partials),
                                                std::move(res.partial.nonNumeric))
                        .get();
        ASSERT_TRUE(resp.success) << resp.errorMessage;
        ASSERT_EQ(resp.series.size(), 1u);
        EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(resp.series[0].fields.at("value").second)[0], 7.25);

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M5 foundation: on the leader, matchIndexOf(peer) reports how far a peer has
// replicated -- the signal a move's catchUp() polls to know a freshly-added learner
// is caught up before promoting it. After a committed write, every follower's match
// index reaches the leader's commit; an unknown node reports kNoIndex.
TEST_F(EngineRf3Test, LeaderTracksPeerMatchIndex) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i) {
                for (auto& [id, r] : reps)
                    if (r.group)
                        r.group->tick().get();
                router.pump().get();
            }
        };
        auto drive = [&](auto& f, int rounds) {
            for (int i = 0; i < rounds && !f.available(); ++i) {
                router.pump().get();
                for (auto& [id, r] : reps)
                    if (r.group)
                        r.group->tick().get();
                router.pump().get();
            }
        };

        for (NodeId id : voters) {
            Replica r;
            fs::path edir = tmpDir("mieng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("mij" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        {
            const std::string key = buildSeriesKey("mi", {{"host", "h1"}}, "value");
            auto f = reps[leader].group->proposeAndAwaitApplied(writeCmd(key, 1.0));
            drive(f, 60);
            ASSERT_TRUE(f.get());
            tick(30);
        }

        const auto commit = reps[leader].group->commitIndex();
        EXPECT_GT(commit, 0u);
        // Both followers have replicated up to at least the committed index.
        EXPECT_GE(reps[leader].group->matchIndexOf(2), commit) << "follower 2 caught up";
        EXPECT_GE(reps[leader].group->matchIndexOf(3), commit) << "follower 3 caught up";
        // A node that is not a member is unknown (kNoIndex == 0).
        EXPECT_EQ(reps[leader].group->matchIndexOf(99), 0u);

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M5 GATE (in-process): a real N->N+1 REPLACE move over live Raft. Group voters
// {1,2,3} with node 4 as an observer; the Mover, driven by the production
// RaftGroupMoveExecutor, replaces follower 3 with node 4: add 4 as a learner (RF
// stays 3), catch it up via the leader's match-index, promote it (RF momentarily 4),
// then remove 3 -> voters {1,2,4}. RF never drops below 3 at any committed step, the
// destination is caught up before promotion, the moved data is present on node 4, and
// the job persisted each forward step. Same harness M3's RF=3 gate uses.
TEST_F(EngineRf3Test, MoverReplacesFollowerAcrossFourNodes) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const std::vector<NodeId> allNodes = {1, 2, 3, 4};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tickAll = [&] {
            for (auto& [id, r] : reps)
                if (r.group)
                    r.group->tick().get();
            router.pump().get();
        };
        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i)
                tickAll();
        };
        auto drive = [&](auto& f, int rounds) {
            for (int i = 0; i < rounds && !f.available(); ++i) {
                router.pump().get();
                tickAll();
            }
        };

        // Every node (incl. the observer 4) knows the initial voter set {1,2,3}; node 4
        // is not a voter until the move adds it.
        for (NodeId id : allNodes) {
            Replica r;
            fs::path edir = tmpDir("mveng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("mvj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        // Commit data BEFORE the move, so node 4 must catch it up.
        {
            const std::string key = buildSeriesKey("mv", {{"host", "h1"}}, "value");
            auto f = reps[leader].group->proposeAndAwaitApplied(writeCmd(key, 3.14));
            drive(f, 60);
            ASSERT_TRUE(f.get());
            tick(20);
        }

        // Drive the replace move 3 -> 4 through the production executor.
        std::vector<movement::MoveStep> persisted;
        cluster::RaftGroupMoveExecutor exec(
            *reps[leader].group,
            [&persisted](const movement::MoveJob& j) {
                persisted.push_back(j.step());
                return seastar::make_ready_future<>();
            });
        movement::MoveJob job(movement::MovePlan{/*vshard=*/1, /*dest=*/4, /*victim=*/3});
        movement::Mover mover(/*minRf=*/3);

        auto mf = mover.run(job, exec);
        // Drive ticks while the move's commitConfig/catchUp await the transitions.
        for (int i = 0; i < 8000 && !mf.available(); ++i)
            tickAll();
        mf.get();
        ASSERT_TRUE(job.done()) << "move must run to completion";

        // Final committed config: node 3 replaced by node 4, RF back to 3.
        auto finalVoters = reps[leader].group->node().config().voters;
        std::sort(finalVoters.begin(), finalVoters.end());
        EXPECT_EQ(finalVoters, (std::vector<NodeId>{1, 2, 4})) << "voters must be {1,2,4} after the replace";
        EXPECT_FALSE(reps[leader].group->node().config().joint());

        // The moved data is present on the NEW replica (node 4 caught up + applied).
        tick(30);
        EXPECT_DOUBLE_EQ(latestOn(*reps[4].engine, "mv", "value"), 3.14) << "node 4 has the moved data";

        // The job persisted forward steps through Promoted/OldRemoved to Done.
        EXPECT_FALSE(persisted.empty());
        EXPECT_EQ(persisted.back(), movement::MoveStep::Done);

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M5 balancing-loop gate (in-process): the PlacementBalancer SELECTS a move from
// load telemetry (respecting failure-domain anti-affinity + disk watermark), and the
// RaftGroupMoveExecutor EXECUTES it over live Raft -- the two M5 bricks composed end
// to end. Node 3 is overloaded, node 4 is free in a distinct domain, so the balancer
// moves VShard 1's replica 3 -> 4, and the group converges to voters {1,2,4}.
TEST_F(EngineRf3Test, BalancerSelectedMoveExecutesEndToEnd) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const std::vector<NodeId> allNodes = {1, 2, 3, 4};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tickAll = [&] {
            for (auto& [id, r] : reps)
                if (r.group)
                    r.group->tick().get();
            router.pump().get();
        };
        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i)
                tickAll();
        };

        for (NodeId id : allNodes) {
            Replica r;
            fs::path edir = tmpDir("baleng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("balj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        // Telemetry: node 3 is heavily loaded; node 4 is empty with headroom. Each node
        // is in its own failure domain so anti-affinity permits 3 -> 4.
        using movement::NodeLoad;
        std::map<NodeId, NodeLoad> loads = {
            {1, NodeLoad{1, "a", 1.0, 100.0, 1}},
            {2, NodeLoad{2, "b", 1.0, 100.0, 1}},
            {3, NodeLoad{3, "c", 1.0, 100.0, 10}},  // overloaded
            {4, NodeLoad{4, "d", 1.0, 100.0, 0}},   // free destination
        };
        std::map<uint16_t, std::vector<NodeId>> placement = {{1, {1, 2, 3}}};

        auto plan = movement::PlacementBalancer::planOneMove(placement, loads, movement::BalancerConfig{});
        ASSERT_TRUE(plan.has_value()) << "balancer must propose a move off the overloaded node";
        EXPECT_EQ(plan->dest, 4u);
        EXPECT_EQ(plan->victim, 3u) << "the overloaded node's replica is the one moved";
        EXPECT_EQ(plan->vshard, 1u);

        // Execute the balancer's chosen move over live Raft.
        cluster::RaftGroupMoveExecutor exec(
            *reps[leader].group, [](const movement::MoveJob&) { return seastar::make_ready_future<>(); });
        movement::MoveJob job(*plan);
        movement::Mover mover(/*minRf=*/3);
        auto mf = mover.run(job, exec);
        for (int i = 0; i < 8000 && !mf.available(); ++i)
            tickAll();
        mf.get();
        ASSERT_TRUE(job.done());

        auto finalVoters = reps[leader].group->node().config().voters;
        std::sort(finalVoters.begin(), finalVoters.end());
        EXPECT_EQ(finalVoters, (std::vector<NodeId>{1, 2, 4}))
            << "the balancer-selected move rebalanced the overloaded node's replica to the free node";

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M5 throttle-safe-forward gate (in-process): a move paused by the SLO throttle mid-
// flight STOPS at a safe forward-committed step -- RF never dropped, no voter removed
// before its replacement -- and resuming re-runs the same job to completion. Also
// confirms MovementThrottle trips on an SLO breach and recovers after the hysteresis
// cool-down (the source of the Mover's mayProceed gate in production).
TEST_F(EngineRf3Test, ThrottlePausesMoveSafeForwardThenResumes) {
    seastar::async([] {
        // The throttle wiring: healthy -> proceed; a breach auto-pauses instantly;
        // it only resumes after `hysteresis` consecutive healthy samples.
        movement::MovementThrottle throttle(movement::SloBudgets{}, /*resumeHysteresisTicks=*/2);
        EXPECT_TRUE(throttle.update(movement::ForegroundSignals{}));  // healthy
        movement::ForegroundSignals breach;
        breach.p99LatencyMs = 600;  // > 500 budget
        EXPECT_FALSE(throttle.update(breach)) << "movement auto-pauses on an SLO breach";
        EXPECT_FALSE(throttle.update(movement::ForegroundSignals{})) << "one good sample is not enough";
        EXPECT_TRUE(throttle.update(movement::ForegroundSignals{})) << "resumes after the hysteresis cool-down";

        const std::vector<NodeId> voters = {1, 2, 3};
        const std::vector<NodeId> allNodes = {1, 2, 3, 4};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tickAll = [&] {
            for (auto& [id, r] : reps)
                if (r.group)
                    r.group->tick().get();
            router.pump().get();
        };
        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i)
                tickAll();
        };

        for (NodeId id : allNodes) {
            Replica r;
            fs::path edir = tmpDir("threng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("thrj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        cluster::RaftGroupMoveExecutor exec(
            *reps[leader].group, [](const movement::MoveJob&) { return seastar::make_ready_future<>(); });
        movement::MoveJob job(movement::MovePlan{/*vshard=*/1, /*dest=*/4, /*victim=*/3});
        movement::Mover mover(/*minRf=*/3);

        // Run 1: pause AFTER the learner is added (mayProceed false on the 2nd check),
        // so the move stops with the learner committed but no voter yet removed.
        int checks = 0;
        auto pausingMayProceed = [&checks] { return ++checks < 2; };
        auto mf1 = mover.run(job, exec, pausingMayProceed);
        for (int i = 0; i < 8000 && !mf1.available(); ++i)
            tickAll();
        mf1.get();
        EXPECT_FALSE(job.done()) << "the throttle paused the move";
        EXPECT_EQ(job.step(), movement::MoveStep::LearnerAdded);
        // SAFE FORWARD: the original voters are all still voters (RF never dropped), and
        // the destination is a committed learner catching up.
        auto midVoters = reps[leader].group->node().config().voters;
        std::sort(midVoters.begin(), midVoters.end());
        EXPECT_EQ(midVoters, (std::vector<NodeId>{1, 2, 3})) << "RF preserved: no voter removed while paused";
        const auto& learners = reps[leader].group->node().config().learners;
        EXPECT_NE(std::find(learners.begin(), learners.end(), 4u), learners.end()) << "learner added before pause";

        // Run 2: resume (mayProceed always true) -> the SAME job completes.
        auto mf2 = mover.run(job, exec, [] { return true; });
        for (int i = 0; i < 8000 && !mf2.available(); ++i)
            tickAll();
        mf2.get();
        ASSERT_TRUE(job.done());
        auto finalVoters = reps[leader].group->node().config().voters;
        std::sort(finalVoters.begin(), finalVoters.end());
        EXPECT_EQ(finalVoters, (std::vector<NodeId>{1, 2, 4})) << "resumed move completes the replace";

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M5 controller job-driver gate (in-process): a persisted group-0 MoveJob is driven
// to completion via the Mover over live Raft, and each advanced step is reported back
// as an updated control::Job (the group-0 UpsertJob the next controller resumes from).
// Proves the bridge from a persisted job to a real membership change.
TEST_F(EngineRf3Test, ControllerDrivesPersistedMoveJobToCompletion) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const std::vector<NodeId> allNodes = {1, 2, 3, 4};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tickAll = [&] {
            for (auto& [id, r] : reps)
                if (r.group)
                    r.group->tick().get();
            router.pump().get();
        };
        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i)
                tickAll();
        };

        for (NodeId id : allNodes) {
            Replica r;
            fs::path edir = tmpDir("cjdeng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("cjdj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        // A persisted group-0 Job carrying a replace-move 3 -> 4.
        movement::MoveJob mj(movement::MovePlan{/*vshard=*/1, /*dest=*/4, /*victim=*/3});
        control::Job job{"job-move-1", 0, false, mj.encode()};

        std::vector<control::Job> persisted;
        auto persistJob = [&persisted](control::Job j) {
            persisted.push_back(std::move(j));
            return seastar::make_ready_future<>();
        };

        auto df = cluster::ControllerJobDriver::driveMoveJob(job, *reps[leader].group, /*minRf=*/3, persistJob);
        for (int i = 0; i < 8000 && !df.available(); ++i)
            tickAll();
        const bool done = df.get();
        EXPECT_TRUE(done) << "the driver ran the persisted job to completion";

        auto finalVoters = reps[leader].group->node().config().voters;
        std::sort(finalVoters.begin(), finalVoters.end());
        EXPECT_EQ(finalVoters, (std::vector<NodeId>{1, 2, 4}));

        // The driver persisted advancing steps, ending done -- what the next controller
        // would resume from (and see already complete).
        ASSERT_FALSE(persisted.empty());
        EXPECT_EQ(persisted.back().id, "job-move-1");
        EXPECT_TRUE(persisted.back().done);
        EXPECT_EQ(persisted.back().step, static_cast<uint32_t>(movement::MoveStep::Done));

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M5 operator-surface gate (in-process): RebalanceOps (the operator `rebalance
// pause/resume/status` verbs) drives the SLO throttle that gates real movement. An
// operator PAUSE prevents a move from proceeding; RESUME lets the same job complete
// over live Raft. Proves the operator control surface actually governs movement.
TEST_F(EngineRf3Test, OperatorPauseResumeGovernsRealMove) {
    seastar::async([] {
        movement::MovementThrottle throttle(movement::SloBudgets{});
        features::RebalanceOps ops(throttle);

        const std::vector<NodeId> voters = {1, 2, 3};
        const std::vector<NodeId> allNodes = {1, 2, 3, 4};
        const NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tickAll = [&] {
            for (auto& [id, r] : reps)
                if (r.group)
                    r.group->tick().get();
            router.pump().get();
        };
        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i)
                tickAll();
        };

        for (NodeId id : allNodes) {
            Replica r;
            fs::path edir = tmpDir("openg" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("opj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        cluster::RaftGroupMoveExecutor exec(
            *reps[leader].group, [](const movement::MoveJob&) { return seastar::make_ready_future<>(); });
        movement::Mover mover(/*minRf=*/3);
        auto mayProceed = [&throttle] { return throttle.mayProceed(); };

        // Operator PAUSE: movement must not proceed; a move started now does nothing.
        ops.pause();
        EXPECT_FALSE(ops.status().running);
        movement::MoveJob job(movement::MovePlan{/*vshard=*/1, /*dest=*/4, /*victim=*/3});
        auto paused = mover.run(job, exec, mayProceed);
        for (int i = 0; i < 200 && !paused.available(); ++i)
            tickAll();
        paused.get();
        EXPECT_FALSE(job.done());
        EXPECT_EQ(job.step(), movement::MoveStep::Planned) << "paused: nothing committed";
        auto pv = reps[leader].group->node().config().voters;
        std::sort(pv.begin(), pv.end());
        EXPECT_EQ(pv, (std::vector<NodeId>{1, 2, 3})) << "no membership change while paused";

        // Operator RESUME: the same job now completes.
        ops.resume();
        EXPECT_TRUE(ops.status().running);
        auto resumed = mover.run(job, exec, mayProceed);
        for (int i = 0; i < 8000 && !resumed.available(); ++i)
            tickAll();
        resumed.get();
        ASSERT_TRUE(job.done());
        auto fv = reps[leader].group->node().config().voters;
        std::sort(fv.begin(), fv.end());
        EXPECT_EQ(fv, (std::vector<NodeId>{1, 2, 4})) << "resumed move completes under operator control";

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& d : journalDirs)
            fs::remove_all(d);
        for (auto& d : engineDirs)
            fs::remove_all(d);
    }).get();
}

// M6 cluster-aware streaming gate (in-process): a subscription over a REPLICATED
// VShard backfills committed writes through a barrier, goes live, and -- across a
// real LEADER FAILOVER (a placement change bumping the Raft term) -- resumes from its
// cursor with NO LOSS and NO DUPLICATE, deduping on the real Raft commit index. Each
// write's (term, commitIndex) is the actual Raft position it committed at.
TEST_F(EngineRf3Test, StreamingSurvivesFailoverNoLossNoDup) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        NodeId leader = 1;
        Router router;
        std::map<NodeId, Replica> reps;
        std::vector<fs::path> journalDirs, engineDirs;

        auto tickAll = [&] {
            for (auto& [id, r] : reps)
                if (r.group)
                    r.group->tick().get();
            router.pump().get();
        };
        auto tick = [&](int rounds) {
            for (int i = 0; i < rounds; ++i)
                tickAll();
        };
        auto drive = [&](auto& f, int rounds) {
            for (int i = 0; i < rounds && !f.available(); ++i) {
                router.pump().get();
                tickAll();
            }
        };

        for (NodeId id : voters) {
            Replica r;
            fs::path edir = tmpDir("streng" + std::to_string(id));
            engineDirs.push_back(edir);
            r.engine = std::make_unique<ScopedShardedEngine>();
            r.engine->startAt(edir.string());
            r.store = std::make_unique<cluster::EngineLocalStore>(**r.engine);
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store, timestar::VShardId{1});
            fs::path jdir = tmpDir("strj" + std::to_string(id));
            journalDirs.push_back(jdir);
            r.writer = std::make_unique<JournalWriter>(jdir, header(), 1u << 20);
            auto recovered = r.writer->open().get();
            RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
            r.persistence = std::make_unique<JournalRaftPersistence>(*r.writer, VShardId{1}, st.nextSeq);
            r.transport = std::make_unique<RouterTransport>(router);
            RaftNode node(id, voters, std::move(st.log), st.hardState, optsFor(id, leader), {});
            r.group = std::make_unique<RaftGroup>(1, std::move(node), *r.persistence, *r.transport, *r.sm);
            router.setGroup(id, r.group.get());
            reps[id] = std::move(r);
        }
        tick(40);
        ASSERT_TRUE(reps[leader].group->isLeader());

        using features::StreamEvent;
        // Commit a write and capture the REAL (term, commitIndex) it landed at.
        auto commitWrite = [&](const std::string& m, double v) -> StreamEvent {
            auto f = reps[leader].group->proposeAndAwaitApplied(writeCmd(buildSeriesKey(m, {{"h", "1"}}, "v"), v));
            drive(f, 80);
            EXPECT_TRUE(f.get());
            tick(10);
            return StreamEvent{.vshard = 1,
                               .term = reps[leader].group->currentTerm(),
                               .index = reps[leader].group->commitIndex(),
                               .payload = m};
        };

        StreamEvent a = commitWrite("a", 1), b = commitWrite("b", 2);
        const uint64_t barrier = reps[leader].group->commitIndex();
        StreamEvent c = commitWrite("c", 3);  // live, arrives after the barrier is taken

        // Fresh subscription: backfill (<= barrier) then live.
        features::SubscriptionCursor cursor;
        auto delivered = features::BackfillLiveStream::deliver(cursor, barrier, {a, b}, {c});
        std::vector<std::string> seen;
        for (auto& e : delivered)
            seen.push_back(e.payload);
        EXPECT_EQ(seen, (std::vector<std::string>{"a", "b", "c"})) << "backfill+live delivers each write once";

        // FAILOVER: partition the leader; a survivor wins a NEW term (placement change).
        router.partition(leader);
        tick(80);
        NodeId newLeader = 0;
        for (NodeId id : voters)
            if (id != leader && reps[id].group->isLeader())
                newLeader = id;
        ASSERT_NE(newLeader, 0u) << "a survivor must take leadership";
        EXPECT_GT(reps[newLeader].group->currentTerm(), a.term) << "failover bumped the term";
        leader = newLeader;

        StreamEvent d = commitWrite("d", 4);  // committed under the new term

        // Resume the SAME cursor: the new event is delivered; re-sent old events (a,b,c,
        // e.g. a redelivery after re-registering on the new leader) are all deduped.
        auto afterFailover = features::BackfillLiveStream::deliver(cursor, d.index, {a, b, c}, {d});
        std::vector<std::string> seen2;
        for (auto& e : afterFailover)
            seen2.push_back(e.payload);
        EXPECT_EQ(seen2, (std::vector<std::string>{"d"})) << "no loss (d delivered), no dup (a,b,c deduped)";
        EXPECT_GT(d.index, c.index) << "commit index is monotonic across failover";

        for (auto& [id, r] : reps) {
            router.setGroup(id, nullptr);
            r.group.reset();
            r.persistence.reset();
            r.writer->close().get();
        }
        reps.clear();
        for (auto& dd : journalDirs)
            fs::remove_all(dd);
        for (auto& dd : engineDirs)
            fs::remove_all(dd);
    }).get();
}
