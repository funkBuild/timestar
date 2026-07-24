// Integration M3 GATE: RF=3 over THREE REAL Engines. A write proposed on the leader
// commits on quorum and applies through EngineDataStateMachine into every replica's
// real Engine; all three converge. A write still commits with one node partitioned
// (2/3 quorum), and the partitioned node catches up on heal -- no acknowledged loss,
// no duplicate, no split brain. This is the Phase-5 RF=3 gate re-proven with the
// enriched command over the real Engine (not the toy store).
#include "../../../lib/cluster/integration/engine_data_state_machine.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
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
            r.sm = std::make_unique<cluster::EngineDataStateMachine>(*r.store);
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
