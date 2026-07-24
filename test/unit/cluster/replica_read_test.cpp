// Phase 6 GATE (core): linearizable replica reads. A follower / non-voting read
// replica serves reads by obtaining a quorum-confirmed ReadIndex from the leader
// and waiting until IT has applied through it -- linearizable from any replica,
// no new Raft protocol. Proves: a follower read equals the leader's data; a read
// never returns an old value (waits for the barrier + apply); a replica that
// cannot reach the leader REJECTS rather than serving stale; session
// (read-your-writes) and bounded-staleness modes; and a learner serves reads.
#include "../../../lib/cluster/data/replica_read.hpp"

#include "../../../lib/cluster/data/data_state_machine.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <vector>

using namespace timestar::raft;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;
namespace data = timestar::data;
namespace fs = std::filesystem;

namespace {

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_repread_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}
JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 1;
    h.bootId.fill(0x44);
    return h;
}
SeriesId128 sid(const std::string& k) {
    return SeriesId128::fromSeriesKey(k);
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
    bool isDown(NodeId id) const { return down_.count(id) > 0; }
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

struct NodeBox {
    fs::path dir;
    NodeId id;
    std::vector<NodeId> voters;
    std::vector<NodeId> learners;
    RaftOptions opts;
    Router* router;
    RouterTransport* transport;
    std::unique_ptr<JournalWriter> writer;
    std::unique_ptr<JournalRaftPersistence> persistence;
    std::unique_ptr<data::DataStateMachine> sm;
    std::unique_ptr<RaftGroup> group;

    seastar::future<> boot() {
        writer = std::make_unique<JournalWriter>(dir, header(), 1u << 20);
        auto recovered = co_await writer->open();
        RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
        persistence = std::make_unique<JournalRaftPersistence>(*writer, VShardId{1}, st.nextSeq);
        sm = std::make_unique<data::DataStateMachine>();
        std::vector<NodeId> bv = voters, bl = learners;
        if (st.snapshot) {
            bv = st.snapshot->config.voters;
            bl = st.snapshot->config.learners;
            co_await sm->applySnapshot(*st.snapshot);
        }
        RaftNode node(id, bv, std::move(st.log), st.hardState, opts, bl);
        group = std::make_unique<RaftGroup>(1, std::move(node), *persistence, *transport, *sm);
        router->setGroup(id, group.get());
    }
};
using Nodes = std::map<NodeId, std::unique_ptr<NodeBox>>;

RaftOptions optsFor(NodeId id) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 30);
    o.heartbeatTimeout = 1;
    return o;
}
seastar::future<> tickAndPump(Nodes& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        for (auto& [id, n] : nodes)
            if (n->group)
                co_await n->group->tick();
        co_await router.pump();
    }
}
template <typename T>
seastar::future<T> drive(seastar::future<T> f, Nodes& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds && !f.available(); ++i) {
        co_await router.pump();
        for (auto& [id, n] : nodes)
            if (n->group)
                co_await n->group->tick();
        co_await router.pump();
    }
    co_return co_await std::move(f);
}
seastar::future<> driveVoid(seastar::future<> f, Nodes& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds && !f.available(); ++i) {
        co_await router.pump();
        for (auto& [id, n] : nodes)
            if (n->group)
                co_await n->group->tick();
        co_await router.pump();
    }
    co_await std::move(f);
}
NodeId currentLeader(Nodes& nodes) {
    NodeId best = 0;
    Term bt = 0;
    for (auto& [id, n] : nodes)
        if (n->group && n->group->isLeader() && n->group->currentTerm() >= bt) {
            best = id;
            bt = n->group->currentTerm();
        }
    return best;
}

// leaderReadIndex/leaderCommit as the reading node would obtain them: model the
// RPC to the leader. If the reader is partitioned, it cannot reach the leader ->
// throw (reject). Otherwise call the leader's readBarrier()/commitIndex().
data::ReplicaVShard::LeaderReadIndexFn leaderReadIndexFor(Nodes& nodes, Router& router, NodeId reader) {
    return [&nodes, &router, reader]() -> seastar::future<LogIndex> {
        if (router.isDown(reader))
            throw data::ReplicaReadUnavailable("reader partitioned from leader");
        NodeId l = currentLeader(nodes);
        if (l == 0 || router.isDown(l))
            throw data::ReplicaReadUnavailable("no reachable leader");
        return nodes[l]->group->readBarrier();
    };
}
data::ReplicaVShard::LeaderCommitFn leaderCommitFor(Nodes& nodes, Router& router, NodeId reader) {
    return [&nodes, &router, reader]() -> seastar::future<LogIndex> {
        if (router.isDown(reader))
            throw data::ReplicaReadUnavailable("reader partitioned from leader");
        NodeId l = currentLeader(nodes);
        if (l == 0 || router.isDown(l))
            throw data::ReplicaReadUnavailable("no reachable leader");
        return seastar::make_ready_future<LogIndex>(nodes[l]->group->commitIndex());
    };
}

data::WritePoints batch(int base, int n, double v0) {
    data::WritePoints wp;
    for (int i = 0; i < n; ++i)
        wp.points.push_back({sid("m,h=" + std::to_string(base + i) + " v"), static_cast<uint64_t>(1000 + i), v0 + i});
    return wp;
}
seastar::future<> makeCluster(Nodes& nodes, std::vector<RouterTransport>& transports, Router& router,
                              const std::string& tag, std::vector<NodeId> learners = {}) {
    const std::vector<NodeId> voters = {1, 2, 3};
    transports.reserve(6);
    std::vector<NodeId> all = voters;
    for (NodeId l : learners)
        all.push_back(l);
    for (NodeId id : all) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir(tag + std::to_string(id));
        box->id = id;
        box->voters = voters;
        box->learners = learners;
        box->opts = optsFor(id);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }
}
seastar::future<> closeAll(Nodes& nodes) {
    for (auto& [id, n] : nodes)
        if (n->writer)
            co_await n->writer->close();
    for (auto& [id, n] : nodes)
        fs::remove_all(n->dir);
}

// A follower serves a linearizable read equal to the leader's data.
seastar::future<> testFollowerLinearizableRead() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, "fl");
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());

    bool ack =
        co_await drive(nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(0, 5, 10.0)})),
                       nodes, router, 60);
    EXPECT_TRUE(ack);

    // Read from FOLLOWER node 2 at Linearizable consistency.
    NodeId f = nodes[2]->group->isLeader() ? 3 : 2;
    EXPECT_FALSE(nodes[f]->group->isLeader());
    data::ReplicaVShard replica(*nodes[f]->group, *nodes[f]->sm, leaderReadIndexFor(nodes, router, f),
                                leaderCommitFor(nodes, router, f));
    data::ReplicaReadResult res = co_await drive(
        replica.read(
            {data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}, data::ReadConsistency::Linearizable, {}, 0}),
        nodes, router, 60);
    EXPECT_EQ(res.partial.raw.size(), 5u);
    EXPECT_GT(res.envelope.appliedIndex, 0u);

    co_await closeAll(nodes);
}

// A partitioned replica cannot confirm a ReadIndex and REJECTS (never stale).
seastar::future<> testPartitionedReplicaRejects() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, "pr");
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    bool ack =
        co_await drive(nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(0, 3, 10.0)})),
                       nodes, router, 60);
    EXPECT_TRUE(ack);

    NodeId f = 2;
    router.partition(f);  // follower isolated from the leader
    data::ReplicaVShard replica(*nodes[f]->group, *nodes[f]->sm, leaderReadIndexFor(nodes, router, f),
                                leaderCommitFor(nodes, router, f));
    bool rejected = false;
    try {
        co_await drive(
            replica.read(
                {data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}, data::ReadConsistency::Linearizable, {}, 0}),
            nodes, router, 40);
    } catch (const data::ReplicaReadUnavailable&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    co_await closeAll(nodes);
}

// Session (read-your-writes): a read carrying a token waits until this replica has
// applied through the token's index before serving -- never an older value.
seastar::future<> testSessionReadYourWrites() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, "se");
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    bool ack =
        co_await drive(nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(0, 4, 10.0)})),
                       nodes, router, 60);
    EXPECT_TRUE(ack);
    const uint64_t writeIndex = nodes[1]->group->appliedIndex();

    NodeId f = nodes[2]->group->isLeader() ? 3 : 2;
    data::ReplicaVShard replica(*nodes[f]->group, *nodes[f]->sm, leaderReadIndexFor(nodes, router, f),
                                leaderCommitFor(nodes, router, f));
    data::ReadEnvelope token{1, nodes[1]->group->currentTerm(), writeIndex};
    data::ReplicaReadResult res = co_await drive(
        replica.read({data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}, data::ReadConsistency::Session, token, 0}),
        nodes, router, 60);
    EXPECT_EQ(res.partial.raw.size(), 4u);
    EXPECT_GE(res.envelope.appliedIndex, writeIndex);  // served at or after the token
    co_await closeAll(nodes);
}

// Bounded staleness: within the lag bound serves; a partitioned reader rejects.
seastar::future<> testBoundedStaleness() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, "bs");
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    bool ack =
        co_await drive(nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(0, 3, 10.0)})),
                       nodes, router, 60);
    EXPECT_TRUE(ack);
    co_await tickAndPump(nodes, router, 10);  // let followers catch up

    NodeId f = nodes[2]->group->isLeader() ? 3 : 2;
    data::ReplicaVShard replica(*nodes[f]->group, *nodes[f]->sm, leaderReadIndexFor(nodes, router, f),
                                leaderCommitFor(nodes, router, f));
    // Generous bound: a caught-up follower serves locally.
    data::ReplicaReadResult res = co_await drive(
        replica.read(
            {data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}, data::ReadConsistency::BoundedStaleness, {}, 100}),
        nodes, router, 30);
    EXPECT_EQ(res.partial.raw.size(), 3u);

    // Partitioned reader cannot fetch the leader commit -> reject.
    router.partition(f);
    bool rejected = false;
    try {
        co_await drive(replica.read({data::QuerySpec{0, 100000, data::AggMethod::Raw, {}},
                                     data::ReadConsistency::BoundedStaleness,
                                     {},
                                     100}),
                       nodes, router, 20);
    } catch (const data::ReplicaReadUnavailable&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    co_await closeAll(nodes);
}

// A replica BEHIND on apply must wait for the barrier and serve the fresh value,
// never its stale local state (the "delayed apply / never serve below the barrier"
// gate). The follower is kept behind during the write, then a linearizable read
// blocks on waitApplied until it catches up.
seastar::future<> testDelayedApplyWaitsForBarrier() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, "da");
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());

    // Keep follower 2 behind: partition it, commit a write via {1,3}.
    router.partition(2);
    bool ack =
        co_await drive(nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(0, 5, 10.0)})),
                       nodes, router, 60);
    EXPECT_TRUE(ack);
    const uint64_t leaderApplied = nodes[1]->group->appliedIndex();  // log index of the write
    EXPECT_EQ(nodes[2]->sm->appliedIndex(), 0u);                     // follower 2 has NOT applied the write

    // Heal 2 and read it linearizably: the read must wait for 2 to apply the write
    // and return the fresh 5 points, never 2's stale (empty) local state.
    router.heal(2);
    data::ReplicaVShard replica(*nodes[2]->group, *nodes[2]->sm, leaderReadIndexFor(nodes, router, 2),
                                leaderCommitFor(nodes, router, 2));
    data::ReplicaReadResult res = co_await drive(
        replica.read(
            {data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}, data::ReadConsistency::Linearizable, {}, 0}),
        nodes, router, 80);
    EXPECT_EQ(res.partial.raw.size(), 5u);                   // fresh, not the stale empty state
    EXPECT_GE(nodes[2]->sm->appliedIndex(), leaderApplied);  // caught up through the barrier
    co_await closeAll(nodes);
}

// A non-voting read replica (learner) serves a linearizable read.
seastar::future<> testLearnerServesReads() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, "ln", /*learners=*/{4});
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());
    bool ack =
        co_await drive(nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(0, 5, 10.0)})),
                       nodes, router, 80);
    EXPECT_TRUE(ack);
    co_await tickAndPump(nodes, router, 30);  // replicate to the learner

    // Learner (node 4, non-voting) serves the read.
    EXPECT_FALSE(nodes[4]->group->isLeader());
    data::ReplicaVShard replica(*nodes[4]->group, *nodes[4]->sm, leaderReadIndexFor(nodes, router, 4),
                                leaderCommitFor(nodes, router, 4));
    data::ReplicaReadResult res = co_await drive(
        replica.read(
            {data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}, data::ReadConsistency::Linearizable, {}, 0}),
        nodes, router, 60);
    EXPECT_EQ(res.partial.raw.size(), 5u);
    co_await closeAll(nodes);
}

}  // namespace

TEST(ReplicaReadTest, FollowerLinearizableRead) {
    testFollowerLinearizableRead().get();
}
TEST(ReplicaReadTest, PartitionedReplicaRejects) {
    testPartitionedReplicaRejects().get();
}
TEST(ReplicaReadTest, SessionReadYourWrites) {
    testSessionReadYourWrites().get();
}
TEST(ReplicaReadTest, BoundedStaleness) {
    testBoundedStaleness().get();
}
TEST(ReplicaReadTest, DelayedApplyWaitsForBarrier) {
    testDelayedApplyWaitsForBarrier().get();
}
TEST(ReplicaReadTest, LearnerServesReads) {
    testLearnerServesReads().get();
}
