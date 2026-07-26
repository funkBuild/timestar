// Phase 5 GATE: an RF=3 replicated VShard tolerates one fail-stop OR partitioned
// node without acknowledged data loss, duplicates, or split brain. Data flows
// through the FULL stack -- ReplicatedVShard -> RaftGroup (proposeAndAwaitApplied
// / readBarrier) -> DataStateMachine over real journal persistence -> in-memory
// partition-controllable router. A write is "acknowledged" only when
// proposeAndAwaitApplied resolves true (durable quorum commit + apply); a read is
// a linearizable leader read behind a ReadIndex barrier.
#include "../../../lib/cluster/data/data_state_machine.hpp"
#include "../../../lib/cluster/data/replicated_vshard.hpp"
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
               ("timestar_rf3_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
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

    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 200000) {
            Envelope e = std::move(queue_.front());
            queue_.pop_front();
            if (down_.count(e.message.to) || down_.count(e.message.from))
                continue;  // partitioned: drop
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
    RaftOptions opts;
    Router* router;
    RouterTransport* transport;

    std::unique_ptr<JournalWriter> writer;
    std::unique_ptr<JournalRaftPersistence> persistence;
    std::unique_ptr<data::DataStateMachine> sm;
    std::unique_ptr<RaftGroup> group;
    std::unique_ptr<data::ReplicatedVShard> rv;

    seastar::future<> boot() {
        writer = std::make_unique<JournalWriter>(dir, header(), 1u << 20);
        auto recovered = co_await writer->open();
        RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
        persistence = std::make_unique<JournalRaftPersistence>(*writer, VShardId{1}, st.nextSeq);
        sm = std::make_unique<data::DataStateMachine>();
        std::vector<NodeId> baseVoters = voters;
        std::vector<NodeId> baseLearners;
        if (st.snapshot) {
            baseVoters = st.snapshot->config.voters;
            baseLearners = st.snapshot->config.learners;
            co_await sm->applySnapshot(*st.snapshot);
        }
        RaftNode node(id, baseVoters, std::move(st.log), st.hardState, opts, baseLearners);
        group = std::make_unique<RaftGroup>(1, std::move(node), *persistence, *transport, *sm);
        rv = std::make_unique<data::ReplicatedVShard>(*group, *sm);
        router->setGroup(id, group.get());
    }

    seastar::future<> crash() {
        router->setGroup(id, nullptr);
        rv.reset();
        group.reset();
        persistence.reset();
        sm.reset();
        co_await writer->close();
        writer.reset();
    }
};

using Nodes = std::map<NodeId, std::unique_ptr<NodeBox>>;

RaftOptions optsFor(NodeId id, NodeId preferredLeader) {
    RaftOptions o;
    // The preferred leader has the shortest timeout; a clear runner-up so a
    // deterministic successor emerges on failover.
    o.electionTimeoutMin = o.electionTimeoutMax =
        (id == preferredLeader ? 2 : (id == (preferredLeader % 3) + 1 ? 5 : 30));
    o.heartbeatTimeout = 1;
    return o;
}

seastar::future<> makeCluster(Nodes& nodes, std::vector<RouterTransport>& transports, Router& router,
                              NodeId preferredLeader, const std::string& tag) {
    const std::vector<NodeId> voters = {1, 2, 3};
    transports.reserve(4);
    for (NodeId id : voters) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir(tag + std::to_string(id));
        box->id = id;
        box->voters = voters;
        box->opts = optsFor(id, preferredLeader);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }
}

seastar::future<> tickAndPump(Nodes& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        for (auto& [id, n] : nodes)
            if (n->group)
                co_await n->group->tick();
        co_await router.pump();
    }
}

// Drive a pending future to completion by pumping the router and ticking while it
// is not yet available (messages must flow for a commit/barrier to resolve).
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

// The real current leader is the one reporting leadership with the HIGHEST term.
// A partitioned stale leader still self-reports isLeader() (this harness's options leave
// checkQuorum off -- the production data plane now enables it, so there the step-down
// happens within an election timeout; the tie-break below must hold either way),
// but at a lower term and unable to commit -- so term breaks the tie. That two
// nodes can momentarily self-report leader at different terms is not split brain:
// only the higher-term one can gather a quorum and acknowledge a write.
NodeId currentLeader(Nodes& nodes) {
    NodeId best = 0;
    Term bestTerm = 0;
    for (auto& [id, n] : nodes)
        if (n->group && n->group->isLeader() && n->group->currentTerm() >= bestTerm) {
            best = id;
            bestTerm = n->group->currentTerm();
        }
    return best;
}

// A raw leader read: sorted (series,ts,value) triples for easy comparison.
struct Triple {
    SeriesId128 s;
    uint64_t t;
    double v;
    bool operator<(const Triple& o) const {
        if (!(s == o.s))
            return s < o.s;
        if (t != o.t)
            return t < o.t;
        return v < o.v;
    }
    bool operator==(const Triple& o) const = default;
};

std::vector<Triple> triples(const data::QueryPartial& p) {
    std::vector<Triple> out;
    for (const auto& d : p.raw)
        out.push_back({d.series, d.timestamp, d.value});
    std::sort(out.begin(), out.end());
    return out;
}

data::WritePoints batch(int base, int n, double vbase) {
    data::WritePoints wp;
    for (int i = 0; i < n; ++i)
        wp.points.push_back(
            {sid("m,h=" + std::to_string(base + i) + " v"), static_cast<uint64_t>(1000 + i), vbase + i});
    return wp;
}

seastar::future<> closeAll(Nodes& nodes) {
    for (auto& [id, n] : nodes) {
        if (n->group)
            co_await n->group->tick();
    }
    for (auto& [id, n] : nodes) {
        if (n->writer)
            co_await n->writer->close();
        fs::remove_all(n->dir);
    }
}

// --- GATE 1: a fail-stop follower does not lose acknowledged data; the surviving
// majority keeps accepting acknowledged writes. ---
seastar::future<> testFailStopFollowerNoLoss() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, /*preferredLeader=*/1, "fs");

    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());

    // Acknowledged write of batch A.
    bool ackA = co_await drive(nodes[1]->rv->write(data::DataCommand{batch(0, 5, 10.0)}), nodes, router, 60);
    EXPECT_TRUE(ackA);

    // A follower fail-stops. The remaining majority {1,2} still has quorum.
    co_await nodes[3]->crash();

    // Acknowledged write of batch B survives the failure.
    bool ackB = co_await drive(nodes[1]->rv->write(data::DataCommand{batch(100, 5, 20.0)}), nodes, router, 60);
    EXPECT_TRUE(ackB);

    // Linearizable leader read returns EVERY acknowledged point (no loss).
    data::QueryPartial got = co_await drive(
        nodes[1]->rv->linearizableRead(data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}), nodes, router, 60);
    EXPECT_EQ(got.raw.size(), 10u);

    co_await closeAll(nodes);
}

// --- GATE 2: killing the leader loses no acknowledged data, and an idempotent
// retry of a batch produces no duplicates (LWW). ---
seastar::future<> testLeaderFailoverNoLossNoDuplicate() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, /*preferredLeader=*/1, "lf");

    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());

    data::WritePoints A = batch(0, 6, 10.0);
    bool ackA = co_await drive(nodes[1]->rv->write(data::DataCommand{A}), nodes, router, 60);
    EXPECT_TRUE(ackA);

    // Kill the leader permanently; the majority elects a successor.
    co_await nodes[1]->crash();
    co_await tickAndPump(nodes, router, 80);
    NodeId leader = currentLeader(nodes);
    EXPECT_NE(leader, 0u);
    if (leader == 0u) {
        co_await closeAll(nodes);
        co_return;
    }

    // No acknowledged data lost: batch A is fully readable on the new leader.
    data::QueryPartial afterFailover = co_await drive(
        nodes[leader]->rv->linearizableRead(data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}), nodes, router, 60);
    EXPECT_EQ(afterFailover.raw.size(), 6u);
    std::vector<Triple> expectA = triples(afterFailover);

    // Idempotent RETRY of batch A (the client re-sends the whole batch after the
    // failover) must NOT duplicate points, then a new batch B adds exactly its own.
    bool ackAretry = co_await drive(nodes[leader]->rv->write(data::DataCommand{A}), nodes, router, 60);
    EXPECT_TRUE(ackAretry);
    bool ackB = co_await drive(nodes[leader]->rv->write(data::DataCommand{batch(200, 4, 30.0)}), nodes, router, 60);
    EXPECT_TRUE(ackB);

    data::QueryPartial got = co_await drive(
        nodes[leader]->rv->linearizableRead(data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}), nodes, router, 60);
    // 6 (A, deduped by LWW despite the retry) + 4 (B) = 10, never 16.
    EXPECT_EQ(got.raw.size(), 10u);
    // The retried A points are unchanged (same (series,ts,value)).
    std::vector<Triple> gotA;
    for (const auto& tr : triples(got))
        if (tr.v < 20.0)  // batch A values are 10..15; B are 30..33
            gotA.push_back(tr);
    EXPECT_EQ(gotA, expectA);

    co_await closeAll(nodes);
}

// --- GATE 3: a partitioned leader cannot acknowledge a write (no split brain);
// the majority makes progress; on heal the orphan never becomes visible. ---
seastar::future<> testPartitionedLeaderNoSplitBrain() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await makeCluster(nodes, transports, router, /*preferredLeader=*/1, "pt");

    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());

    bool ackC = co_await drive(nodes[1]->rv->write(data::DataCommand{batch(0, 3, 10.0)}), nodes, router, 60);
    EXPECT_TRUE(ackC);

    // Partition the leader. Its attempted write can never reach a quorum, so it
    // must NEVER be acknowledged. Start it and pump -- it stays pending.
    router.partition(1);
    seastar::future<bool> orphan = nodes[1]->rv->write(data::DataCommand{batch(500, 3, 99.0)});
    co_await tickAndPump(nodes, router, 30);
    EXPECT_FALSE(orphan.available());  // not acknowledged while partitioned

    // The majority {2,3} elects a new leader and accepts an acknowledged write.
    co_await tickAndPump(nodes, router, 80);
    NodeId leader = currentLeader(nodes);
    EXPECT_NE(leader, 0u);
    EXPECT_NE(leader, 1u);  // node 1 must not be a second (split-brain) leader of the majority
    bool ackReal = false;
    if (leader != 0u)
        ackReal = co_await drive(nodes[leader]->rv->write(data::DataCommand{batch(600, 2, 40.0)}), nodes, router, 60);
    EXPECT_TRUE(ackReal);

    // Heal the old leader: it learns the higher term, steps down, and its orphan
    // write fails (leadership lost) rather than ever being acknowledged.
    router.heal(1);
    bool orphanThrew = false;
    try {
        co_await drive(std::move(orphan), nodes, router, 80);
    } catch (const std::runtime_error&) {
        orphanThrew = true;
    }
    EXPECT_TRUE(orphanThrew);
    EXPECT_NE(nodes[1]->group->role(), Role::Leader);

    // A linearizable read on the current leader shows committed + real, never the
    // orphan (value 99.0).
    NodeId finalLeader = currentLeader(nodes);
    EXPECT_NE(finalLeader, 0u);
    if (finalLeader != 0u) {
        data::QueryPartial got = co_await drive(
            nodes[finalLeader]->rv->linearizableRead(data::QuerySpec{0, 100000, data::AggMethod::Raw, {}}), nodes,
            router, 60);
        EXPECT_EQ(got.raw.size(), 5u);  // 3 committed + 2 real
        for (const auto& tr : triples(got))
            EXPECT_NE(tr.v, 99.0) << "orphan became visible -- split brain";
    }

    co_await closeAll(nodes);
}

}  // namespace

TEST(ReplicatedRf3Test, FailStopFollowerNoLoss) {
    testFailStopFollowerNoLoss().get();
}
TEST(ReplicatedRf3Test, LeaderFailoverNoLossNoDuplicate) {
    testLeaderFailoverNoLossNoDuplicate().get();
}
TEST(ReplicatedRf3Test, PartitionedLeaderNoSplitBrain) {
    testPartitionedLeaderNoSplitBrain().get();
}
