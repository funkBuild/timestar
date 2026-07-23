// Failure tests: crash-recovery and partition safety through the FULL async
// stack (RaftGroup driver + real journal-backed persistence + an in-memory
// partition-controllable router). Proves no acknowledged (committed) entry is
// lost across a crash, and that a partitioned old leader never diverges the log.
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <unistd.h>
#include <vector>

using namespace timestar::raft;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;

namespace fs = std::filesystem;

namespace {

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() / ("timestar_raftfail_" + tag + "_" + std::to_string(::getpid()) +
                                            "_" + std::to_string(seq.fetch_add(1)));
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

class RecordingSM : public RaftStateMachine {
public:
    std::vector<std::string> applied;
    seastar::future<> apply(LogEntry e) override {
        applied.push_back(e.data);
        return seastar::make_ready_future<>();
    }
    seastar::future<> applySnapshot(Snapshot) override { return seastar::make_ready_future<>(); }
};

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

// One node's rebuildable state: its journal dir + writer + persistence + SM +
// group. A "crash" drops the in-memory group/SM but keeps the journal on disk;
// recovery rebuilds from it.
struct NodeBox {
    fs::path dir;
    NodeId id;
    std::vector<NodeId> voters;
    RaftOptions opts;
    Router* router;
    RouterTransport* transport;

    std::unique_ptr<JournalWriter> writer;
    std::unique_ptr<JournalRaftPersistence> persistence;
    std::unique_ptr<RecordingSM> sm;
    std::unique_ptr<RaftGroup> group;

    seastar::future<> boot() {
        writer = std::make_unique<JournalWriter>(dir, header(), 1u << 20);
        auto recovered = co_await writer->open();
        RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
        persistence = std::make_unique<JournalRaftPersistence>(*writer, VShardId{1}, st.nextSeq);
        sm = std::make_unique<RecordingSM>();
        // A recovered snapshot supplies the base membership + the applied state.
        std::vector<NodeId> baseVoters = voters;
        std::vector<NodeId> baseLearners;
        if (st.snapshot) {
            baseVoters = st.snapshot->config.voters;
            baseLearners = st.snapshot->config.learners;
            co_await sm->applySnapshot(*st.snapshot);
        }
        RaftNode node(id, baseVoters, std::move(st.log), st.hardState, opts, baseLearners);
        group = std::make_unique<RaftGroup>(1, std::move(node), *persistence, *transport, *sm);
        router->setGroup(id, group.get());
    }

    seastar::future<> crash() {
        router->setGroup(id, nullptr);
        // Drop in-memory group + SM; close the journal (its files persist).
        group.reset();
        persistence.reset();
        sm.reset();
        co_await writer->close();
        writer.reset();
    }
};

RaftOptions optsFor(NodeId id) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 20);  // node 1 leads by default
    o.heartbeatTimeout = 1;
    return o;
}

seastar::future<> tickAndPump(std::map<NodeId, std::unique_ptr<NodeBox>>& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        for (auto& [id, n] : nodes)
            if (n->group)
                co_await n->group->tick();
        co_await router.pump();
    }
}

seastar::future<> testCrashRecoveryPreservesCommitted() {
    const std::vector<NodeId> voters = {1, 2, 3};
    Router router;
    std::vector<RouterTransport> transports;
    transports.reserve(4);
    std::map<NodeId, std::unique_ptr<NodeBox>> nodes;
    for (NodeId id : voters) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir("n" + std::to_string(id));
        box->id = id;
        box->voters = voters;
        box->opts = optsFor(id);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }

    // Elect node 1 and commit three entries.
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());
    co_await nodes[1]->group->propose("a");
    co_await nodes[1]->group->propose("b");
    co_await nodes[1]->group->propose("c");
    co_await router.pump();
    const LogIndex committed = nodes[1]->group->commitIndex();
    EXPECT_GE(committed, 4u);  // noop@1 + a,b,c

    // CRASH node 2 and recover it from its journal.
    co_await nodes[2]->crash();
    co_await nodes[2]->boot();
    // Its recovered log must already hold every committed entry (durable).
    EXPECT_GE(nodes[2]->group->node().log().lastIndex(), committed);

    // Let the leader heartbeat; the recovered node re-applies committed entries.
    co_await tickAndPump(nodes, router, 40);
    EXPECT_EQ(nodes[2]->sm->applied.size(), 3u);
    if (nodes[2]->sm->applied.size() == 3u) {
        EXPECT_EQ(nodes[2]->sm->applied[0], "a");
        EXPECT_EQ(nodes[2]->sm->applied[2], "c");
    }
    EXPECT_EQ(nodes[2]->group->commitIndex(), nodes[1]->group->commitIndex());

    for (auto& [id, n] : nodes) {
        if (n->group)
            co_await n->group->tick();  // flush any pending
    }
    for (auto& [id, n] : nodes) {
        if (n->writer)
            co_await n->writer->close();
        fs::remove_all(n->dir);
    }
}

seastar::future<> testLeaderCrashElectsNewLeaderNoLoss() {
    const std::vector<NodeId> voters = {1, 2, 3};
    Router router;
    std::vector<RouterTransport> transports;
    transports.reserve(4);
    std::map<NodeId, std::unique_ptr<NodeBox>> nodes;
    // Symmetric-ish timeouts so a new leader can emerge after node 1 dies.
    auto opts = [](NodeId id) {
        RaftOptions o;
        o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : (id == 2 ? 4 : 30));
        o.heartbeatTimeout = 1;
        return o;
    };
    for (NodeId id : voters) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir("l" + std::to_string(id));
        box->id = id;
        box->voters = voters;
        box->opts = opts(id);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }

    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());
    co_await nodes[1]->group->propose("x");
    co_await nodes[1]->group->propose("y");
    co_await router.pump();
    EXPECT_GE(nodes[1]->group->commitIndex(), 3u);

    // Kill the leader permanently.
    co_await nodes[1]->crash();

    // Node 2 (shortest remaining timeout) should win and keep committing.
    co_await tickAndPump(nodes, router, 60);
    const bool newLeader = nodes[2]->group->isLeader() || nodes[3]->group->isLeader();
    EXPECT_TRUE(newLeader);
    NodeId leader = nodes[2]->group->isLeader() ? 2u : 3u;
    // The previously-committed entries survive on the new leader.
    EXPECT_GE(nodes[leader]->group->node().log().lastIndex(), 3u);
    co_await nodes[leader]->group->propose("z");
    co_await tickAndPump(nodes, router, 40);
    // Both surviving nodes applied x, y, z.
    for (NodeId id : {2u, 3u}) {
        EXPECT_EQ(nodes[id]->sm->applied.size(), 3u) << "node " << id;
    }

    for (auto& [id, n] : nodes) {
        if (n->writer)
            co_await n->writer->close();
        fs::remove_all(n->dir);
    }
}

seastar::future<> testPartitionedLeaderCannotCommitAndReconciles() {
    const std::vector<NodeId> voters = {1, 2, 3};
    Router router;
    std::vector<RouterTransport> transports;
    transports.reserve(4);
    std::map<NodeId, std::unique_ptr<NodeBox>> nodes;
    auto opts = [](NodeId id) {
        RaftOptions o;
        o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : (id == 2 ? 4 : 30));
        o.heartbeatTimeout = 1;
        return o;
    };
    for (NodeId id : voters) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir("p" + std::to_string(id));
        box->id = id;
        box->voters = voters;
        box->opts = opts(id);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }

    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());
    co_await nodes[1]->group->propose("committed");
    co_await router.pump();
    const LogIndex safeCommit = nodes[1]->group->commitIndex();

    // Partition the leader from the majority, then have it accept a write. With no
    // majority it must NOT advance commit past the safe point.
    router.partition(1);
    co_await nodes[1]->group->propose("orphan");  // will be overwritten
    co_await router.pump();
    EXPECT_EQ(nodes[1]->group->commitIndex(), safeCommit);  // no stale-leader commit

    // The majority {2,3} elects a new leader and commits a different entry.
    co_await tickAndPump(nodes, router, 60);
    NodeId leader = nodes[2]->group->isLeader() ? 2u : (nodes[3]->group->isLeader() ? 3u : 0u);
    EXPECT_NE(leader, 0u);
    if (leader != 0u) {
        co_await nodes[leader]->group->propose("real");
        co_await tickAndPump(nodes, router, 30);
    }

    // Heal the old leader: it learns the higher term, steps down, and its orphan
    // entry is overwritten by the majority's log -- no divergence.
    router.heal(1);
    co_await tickAndPump(nodes, router, 60);
    EXPECT_NE(nodes[1]->group->role(), Role::Leader);
    // Old leader's applied history contains "committed" and "real", never "orphan".
    for (const auto& v : nodes[1]->sm->applied)
        EXPECT_NE(v, "orphan");
    EXPECT_EQ(nodes[1]->group->commitIndex(), nodes[leader]->group->commitIndex());

    for (auto& [id, n] : nodes) {
        if (n->writer)
            co_await n->writer->close();
        fs::remove_all(n->dir);
    }
}

}  // namespace

TEST(RaftFailureTest, PartitionedLeaderCannotCommitAndReconciles) {
    testPartitionedLeaderCannotCommitAndReconciles().get();
}

TEST(RaftFailureTest, CrashRecoveryPreservesCommittedEntries) {
    testCrashRecoveryPreservesCommitted().get();
}

TEST(RaftFailureTest, LeaderCrashElectsNewLeaderNoLoss) {
    testLeaderCrashElectsNewLeaderNoLoss().get();
}
