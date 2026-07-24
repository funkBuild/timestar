// Phase 7 GATE: a loaded RF=3 cluster grows N->N+1 without falling below RF,
// losing acknowledged data, or double-counting. A 4th node joins and takes over
// one VShard replica via the real move workflow -- add learner, catch up by
// InstallSnapshot + log replay, promote to voter, then remove the old voter --
// over the async RaftGroup + journal + partition-router harness. At every
// committed membership the voter count stays >= 3, and the destination's applied
// state is byte-identical to the source at cutover (no loss, no double-count).
#include "../../../lib/cluster/data/data_state_machine.hpp"
#include "../../../lib/cluster/movement/move_job.hpp"
#include "../../../lib/cluster/movement/mover.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <deque>
#include <filesystem>
#include <functional>
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
namespace mv = timestar::movement;
namespace fs = std::filesystem;

namespace {

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_grow_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
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
    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 400000) {
            Envelope e = std::move(queue_.front());
            queue_.pop_front();
            auto it = groups_.find(e.message.to);
            if (it != groups_.end() && it->second)
                co_await it->second->step(std::move(e.message));
        }
    }

private:
    std::map<NodeId, RaftGroup*> groups_;
    std::deque<Envelope> queue_;
};
seastar::future<> RouterTransport::send(Envelope env) {
    r_.enqueue(std::move(env));
    return seastar::make_ready_future<>();
}

struct NodeBox {
    fs::path dir;
    NodeId id;
    std::vector<NodeId> knownVoters;
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
        std::vector<NodeId> bv = knownVoters, bl;
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
    o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 40);
    o.heartbeatTimeout = 1;
    return o;
}
seastar::future<> pumpTick(Nodes& nodes, Router& router) {
    co_await router.pump();
    for (auto& [id, n] : nodes)
        if (n->group)
            co_await n->group->tick();
    co_await router.pump();
}
template <typename T>
seastar::future<T> drive(seastar::future<T> f, Nodes& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds && !f.available(); ++i)
        co_await pumpTick(nodes, router);
    co_return co_await std::move(f);
}
seastar::future<bool> driveUntil(std::function<bool()> pred, Nodes& nodes, Router& router, int rounds) {
    for (int i = 0; i < rounds; ++i) {
        if (pred())
            co_return true;
        co_await pumpTick(nodes, router);
    }
    co_return pred();
}
bool sameSet(std::vector<NodeId> a, std::vector<NodeId> b) {
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    return a == b;
}
data::WritePoints batch(int base, int n, double v0) {
    data::WritePoints wp;
    for (int i = 0; i < n; ++i)
        wp.points.push_back({sid("m,h=" + std::to_string(base + i) + " v"), static_cast<uint64_t>(1000 + i), v0 + i});
    return wp;
}
size_t rawCount(data::DataStateMachine& sm) {
    return sm.query(data::QuerySpec{0, 1000000, data::AggMethod::Raw, {}}).raw.size();
}

// Commit a membership target on the leader and drive until it commits + leaves
// joint with the target voters active. Returns the committed voter count.
seastar::future<size_t> commitConfig(NodeBox& leader, mv::ConfigTarget target, Nodes& nodes, Router& router) {
    bool ok = co_await leader.group->proposeConfChange(target.voters, target.learners);
    EXPECT_TRUE(ok);
    co_await driveUntil(
        [&]() {
            const auto& c = leader.group->node().config();
            return !c.joint() && sameSet(c.voters, target.voters);
        },
        nodes, router, 300);
    co_return leader.group->node().config().voters.size();
}

seastar::future<> testGrowNtoNplus1() {
    Router router;
    std::vector<RouterTransport> transports;
    transports.reserve(6);
    Nodes nodes;
    // Nodes 1,2,3 are the RF=3 voters; node 4 is an observer that knows the config.
    for (NodeId id : {1, 2, 3, 4}) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir("n" + std::to_string(id));
        box->id = id;
        box->knownVoters = {1, 2, 3};
        box->opts = optsFor(id);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }

    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());

    // Load the group with acknowledged writes (committed on {1,2,3}). Node 4, not
    // in the config, receives none of them yet.
    for (int b = 0; b < 4; ++b) {
        bool ack = co_await drive(
            nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(b * 10, 5, 10.0 + b)})),
            nodes, router, 80);
        EXPECT_TRUE(ack);
    }
    EXPECT_EQ(rawCount(*nodes[1]->sm), 20u);
    EXPECT_EQ(rawCount(*nodes[4]->sm), 0u);  // observer has nothing

    // Compact the leader's log so a fresh learner MUST install a snapshot (the
    // gate's snapshot-transfer path) rather than replay the whole log.
    co_await nodes[1]->group->compact(nodes[1]->group->appliedIndex(), nodes[1]->sm->snapshot());

    // CONCURRENT write AFTER the snapshot boundary: the destination must apply the
    // snapshot (through S) AND replay this later entry, matching the source's
    // logical hash at cutover (docs/clustering.md rebalancing gate).
    bool ackC = co_await drive(
        nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(200, 5, 99.0)})), nodes,
        router, 80);
    EXPECT_TRUE(ackC);
    const size_t sourceCount = rawCount(*nodes[1]->sm);
    EXPECT_EQ(sourceCount, 25u);

    // Drive the move: node 4 replaces node 2. victim is a FOLLOWER so the leader
    // never has to step down mid-move.
    mv::MoveJob job(mv::MovePlan{/*vshard=*/1, /*dest=*/4, /*victim=*/2});
    const size_t minRf = 3;
    while (!job.done()) {
        const mv::MoveStep next = job.nextStep();
        if (next == mv::MoveStep::CaughtUp) {
            // Node 4 is below the snapshot boundary: it installs the snapshot and
            // replays to the cutover. Drive until it has caught up to the commit.
            co_await driveUntil([&]() { return nodes[4]->group->appliedIndex() >= nodes[1]->group->commitIndex(); },
                                nodes, router, 300);
            EXPECT_GE(nodes[4]->group->appliedIndex(), nodes[1]->group->commitIndex());
            job.advance(mv::MoveStep::CaughtUp);
            continue;
        }
        auto target =
            job.nextConfig(nodes[1]->group->node().config().voters, nodes[1]->group->node().config().learners);
        if (!target) {
            job.advance(next);
            continue;
        }
        // GATE invariant: never propose a membership below RF.
        EXPECT_TRUE(mv::MoveJob::maintainsRf(nodes[1]->group->node().config().voters, target->voters, minRf));
        size_t committedVoters = co_await commitConfig(*nodes[1], *target, nodes, router);
        EXPECT_GE(committedVoters, minRf) << "RF dropped below 3 at step " << static_cast<int>(next);
        job.advance(next);
    }

    // Final membership: {1,3,4}, RF=3, node 2 gone, node 4 in.
    const auto& finalCfg = nodes[1]->group->node().config();
    EXPECT_EQ(finalCfg.voters.size(), 3u);
    EXPECT_TRUE(sameSet(finalCfg.voters, {1, 3, 4}));

    // Catch node 4 fully up to the final commit, then prove NO acknowledged data
    // lost and NO double-count: its applied state is byte-identical to the source.
    co_await driveUntil([&]() { return nodes[4]->group->appliedIndex() >= nodes[1]->group->commitIndex(); }, nodes,
                        router, 200);
    EXPECT_EQ(rawCount(*nodes[4]->sm), sourceCount);                // every point present, once
    EXPECT_EQ(nodes[4]->sm->snapshot(), nodes[1]->sm->snapshot());  // logical hash == source

    for (auto& [id, n] : nodes)
        if (n->writer)
            co_await n->writer->close();
    for (auto& [id, n] : nodes)
        fs::remove_all(n->dir);
}

// A real MoveExecutor over the async harness: it drives the group itself (pump +
// tick) so each step completes -- commitConfig leaves joint, catchUp waits for the
// destination to apply through the commit. This lets the REAL Mover::run() drive a
// move over real Raft (not just the mock).
class HarnessExecutor : public mv::MoveExecutor {
public:
    NodeBox* leader;
    Nodes* nodes;
    Router* router;
    std::vector<std::vector<NodeId>> committedVoters;  // history for the RF invariant

    std::vector<NodeId> voters() const override { return leader->group->node().config().voters; }
    std::vector<NodeId> learners() const override { return leader->group->node().config().learners; }

    seastar::future<bool> commitConfig(mv::ConfigTarget target) override {
        if (!leader->group->isLeader())
            co_return false;
        bool ok = co_await leader->group->proposeConfChange(target.voters, target.learners);
        if (!ok)
            co_return false;
        co_await driveUntil(
            [&]() {
                const auto& c = leader->group->node().config();
                return !c.joint() && sameSet(c.voters, target.voters);
            },
            *nodes, *router, 400);
        const auto& c = leader->group->node().config();
        bool done = !c.joint() && sameSet(c.voters, target.voters);
        if (done)
            committedVoters.push_back(c.voters);
        co_return done;
    }
    seastar::future<uint64_t> catchUp(NodeId dest) override {
        const uint64_t target = leader->group->commitIndex();
        co_await driveUntil([&]() { return (*nodes)[dest]->group->appliedIndex() >= target; }, *nodes, *router, 400);
        co_return (*nodes)[dest]->group->appliedIndex();
    }
    seastar::future<> persist(const mv::MoveJob&) override { return seastar::make_ready_future<>(); }
};

seastar::future<> testGrowViaRealMover() {
    Router router;
    std::vector<RouterTransport> transports;
    transports.reserve(6);
    Nodes nodes;
    for (NodeId id : {1, 2, 3, 4}) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir("rm" + std::to_string(id));
        box->id = id;
        box->knownVoters = {1, 2, 3};
        box->opts = optsFor(id);
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot();
        nodes[id] = std::move(box);
    }
    co_await nodes[1]->group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1]->group->isLeader());
    for (int b = 0; b < 3; ++b) {
        bool ack = co_await drive(
            nodes[1]->group->proposeAndAwaitApplied(encodeDataCommand(data::DataCommand{batch(b * 10, 5, 10.0 + b)})),
            nodes, router, 80);
        EXPECT_TRUE(ack);
    }
    co_await nodes[1]->group->compact(nodes[1]->group->appliedIndex(), nodes[1]->sm->snapshot());
    const size_t sourceCount = rawCount(*nodes[1]->sm);

    // Drive the WHOLE move through the real Mover over real Raft.
    HarnessExecutor exec;
    exec.leader = nodes[1].get();
    exec.nodes = &nodes;
    exec.router = &router;
    mv::MoveJob job(mv::MovePlan{/*vshard=*/1, /*dest=*/4, /*victim=*/2});
    mv::Mover mover(/*minRf=*/3);
    co_await mover.run(job, exec);

    EXPECT_TRUE(job.done());
    for (const auto& v : exec.committedVoters)
        EXPECT_GE(v.size(), 3u) << "RF dropped below 3 during the real-Mover move";
    const auto& finalCfg = nodes[1]->group->node().config();
    EXPECT_TRUE(sameSet(finalCfg.voters, {1, 3, 4}));
    co_await driveUntil([&]() { return nodes[4]->group->appliedIndex() >= nodes[1]->group->commitIndex(); }, nodes,
                        router, 200);
    EXPECT_EQ(rawCount(*nodes[4]->sm), sourceCount);
    EXPECT_EQ(nodes[4]->sm->snapshot(), nodes[1]->sm->snapshot());

    for (auto& [id, n] : nodes)
        if (n->writer)
            co_await n->writer->close();
    for (auto& [id, n] : nodes)
        fs::remove_all(n->dir);
}

}  // namespace

TEST(MovementGrowthTest, GrowNtoNplus1MaintainsRfNoLoss) {
    testGrowNtoNplus1().get();
}
TEST(MovementGrowthTest, GrowViaRealMoverOverRealRaft) {
    testGrowViaRealMover().get();
}
