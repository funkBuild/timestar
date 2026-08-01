// write-scaleout 3f (BLOCKER fix): a Raft propose waiter must be bounded.
//
// The defect: RaftGroup::proposeAndAwaitApplied resolves only from drainReady. A leader
// that has LOST QUORUM -- two of three replicas down or partitioned -- keeps ticking but
// can never commit, and with checkQuorum off it never steps down either, so nothing ever
// resolves or fails the waiter. It suspends FOREVER.
//
// That is worse than the remote black-hole the per-attempt deadline was introduced for:
// the coordinator's deadline is only checked BETWEEN attempts, so an attempt that never
// returns is never timed out, its WriteAdmission charge is never released, and the shard
// starts 503-ing behind one wedged write. It also violates fail-closed -- RF=3 with two
// nodes down must FAIL a write, not hang.
//
// Both halves are pinned here, in one test, against REAL RaftGroups over a partitionable
// in-memory router: the no-deadline overload is shown to hang (and is then healed and
// resolved, so nothing is left dangling), and the deadline overload is shown to fail
// inside its deadline.
#include "../../../lib/cluster/data/data_command.hpp"
#include "../../../lib/cluster/data/data_state_machine.hpp"
#include "../../../lib/cluster/data/write_errors.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
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
               ("timestar_pdl_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
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
    Router* router = nullptr;
    RouterTransport* transport = nullptr;
    std::unique_ptr<JournalWriter> writer;
    std::unique_ptr<JournalRaftPersistence> persistence;
    std::unique_ptr<data::DataStateMachine> sm;
    std::unique_ptr<RaftGroup> group;

    seastar::future<> boot(const std::vector<NodeId>& voters, RaftOptions opts) {
        writer = std::make_unique<JournalWriter>(dir, header(), 1u << 20);
        auto recovered = co_await writer->open();
        RecoveredRaftState st = recoverRaftState(recovered, VShardId{1});
        persistence = std::make_unique<JournalRaftPersistence>(*writer, VShardId{1}, st.nextSeq);
        sm = std::make_unique<data::DataStateMachine>();
        RaftNode node(id, voters, std::move(st.log), st.hardState, opts, {});
        group = std::make_unique<RaftGroup>(1, std::move(node), *persistence, *transport, *sm);
        router->setGroup(id, group.get());
    }
    seastar::future<> shutdown() {
        router->setGroup(id, nullptr);
        group.reset();
        persistence.reset();
        sm.reset();
        if (writer)
            co_await writer->close();
        writer.reset();
    }
};

using Nodes = std::map<NodeId, std::unique_ptr<NodeBox>>;

RaftOptions optsFor(NodeId id, NodeId preferredLeader, bool checkQuorum) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == preferredLeader ? 2 : 30);
    o.heartbeatTimeout = 1;
    o.checkQuorum = checkQuorum;
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

// The Phase-5 DataStateMachine used here speaks DataCommand (it is the reactor-free
// double the RF=3 gate runs on), not the production EngineDataStateMachine's
// ReplicatedCommand. Either would do: what is under test is the WAITER, not the payload.
std::string cmd(const std::string& key, double v) {
    data::WritePoints wp;
    wp.points.push_back(data::DataPoint{SeriesId128::fromSeriesKey(key), 1'700'000'000'000'000'000ULL, v});
    return data::encodeDataCommand(wp);
}

// Boot a 3-voter group with node 1 preferred leader, elect it, and commit one write.
seastar::future<> bootLedCluster(Nodes& nodes, std::vector<RouterTransport>& transports, Router& router,
                                 bool checkQuorum, const std::string& tag) {
    transports.reserve(4);
    for (NodeId id : {1, 2, 3}) {
        transports.emplace_back(router);
        auto box = std::make_unique<NodeBox>();
        box->dir = tmpDir(tag + std::to_string(id));
        box->id = id;
        box->router = &router;
        box->transport = &transports.back();
        co_await box->boot({1, 2, 3}, optsFor(id, 1, checkQuorum));
        nodes[id] = std::move(box);
    }
    co_await tickAndPump(nodes, router, 12);
    EXPECT_TRUE(nodes[1]->group->isLeader()) << "node 1 must lead before quorum is removed";
    auto f = nodes[1]->group->proposeAndAwaitApplied(cmd("healthy", 1.0));
    for (int i = 0; i < 40 && !f.available(); ++i)
        co_await tickAndPump(nodes, router, 1);
    EXPECT_TRUE(co_await std::move(f)) << "a healthy quorum must commit";
}

seastar::future<> teardown(Nodes& nodes) {
    for (auto& [id, n] : nodes)
        co_await n->shutdown();
    for (auto& [id, n] : nodes)
        fs::remove_all(n->dir);
}

// checkQuorum OFF -- the pre-fix world, where NOTHING else can save the write. Pins both
// the defect and the fix: the unbounded waiter is shown still pending after generous
// driving, and the bounded one fails inside its deadline.
seastar::future<> testDeadlineIsTheOnlyEscape() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await bootLedCluster(nodes, transports, router, /*checkQuorum=*/false, "nq");

    router.partition(2);
    router.partition(3);  // quorum lost: the leader can append, never commit

    // (1) THE DEFECT. Asserted, not described: after 60 rounds of ticking and pumping the
    // unbounded waiter has not resolved and never will -- with checkQuorum off the leader
    // does not step down, so no drainReady will ever resolve or fail it.
    auto hung = nodes[1]->group->proposeAndAwaitApplied(cmd("hangs", 2.0));
    co_await tickAndPump(nodes, router, 60);
    EXPECT_FALSE(hung.available()) << "an unbounded propose resolved without a quorum -- this test's premise is broken";
    EXPECT_EQ(nodes[1]->group->pendingApplyWaiters(), 1u);

    // (2) THE FIX. Same condition, bounded: it fails inside its deadline.
    const auto t0 = std::chrono::steady_clock::now();
    data::WriteFailure kind = data::WriteFailure::None;
    bool threw = false;
    try {
        co_await nodes[1]->group->proposeAndAwaitApplied(cmd("bounded", 3.0),
                                                         seastar::lowres_clock::now() + std::chrono::milliseconds(300));
    } catch (...) {
        threw = true;
        kind = data::classifyLocalWriteFailure(std::current_exception());
    }
    const auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_TRUE(threw) << "a bounded propose against a lost quorum must FAIL, not hang (fail-closed)";
    EXPECT_LT(elapsedMs, 5000) << "the bounded propose took " << elapsedMs
                               << "ms -- the deadline is not reaching the waiter";
    // It must be RETRYABLE and AMBIGUOUS: the entry is appended locally and a later quorum
    // may still commit it, so the retry is safe only under LWW re-application.
    EXPECT_EQ(kind, data::WriteFailure::Transport) << "kind=" << data::writeFailureName(kind);
    EXPECT_TRUE(data::isRetryableWriteFailure(kind));
    EXPECT_TRUE(data::isAmbiguousWriteFailure(kind));
    EXPECT_EQ(nodes[1]->group->pendingApplyWaiters(), 1u)
        << "the timed-out waiter must be reclaimed; only the deliberately unbounded waiter remains";

    // Heal so the hung waiter resolves; never leave a promise with a pending future.
    router.heal(2);
    router.heal(3);
    for (int i = 0; i < 400 && !hung.available(); ++i)
        co_await tickAndPump(nodes, router, 1);
    EXPECT_TRUE(hung.available()) << "healing must resolve the previously-hung waiter";
    co_await std::move(hung).then_wrapped([](seastar::future<bool> f) { f.ignore_ready_future(); });
    EXPECT_EQ(nodes[1]->group->pendingApplyWaiters(), 0u);
    co_await teardown(nodes);
}

// checkQuorum ON. This is the belt to the deadline's braces, and the property CheckQuorum
// is WANTED for -- it builds its own RaftOptions, and the production data plane currently
// leaves the flag off (debt D-9/D-29: the bypass is built and proven, the measured
// one-node-down cost is not yet worth paying), so this test is what keeps the behaviour
// pinned for the release that turns it on. A
// leader that stops hearing from a majority STEPS DOWN within an election timeout, so a
// quorum-less write fails on its own: either the waiter is failed by the step-down
// (LeadershipLost) or the propose is refused outright once it is no longer leader. What
// must NOT happen is what the unbounded test above shows: hanging.
seastar::future<> testCheckQuorumFailsWritesWithoutADeadline() {
    Router router;
    std::vector<RouterTransport> transports;
    Nodes nodes;
    co_await bootLedCluster(nodes, transports, router, /*checkQuorum=*/true, "cq");

    router.partition(2);
    router.partition(3);

    bool settled = false;
    bool committed = false;
    data::WriteFailure kind = data::WriteFailure::None;
    auto f = nodes[1]->group->proposeAndAwaitApplied(cmd("cq", 2.0));
    for (int i = 0; i < 200 && !f.available(); ++i)
        co_await tickAndPump(nodes, router, 1);
    if (f.available()) {
        settled = true;
        try {
            committed = co_await std::move(f);
        } catch (...) {
            kind = data::classifyLocalWriteFailure(std::current_exception());
        }
    } else {
        co_await std::move(f).then_wrapped([](seastar::future<bool> g) { g.ignore_ready_future(); });
    }
    EXPECT_TRUE(settled) << "with checkQuorum the leader must step down and drain its waiters";
    EXPECT_FALSE(committed) << "a write can NEVER commit without a quorum";
    if (kind != data::WriteFailure::None) {
        EXPECT_EQ(kind, data::WriteFailure::LeadershipLost) << "kind=" << data::writeFailureName(kind);
        EXPECT_TRUE(data::isRetryableWriteFailure(kind));
    }
    EXPECT_FALSE(nodes[1]->group->isLeader()) << "a leader without a quorum must not keep believing it leads";

    co_await teardown(nodes);
}

}  // namespace

TEST(RaftProposeDeadlineTest, QuorumLossHangsUnboundedAndFailsWithinTheDeadline) {
    testDeadlineIsTheOnlyEscape().get();
}
TEST(RaftProposeDeadlineTest, CheckQuorumFailsAQuorumLessWriteOnItsOwn) {
    testCheckQuorumFailsWritesWithoutADeadline().get();
}
