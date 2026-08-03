// Async driver test: a network of RaftGroup drivers on the reactor, with
// in-memory persistence, a queue-routing transport, and recording state
// machines. Exercises the real Ready/persist/send/apply/advance loop (not the
// bare state machine) end to end.
#include "../../../lib/cluster/raft/raft_group.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <deque>
#include <system_error>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/later.hh>
#include <stdexcept>
#include <vector>

using namespace timestar::raft;

namespace {

std::vector<std::string> splitLines(const std::string& data) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : data) {
        if (c == '\n') {
            if (!cur.empty())
                out.push_back(cur);
            cur.clear();
        } else
            cur.push_back(c);
    }
    if (!cur.empty())
        out.push_back(cur);
    return out;
}

class NoopPersistence : public RaftPersistence {
public:
    // The snapshot payloads this persistence was handed, in order. Recorded (rather than
    // discarded like the rest) because `drainReady` persists and then applies the SAME
    // Snapshot, and debt D-32 made the second of those a MOVE -- so "both saw the whole
    // payload" is now an invariant with a way to break it. See
    // testAReceivedSnapshotIsPersistedAndAppliedWithItsWholePayload.
    std::vector<std::string> snapshots;
    bool failNextSync = false;
    bool available = true;
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<LogEntry>) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistSnapshot(Snapshot s, bool) override {
        snapshots.push_back(std::move(s.data));
        return seastar::make_ready_future<>();
    }
    seastar::future<> sync() override {
        if (!failNextSync)
            return seastar::make_ready_future<>();
        failNextSync = false;
        available = false;
        return seastar::make_exception_future<>(
            std::system_error(std::make_error_code(std::errc::no_space_on_device), "Raft journal sync"));
    }
    bool durabilityAvailable() const override { return available; }
};

class RecordingSM : public RaftStateMachine {
public:
    std::vector<std::string> applied;
    seastar::future<> apply(LogEntry e) override {
        applied.push_back(e.data);
        return seastar::make_ready_future<>();
    }
    seastar::future<> applySnapshot(Snapshot s) override {
        applied = splitLines(s.data);
        return seastar::make_ready_future<>();
    }
};

class NullTransport : public RaftTransport {
public:
    seastar::future<> send(Envelope) override { return seastar::make_ready_future<>(); }
};

class PausingSnapshotSM : public RaftStateMachine {
public:
    bool snapshotStarted = false;
    seastar::promise<> releaseSnapshot;

    seastar::future<> apply(LogEntry) override { return seastar::make_ready_future<>(); }
    seastar::future<> applySnapshot(Snapshot) override {
        snapshotStarted = true;
        return releaseSnapshot.get_future();
    }
};

class GroupNetwork;

class QueueTransport : public RaftTransport {
public:
    explicit QueueTransport(GroupNetwork& net) : net_(net) {}
    seastar::future<> send(Envelope env) override;

private:
    GroupNetwork& net_;
};

class GroupNetwork {
public:
    GroupNetwork(std::vector<NodeId> voters, RaftOptions opts) : voters_(voters) {
        for (NodeId id : voters) {
            auto persistence = std::make_unique<NoopPersistence>();
            auto sm = std::make_unique<RecordingSM>();
            auto transport = std::make_unique<QueueTransport>(*this);
            RaftNode node(id, voters, RaftLog{}, HardState{}, opts);
            auto group = std::make_unique<RaftGroup>(/*groupId=*/1, std::move(node), *persistence, *transport, *sm);
            persistence_[id] = std::move(persistence);
            sm_[id] = std::move(sm);
            transport_[id] = std::move(transport);
            groups_[id] = std::move(group);
        }
    }

    // When set, every outbound send FAILS -- which makes `RaftGroup::drainReady` throw
    // AFTER the core has already acted on the input that produced those messages. That
    // asymmetry is the point: see testAnArmedTransferSurvivesADrainFailure.
    bool failSends = false;

    RaftGroup& group(NodeId id) { return *groups_.at(id); }
    const std::vector<std::string>& applied(NodeId id) const { return sm_.at(id)->applied; }
    const std::vector<std::string>& persistedSnapshots(NodeId id) const { return persistence_.at(id)->snapshots; }
    void failNextSync(NodeId id) { persistence_.at(id)->failNextSync = true; }

    void enqueue(Envelope e) { queue_.push_back(std::move(e)); }
    void discardQueued() { queue_.clear(); }
    size_t queued() const { return queue_.size(); }

    // Deliver all queued envelopes (and any they cascade) until quiescent.
    seastar::future<bool> pumpOne() {
        if (queue_.empty())
            co_return false;
        Envelope e = std::move(queue_.front());
        queue_.pop_front();
        auto it = groups_.find(e.message.to);
        if (it != groups_.end())
            co_await it->second->step(std::move(e.message));
        co_return true;
    }

    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 100000)
            co_await pumpOne();
    }

    seastar::future<> tickAll() {
        for (NodeId id : voters_)
            co_await groups_[id]->tick();
    }

    NodeId leader() const {
        for (NodeId id : voters_)
            if (groups_.at(id)->isLeader())
                return id;
        return kNoNode;
    }

private:
    std::vector<NodeId> voters_;
    std::map<NodeId, std::unique_ptr<NoopPersistence>> persistence_;
    std::map<NodeId, std::unique_ptr<RecordingSM>> sm_;
    std::map<NodeId, std::unique_ptr<QueueTransport>> transport_;
    std::map<NodeId, std::unique_ptr<RaftGroup>> groups_;
    std::deque<Envelope> queue_;
};

seastar::future<> QueueTransport::send(Envelope env) {
    if (net_.failSends)
        return seastar::make_exception_future<>(std::runtime_error("transport is down"));
    net_.enqueue(std::move(env));
    return seastar::make_ready_future<>();
}

RaftOptions opts() {
    RaftOptions o;
    o.electionTimeoutMin = 3;
    o.electionTimeoutMax = 3;
    o.heartbeatTimeout = 1;
    return o;
}

seastar::future<> testElectAndReplicate() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    co_await net.group(1).propose("alpha");
    co_await net.group(1).propose("beta");
    co_await net.pump();

    for (NodeId id : {1u, 2u, 3u}) {
        if (net.applied(id).size() != 2u) {
            ADD_FAILURE() << "node " << id << " applied " << net.applied(id).size();
            continue;
        }
        EXPECT_EQ(net.applied(id)[0], "alpha");
        EXPECT_EQ(net.applied(id)[1], "beta");
    }
}

seastar::future<> testElectionViaTicks() {
    GroupNetwork net({1, 2, 3}, opts());
    // Drive election purely through timer ticks (no explicit campaign).
    for (int i = 0; i < 4; ++i) {
        co_await net.group(1).tick();
        co_await net.pump();
    }
    EXPECT_NE(net.leader(), kNoNode);
    co_await net.group(net.leader()).propose("x");
    co_await net.pump();
    EXPECT_EQ(net.applied(net.leader()).size(), 1u);
}

void testRecoveredTailIsAccountedForGroupLifetime() {
    RaftLog recovered;
    LogEntry first;
    first.term = 3;
    first.data = "recovered-one";
    LogEntry second;
    second.term = 3;
    second.data = "recovered-two";
    recovered.append({first, second});
    const size_t expected = estimatedLogEntryBytes(first.data.size()) + estimatedLogEntryBytes(second.data.size());

    UncommittedProposalBudget budget(expected * 2, expected * 2);
    RaftOptions o = opts();
    o.uncommittedProposalBudget = &budget;
    NoopPersistence persistence;
    NullTransport transport;
    RecordingSM sm;
    {
        RaftNode node(1, {1, 2, 3}, std::move(recovered), HardState{3, kNoNode}, o);
        RaftGroup group(17, std::move(node), persistence, transport, sm);
        EXPECT_EQ(budget.current(), expected)
            << "recovered entries are uncommitted until a current-term quorum proves otherwise";
        EXPECT_EQ(budget.groupCurrent(17), expected);
    }
    EXPECT_EQ(budget.current(), 0u) << "destroying a hosted group must release its shard contribution";
}

// PERSIST AND APPLY BOTH GET THE WHOLE PAYLOAD (debt D-32). `drainReady` hands the same
// Snapshot to `persistSnapshot` and then to `applySnapshot`, and both take it BY VALUE.
// D-32 turned the second into a MOVE, which is the saving -- and which makes "persist must
// not be the one that moves" a live rule rather than an observation: swapping them leaves
// the state machine installing an EMPTY snapshot, i.e. a replica that reports itself
// caught up over a hole, with nothing else in this suite noticing.
seastar::future<> testASnapshotIsPersistedAndAppliedWithItsWholePayload() {
    GroupNetwork net({1, 2, 3}, opts());
    const std::string payload = "alpha\nbeta\ngamma\n";

    InstallSnapshot is;
    is.term = 1;
    is.leaderId = 2;
    is.lastIncludedIndex = 4;
    is.lastIncludedTerm = 1;
    is.config.voters = {1, 2, 3};
    is.data = payload;
    is.offset = 0;
    is.totalBytes = payload.size();
    is.done = true;
    co_await net.group(1).step(Message{.to = 1, .from = 2, .payload = is});
    co_await net.pump();

    // ASSERT_* cannot be used in a coroutine (it returns void), so guard explicitly.
    EXPECT_EQ(net.persistedSnapshots(1).size(), 1u);
    if (net.persistedSnapshots(1).size() != 1u)
        co_return;
    EXPECT_EQ(net.persistedSnapshots(1)[0], payload) << "the journal must get the whole payload";
    EXPECT_EQ(net.applied(1), (std::vector<std::string>{"alpha", "beta", "gamma"}))
        << "the state machine must get the whole payload too -- an empty install here is silent loss";
    // The core still holds it as SERVABLE state, so this node can catch a follower up.
    EXPECT_EQ(net.group(1).node().servableSnapshot().data, payload);
}

// A snapshot install is deliberately two-phase inside Engine today: verified TSM data is
// registered, then the authenticated catalog is rebuilt into NativeIndex. Cluster reads
// are still atomic at the Raft boundary only if the group does NOT publish appliedIndex
// until that whole state-machine future resolves. EngineLocalStore's production apply
// fence reads this exact commit/applied gap; advancing early would let a query observe
// installed values with an old/empty discovery index.
seastar::future<> testSnapshotApplyLagSpansTheWholeStateMachineInstall() {
    NoopPersistence persistence;
    NullTransport transport;
    PausingSnapshotSM sm;
    RaftNode node(/*id=*/1, /*voters=*/{1, 2}, RaftLog{}, HardState{}, opts());
    RaftGroup group(/*groupId=*/1, std::move(node), persistence, transport, sm);

    InstallSnapshot snapshot;
    snapshot.term = 1;
    snapshot.leaderId = 2;
    snapshot.lastIncludedIndex = 7;
    snapshot.lastIncludedTerm = 1;
    snapshot.config.voters = {1, 2};
    snapshot.data = "complete snapshot payload";
    snapshot.offset = 0;
    snapshot.totalBytes = snapshot.data.size();
    snapshot.done = true;

    auto installing = group.step(Message{.to = 1, .from = 2, .payload = snapshot});
    co_await seastar::yield();
    EXPECT_TRUE(sm.snapshotStarted) << "the test must be paused inside state-machine installation";
    EXPECT_FALSE(installing.available());
    EXPECT_EQ(group.commitIndex(), 7u) << "the durable snapshot boundary is already committed";
    EXPECT_EQ(group.appliedIndex(), 0u) << "partial data/catalog publication must not become readable";
    EXPECT_EQ(group.applyLag(), 7u) << "the production read fence must see and reject this partial interval";

    sm.releaseSnapshot.set_value();
    co_await std::move(installing);
    EXPECT_EQ(group.appliedIndex(), 7u);
    EXPECT_EQ(group.applyLag(), 0u);
}

seastar::future<> testConfChangeThroughDriver() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);
    bool ok = co_await net.group(1).proposeConfChange({1, 2}, {});  // shrink to {1,2}
    EXPECT_TRUE(ok);
    co_await net.pump();
    EXPECT_FALSE(net.group(1).node().config().joint());
    EXPECT_EQ(net.group(1).node().config().voters, (std::vector<NodeId>{1, 2}));
}

seastar::future<> testAwaitedConfChangeDoesNotAckAtTheJointBoundary() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    const LogIndex before = net.group(1).node().log().lastIndex();
    auto changing = net.group(1).proposeConfChangeAndAwaitApplied({1, 2}, {});
    co_await seastar::yield();
    const LogIndex jointIndex = before + 1;

    // Deliver only until the joint entry applies. The core has appended final
    // Cnew by this point, but it has not necessarily committed it. This is the
    // exact boundary the old controller incorrectly treated as success.
    for (int i = 0; i < 100 && net.group(1).appliedIndex() < jointIndex; ++i) {
        EXPECT_TRUE(co_await net.pumpOne());
        co_await seastar::yield();
    }
    EXPECT_EQ(net.group(1).appliedIndex(), jointIndex);
    EXPECT_GT(net.group(1).node().latestConfigIndex(), jointIndex);
    EXPECT_FALSE(changing.available()) << "membership must wait for applied final Cnew";

    co_await net.pump();
    co_await seastar::yield();
    EXPECT_TRUE(co_await std::move(changing));
    EXPECT_FALSE(net.group(1).node().config().joint());
    EXPECT_EQ(net.group(1).node().config().voters, (std::vector<NodeId>{1, 2}));
}

seastar::future<> testAwaitedConfChangeSucceedsWhenFinalConfigRemovesLeader() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    auto removingSelf = net.group(1).proposeConfChangeAndAwaitApplied({2, 3}, {});
    co_await seastar::yield();
    EXPECT_FALSE(removingSelf.available());
    co_await net.pump();
    co_await seastar::yield();

    EXPECT_TRUE(co_await std::move(removingSelf));
    EXPECT_FALSE(net.group(1).isLeader());
    EXPECT_EQ(net.group(1).node().config().voters, (std::vector<NodeId>{2, 3}));
}

seastar::future<> testSingleVoterLearnerChangeTracksTheJointEntry() {
    NoopPersistence persistence;
    NullTransport transport;
    RecordingSM sm;
    RaftNode node(/*id=*/1, /*voters=*/{1}, RaftLog{}, HardState{}, opts());
    RaftGroup group(/*groupId=*/1, std::move(node), persistence, transport, sm);
    co_await group.campaign();
    EXPECT_TRUE(group.isLeader());

    EXPECT_TRUE(co_await group.proposeConfChangeAndAwaitApplied({1}, {2}));
    EXPECT_FALSE(group.node().config().joint());
    EXPECT_EQ(group.node().config().voters, (std::vector<NodeId>{1}));
    EXPECT_EQ(group.node().config().learners, (std::vector<NodeId>{2}));
}

seastar::future<> testConfigChangeDeadlineReclaimsJointWaiter() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    // Do not deliver the joint proposal to either follower. With CheckQuorum
    // disabled this leader remains leader, so only the request deadline can
    // release the joint-entry waiter.
    auto changing = net.group(1).proposeConfChangeAndAwaitApplied(
        {1, 2}, {}, seastar::lowres_clock::now() + std::chrono::milliseconds(20));
    co_await seastar::yield();
    EXPECT_EQ(net.group(1).pendingApplyWaiters(), 1u);
    EXPECT_THROW(co_await std::move(changing), seastar::timed_out_error);
    EXPECT_EQ(net.group(1).pendingApplyWaiters(), 0u);
    EXPECT_EQ(net.group(1).pendingConfigWaiters(), 0u);
}

seastar::future<> testConfigChangeDeadlineReclaimsFinalWaiter() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    const LogIndex jointIndex = net.group(1).node().log().lastIndex() + 1;
    auto changing = net.group(1).proposeConfChangeAndAwaitApplied(
        {1, 2}, {}, seastar::lowres_clock::now() + std::chrono::milliseconds(100));
    co_await seastar::yield();
    for (int i = 0; i < 100 && net.group(1).appliedIndex() < jointIndex; ++i) {
        EXPECT_TRUE(co_await net.pumpOne());
        co_await seastar::yield();
    }
    EXPECT_EQ(net.group(1).appliedIndex(), jointIndex);
    co_await seastar::yield();
    EXPECT_EQ(net.group(1).pendingApplyWaiters(), 0u);
    EXPECT_EQ(net.group(1).pendingConfigWaiters(), 1u);

    const LogIndex beforeRetry = net.group(1).node().log().lastIndex();
    EXPECT_FALSE(co_await net.group(1).proposeConfChange({1, 3}, {}))
        << "final Cnew is stable-looking but still uncommitted; a retry must not overlap it";
    EXPECT_EQ(net.group(1).node().log().lastIndex(), beforeRetry)
        << "refusing an overlapping membership change must happen before append";

    // The final Cnew is appended locally, but discard its queued replication.
    // The same absolute operation deadline must reclaim this second waiter.
    net.discardQueued();
    EXPECT_THROW(co_await std::move(changing), seastar::timed_out_error);
    EXPECT_EQ(net.group(1).pendingConfigWaiters(), 0u);
}

seastar::future<> testReadyDrainFailuresReclaimProposalWaiters() {
    {
        GroupNetwork net({1, 2, 3}, opts());
        co_await net.group(1).campaign();
        co_await net.pump();
        EXPECT_EQ(net.leader(), 1u);

        net.failSends = true;
        EXPECT_THROW(co_await net.group(1).proposeAndAwaitApplied("will-not-send"), std::runtime_error);
        EXPECT_EQ(net.group(1).pendingApplyWaiters(), 0u)
            << "a Ready-drain error must not strand an unreachable ordinary proposal waiter";
    }
    {
        GroupNetwork net({1, 2, 3}, opts());
        co_await net.group(1).campaign();
        co_await net.pump();
        EXPECT_EQ(net.leader(), 1u);

        net.failSends = true;
        EXPECT_THROW(co_await net.group(1).proposeConfChangeAndAwaitApplied({1, 2}, {}), std::runtime_error);
        EXPECT_EQ(net.group(1).pendingApplyWaiters(), 0u)
            << "a Ready-drain error must not strand an unreachable membership waiter";
        EXPECT_EQ(net.group(1).pendingConfigWaiters(), 0u);
    }
    {
        GroupNetwork net({1, 2, 3}, opts());
        co_await net.group(1).campaign();
        co_await net.pump();
        EXPECT_EQ(net.leader(), 1u);

        net.failSends = true;
        EXPECT_THROW(co_await net.group(1).readBarrier(), std::runtime_error);
        EXPECT_EQ(net.group(1).pendingReadWaiters(), 0u);
        EXPECT_EQ(net.group(1).confirmedReadWaiters(), 0u);
        EXPECT_EQ(net.group(1).node().pendingReadIndexes(), 0u);
    }
}

// A failed durable Ready is not a transient tick error. The journal writer is fenced
// permanently after ENOSPC/EIO, so continuing to heartbeat from the in-memory leader
// pins leadership on a replica that can never accept another write. Quarantine must
// happen before this Ready sends anything, release every caller, and make the failed
// replica behave like an offline node so the other two voters can recover quorum.
seastar::future<> testDurabilityFailureQuarantinesReplicaAndAllowsReelection() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    auto barrier = net.group(1).readBarrier();
    co_await seastar::yield();
    EXPECT_EQ(net.group(1).pendingReadWaiters(), 1u);
    net.discardQueued();

    const LogIndex before = net.group(1).node().log().lastIndex();
    net.failNextSync(1);
    EXPECT_THROW(co_await net.group(1).proposeAndAwaitApplied("will-not-send"), DurabilityUnavailableError);
    EXPECT_EQ(net.queued(), 0u) << "no message from a Ready may precede its durability barrier";
    EXPECT_FALSE(net.group(1).durabilityAvailable());
    EXPECT_FALSE(net.group(1).isLeader()) << "routing must not advertise a fenced in-memory leader";
    EXPECT_EQ(net.group(1).leader(), kNoNode);
    EXPECT_EQ(net.group(1).pendingApplyWaiters(), 0u);
    EXPECT_EQ(net.group(1).pendingReadWaiters(), 0u);
    EXPECT_EQ(net.group(1).confirmedReadWaiters(), 0u);
    EXPECT_EQ(net.group(1).node().pendingReadIndexes(), 0u);
    EXPECT_THROW(co_await std::move(barrier), DurabilityUnavailableError);

    const LogIndex failedTail = net.group(1).node().log().lastIndex();
    EXPECT_EQ(failedTail, before + 1);
    EXPECT_THROW(co_await net.group(1).propose("must-fail-immediately"), DurabilityUnavailableError);
    EXPECT_EQ(net.group(1).node().log().lastIndex(), failedTail)
        << "a quarantined replica must reject before mutating the Raft core";
    co_await net.group(1).tick(100);
    EXPECT_EQ(net.queued(), 0u) << "a quarantined former leader must stop heartbeats";

    co_await net.group(2).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 2u) << "the healthy quorum must be able to replace the disk-failed leader";
    EXPECT_TRUE(co_await net.group(2).propose("after-failure"));
    co_await net.pump();
    EXPECT_EQ(net.applied(1).size(), 0u);
    EXPECT_EQ(net.applied(2), (std::vector<std::string>{"after-failure"}));
    EXPECT_EQ(net.applied(3), (std::vector<std::string>{"after-failure"}));
}

seastar::future<> testSharedPersistenceFailureQuarantinesHeartbeatOnlyGroups() {
    NoopPersistence persistence;
    NullTransport transport;
    RecordingSM firstSm;
    RecordingSM secondSm;
    RaftGroup first(1, RaftNode(1, {1}, RaftLog{}, HardState{}, opts()), persistence, transport, firstSm);
    RaftGroup second(2, RaftNode(1, {1}, RaftLog{}, HardState{}, opts()), persistence, transport, secondSm);
    co_await first.campaign();
    co_await second.campaign();
    EXPECT_TRUE(first.isLeader());
    EXPECT_TRUE(second.isLeader());

    persistence.failNextSync = true;
    EXPECT_THROW(co_await first.propose("trigger-shared-failure"), DurabilityUnavailableError);
    EXPECT_FALSE(first.durabilityAvailable());
    EXPECT_FALSE(second.durabilityAvailable())
        << "a heartbeat-only group must observe the shared writer fence without issuing its own append";
    EXPECT_FALSE(second.isLeader());
    EXPECT_EQ(second.leader(), kNoNode);

    co_await second.tick();
    EXPECT_FALSE(second.durabilityFailureReason().empty())
        << "the next driver pass must make the shared failure permanent and release its waiters";
    EXPECT_THROW(co_await second.propose("must-not-append"), DurabilityUnavailableError);
}

seastar::future<> testReadBarrierDeadlineReclaimsExactWaiter() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    auto barrier = net.group(1).readBarrier(seastar::lowres_clock::now() + std::chrono::milliseconds(20));
    co_await seastar::yield();
    EXPECT_EQ(net.group(1).pendingReadWaiters(), 1u);
    net.discardQueued();
    EXPECT_THROW(co_await std::move(barrier), seastar::timed_out_error);
    EXPECT_EQ(net.group(1).pendingReadWaiters(), 0u);
    EXPECT_EQ(net.group(1).confirmedReadWaiters(), 0u);
    EXPECT_EQ(net.group(1).node().pendingReadIndexes(), 0u)
        << "a partitioned leader must not retain one core request per expired caller";

    // A late response for the expired context must not recreate an orphaned
    // confirmed-read record, and a later barrier must still complete normally.
    co_await net.tickAll();
    co_await net.pump();
    EXPECT_EQ(net.group(1).confirmedReadWaiters(), 0u);
    EXPECT_EQ(net.group(1).node().pendingReadIndexes(), 0u);
    auto retry = net.group(1).readBarrier(seastar::lowres_clock::now() + std::chrono::milliseconds(100));
    co_await net.pump();
    EXPECT_GT(co_await std::move(retry), 0u);
}

// THE DRIVER MUST PROPAGATE "did a transfer actually start?" (debt D-24). The balancer's
// `transfers_initiated` counter reaches RaftNode through THIS seam, so a driver that
// swallowed the answer would leave the counter inflated no matter what the core reports --
// and the deposed-primary gate asserts an anti-vacuity floor on it.
seastar::future<> testTransferLeadershipReportsThroughTheDriver() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    EXPECT_FALSE(co_await net.group(2).transferLeadership(3)) << "a FOLLOWER cannot transfer anything";
    EXPECT_FALSE(co_await net.group(1).transferLeadership(1)) << "transferring to ourselves starts nothing";
    EXPECT_FALSE(co_await net.group(1).transferLeadership(9)) << "node 9 is not a voter of this group";

    EXPECT_TRUE(co_await net.group(1).transferLeadership(2)) << "a genuine transfer must report itself started";
    EXPECT_TRUE(net.group(1).transferInFlight());
    EXPECT_FALSE(co_await net.group(1).transferLeadership(2))
        << "the F2 re-arm guard ignored the repeat request but reported it as a transfer initiated";
}

// AN ARMED TRANSFER MUST STILL BE ACCOUNTED WHEN THE DRAIN FAILS (debt D-24, review
// finding 6). `transferLeadership` arms the transfer inside the core and THEN drains
// Ready, which persists and sends -- and can throw. A future carries a value or an
// exception and never both, so a caller reading only the returned bool records nothing
// while the group refuses proposals for the whole abandon window: the inflation this row
// fixed, with the sign flipped (an undercount, and a target whose deficit is never
// charged). The out-param is written before the drain, so it survives.
seastar::future<> testAnArmedTransferSurvivesADrainFailure() {
    GroupNetwork net({1, 2, 3}, opts());
    co_await net.group(1).campaign();
    co_await net.pump();
    EXPECT_EQ(net.leader(), 1u);

    net.failSends = true;  // the drain that follows the arming will now throw
    bool armed = false;
    bool threw = false;
    try {
        co_await net.group(1).transferLeadership(2, &armed);
    } catch (...) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "a persist/send failure must propagate, not be swallowed";
    EXPECT_TRUE(armed) << "the transfer IS armed (the group refuses proposals until the window expires) but the "
                       << "caller was told nothing -- transfers_initiated undercounts and the target's deficit "
                       << "is never charged";
    EXPECT_TRUE(net.group(1).transferInFlight()) << "the premise: the core really did arm it";
}

}  // namespace

TEST(RaftGroupTest, AnArmedTransferSurvivesADrainFailure) {
    testAnArmedTransferSurvivesADrainFailure().get();
}

TEST(RaftGroupTest, TransferLeadershipReportsThroughTheDriver) {
    testTransferLeadershipReportsThroughTheDriver().get();
}

TEST(RaftGroupTest, ElectsAndReplicatesThroughTheAsyncDriver) {
    testElectAndReplicate().get();
}

TEST(RaftGroupTest, ElectsViaTimerTicks) {
    testElectionViaTicks().get();
}

TEST(RaftGroupTest, RecoveredUncommittedTailIsAccountedForGroupLifetime) {
    testRecoveredTailIsAccountedForGroupLifetime();
}

TEST(RaftGroupTest, AReceivedSnapshotIsPersistedAndAppliedWithItsWholePayload) {
    testASnapshotIsPersistedAndAppliedWithItsWholePayload().get();
}

TEST(RaftGroupTest, SnapshotApplyLagSpansTheWholeStateMachineInstall) {
    testSnapshotApplyLagSpansTheWholeStateMachineInstall().get();
}

TEST(RaftGroupTest, ConfigChangeThroughTheAsyncDriver) {
    testConfChangeThroughDriver().get();
}

TEST(RaftGroupTest, AwaitedConfigChangeDoesNotAckAtTheJointBoundary) {
    testAwaitedConfChangeDoesNotAckAtTheJointBoundary().get();
}

TEST(RaftGroupTest, AwaitedConfigChangeSucceedsWhenFinalConfigRemovesLeader) {
    testAwaitedConfChangeSucceedsWhenFinalConfigRemovesLeader().get();
}

TEST(RaftGroupTest, SingleVoterLearnerChangeTracksTheJointEntry) {
    testSingleVoterLearnerChangeTracksTheJointEntry().get();
}

TEST(RaftGroupTest, ConfigChangeDeadlineReclaimsJointWaiter) {
    testConfigChangeDeadlineReclaimsJointWaiter().get();
}

TEST(RaftGroupTest, ConfigChangeDeadlineReclaimsFinalWaiter) {
    testConfigChangeDeadlineReclaimsFinalWaiter().get();
}

TEST(RaftGroupTest, ReadyDrainFailuresReclaimProposalWaiters) {
    testReadyDrainFailuresReclaimProposalWaiters().get();
}

TEST(RaftGroupTest, DurabilityFailureQuarantinesReplicaAndAllowsReelection) {
    testDurabilityFailureQuarantinesReplicaAndAllowsReelection().get();
}

TEST(RaftGroupTest, SharedPersistenceFailureQuarantinesHeartbeatOnlyGroups) {
    testSharedPersistenceFailureQuarantinesHeartbeatOnlyGroups().get();
}

TEST(RaftGroupTest, ReadBarrierDeadlineReclaimsExactWaiter) {
    testReadBarrierDeadlineReclaimsExactWaiter().get();
}
