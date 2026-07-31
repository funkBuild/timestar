// Async driver test: a network of RaftGroup drivers on the reactor, with
// in-memory persistence, a queue-routing transport, and recording state
// machines. Exercises the real Ready/persist/send/apply/advance loop (not the
// bare state machine) end to end.
#include "../../../lib/cluster/raft/raft_group.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
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
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<LogEntry>) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistSnapshot(Snapshot s, bool) override {
        snapshots.push_back(std::move(s.data));
        return seastar::make_ready_future<>();
    }
    seastar::future<> sync() override { return seastar::make_ready_future<>(); }
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

    void enqueue(Envelope e) { queue_.push_back(std::move(e)); }

    // Deliver all queued envelopes (and any they cascade) until quiescent.
    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 100000) {
            Envelope e = std::move(queue_.front());
            queue_.pop_front();
            auto it = groups_.find(e.message.to);
            if (it != groups_.end())
                co_await it->second->step(std::move(e.message));
        }
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

TEST(RaftGroupTest, AReceivedSnapshotIsPersistedAndAppliedWithItsWholePayload) {
    testASnapshotIsPersistedAndAppliedWithItsWholePayload().get();
}

TEST(RaftGroupTest, ConfigChangeThroughTheAsyncDriver) {
    testConfChangeThroughDriver().get();
}
