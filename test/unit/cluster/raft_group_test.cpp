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
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<LogEntry>) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistSnapshot(Snapshot, bool) override { return seastar::make_ready_future<>(); }
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
            auto group = std::make_unique<RaftGroup>(/*groupId=*/1, std::move(node), *persistence,
                                                     *transport, *sm);
            persistence_[id] = std::move(persistence);
            sm_[id] = std::move(sm);
            transport_[id] = std::move(transport);
            groups_[id] = std::move(group);
        }
    }

    RaftGroup& group(NodeId id) { return *groups_.at(id); }
    const std::vector<std::string>& applied(NodeId id) const { return sm_.at(id)->applied; }

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

}  // namespace

TEST(RaftGroupTest, ElectsAndReplicatesThroughTheAsyncDriver) {
    testElectAndReplicate().get();
}

TEST(RaftGroupTest, ElectsViaTimerTicks) {
    testElectionViaTicks().get();
}

TEST(RaftGroupTest, ConfigChangeThroughTheAsyncDriver) {
    testConfChangeThroughDriver().get();
}
