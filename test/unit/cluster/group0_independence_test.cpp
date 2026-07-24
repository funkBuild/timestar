// Phase-3 gate 1: existing data Raft groups continue serving through
// control-plane (group-0) loss, and the cluster reconverges afterward. Two
// independent Raft groups (group 0 = control, group 7 = data) on the same nodes
// but with DIFFERENT leaders; isolating the group-0 leader must not stop the
// data group, which is led elsewhere. In-memory router; no sockets.
#include "../../../lib/cluster/control/group0_state_machine.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <vector>

using namespace timestar::control;
using namespace timestar::raft;

namespace {

class NoopPersistence : public RaftPersistence {
public:
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<LogEntry>) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistSnapshot(Snapshot) override { return seastar::make_ready_future<>(); }
    seastar::future<> sync() override { return seastar::make_ready_future<>(); }
};

class DataSM : public RaftStateMachine {
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
    RouterTransport(Router& r, NodeId self) : r_(r), self_(self) {}
    seastar::future<> send(Envelope env) override;

private:
    Router& r_;
    NodeId self_;
};

// Routes by (groupId, destination node). Partition drops by node.
class Router {
public:
    void setGroup(uint16_t gid, NodeId node, RaftGroup* g) { groups_[{gid, node}] = g; }
    void enqueue(uint16_t gid, Envelope e) { queue_.push_back({gid, std::move(e)}); }
    void isolate(NodeId n) { down_.insert(n); }
    void heal(NodeId n) { down_.erase(n); }

    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 200000) {
            auto [gid, e] = std::move(queue_.front());
            queue_.pop_front();
            if (down_.count(e.message.to) || down_.count(e.message.from))
                continue;
            auto it = groups_.find({gid, e.message.to});
            if (it != groups_.end() && it->second)
                co_await it->second->step(std::move(e.message));
        }
    }

private:
    std::map<std::pair<uint16_t, NodeId>, RaftGroup*> groups_;
    std::set<NodeId> down_;
    std::deque<std::pair<uint16_t, Envelope>> queue_;
};

seastar::future<> RouterTransport::send(Envelope env) {
    const uint16_t gid = env.groupId;
    r_.enqueue(gid, std::move(env));
    return seastar::make_ready_future<>();
}

struct GroupBox {
    std::unique_ptr<NoopPersistence> persistence;
    std::unique_ptr<RaftStateMachine> sm;
    std::unique_ptr<RaftGroup> group;
};

RaftOptions opts(unsigned timeout) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = timeout;
    o.heartbeatTimeout = 1;
    return o;
}

seastar::future<> testDataGroupContinuesThroughControlPlaneLoss() {
    const std::vector<NodeId> voters = {1, 2, 3};
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    // key: (gid, node)
    std::map<std::pair<uint16_t, NodeId>, GroupBox> boxes;

    auto make = [&](uint16_t gid, NodeId id, unsigned timeout, bool control) {
        transports.push_back(std::make_unique<RouterTransport>(router, id));
        GroupBox b;
        b.persistence = std::make_unique<NoopPersistence>();
        if (control)
            b.sm = std::make_unique<Group0StateMachine>();
        else
            b.sm = std::make_unique<DataSM>();
        RaftNode rn(id, voters, RaftLog{}, HardState{}, opts(timeout));
        b.group = std::make_unique<RaftGroup>(gid, std::move(rn), *b.persistence, *transports.back(),
                                              *b.sm);
        router.setGroup(gid, id, b.group.get());
        boxes[{gid, id}] = std::move(b);
    };

    // Group 0 (control): node 1 leads; staggered timeouts so node 2 wins the
    // re-election cleanly after node 1 is isolated (no split-vote livelock).
    make(0, 1, 2, true);
    make(0, 2, 6, true);
    make(0, 3, 12, true);
    // Group 7 (data): node 2 leads.
    make(7, 1, 20, false);
    make(7, 2, 2, false);
    make(7, 3, 20, false);

    auto& g0_1 = *boxes[{0, 1}].group;
    auto& g7_2 = *boxes[{7, 2}].group;
    auto* data2 = static_cast<DataSM*>(boxes[{7, 2}].sm.get());
    auto* data3 = static_cast<DataSM*>(boxes[{7, 3}].sm.get());

    co_await g0_1.campaign();
    co_await g7_2.campaign();
    co_await router.pump();
    EXPECT_TRUE(g0_1.isLeader());   // control-plane leader = node 1
    EXPECT_TRUE(g7_2.isLeader());   // data-plane leader = node 2

    // A data write commits normally.
    co_await g7_2.propose("d1");
    co_await router.pump();
    EXPECT_EQ(data3->applied.size(), 1u);

    // CONTROL-PLANE LOSS: isolate the group-0 leader (node 1). Group 0 can no
    // longer commit through node 1; the data group (led by node 2) is untouched.
    router.isolate(1);

    // The data plane keeps serving reads and writes THROUGHOUT the control-plane
    // disruption -- node 2 still has the {2,3} majority for group 7.
    co_await g7_2.propose("d2");
    co_await g7_2.propose("d3");
    co_await router.pump();
    EXPECT_EQ(data2->applied.size(), 3u);
    EXPECT_EQ(data3->applied.size(), 3u);

    // The control plane recovers on its own: group 0 re-elects among {2,3}.
    for (int i = 0; i < 40; ++i) {
        co_await boxes[{0, 2}].group->tick();
        co_await boxes[{0, 3}].group->tick();
        co_await router.pump();
    }
    const bool g0Recovered = boxes[{0, 2}].group->isLeader() || boxes[{0, 3}].group->isLeader();
    EXPECT_TRUE(g0Recovered);

    // And the data group kept every committed write (never regressed).
    EXPECT_EQ(data2->applied.size(), 3u);
    EXPECT_EQ(data2->applied[0], "d1");
    EXPECT_EQ(data2->applied[2], "d3");
}

}  // namespace

TEST(Group0IndependenceTest, DataGroupContinuesThroughControlPlaneLoss) {
    testDataGroupContinuesThroughControlPlaneLoss().get();
}
