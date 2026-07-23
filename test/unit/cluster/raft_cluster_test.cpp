// End-to-end multi-node Raft: a deterministic in-memory network drives real
// elections and log replication across several RaftNode state machines. No
// reactor, no I/O -- the network stands in for the Seastar-RPC + journal driver
// and honours the Ready contract (persist hardState+entries, then send, then
// apply committed).
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <set>
#include <vector>

using namespace timestar::raft;

namespace {

class Network {
public:
    Network(std::vector<NodeId> ids, RaftOptions opts) : ids_(ids) {
        for (NodeId id : ids) {
            nodes_[id] = std::make_unique<RaftNode>(id, ids, RaftLog{}, HardState{}, opts);
            applied_[id] = {};
        }
    }

    RaftNode& node(NodeId id) { return *nodes_.at(id); }
    const std::vector<std::string>& applied(NodeId id) const { return applied_.at(id); }

    // Drain every node's Ready, route messages to reachable peers, apply
    // committed entries, and repeat until the system is quiescent.
    void run() {
        bool progress = true;
        int guard = 0;
        while (progress && guard++ < 100000) {
            progress = false;
            for (NodeId id : ids_) {
                RaftNode& n = *nodes_[id];
                if (!n.hasReady())
                    continue;
                RaftNode::Ready rd = n.ready();
                n.advance(rd);
                for (auto& e : rd.committed) {
                    if (!e.data.empty())  // skip no-op commit markers
                        applied_[id].push_back(e.data);
                }
                for (auto& m : rd.messages) {
                    if (reachable(m.from, m.to))
                        inflight_.push_back(m);
                }
                progress = true;
            }
            std::deque<Message> batch;
            batch.swap(inflight_);
            for (auto& m : batch) {
                if (nodes_.count(m.to)) {
                    nodes_[m.to]->step(m);
                    progress = true;
                }
            }
        }
    }

    void tickAll() {
        for (NodeId id : ids_)
            nodes_[id]->tick();
    }

    // Partition control: drop messages between partitioned groups.
    void isolate(NodeId id) { isolated_.insert(id); }
    void heal(NodeId id) { isolated_.erase(id); }

    NodeId leader() const {
        for (NodeId id : ids_)
            if (nodes_.at(id)->isLeader())
                return id;
        return kNoNode;
    }
    int leaderCount() const {
        int n = 0;
        for (NodeId id : ids_)
            if (nodes_.at(id)->isLeader())
                ++n;
        return n;
    }

private:
    bool reachable(NodeId from, NodeId to) const {
        return isolated_.count(from) == 0 && isolated_.count(to) == 0;
    }

    std::vector<NodeId> ids_;
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
    std::map<NodeId, std::vector<std::string>> applied_;
    std::set<NodeId> isolated_;
    std::deque<Message> inflight_;
};

RaftOptions opts() {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;  // deterministic; we drive campaigns explicitly
    o.heartbeatTimeout = 1;
    return o;
}

}  // namespace

TEST(RaftClusterTest, ElectsExactlyOneLeader) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    EXPECT_EQ(net.leaderCount(), 1);
    EXPECT_EQ(net.leader(), 1u);
    EXPECT_EQ(net.node(2).role(), Role::Follower);
    EXPECT_EQ(net.node(3).role(), Role::Follower);
    EXPECT_EQ(net.node(2).currentTerm(), net.node(1).currentTerm());
    EXPECT_EQ(net.node(2).leader(), 1u);
}

TEST(RaftClusterTest, ReplicatesAndCommitsAProposal) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    EXPECT_TRUE(net.node(1).propose("alpha"));
    EXPECT_TRUE(net.node(1).propose("beta"));
    net.run();

    // Every node commits and applies both commands, in order.
    for (NodeId id : {1u, 2u, 3u}) {
        ASSERT_EQ(net.applied(id).size(), 2u) << "node " << id;
        EXPECT_EQ(net.applied(id)[0], "alpha");
        EXPECT_EQ(net.applied(id)[1], "beta");
    }
    EXPECT_GE(net.node(1).commitIndex(), 3u);  // noop@1 + alpha@2 + beta@3
    EXPECT_EQ(net.node(2).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftClusterTest, CommitsWithOneNodeDownButMajorityUp) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    net.isolate(3);  // node 3 offline; 1 and 2 still form a majority
    EXPECT_TRUE(net.node(1).propose("gamma"));
    net.run();

    EXPECT_EQ(net.applied(1).size(), 1u);
    EXPECT_EQ(net.applied(2).size(), 1u);
    EXPECT_EQ(net.applied(1)[0], "gamma");
    EXPECT_EQ(net.applied(3).size(), 0u);  // isolated, never received it

    // Heal node 3: a heartbeat catches it up.
    net.heal(3);
    net.tickAll();
    net.run();
    EXPECT_EQ(net.applied(3).size(), 1u);
    EXPECT_EQ(net.applied(3)[0], "gamma");
}

TEST(RaftClusterTest, NewLeaderCatchesUpALaggingFollower) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    // Node 3 misses a batch of proposals entirely.
    net.isolate(3);
    net.node(1).propose("a");
    net.node(1).propose("b");
    net.node(1).propose("c");
    net.run();
    ASSERT_EQ(net.applied(3).size(), 0u);

    // Rejoin: leader backtracks nextIndex via the conflict hint and refills 3.
    net.heal(3);
    net.tickAll();
    net.run();
    ASSERT_EQ(net.applied(3).size(), 3u);
    EXPECT_EQ(net.applied(3)[0], "a");
    EXPECT_EQ(net.applied(3)[2], "c");
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftClusterTest, LeaderStepsDownWhenPartitionedFromMajority) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    // Partition the leader away from the majority; elect a new leader among {2,3}.
    net.isolate(1);
    net.node(2).campaign();
    net.run();
    EXPECT_EQ(net.node(2).currentTerm(), 2u);
    NodeId newLeader = net.node(2).isLeader() ? 2u : (net.node(3).isLeader() ? 3u : kNoNode);
    ASSERT_NE(newLeader, kNoNode);

    // The new leader commits a proposal without the old one.
    EXPECT_TRUE(net.node(newLeader).propose("post-partition"));
    net.run();
    EXPECT_EQ(net.applied(newLeader).size(), 1u);

    // Heal: the old leader learns the higher term and steps down.
    net.heal(1);
    net.tickAll();
    net.run();
    EXPECT_EQ(net.node(1).role(), Role::Follower);
    EXPECT_EQ(net.node(1).currentTerm(), 2u);
    EXPECT_EQ(net.applied(1).size(), 1u);  // catches up the new leader's entry
}
