// Group-0 control plane end to end: cluster init on one node, then admitting
// nodes in distinct failure domains GROWS the meta-voter set across those
// domains via joint consensus -- with no operator placement (Phase 3 gate 2).
// Runs the real group-0 RaftGroup + Group0StateMachine + Group0Controller over
// an in-memory router (no sockets).
#include "../../../lib/cluster/control/group0_controller.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <vector>

using namespace timestar::control;
using timestar::raft::Envelope;
using timestar::raft::HardState;
using timestar::raft::Message;
using timestar::raft::NodeId;
using timestar::raft::RaftGroup;
using timestar::raft::RaftLog;
using timestar::raft::RaftNode;
using timestar::raft::RaftOptions;
using timestar::raft::RaftPersistence;
using timestar::raft::RaftTransport;
using timestar::raft::Snapshot;

namespace {

class NoopPersistence : public RaftPersistence {
public:
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<timestar::raft::LogEntry>) override {
        return seastar::make_ready_future<>();
    }
    seastar::future<> persistSnapshot(Snapshot) override { return seastar::make_ready_future<>(); }
    seastar::future<> sync() override { return seastar::make_ready_future<>(); }
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
    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 200000) {
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
    std::unique_ptr<NoopPersistence> persistence;
    std::unique_ptr<Group0StateMachine> sm;
    std::unique_ptr<RaftGroup> group;
};

RaftOptions optsFor(NodeId id) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 50);  // node 1 leads
    o.heartbeatTimeout = 1;
    return o;
}

NodeRecord rec(NodeId id, std::string dom) {
    NodeRecord r;
    r.raftId = id;
    r.uuid = "uuid-" + std::to_string(id);
    r.address = "mem:" + std::to_string(id);
    r.failureDomain = std::move(dom);
    r.state = NodeState::Active;
    return r;
}

seastar::future<> testClusterInitGrowsMetaVotersAcrossDomains() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    std::map<NodeId, NodeBox> nodes;

    // group-0 voters start as {1}; nodes 2 and 3 are observers that know that.
    auto makeNode = [&](NodeId id, std::vector<NodeId> knownVoters) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, knownVoters, RaftLog{}, HardState{}, optsFor(id));
        box.group = std::make_unique<RaftGroup>(/*groupId=*/0, std::move(rn), *box.persistence,
                                                *transports.back(), *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1, {1});
    makeNode(2, {1});
    makeNode(3, {1});

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, /*metaTarget=*/3);

    // cluster init on node 1 (group of one).
    co_await nodes[1].group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1].group->isLeader());
    co_await controller.initCluster("cluster-xyz", rec(1, "rack-a"));
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().clusterUuid, "cluster-xyz");
    EXPECT_EQ(nodes[1].group->node().config().voters, (std::vector<NodeId>{1}));

    // Admit node 2 (rack-b), let it commit, then reconcile: meta voters -> {1,2}.
    co_await controller.admitNode(rec(2, "rack-b"));
    co_await router.pump();
    co_await controller.reconcileMetaVoters();
    co_await router.pump();
    EXPECT_EQ(nodes[1].group->node().config().voters, (std::vector<NodeId>{1, 2}));

    // Admit node 3 (rack-c), commit, reconcile: meta voters -> {1,2,3} (one per domain).
    co_await controller.admitNode(rec(3, "rack-c"));
    co_await router.pump();
    co_await controller.reconcileMetaVoters();
    co_await router.pump();
    EXPECT_EQ(nodes[1].group->node().config().voters, (std::vector<NodeId>{1, 2, 3}));
    EXPECT_FALSE(nodes[1].group->node().config().joint());

    // The state machine mirrors the voter set, and the new voters converged on
    // the full control state (cluster id, all node records, meta voters).
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().metaVoters, (std::vector<NodeId>{1, 2, 3}));
    for (NodeId id : {2u, 3u}) {
        EXPECT_EQ(nodes[id].sm->state().clusterUuid, "cluster-xyz") << "node " << id;
        EXPECT_EQ(nodes[id].sm->state().nodes.size(), 3u) << "node " << id;
    }
    // Failure-domain diversity: the three voters span three distinct domains.
    std::set<std::string> domains;
    for (NodeId v : nodes[1].group->node().config().voters)
        domains.insert(nodes[1].sm->state().nodes.at(v).failureDomain);
    EXPECT_EQ(domains.size(), 3u);

    // A controller epoch was stamped.
    EXPECT_GT(nodes[1].sm->state().controllerTerm, 0u);
}

seastar::future<> testReadBarrierReconcilesControlMap() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    std::map<NodeId, NodeBox> nodes;
    auto makeNode = [&](NodeId id, std::vector<NodeId> knownVoters) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, knownVoters, RaftLog{}, HardState{}, optsFor(id));
        box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(),
                                                *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1, {1, 2, 3});
    makeNode(2, {1, 2, 3});
    makeNode(3, {1, 2, 3});

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 3);
    co_await nodes[1].group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1].group->isLeader());
    co_await controller.initCluster("c1", rec(1, "rack-a"));
    co_await router.pump();

    // A committed topology change bumps the map epoch.
    co_await controller.proposeCommand(SetDesiredPlacement{5, {1, 2, 3}});
    co_await router.pump();
    const uint64_t epoch = nodes[1].sm->state().mapEpoch;
    EXPECT_GE(epoch, 1u);

    // Reconcile via a ReadIndex barrier: start it, drive the confirmation round,
    // then await it. The barrier index is at least the committed placement, and
    // the state read behind it reflects the new epoch (linearizable).
    auto rb = nodes[1].group->readBarrier();  // enqueues the heartbeat round
    co_await router.pump();                    // deliver heartbeats + replies -> confirm
    timestar::raft::LogIndex readIndex = co_await std::move(rb);
    EXPECT_GT(readIndex, 0u);
    EXPECT_EQ(nodes[1].sm->state().mapEpoch, epoch);
    EXPECT_EQ(nodes[1].sm->state().desiredPlacement.at(5), (std::vector<NodeId>{1, 2, 3}));

    // A follower's read barrier is rejected (redirect to the leader).
    bool rejected = false;
    try {
        co_await nodes[2].group->readBarrier();
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
}

seastar::future<> testJoinTokenGatesAdmission() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    std::map<NodeId, NodeBox> nodes;
    auto makeNode = [&](NodeId id) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, {1}, RaftLog{}, HardState{}, optsFor(id));
        box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(),
                                                *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1);

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 3);
    co_await nodes[1].group->campaign();
    co_await router.pump();
    co_await controller.initCluster("c1", rec(1, "rack-a"));
    co_await router.pump();

    // A node presenting an UNMINTED token is rejected.
    co_await controller.admitNodeWithToken(rec(9, "rack-x"), "bad-token");
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().nodes.count(9), 0u);

    // Mint a token, then the node joins with it -> admitted, token consumed.
    co_await controller.mintJoinToken("join-42");
    co_await router.pump();
    co_await controller.admitNodeWithToken(rec(2, "rack-b"), "join-42");
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().nodes.count(2), 1u);
    EXPECT_EQ(nodes[1].sm->state().nodes.at(2).state, NodeState::Active);
    EXPECT_EQ(nodes[1].sm->state().joinTokens.count("join-42"), 0u);
}

}  // namespace

TEST(Group0ControllerTest, ClusterInitGrowsMetaVotersAcrossFailureDomains) {
    testClusterInitGrowsMetaVotersAcrossDomains().get();
}

TEST(Group0ControllerTest, JoinTokenGatesAdmission) {
    testJoinTokenGatesAdmission().get();
}

TEST(Group0ControllerTest, ReadBarrierReconcilesControlMap) {
    testReadBarrierReconcilesControlMap().get();
}
