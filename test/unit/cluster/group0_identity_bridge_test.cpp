// Integration M3: the node-identity <-> group-0 bootstrap seam. A node's
// persistent node_uuid (node.json) flows into the control-plane NodeRecord it
// presents to the REAL Group0Controller::initCluster, and the cluster UUID that
// group-0 mints flows back and binds into node.json -- verified end to end over
// the in-memory group-0 Raft harness (same pattern as group0_controller_test).
#include "../../../lib/cluster/integration/group0_identity_bridge.hpp"

#include "../../../lib/cluster/control/group0_controller.hpp"
#include "../../../lib/cluster/integration/node_identity.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <vector>

using namespace timestar::control;
using timestar::cluster::bindClusterUuid;
using timestar::cluster::NodeIdentity;
using timestar::cluster::nodeRecordFrom;
using timestar::raft::Envelope;
using timestar::raft::HardState;
using timestar::raft::RaftGroup;
using timestar::raft::RaftLog;
using timestar::raft::RaftNode;
using timestar::raft::RaftOptions;
using timestar::raft::RaftPersistence;
using timestar::raft::RaftTransport;
using timestar::raft::Snapshot;
using NodeIdT = timestar::raft::NodeId;

namespace {

class NoopPersistence : public RaftPersistence {
public:
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<timestar::raft::LogEntry>) override {
        return seastar::make_ready_future<>();
    }
    seastar::future<> persistSnapshot(Snapshot, bool) override { return seastar::make_ready_future<>(); }
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
    void setGroup(NodeIdT id, RaftGroup* g) { groups_[id] = g; }
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
    std::map<NodeIdT, RaftGroup*> groups_;
    std::deque<Envelope> queue_;
};

seastar::future<> RouterTransport::send(Envelope env) {
    r_.enqueue(std::move(env));
    return seastar::make_ready_future<>();
}

RaftOptions leaderOpts() {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = 2;
    o.heartbeatTimeout = 1;
    return o;
}

std::filesystem::path freshDir(const std::string& tag) {
    auto base = std::filesystem::temp_directory_path() /
                ("g0_bridge_test_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    return base;
}

// The bootstrap flow under test: mint identity -> build NodeRecord from it ->
// initCluster on a real group-0 -> bind the minted cluster uuid back into node.json.
seastar::future<> runBootstrap(std::filesystem::path dir, std::string* outNodeUuid,
                               std::string* outClusterUuid) {
    NodeIdentity id = NodeIdentity::loadOrCreate(dir);
    *outNodeUuid = id.node_uuid;

    Router router;
    RouterTransport transport(router);
    NoopPersistence persistence;
    Group0StateMachine sm;
    RaftNode rn(1, {1}, RaftLog{}, HardState{}, leaderOpts());
    RaftGroup group(/*groupId=*/0, std::move(rn), persistence, transport, sm);
    router.setGroup(1, &group);

    Group0Controller controller(group, sm, /*metaTarget=*/3);
    co_await group.campaign();
    co_await router.pump();
    EXPECT_TRUE(group.isLeader());

    // node_uuid from node.json is what the cluster records for raft node 1.
    NodeRecord self = nodeRecordFrom(id, /*raftId=*/1, "mem:1", "rack-a");
    EXPECT_EQ(self.uuid, id.node_uuid);

    co_await controller.initCluster("cluster-boot-uuid", self);
    co_await router.pump();

    const std::string minted = sm.state().clusterUuid;
    EXPECT_EQ(minted, "cluster-boot-uuid");
    // group-0 recorded this node under the identity's node_uuid.
    EXPECT_EQ(sm.state().nodes.at(1).uuid, id.node_uuid);

    // Bind the minted cluster uuid back into node.json and confirm persistence.
    EXPECT_TRUE(bindClusterUuid(id, dir, minted));
    *outClusterUuid = minted;
    co_return;
}

}  // namespace

TEST(Group0IdentityBridge, IdentityFlowsThroughInitClusterAndBindsBack) {
    auto dir = freshDir("flow");
    std::string nodeUuid, clusterUuid;
    runBootstrap(dir, &nodeUuid, &clusterUuid).get();

    // Reload node.json: node_uuid stable, cluster_uuid now bound.
    auto reloaded = NodeIdentity::loadOrCreate(dir);
    EXPECT_EQ(reloaded.node_uuid, nodeUuid);
    EXPECT_EQ(reloaded.cluster_uuid, "cluster-boot-uuid");
    std::filesystem::remove_all(dir);
}

TEST(Group0IdentityBridge, RebindSameClusterIsIdempotent) {
    auto dir = freshDir("idem");
    auto id = NodeIdentity::loadOrCreate(dir);
    EXPECT_TRUE(bindClusterUuid(id, dir, "c1"));
    EXPECT_FALSE(bindClusterUuid(id, dir, "c1"));  // no-op second time
    EXPECT_EQ(NodeIdentity::loadOrCreate(dir).cluster_uuid, "c1");
    std::filesystem::remove_all(dir);
}

TEST(Group0IdentityBridge, RebindDifferentClusterIsRefused) {
    auto dir = freshDir("crosswire");
    auto id = NodeIdentity::loadOrCreate(dir);
    EXPECT_TRUE(bindClusterUuid(id, dir, "c1"));
    EXPECT_THROW(bindClusterUuid(id, dir, "c2"), std::runtime_error);
    // node.json still bound to the original cluster, untouched.
    EXPECT_EQ(NodeIdentity::loadOrCreate(dir).cluster_uuid, "c1");
    std::filesystem::remove_all(dir);
}

TEST(Group0IdentityBridge, StaticTopologyIsDurableAndCannotBeEditedInPlace) {
    auto dir = freshDir("static_topology");
    auto id = NodeIdentity::loadOrCreate(dir);
    EXPECT_TRUE(bindStaticTopology(id, dir, "rf=3;peers=3:a:1;3:b:2;3:c:3;"));
    EXPECT_FALSE(bindStaticTopology(id, dir, "rf=3;peers=3:a:1;3:b:2;3:c:3;"));
    EXPECT_THROW(bindStaticTopology(id, dir, "rf=3;peers=3:a:1;3:b:2;3:d:4;"), std::runtime_error);
    EXPECT_EQ(NodeIdentity::loadOrCreate(dir).static_topology, "rf=3;peers=3:a:1;3:b:2;3:c:3;");
    std::filesystem::remove_all(dir);
}

TEST(Group0IdentityBridge, EmptyNodeUuidRejectedInRecord) {
    NodeIdentity blank;  // no node_uuid
    EXPECT_THROW(nodeRecordFrom(blank, 1, "mem:1", "rack-a"), std::invalid_argument);
}
