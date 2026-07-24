// Phase 4: the /subscribe cluster guard and the operator surface (cluster
// status / vshard describe / placement explain). Pure -- no reactor.
#include "../../../lib/cluster/data/cluster_inspector.hpp"
#include "../../../lib/cluster/data/subscribe_policy.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;
using timestar::control::ControlMap;
using timestar::raft::NodeId;

namespace {

ControlMap mapWith(std::map<uint16_t, std::vector<NodeId>> placement, uint64_t epoch = 1) {
    ControlMap m;
    m.epoch = epoch;
    m.placement = std::move(placement);
    return m;
}

}  // namespace

TEST(SubscribePolicyTest, SingleNodeAllowsFullScope) {
    // All VShards local -> not clustered -> ordinary subscription.
    VShardDirectory dir(1, mapWith({{0, {1}}, {1, {1}}, {2, {1}}}));
    EXPECT_FALSE(isClustered(dir));
    SubscribeDecision d = evaluateSubscribe(isClustered(dir), /*rejectInCluster=*/true);
    EXPECT_FALSE(d.rejected);
    EXPECT_FALSE(d.nodeLocalOnly);
}

TEST(SubscribePolicyTest, ClusterModeNeverSilentlyDegrades) {
    // Some VShard owned by another node -> clustered.
    VShardDirectory dir(1, mapWith({{0, {1}}, {1, {2}}, {2, {3}}}));
    EXPECT_TRUE(isClustered(dir));

    // Reject policy: a clear error, never a silent node-local stream.
    SubscribeDecision reject = evaluateSubscribe(true, /*rejectInCluster=*/true);
    EXPECT_TRUE(reject.rejected);
    EXPECT_FALSE(reject.reason.empty());

    // Mark policy: allowed but explicitly node-local.
    SubscribeDecision mark = evaluateSubscribe(true, /*rejectInCluster=*/false);
    EXPECT_FALSE(mark.rejected);
    EXPECT_TRUE(mark.nodeLocalOnly);
    EXPECT_FALSE(mark.reason.empty());

    // The forbidden outcome (clustered, not rejected, not marked) is unreachable.
    for (bool policy : {true, false}) {
        SubscribeDecision d = evaluateSubscribe(true, policy);
        EXPECT_TRUE(d.rejected || d.nodeLocalOnly) << "silent node-local subscription in cluster mode";
    }
}

TEST(ClusterInspectorTest, PlacementExplain) {
    VShardDirectory dir(2, mapWith({{5, {2}}, {6, {3}}}));
    ClusterInspector insp(dir);

    PlacementExplain local = insp.explainVShard(5, /*coreCount=*/4);
    EXPECT_EQ(local.ownerNode, 2u);
    EXPECT_TRUE(local.local);
    EXPECT_EQ(local.core, assignCore(timestar::VShardId{5}, 4));

    PlacementExplain remote = insp.explainVShard(6, 4);
    EXPECT_EQ(remote.ownerNode, 3u);
    EXPECT_FALSE(remote.local);

    PlacementExplain unassigned = insp.explainVShard(9, 4);
    EXPECT_EQ(unassigned.ownerNode, kNoNode);
}

TEST(ClusterInspectorTest, DescribeVShard) {
    VShardDirectory dir(1, mapWith({{7, {1, 2, 3}}}));  // RF=3 primary-first
    ClusterInspector insp(dir);
    VShardDescribe d = insp.describeVShard(7);
    EXPECT_TRUE(d.assigned);
    EXPECT_EQ(d.owner, 1u);
    EXPECT_EQ(d.replicas, (std::vector<NodeId>{1, 2, 3}));

    EXPECT_FALSE(insp.describeVShard(99).assigned);
}

TEST(ClusterInspectorTest, Status) {
    VShardDirectory dir(1, mapWith({{0, {1}}, {1, {2}}, {2, {3}}}, /*epoch=*/9));
    ClusterInspector insp(dir);
    ClusterStatus s = insp.status({1, 2, 3});
    EXPECT_EQ(s.self, 1u);
    EXPECT_EQ(s.mapEpoch, 9u);
    EXPECT_EQ(s.nodeCount, 3u);
    EXPECT_EQ(s.assignedVShards, 3u);
    EXPECT_TRUE(s.controlPlaneHighlyAvailable);  // 3 meta voters
    EXPECT_TRUE(s.clustered);

    // A single-node deployment: not clustered, not HA.
    VShardDirectory solo(1, mapWith({{0, {1}}, {1, {1}}}));
    ClusterStatus ss = ClusterInspector(solo).status({1});
    EXPECT_EQ(ss.nodeCount, 1u);
    EXPECT_FALSE(ss.clustered);
    EXPECT_FALSE(ss.controlPlaneHighlyAvailable);
}
