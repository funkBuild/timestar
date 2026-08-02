// Phase 7 sub-epic bricks: SLO throttle (auto-pause), placement + leadership
// balancers (constraints, hysteresis), scrubber (quarantine + repair), and
// resumable verified snapshot streaming.
#include "../../../lib/cluster/movement/leadership_balancer.hpp"
#include "../../../lib/cluster/movement/movement_throttle.hpp"
#include "../../../lib/cluster/movement/placement_balancer.hpp"
#include "../../../lib/cluster/movement/scrubber.hpp"
#include "../../../lib/cluster/movement/snapshot_stream.hpp"

#include <gtest/gtest.h>

using namespace timestar::movement;

// ---- MovementThrottle ----
TEST(MovementThrottle, AutoPausesOnBreachAndResumesAfterHysteresis) {
    MovementThrottle t(SloBudgets{}, /*resumeHysteresisTicks=*/3);
    EXPECT_TRUE(t.update(ForegroundSignals{}));  // healthy -> proceed
    // A p99 breach pauses immediately.
    EXPECT_FALSE(t.update(ForegroundSignals{.p99LatencyMs = 999}));
    EXPECT_TRUE(t.autoPaused());
    // One healthy sample is not enough (hysteresis = 3).
    EXPECT_FALSE(t.update(ForegroundSignals{}));
    EXPECT_FALSE(t.update(ForegroundSignals{}));
    EXPECT_TRUE(t.update(ForegroundSignals{}));  // third healthy -> resume
    EXPECT_FALSE(t.autoPaused());
}
TEST(MovementThrottle, EveryBudgetTriggersPause) {
    for (auto s : {ForegroundSignals{.errorRate = 0.5}, ForegroundSignals{.reactorStallMs = 500},
                   ForegroundSignals{.queueDepth = 100000}, ForegroundSignals{.diskLatencyMs = 999}}) {
        MovementThrottle t(SloBudgets{});
        EXPECT_FALSE(t.update(s));
    }
}
TEST(MovementThrottle, ManualPauseAndCancelAreSticky) {
    MovementThrottle t(SloBudgets{});
    t.manualPause();
    EXPECT_FALSE(t.update(ForegroundSignals{}));  // healthy but manually paused
    t.manualResume();
    EXPECT_TRUE(t.update(ForegroundSignals{}));
    t.cancel();
    EXPECT_FALSE(t.update(ForegroundSignals{}));  // cancelled: never proceeds again
    EXPECT_TRUE(t.cancelled());
}

// ---- PlacementBalancer ----
TEST(PlacementBalancer, MovesFromLoadedToLightRespectingDomainAndDisk) {
    // Node 1 (rack-a) overloaded; node 4 (rack-b) idle with free disk.
    std::map<uint16_t, std::vector<NodeId>> placement = {{10, {1, 2, 3}}};
    std::map<NodeId, NodeLoad> loads = {
        {1, {1, "rack-a", 1.0, 90, 5}},
        {2, {2, "rack-b", 1.0, 90, 1}},
        {3, {3, "rack-c", 1.0, 90, 1}},
        {4, {4, "rack-d", 1.0, 90, 0}},
    };
    auto move = PlacementBalancer::planOneMove(/*mapEpoch=*/9, placement, loads, BalancerConfig{});
    ASSERT_TRUE(move.has_value());
    EXPECT_EQ(move->victim, 1);  // from the most-loaded node
    EXPECT_EQ(move->dest, 4);    // to the least-loaded eligible node
    EXPECT_EQ(move->vshard, 10);
    EXPECT_EQ(move->mapEpoch, 9u);
    EXPECT_EQ(move->sourceVoters, (std::vector<NodeId>{1, 2, 3}));
}
TEST(PlacementBalancer, RejectsDiskWatermarkAndDomainClash) {
    std::map<uint16_t, std::vector<NodeId>> placement = {{10, {1, 2, 3}}};
    // Only candidate destination (node 4) is BELOW the disk watermark -> no move.
    std::map<NodeId, NodeLoad> lowDisk = {
        {1, {1, "rack-a", 1.0, 90, 5}},
        {2, {2, "rack-b", 1.0, 90, 1}},
        {3, {3, "rack-c", 1.0, 90, 1}},
        {4, {4, "rack-d", 1.0, /*disk*/ 5, 0}},
    };
    EXPECT_FALSE(PlacementBalancer::planOneMove(9, placement, lowDisk, BalancerConfig{}).has_value());

    // The only idle node shares a failure domain with an existing replica -> the
    // anti-affinity constraint forbids the move.
    std::map<NodeId, NodeLoad> clash = {
        {1, {1, "rack-a", 1.0, 90, 5}},
        {2, {2, "rack-b", 1.0, 90, 1}},
        {3, {3, "rack-c", 1.0, 90, 1}},
        {4, {4, "rack-b", 1.0, 90, 0}},  // same domain as node 2
    };
    EXPECT_FALSE(PlacementBalancer::planOneMove(9, placement, clash, BalancerConfig{}).has_value());
}
TEST(PlacementBalancer, RefusesMoveToUnlabeledDomain) {
    // Destination has NO failure-domain label: distinctness cannot be proven, so
    // the hard anti-affinity constraint refuses the move (don't risk co-locating
    // two replicas in an unknown/shared domain).
    std::map<uint16_t, std::vector<NodeId>> placement = {{10, {1, 2, 3}}};
    std::map<NodeId, NodeLoad> loads = {
        {1, {1, "rack-a", 1.0, 90, 5}},
        {2, {2, "rack-b", 1.0, 90, 1}},
        {3, {3, "rack-c", 1.0, 90, 1}},
        {4, {4, "", 1.0, 90, 0}},  // unlabeled destination
    };
    EXPECT_FALSE(PlacementBalancer::planOneMove(9, placement, loads, BalancerConfig{}).has_value());
}
TEST(PlacementBalancer, HysteresisSuppressesMinorImbalance) {
    std::map<uint16_t, std::vector<NodeId>> placement = {{10, {1, 2, 3}}};
    // Nearly balanced (5 vs 4 replicas) -> below the 15% hysteresis -> no churn.
    std::map<NodeId, NodeLoad> loads = {
        {1, {1, "rack-a", 1.0, 90, 5}},
        {2, {2, "rack-b", 1.0, 90, 4}},
        {3, {3, "rack-c", 1.0, 90, 4}},
        {4, {4, "rack-d", 1.0, 90, 5}},
    };
    EXPECT_FALSE(PlacementBalancer::planOneMove(9, placement, loads, BalancerConfig{}).has_value());
}

TEST(PlacementBalancer, RefusesUnversionedTopology) {
    std::map<uint16_t, std::vector<NodeId>> placement = {{10, {1, 2, 3}}};
    std::map<NodeId, NodeLoad> loads = {
        {1, {1, "rack-a", 1.0, 90, 5}},
        {2, {2, "rack-b", 1.0, 90, 1}},
        {3, {3, "rack-c", 1.0, 90, 1}},
        {4, {4, "rack-d", 1.0, 90, 0}},
    };
    EXPECT_FALSE(PlacementBalancer::planOneMove(/*mapEpoch=*/0, placement, loads, BalancerConfig{}).has_value());
}

// ---- LeadershipBalancer ----
TEST(LeadershipBalancer, TransfersFromBusyLeaderToLightReplica) {
    std::map<uint16_t, NodeId> leaders = {{10, 1}, {11, 1}, {12, 2}};
    std::map<uint16_t, std::vector<NodeId>> replicas = {{10, {1, 2, 3}}, {11, {1, 2, 3}}, {12, {2, 3, 1}}};
    std::map<NodeId, double> load = {{1, 100.0}, {2, 20.0}, {3, 10.0}};
    auto t = LeadershipBalancer::planOneTransfer(leaders, replicas, load, /*hysteresis=*/0.2);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->target, 3);  // lightest node, which IS a replica of node 1's groups
}
TEST(LeadershipBalancer, HysteresisSuppressesBalancedLeadership) {
    std::map<uint16_t, NodeId> leaders = {{10, 1}, {11, 2}};
    std::map<uint16_t, std::vector<NodeId>> replicas = {{10, {1, 2}}, {11, {1, 2}}};
    std::map<NodeId, double> load = {{1, 51.0}, {2, 49.0}};
    EXPECT_FALSE(LeadershipBalancer::planOneTransfer(leaders, replicas, load, 0.2).has_value());
}

// ---- Scrubber ----
TEST(Scrubber, QuarantinesCorruptAndClearsAfterRepair) {
    std::map<std::string, std::string> live = {{"objA", "hashA"}, {"objB", "CORRUPT"}};
    Scrubber s([&](const std::string& id) { return live[id]; });
    auto repairs = s.scrub({{"objA", "hashA"}, {"objB", "hashB"}});
    ASSERT_EQ(repairs.size(), 1u);
    EXPECT_EQ(repairs[0].objectId, "objB");
    EXPECT_TRUE(s.isQuarantined("objB"));
    EXPECT_FALSE(s.isQuarantined("objA"));  // healthy object untouched
    // After re-replication the object verifies and leaves quarantine.
    live["objB"] = "hashB";
    s.clearQuarantine("objB");
    EXPECT_TRUE(s.scrub({{"objB", "hashB"}}).empty());
    EXPECT_FALSE(s.isQuarantined("objB"));
}
TEST(Scrubber, DoesNotReEmitRepairForAlreadyQuarantined) {
    std::map<std::string, std::string> live = {{"o", "CORRUPT"}};
    Scrubber s([&](const std::string& id) { return live[id]; });
    EXPECT_EQ(s.scrub({{"o", "good"}}).size(), 1u);  // first pass: quarantine + one repair
    EXPECT_TRUE(s.scrub({{"o", "good"}}).empty());   // still corrupt but quarantined: no repeat repair
    EXPECT_TRUE(s.isQuarantined("o"));
}

// ---- SnapshotStream ----
TEST(SnapshotStream, ResumableRoundTripAndVerification) {
    std::string payload = "the whole-vshard snapshot payload bytes, arbitrary length 12345";
    SnapshotStreamSource src(payload, /*chunkSize=*/8);
    SnapshotStreamSink sink(src.chunkCount(), src.wholeHash());
    // Deliver chunks OUT OF ORDER and re-send one (resumable/idempotent).
    for (uint32_t i = src.chunkCount(); i-- > 0;)
        sink.accept(src.chunk(i));
    sink.accept(src.chunk(0));  // duplicate: no-op
    EXPECT_TRUE(sink.complete());
    EXPECT_EQ(sink.finish(), payload);
}
TEST(SnapshotStream, PerChunkHashMismatchThrows) {
    SnapshotStreamSource src("abcdefgh", 4);
    SnapshotStreamSink sink(src.chunkCount(), src.wholeHash());
    SnapshotChunk c = src.chunk(0);
    c.data[0] ^= 0xff;  // corrupt payload, stale hash
    EXPECT_THROW(sink.accept(c), SnapshotVerifyError);
}
TEST(SnapshotStream, IncompleteAndWholeHashMismatchThrow) {
    SnapshotStreamSource src("abcdefgh", 4);
    SnapshotStreamSink sink(src.chunkCount(), src.wholeHash());
    sink.accept(src.chunk(0));
    EXPECT_FALSE(sink.complete());
    EXPECT_THROW(sink.finish(), SnapshotVerifyError);  // incomplete
    // A sink told the wrong whole-hash rejects even a fully-delivered payload.
    SnapshotStreamSink bad(src.chunkCount(), src.wholeHash() ^ 1);
    bad.accept(src.chunk(0));
    bad.accept(src.chunk(1));
    EXPECT_THROW(bad.finish(), SnapshotVerifyError);
}
