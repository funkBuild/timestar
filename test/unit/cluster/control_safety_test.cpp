// Epoch-regression freezing and control-map caching (Phase 3 safety mechanisms).
// Pure -- no reactor.
#include "../../../lib/cluster/control/control_map_cache.hpp"
#include "../../../lib/cluster/control/epoch_regression_guard.hpp"

#include <gtest/gtest.h>

using namespace timestar::control;

TEST(EpochRegressionGuardTest, AdvancesForwardActuatesFreely) {
    EpochRegressionGuard g;
    EXPECT_TRUE(g.observe(10, 3));
    EXPECT_TRUE(g.observe(20, 5));   // forward
    EXPECT_TRUE(g.observe(20, 5));   // equal is fine
    EXPECT_FALSE(g.frozen());
    EXPECT_EQ(g.highestApplied(), 20u);
    EXPECT_EQ(g.highestEpoch(), 5u);
}

TEST(EpochRegressionGuardTest, FreezesOnRegressionAndLatches) {
    EpochRegressionGuard g;
    g.observe(100, 9);
    // A restored/rolled-back group-0 state at a LOWER index -> freeze.
    EXPECT_FALSE(g.observe(50, 4));
    EXPECT_TRUE(g.frozen());
    // Once frozen, even a forward observation stays frozen (latched) until an
    // explicit operator supersession.
    EXPECT_FALSE(g.observe(200, 12));
    EXPECT_TRUE(g.frozen());
}

TEST(EpochRegressionGuardTest, EpochRegressionAloneFreezes) {
    EpochRegressionGuard g;
    g.observe(100, 9);
    EXPECT_FALSE(g.observe(101, 8));  // index up but epoch down -> still a regression
    EXPECT_TRUE(g.frozen());
}

TEST(EpochRegressionGuardTest, SupersessionClearsFreeze) {
    EpochRegressionGuard g;
    g.observe(100, 9);
    EXPECT_FALSE(g.observe(50, 4));
    EXPECT_TRUE(g.frozen());
    // Operator ceremony adopts the restored state as authoritative.
    g.supersede(50, 4);
    EXPECT_FALSE(g.frozen());
    EXPECT_TRUE(g.observe(51, 4));  // now actuates from the new baseline
}

TEST(EpochRegressionGuardTest, SurvivesRestart) {
    EpochRegressionGuard g;
    g.observe(100, 9);
    g.observe(40, 3);  // freeze
    ASSERT_TRUE(g.frozen());
    EpochRegressionGuard r;
    ASSERT_TRUE(r.load(g.serialize()));
    EXPECT_TRUE(r.frozen());  // freeze survives restart
    EXPECT_EQ(r.highestApplied(), 100u);
    EXPECT_EQ(r.highestEpoch(), 9u);
}

TEST(ControlMapCacheTest, UpdatesForwardNeverRegresses) {
    ControlMapCache c;
    EXPECT_TRUE(c.update({1, {{5, {1, 2, 3}}}}));
    EXPECT_EQ(c.epoch(), 1u);
    EXPECT_TRUE(c.update({2, {{5, {2, 3, 4}}}}));
    EXPECT_EQ(c.epoch(), 2u);
    // A stale (lower-epoch) notification is rejected.
    EXPECT_FALSE(c.update({1, {{5, {9, 9, 9}}}}));
    EXPECT_EQ(c.epoch(), 2u);
    EXPECT_EQ(c.current().placement.at(5), (std::vector<NodeId>{2, 3, 4}));
    // Same epoch, same content -> no change reported.
    EXPECT_FALSE(c.update({2, {{5, {2, 3, 4}}}}));
}

TEST(ControlMapCacheTest, SurvivesRestart) {
    ControlMapCache c;
    c.update({7, {{1, {1, 2}}, {2, {3, 4, 5}}}});
    ControlMapCache r;
    ASSERT_TRUE(r.load(c.serialize()));
    EXPECT_EQ(r.current(), c.current());
    EXPECT_EQ(r.current().placement.at(2), (std::vector<NodeId>{3, 4, 5}));
    // Corrupt blob leaves the cache unchanged.
    ControlMapCache other;
    other.update({3, {{9, {9}}}});
    EXPECT_FALSE(other.load(c.serialize().substr(0, 5)));
    EXPECT_EQ(other.epoch(), 3u);
}
