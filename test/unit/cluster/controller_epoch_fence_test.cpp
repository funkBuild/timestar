// Controller term fencing (data-node side): a deposed controller's lower-epoch
// job-step RPCs are rejected. Pure -- no reactor.
#include "../../../lib/cluster/control/controller_epoch_fence.hpp"

#include <gtest/gtest.h>

using namespace timestar::control;

TEST(ControllerEpochFenceTest, RejectsDeposedControllerSteps) {
    ControllerEpochFence fence;
    // Current controller (epoch 5) actuates group 42.
    EXPECT_TRUE(fence.accept(42, 5));
    EXPECT_EQ(fence.highestSeen(42), 5u);
    // Same controller sends more steps (equal epoch) -- still accepted.
    EXPECT_TRUE(fence.accept(42, 5));
    // A deposed controller (epoch 4) tries a late step -- fenced out.
    EXPECT_FALSE(fence.accept(42, 4));
    EXPECT_EQ(fence.highestSeen(42), 5u);  // high-water unchanged
    // A newly-elected controller (epoch 7) supersedes.
    EXPECT_TRUE(fence.accept(42, 7));
    EXPECT_EQ(fence.highestSeen(42), 7u);
    // The old epoch-5 controller is now also fenced.
    EXPECT_FALSE(fence.accept(42, 5));
}

TEST(ControllerEpochFenceTest, PerGroupIndependence) {
    ControllerEpochFence fence;
    EXPECT_TRUE(fence.accept(1, 9));
    // A different group starts fresh -- a low epoch there is accepted.
    EXPECT_TRUE(fence.accept(2, 3));
    EXPECT_EQ(fence.highestSeen(1), 9u);
    EXPECT_EQ(fence.highestSeen(2), 3u);
    // Group 1 still fences a low epoch.
    EXPECT_FALSE(fence.accept(1, 8));
}

TEST(ControllerEpochFenceTest, SurvivesRestart) {
    ControllerEpochFence fence;
    fence.accept(1, 5);
    fence.accept(2, 11);
    const std::string blob = fence.serialize();

    ControllerEpochFence restored;
    ASSERT_TRUE(restored.load(blob));
    EXPECT_EQ(restored.highestSeen(1), 5u);
    EXPECT_EQ(restored.highestSeen(2), 11u);
    // A stale controller stays fenced across the restart.
    EXPECT_FALSE(restored.accept(1, 4));
    EXPECT_FALSE(restored.accept(2, 10));

    // Corrupt/truncated blob leaves the fence unchanged.
    ControllerEpochFence other;
    other.accept(3, 100);
    EXPECT_FALSE(other.load(blob.substr(0, 3)));
    EXPECT_EQ(other.highestSeen(3), 100u);
}
