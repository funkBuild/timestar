#include "../../../lib/storage/vshard_mem_accounting.hpp"

#include <gtest/gtest.h>

#include <vector>

namespace {

using timestar::VShardId;
using timestar::VShardMemoryAccounting;
using timestar::VShardMemUsage;

TEST(VShardMemAccountingTest, AddAccumulatesPerVShardAndTotals) {
    VShardMemoryAccounting acc(0, 0);  // caps disabled
    EXPECT_EQ(acc.usage(VShardId{1}), (VShardMemUsage{0, 0}));

    acc.add(VShardId{1}, 100, 3);
    acc.add(VShardId{1}, 50, 2);
    acc.add(VShardId{2}, 40, 1);
    EXPECT_EQ(acc.usage(VShardId{1}), (VShardMemUsage{150, 5}));
    EXPECT_EQ(acc.usage(VShardId{2}), (VShardMemUsage{40, 1}));
    EXPECT_EQ(acc.totalBytes(), 190u);
    EXPECT_EQ(acc.totalPoints(), 6u);
    EXPECT_EQ(acc.trackedVShards(), 2u);
}

TEST(VShardMemAccountingTest, PerVShardRolloverTripsAtCap) {
    VShardMemoryAccounting acc(/*perVShard=*/100, /*global=*/0);
    acc.add(VShardId{1}, 99);
    EXPECT_FALSE(acc.vshardNeedsRollover(VShardId{1}));
    acc.add(VShardId{1}, 1);  // reaches the cap exactly
    EXPECT_TRUE(acc.vshardNeedsRollover(VShardId{1}));
    EXPECT_FALSE(acc.vshardNeedsRollover(VShardId{2}));  // untracked
    EXPECT_FALSE(acc.globalNeedsRollover());             // global disabled
}

TEST(VShardMemAccountingTest, GlobalRolloverTripsOnTotal) {
    VShardMemoryAccounting acc(/*perVShard=*/0, /*global=*/1000);
    acc.add(VShardId{1}, 400);
    acc.add(VShardId{2}, 400);
    EXPECT_FALSE(acc.globalNeedsRollover());
    acc.add(VShardId{3}, 200);  // total 1000
    EXPECT_TRUE(acc.globalNeedsRollover());
    // No per-VShard trip since that cap is disabled.
    EXPECT_FALSE(acc.vshardNeedsRollover(VShardId{1}));
}

TEST(VShardMemAccountingTest, OverCapListIsSortedAndCapAware) {
    VShardMemoryAccounting acc(100, 0);
    acc.add(VShardId{5}, 150);
    acc.add(VShardId{1}, 100);
    acc.add(VShardId{9}, 20);  // under cap
    acc.add(VShardId{3}, 100);
    EXPECT_EQ(acc.overCapVShards(), (std::vector<VShardId>{VShardId{1}, VShardId{3}, VShardId{5}}));

    VShardMemoryAccounting disabled(0, 0);
    disabled.add(VShardId{1}, 1'000'000);
    EXPECT_TRUE(disabled.overCapVShards().empty());  // per-VShard cap disabled
}

TEST(VShardMemAccountingTest, ResetClearsVShardAndDecrementsTotals) {
    VShardMemoryAccounting acc(0, 0);
    acc.add(VShardId{1}, 100, 4);
    acc.add(VShardId{2}, 60, 2);
    acc.reset(VShardId{1});
    EXPECT_EQ(acc.usage(VShardId{1}), (VShardMemUsage{0, 0}));
    EXPECT_EQ(acc.usage(VShardId{2}), (VShardMemUsage{60, 2}));
    EXPECT_EQ(acc.totalBytes(), 60u);
    EXPECT_EQ(acc.totalPoints(), 2u);
    EXPECT_EQ(acc.trackedVShards(), 1u);

    acc.reset(VShardId{999});  // untracked -> no-op
    EXPECT_EQ(acc.totalBytes(), 60u);
}

TEST(VShardMemAccountingTest, ResetAllClearsEverything) {
    VShardMemoryAccounting acc(0, 0);
    acc.add(VShardId{1}, 100);
    acc.add(VShardId{2}, 200);
    acc.resetAll();
    EXPECT_EQ(acc.totalBytes(), 0u);
    EXPECT_EQ(acc.totalPoints(), 0u);
    EXPECT_EQ(acc.trackedVShards(), 0u);
    EXPECT_EQ(acc.usage(VShardId{1}), (VShardMemUsage{0, 0}));
}

}  // namespace
