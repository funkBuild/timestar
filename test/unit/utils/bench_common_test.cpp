#include "../../../bin/bench_common.hpp"

#include <gtest/gtest.h>

#include <limits>

using timestar::bench::shouldPreGenerateArrayPayloads;

TEST(BenchCommonTest, SmallArrayCampaignsRemainPregenerated) {
    EXPECT_TRUE(shouldPreGenerateArrayPayloads(10'000, 5, 10));
}

TEST(BenchCommonTest, CampaignMemoryDependsOnConcurrencyNotRunLength) {
    // The former fault-gate shape conservatively reserves ~3 GB when all
    // payloads are retained. It must select bounded generation under the
    // benchmark driver's 1-GiB process budget.
    EXPECT_FALSE(shouldPreGenerateArrayPayloads(10'000, 1'000, 10));
}

TEST(BenchCommonTest, PregenerationBudgetBoundaryIsExact) {
    constexpr size_t payloadBytes = 512 + 2'000 * 10 * 30;
    EXPECT_TRUE(shouldPreGenerateArrayPayloads(2'000, 3, 10, payloadBytes * 3));
    EXPECT_FALSE(shouldPreGenerateArrayPayloads(2'000, 4, 10, payloadBytes * 3));
}

TEST(BenchCommonTest, OverflowingEstimateFailsClosed) {
    EXPECT_FALSE(shouldPreGenerateArrayPayloads(std::numeric_limits<size_t>::max(), 1, 10));
    EXPECT_FALSE(shouldPreGenerateArrayPayloads(1, 1, std::numeric_limits<size_t>::max()));
}

TEST(BenchCommonTest, EmptyCampaignDoesNotForceLazyGeneration) {
    EXPECT_TRUE(
        shouldPreGenerateArrayPayloads(std::numeric_limits<size_t>::max(), 0, std::numeric_limits<size_t>::max(), 0));
}
