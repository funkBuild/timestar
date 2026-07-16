// Dedicated tests for BlockAggregator: SIMD fold paths, addBlockStats,
// single-bucket optimization, multi-bucket routing, and fold-to-single-state mode.

#include "../../../lib/query/block_aggregator.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <random>
#include <vector>

using namespace timestar;

// ============================================================================
// addBlockStats — SIMD fold path for pre-computed block statistics
// ============================================================================

// Helper: create a BlockAggregator in fold-to-single-state mode (interval=0,
// methodAware=true) — the primary path that exercises addBlockStats.
static BlockAggregator makeFoldAggregator(AggregationMethod method) {
    BlockAggregator ba(0, method);
    ba.enableFoldToSingleState();
    return ba;
}

TEST(BlockAggregatorBlockStats, SumViaTwoBlocks) {
    auto ba = makeFoldAggregator(AggregationMethod::SUM);

    // Block 1: 5 points, sum=50, min=5, max=15
    ba.addBlockStats(/*sum=*/50.0, /*bmin=*/5.0, /*bmax=*/15.0, /*count=*/5,
                     /*minTime=*/1000, /*maxTime=*/1400);

    // Block 2: 3 points, sum=30, min=8, max=12
    ba.addBlockStats(/*sum=*/30.0, /*bmin=*/8.0, /*bmax=*/12.0, /*count=*/3,
                     /*minTime=*/1500, /*maxTime=*/1700);

    auto state = ba.takeSingleState();
    EXPECT_EQ(state.count, 8u);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 80.0);
    EXPECT_EQ(ba.pointCount(), 8u);
}

TEST(BlockAggregatorBlockStats, AvgViaTwoBlocks) {
    auto ba = makeFoldAggregator(AggregationMethod::AVG);

    // Block 1: 4 points, sum=40 => mean=10
    ba.addBlockStats(40.0, 5.0, 15.0, 4, 1000, 1300);

    // Block 2: 6 points, sum=120 => mean=20
    ba.addBlockStats(120.0, 10.0, 30.0, 6, 1400, 1900);

    auto state = ba.takeSingleState();
    EXPECT_EQ(state.count, 10u);
    // avg = (40 + 120) / 10 = 16.0
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 16.0);
}

TEST(BlockAggregatorBlockStats, MinAcrossBlocks) {
    auto ba = makeFoldAggregator(AggregationMethod::MIN);

    ba.addBlockStats(100.0, 3.0, 20.0, 5, 1000, 1400);
    ba.addBlockStats(50.0, 7.0, 15.0, 3, 1500, 1700);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MIN), 3.0);
    EXPECT_EQ(state.count, 8u);
}

TEST(BlockAggregatorBlockStats, MaxAcrossBlocks) {
    auto ba = makeFoldAggregator(AggregationMethod::MAX);

    ba.addBlockStats(100.0, 3.0, 20.0, 5, 1000, 1400);
    ba.addBlockStats(50.0, 7.0, 25.0, 3, 1500, 1700);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MAX), 25.0);
    EXPECT_EQ(state.count, 8u);
}

TEST(BlockAggregatorBlockStats, CountAcrossBlocks) {
    auto ba = makeFoldAggregator(AggregationMethod::COUNT);

    ba.addBlockStats(10.0, 1.0, 5.0, 100, 1000, 2000);
    ba.addBlockStats(20.0, 2.0, 6.0, 250, 2001, 3000);
    ba.addBlockStats(30.0, 3.0, 7.0, 50, 3001, 4000);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::COUNT), 400.0);
}

TEST(BlockAggregatorBlockStats, SpreadAcrossBlocks) {
    auto ba = makeFoldAggregator(AggregationMethod::SPREAD);

    // Block 1: min=2, max=10
    ba.addBlockStats(60.0, 2.0, 10.0, 5, 1000, 1400);
    // Block 2: min=5, max=18
    ba.addBlockStats(80.0, 5.0, 18.0, 4, 1500, 1800);

    auto state = ba.takeSingleState();
    // Global min=2, max=18 => spread = 16
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SPREAD), 16.0);
}

TEST(BlockAggregatorBlockStats, LatestFromBlockWithHighestTimestamp) {
    auto ba = makeFoldAggregator(AggregationMethod::LATEST);

    // Block 1: lastTs=2000, latestValue=42.0
    ba.addBlockStats(100.0, 5.0, 20.0, 5, 1000, 2000,
                     /*m2=*/0.0, /*firstValue=*/10.0, /*latestValue=*/42.0);
    // Block 2: lastTs=3000, latestValue=99.0
    ba.addBlockStats(200.0, 8.0, 30.0, 5, 2001, 3000,
                     0.0, 15.0, 99.0);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::LATEST), 99.0);
    EXPECT_EQ(state.latestTimestamp, 3000u);
}

TEST(BlockAggregatorBlockStats, LatestTakesEarlierBlockWhenItHasHigherTimestamp) {
    auto ba = makeFoldAggregator(AggregationMethod::LATEST);

    // Add block with higher timestamp first
    ba.addBlockStats(200.0, 8.0, 30.0, 5, 2001, 5000,
                     0.0, 15.0, 77.0);
    // Then add block with lower timestamp
    ba.addBlockStats(100.0, 5.0, 20.0, 5, 1000, 2000,
                     0.0, 10.0, 42.0);

    auto state = ba.takeSingleState();
    // Should take the value from the block with maxTime=5000
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::LATEST), 77.0);
    EXPECT_EQ(state.latestTimestamp, 5000u);
}

TEST(BlockAggregatorBlockStats, FirstFromBlockWithLowestTimestamp) {
    auto ba = makeFoldAggregator(AggregationMethod::FIRST);

    // Block 1: firstTs=1000, firstValue=10.0
    ba.addBlockStats(100.0, 5.0, 20.0, 5, 1000, 2000,
                     0.0, 10.0, 42.0);
    // Block 2: firstTs=500, firstValue=3.0
    ba.addBlockStats(200.0, 3.0, 30.0, 5, 500, 900,
                     0.0, 3.0, 8.0);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::FIRST), 3.0);
    EXPECT_EQ(state.firstTimestamp, 500u);
}

// ============================================================================
// canUseBlockStats
// ============================================================================

TEST(BlockAggregatorCanUse, FoldModeReturnsTrue) {
    auto ba = makeFoldAggregator(AggregationMethod::SUM);
    EXPECT_TRUE(ba.canUseBlockStats(1000, 2000));
}

TEST(BlockAggregatorCanUse, NonFoldNonBucketedReturnsFalse) {
    // interval=0, no fold mode => stores raw values, cannot use block stats
    BlockAggregator ba(0, AggregationMethod::SUM);
    EXPECT_FALSE(ba.canUseBlockStats(1000, 2000));
}

TEST(BlockAggregatorCanUse, MedianAlwaysFalse) {
    auto ba = makeFoldAggregator(AggregationMethod::MEDIAN);
    // MEDIAN always returns false regardless of fold mode
    EXPECT_FALSE(ba.canUseBlockStats(1000, 2000));
}

TEST(BlockAggregatorCanUse, LatestRequiresExtendedStats) {
    auto ba = makeFoldAggregator(AggregationMethod::LATEST);
    EXPECT_FALSE(ba.canUseBlockStats(1000, 2000, /*hasExtendedStats=*/false));
    EXPECT_TRUE(ba.canUseBlockStats(1000, 2000, /*hasExtendedStats=*/true));
}

TEST(BlockAggregatorCanUse, FirstRequiresExtendedStats) {
    auto ba = makeFoldAggregator(AggregationMethod::FIRST);
    EXPECT_FALSE(ba.canUseBlockStats(1000, 2000, false));
    EXPECT_TRUE(ba.canUseBlockStats(1000, 2000, true));
}

TEST(BlockAggregatorCanUse, StddevRequiresExtendedStats) {
    auto ba = makeFoldAggregator(AggregationMethod::STDDEV);
    EXPECT_FALSE(ba.canUseBlockStats(1000, 2000, false));
    EXPECT_TRUE(ba.canUseBlockStats(1000, 2000, true));
}

TEST(BlockAggregatorCanUse, MultiBucketSameBucketTrue) {
    // interval=1000, block fits in one bucket [1000, 1999]
    uint64_t interval = 1000;
    uint64_t start = 0;
    uint64_t end = 5000;
    BlockAggregator ba(interval, start, end, AggregationMethod::SUM, true);
    // Block 1200..1800 => bucket [1000, 2000) — single bucket
    EXPECT_TRUE(ba.canUseBlockStats(1200, 1800));
}

TEST(BlockAggregatorCanUse, MultiBucketCrossingBoundaryFalse) {
    uint64_t interval = 1000;
    uint64_t start = 0;
    uint64_t end = 5000;
    BlockAggregator ba(interval, start, end, AggregationMethod::SUM, true);
    // Block 800..1200 => buckets [0, 1000) and [1000, 2000) — crosses boundary
    EXPECT_FALSE(ba.canUseBlockStats(800, 1200));
}

// ============================================================================
// Single-bucket optimization (interval spans entire query range)
// ============================================================================

TEST(BlockAggregatorSingleBucket, AllPointsGoToOneBucket) {
    // Query range [0, 999] with interval=1000 => 1 bucket
    uint64_t interval = 1000;
    BlockAggregator ba(interval, 0, 999, AggregationMethod::SUM, true);

    ba.addPoint(100, 10.0);
    ba.addPoint(200, 20.0);
    ba.addPoint(500, 30.0);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);

    auto it = buckets.begin();
    EXPECT_DOUBLE_EQ(it->second.getValue(AggregationMethod::SUM), 60.0);
    EXPECT_EQ(it->second.count, 3u);
}

TEST(BlockAggregatorSingleBucket, AddPointsViaBatch) {
    uint64_t interval = 10000;
    BlockAggregator ba(interval, 0, 9999, AggregationMethod::AVG, true);

    std::vector<uint64_t> ts = {100, 200, 300, 400, 500, 600, 700, 800};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    ba.addPoints(ts, vals);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);

    auto& state = buckets.begin()->second;
    // avg = (1+2+3+4+5+6+7+8)/8 = 36/8 = 4.5
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 4.5);
    EXPECT_EQ(state.count, 8u);
}

TEST(BlockAggregatorSingleBucket, AddBlockStatsToSingleBucket) {
    uint64_t interval = 10000;
    BlockAggregator ba(interval, 0, 9999, AggregationMethod::MAX, true);

    EXPECT_TRUE(ba.canUseBlockStats(100, 500));

    ba.addBlockStats(50.0, 5.0, 15.0, 5, 100, 500);
    ba.addBlockStats(80.0, 3.0, 22.0, 4, 600, 900);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_DOUBLE_EQ(buckets.begin()->second.getValue(AggregationMethod::MAX), 22.0);
    EXPECT_EQ(buckets.begin()->second.count, 9u);
}

// ============================================================================
// Multi-bucket (interval divides the range into multiple time windows)
// ============================================================================

TEST(BlockAggregatorMultiBucket, PointsSeparateIntoBuckets) {
    uint64_t interval = 100;
    uint64_t start = 0;
    uint64_t end = 500;
    BlockAggregator ba(interval, start, end, AggregationMethod::SUM, true);

    // Bucket [0, 100):
    ba.addPoint(10, 1.0);
    ba.addPoint(50, 2.0);
    // Bucket [100, 200):
    ba.addPoint(110, 3.0);
    ba.addPoint(190, 4.0);
    // Bucket [300, 400):
    ba.addPoint(350, 5.0);

    auto buckets = ba.takeBucketStates();
    EXPECT_EQ(buckets.size(), 3u);

    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::SUM), 3.0);
    EXPECT_EQ(buckets[0].count, 2u);
    EXPECT_DOUBLE_EQ(buckets[100].getValue(AggregationMethod::SUM), 7.0);
    EXPECT_EQ(buckets[100].count, 2u);
    EXPECT_DOUBLE_EQ(buckets[300].getValue(AggregationMethod::SUM), 5.0);
    EXPECT_EQ(buckets[300].count, 1u);
}

TEST(BlockAggregatorMultiBucket, BatchPointsSpanMultipleBuckets) {
    uint64_t interval = 100;
    uint64_t start = 0;
    uint64_t end = 400;
    BlockAggregator ba(interval, start, end, AggregationMethod::MIN, true);

    // Timestamps are monotonically increasing (as from a TSM block)
    std::vector<uint64_t> ts = {10, 50, 90, 110, 150, 220, 280, 310};
    std::vector<double> vals = {9.0, 3.0, 7.0, 12.0, 8.0, 15.0, 4.0, 11.0};
    ba.addPoints(ts, vals);

    auto buckets = ba.takeBucketStates();
    // Bucket [0,100): {9, 3, 7} => min=3
    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::MIN), 3.0);
    // Bucket [100,200): {12, 8} => min=8
    EXPECT_DOUBLE_EQ(buckets[100].getValue(AggregationMethod::MIN), 8.0);
    // Bucket [200,300): {15, 4} => min=4
    EXPECT_DOUBLE_EQ(buckets[200].getValue(AggregationMethod::MIN), 4.0);
    // Bucket [300,400): {11} => min=11
    EXPECT_DOUBLE_EQ(buckets[300].getValue(AggregationMethod::MIN), 11.0);
}

TEST(BlockAggregatorMultiBucket, AddBlockStatsToMultiBucket) {
    uint64_t interval = 1000;
    uint64_t start = 0;
    uint64_t end = 5000;
    BlockAggregator ba(interval, start, end, AggregationMethod::SUM, true);

    // Block entirely within bucket [1000, 2000)
    EXPECT_TRUE(ba.canUseBlockStats(1100, 1900));
    ba.addBlockStats(50.0, 5.0, 15.0, 5, 1100, 1900);

    // Block entirely within bucket [3000, 4000)
    EXPECT_TRUE(ba.canUseBlockStats(3200, 3800));
    ba.addBlockStats(90.0, 10.0, 25.0, 3, 3200, 3800);

    auto buckets = ba.takeBucketStates();
    EXPECT_EQ(buckets.count(1000), 1u);
    EXPECT_DOUBLE_EQ(buckets[1000].getValue(AggregationMethod::SUM), 50.0);
    EXPECT_EQ(buckets.count(3000), 1u);
    EXPECT_DOUBLE_EQ(buckets[3000].getValue(AggregationMethod::SUM), 90.0);
}

// ============================================================================
// Non-bucketed mode (interval=0) — raw timestamp/value storage
// ============================================================================

TEST(BlockAggregatorNonBucketed, StoresRawTimestampsAndValues) {
    BlockAggregator ba(0, AggregationMethod::AVG);

    ba.addPoint(100, 1.0);
    ba.addPoint(200, 2.0);
    ba.addPoint(300, 3.0);

    EXPECT_EQ(ba.pointCount(), 3u);

    auto ts = ba.takeTimestamps();
    auto vals = ba.takeValues();
    ASSERT_EQ(ts.size(), 3u);
    ASSERT_EQ(vals.size(), 3u);
    EXPECT_EQ(ts[0], 100u);
    EXPECT_EQ(ts[2], 300u);
    EXPECT_DOUBLE_EQ(vals[1], 2.0);
}

TEST(BlockAggregatorNonBucketed, BatchAddStoresRaw) {
    BlockAggregator ba(0, AggregationMethod::SUM);

    std::vector<uint64_t> timestamps = {10, 20, 30, 40, 50};
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    ba.addPoints(timestamps, values);

    EXPECT_EQ(ba.pointCount(), 5u);
    auto ts = ba.takeTimestamps();
    auto vals = ba.takeValues();
    ASSERT_EQ(ts.size(), 5u);
    ASSERT_EQ(vals.size(), 5u);
    EXPECT_EQ(ts[4], 50u);
    EXPECT_DOUBLE_EQ(vals[4], 5.0);
}

// ============================================================================
// Fold-to-single-state mode (interval=0, streaming)
// ============================================================================

TEST(BlockAggregatorFoldMode, ScalarFoldsIntoSingleState) {
    auto ba = makeFoldAggregator(AggregationMethod::SUM);

    ba.addPoint(100, 10.0);
    ba.addPoint(200, 20.0);
    ba.addPoint(300, 30.0);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 60.0);
    EXPECT_EQ(state.count, 3u);
}

TEST(BlockAggregatorFoldMode, BatchFoldsViaSIMDPath) {
    // Batch of >= 4 points in methodAware fold mode triggers SIMD path
    auto ba = makeFoldAggregator(AggregationMethod::SUM);

    std::vector<uint64_t> ts = {100, 200, 300, 400, 500, 600, 700, 800};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 36.0);
    EXPECT_EQ(state.count, 8u);
}

TEST(BlockAggregatorFoldMode, AvgViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::AVG);

    std::vector<uint64_t> ts = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<double> vals = {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    // avg = 550 / 10 = 55
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 55.0);
}

TEST(BlockAggregatorFoldMode, MinViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::MIN);

    std::vector<uint64_t> ts = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<double> vals = {50.0, 30.0, 70.0, 10.0, 90.0, 20.0, 60.0, 40.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MIN), 10.0);
}

TEST(BlockAggregatorFoldMode, MaxViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::MAX);

    std::vector<uint64_t> ts = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<double> vals = {50.0, 30.0, 70.0, 10.0, 90.0, 20.0, 60.0, 40.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MAX), 90.0);
}

TEST(BlockAggregatorFoldMode, SpreadViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::SPREAD);

    std::vector<uint64_t> ts = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<double> vals = {50.0, 30.0, 70.0, 10.0, 90.0, 20.0, 60.0, 40.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    // min=10, max=90 => spread=80
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SPREAD), 80.0);
}

TEST(BlockAggregatorFoldMode, CountViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::COUNT);

    std::vector<uint64_t> ts = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::COUNT), 8.0);
}

TEST(BlockAggregatorFoldMode, LatestViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::LATEST);

    // Monotonically increasing timestamps (as from TSM block)
    std::vector<uint64_t> ts = {100, 200, 300, 400, 500};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 99.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::LATEST), 99.0);
    EXPECT_EQ(state.latestTimestamp, 500u);
}

TEST(BlockAggregatorFoldMode, FirstViaSIMDBatch) {
    auto ba = makeFoldAggregator(AggregationMethod::FIRST);

    std::vector<uint64_t> ts = {100, 200, 300, 400, 500};
    std::vector<double> vals = {42.0, 2.0, 3.0, 4.0, 5.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::FIRST), 42.0);
    EXPECT_EQ(state.firstTimestamp, 100u);
}

// ============================================================================
// addTimestampsOnly (COUNT optimization)
// ============================================================================

TEST(BlockAggregatorCountOnly, IsCountOnlyTrue) {
    BlockAggregator ba(100, AggregationMethod::COUNT);
    EXPECT_TRUE(ba.isCountOnly());
}

TEST(BlockAggregatorCountOnly, IsCountOnlyFalseForOtherMethods) {
    BlockAggregator ba(100, AggregationMethod::SUM);
    EXPECT_FALSE(ba.isCountOnly());
}

TEST(BlockAggregatorCountOnly, FoldModeCountsTimestamps) {
    auto ba = makeFoldAggregator(AggregationMethod::COUNT);

    std::vector<uint64_t> ts = {100, 200, 300, 400, 500};
    ba.addTimestampsOnly(ts);

    auto state = ba.takeSingleState();
    EXPECT_EQ(state.count, 5u);
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::COUNT), 5.0);
}

TEST(BlockAggregatorCountOnly, SingleBucketCountsTimestamps) {
    uint64_t interval = 10000;
    BlockAggregator ba(interval, 0, 9999, AggregationMethod::COUNT, true);

    std::vector<uint64_t> ts = {100, 200, 300};
    ba.addTimestampsOnly(ts);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_EQ(buckets.begin()->second.count, 3u);
}

TEST(BlockAggregatorCountOnly, MultiBucketCountsTimestamps) {
    uint64_t interval = 100;
    BlockAggregator ba(interval, 0, 500, AggregationMethod::COUNT, true);

    std::vector<uint64_t> ts = {10, 50, 110, 210, 310};
    ba.addTimestampsOnly(ts);

    auto buckets = ba.takeBucketStates();
    EXPECT_EQ(buckets[0].count, 2u);
    EXPECT_EQ(buckets[100].count, 1u);
    EXPECT_EQ(buckets[200].count, 1u);
    EXPECT_EQ(buckets[300].count, 1u);
}

// ============================================================================
// SIMD batch fold in single-bucket mode
// ============================================================================

TEST(BlockAggregatorSingleBucketSIMD, SumFoldsBatch) {
    uint64_t interval = 100000;
    BlockAggregator ba(interval, 0, 99999, AggregationMethod::SUM, true);

    // 8 points => triggers SIMD path (>= 4)
    std::vector<uint64_t> ts = {100, 200, 300, 400, 500, 600, 700, 800};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    ba.addPoints(ts, vals);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_DOUBLE_EQ(buckets.begin()->second.getValue(AggregationMethod::SUM), 36.0);
}

TEST(BlockAggregatorSingleBucketSIMD, MinMaxFoldsBatch) {
    uint64_t interval = 100000;
    BlockAggregator ba(interval, 0, 99999, AggregationMethod::MIN, true);

    std::vector<uint64_t> ts = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<double> vals = {50.0, 30.0, 70.0, 10.0, 90.0, 20.0, 60.0, 40.0};
    ba.addPoints(ts, vals);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_DOUBLE_EQ(buckets.begin()->second.getValue(AggregationMethod::MIN), 10.0);
}

// ============================================================================
// SIMD batch fold in multi-bucket mode
// ============================================================================

TEST(BlockAggregatorMultiBucketSIMD, BatchFoldsPerBucket) {
    // Each bucket gets enough points to trigger SIMD (>= 4 consecutive)
    uint64_t interval = 100;
    uint64_t start = 0;
    uint64_t end = 300;
    BlockAggregator ba(interval, start, end, AggregationMethod::SUM, true);

    // 5 points in bucket [0,100), 5 in [100,200)
    std::vector<uint64_t> ts = {10, 20, 30, 40, 50, 110, 120, 130, 140, 150};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0};
    ba.addPoints(ts, vals);

    auto buckets = ba.takeBucketStates();
    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::SUM), 15.0);
    EXPECT_DOUBLE_EQ(buckets[100].getValue(AggregationMethod::SUM), 150.0);
}

// ============================================================================
// Edge cases
// ============================================================================

TEST(BlockAggregatorEdge, EmptyAggregator) {
    auto ba = makeFoldAggregator(AggregationMethod::SUM);
    auto state = ba.takeSingleState();
    EXPECT_EQ(state.count, 0u);
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::SUM)));
}

TEST(BlockAggregatorEdge, SinglePointScalar) {
    auto ba = makeFoldAggregator(AggregationMethod::AVG);
    ba.addPoint(100, 42.0);
    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 42.0);
    EXPECT_EQ(state.count, 1u);
}

TEST(BlockAggregatorEdge, SingleBlockStats) {
    auto ba = makeFoldAggregator(AggregationMethod::AVG);
    ba.addBlockStats(100.0, 5.0, 25.0, 4, 1000, 2000);
    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 25.0);
    EXPECT_EQ(state.count, 4u);
}

TEST(BlockAggregatorEdge, MixedAddPointAndAddBlockStats) {
    // Mix scalar addPoint with addBlockStats — both fold into the same state
    auto ba = makeFoldAggregator(AggregationMethod::SUM);

    ba.addPoint(100, 10.0);
    ba.addPoint(200, 20.0);
    ba.addBlockStats(30.0, 30.0, 30.0, 1, 300, 300);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 60.0);
    EXPECT_EQ(state.count, 3u);
}

TEST(BlockAggregatorEdge, LargeBatchSIMDFold) {
    // 1000 points to exercise SIMD with non-trivial data
    auto ba = makeFoldAggregator(AggregationMethod::SUM);

    std::vector<uint64_t> ts(1000);
    std::vector<double> vals(1000);
    double expectedSum = 0.0;
    for (size_t i = 0; i < 1000; ++i) {
        ts[i] = i * 100;
        vals[i] = static_cast<double>(i);
        expectedSum += vals[i];
    }
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), expectedSum);
    EXPECT_EQ(state.count, 1000u);
}

TEST(BlockAggregatorEdge, SmallBatchFallsBackToScalar) {
    // 3 points — below the threshold of 4 for SIMD, uses scalar path
    auto ba = makeFoldAggregator(AggregationMethod::SUM);

    std::vector<uint64_t> ts = {1, 2, 3};
    std::vector<double> vals = {10.0, 20.0, 30.0};
    ba.addPoints(ts, vals);

    auto state = ba.takeSingleState();
    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 60.0);
    EXPECT_EQ(state.count, 3u);
}

TEST(BlockAggregatorEdge, PointCountTracksAcrossAllPaths) {
    uint64_t interval = 1000;
    BlockAggregator ba(interval, 0, 9999, AggregationMethod::SUM, true);

    ba.addPoint(100, 1.0);
    ba.addPoint(200, 2.0);

    std::vector<uint64_t> ts = {300, 400, 500};
    std::vector<double> vals = {3.0, 4.0, 5.0};
    ba.addPoints(ts, vals);

    ba.addBlockStats(10.0, 1.0, 5.0, 2, 600, 700);

    std::vector<uint64_t> countTs = {800, 900};
    ba.addTimestampsOnly(countTs);

    EXPECT_EQ(ba.pointCount(), 9u);
}

// ============================================================================
// Epoch-aligned bucket layout (regression: single-bucket collapse)
//
// Buckets are epoch-aligned: floor(ts / interval) * interval, with endTime
// inclusive.  The constructor used to compute the bucket count as
// ceil((endTime - startTime) / interval), which under-counted for misaligned
// or boundary-inclusive ranges and wrongly engaged the single-bucket
// optimisation — collapsing distinct epoch buckets into one bucket stamped
// floor(startTime / interval).
// ============================================================================

TEST(BlockAggregatorEpochBuckets, MisalignedRangeShorterThanIntervalSpansTwoBuckets) {
    // Range [3, 12] is 9 units with interval 10 — but overlaps epoch buckets
    // [0,10) and [10,20).  Must NOT collapse into a single bucket.
    BlockAggregator ba(10, 3, 12, AggregationMethod::SUM, true);
    ba.addPoint(5, 1.0);
    ba.addPoint(11, 2.0);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 2u);
    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::SUM), 1.0);
    EXPECT_DOUBLE_EQ(buckets[10].getValue(AggregationMethod::SUM), 2.0);
}

TEST(BlockAggregatorEpochBuckets, InclusiveEndTimeOnBucketBoundaryCountsBothBuckets) {
    // endTime is inclusive: [0, 10] with interval 10 covers buckets 0 and 10.
    BlockAggregator ba(10, 0, 10, AggregationMethod::AVG, true);
    ba.addPoint(0, 1.0);
    ba.addPoint(10, 3.0);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 2u);
    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::AVG), 1.0);
    EXPECT_DOUBLE_EQ(buckets[10].getValue(AggregationMethod::AVG), 3.0);
}

TEST(BlockAggregatorEpochBuckets, SingleBucketGuardRoutesOutOfRangePoints) {
    // [12, 15] fits in one epoch bucket (10) — single-bucket optimisation
    // engages.  Points fed from outside the constructed range (e.g. memory
    // fallback data over a wider range) must land in their own epoch buckets,
    // not the cached one.
    BlockAggregator ba(10, 12, 15, AggregationMethod::SUM, true);
    ba.addPoint(13, 1.0);  // in the cached bucket
    ba.addPoint(25, 2.0);  // bucket 20
    ba.addPoint(5, 3.0);   // bucket 0 (below the cached bucket)

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 3u);
    EXPECT_DOUBLE_EQ(buckets[10].getValue(AggregationMethod::SUM), 1.0);
    EXPECT_DOUBLE_EQ(buckets[20].getValue(AggregationMethod::SUM), 2.0);
    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::SUM), 3.0);
}

TEST(BlockAggregatorEpochBuckets, SingleBucketGuardRoutesBatchesSpanningBuckets) {
    BlockAggregator ba(10, 12, 15, AggregationMethod::SUM, true);
    std::vector<uint64_t> ts = {13, 14, 25, 26, 27};
    std::vector<double> vals = {1.0, 2.0, 3.0, 4.0, 5.0};
    ba.addPoints(ts, vals);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 2u);
    EXPECT_DOUBLE_EQ(buckets[10].getValue(AggregationMethod::SUM), 3.0);
    EXPECT_DOUBLE_EQ(buckets[20].getValue(AggregationMethod::SUM), 12.0);
    EXPECT_EQ(buckets[10].count, 2u);
    EXPECT_EQ(buckets[20].count, 3u);
}

TEST(BlockAggregatorEpochBuckets, SingleBucketGuardRoutesRangeAdds) {
    BlockAggregator ba(10, 12, 15, AggregationMethod::MIN, true);
    std::vector<uint64_t> ts = {13, 21, 22, 23, 24, 29};
    std::vector<double> vals = {7.0, 5.0, 4.0, 6.0, 8.0, 9.0};
    ba.addPointsRange(ts, vals, 0, ts.size());

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 2u);
    EXPECT_DOUBLE_EQ(buckets[10].getValue(AggregationMethod::MIN), 7.0);
    EXPECT_DOUBLE_EQ(buckets[20].getValue(AggregationMethod::MIN), 4.0);
}

TEST(BlockAggregatorEpochBuckets, UnusedPreinsertedSingleBucketIsDropped) {
    // All data lands outside the cached single bucket: the pre-inserted empty
    // state must not surface as a bogus bucket.
    BlockAggregator ba(10, 12, 15, AggregationMethod::SUM, true);
    ba.addPoint(25, 2.0);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_EQ(buckets.count(20), 1u);
}

TEST(BlockAggregatorEpochBuckets, NoDataYieldsNoBuckets) {
    BlockAggregator ba(10, 12, 15, AggregationMethod::SUM, true);
    auto buckets = ba.takeBucketStates();
    EXPECT_TRUE(buckets.empty());
}

TEST(BlockAggregatorEpochBuckets, AddBlockStatsRoutesByBlockBucketNotCachedBucket) {
    BlockAggregator ba(10, 12, 15, AggregationMethod::SUM, true);

    // Block in a DIFFERENT epoch bucket than the cached single bucket: usable
    // (fits one bucket) but must be routed to ITS bucket.
    EXPECT_TRUE(ba.canUseBlockStats(25, 27));
    ba.addBlockStats(9.0, 4.0, 5.0, 2, 25, 27);

    // Block spanning two epoch buckets is never usable — even in single-bucket mode.
    EXPECT_FALSE(ba.canUseBlockStats(5, 25));

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 1u);
    EXPECT_DOUBLE_EQ(buckets[20].getValue(AggregationMethod::SUM), 9.0);
}

TEST(BlockAggregatorEpochBuckets, AddTimestampsOnlyGuardRoutesOutOfRange) {
    BlockAggregator ba(10, 12, 15, AggregationMethod::COUNT, true);
    std::vector<uint64_t> ts = {13, 25};
    ba.addTimestampsOnly(ts);

    auto buckets = ba.takeBucketStates();
    ASSERT_EQ(buckets.size(), 2u);
    EXPECT_EQ(buckets[10].count, 1u);
    EXPECT_EQ(buckets[20].count, 1u);
}

// ============================================================================
// sortTimestamps (non-bucketed mode)
// ============================================================================

TEST(BlockAggregatorSort, SortAlreadySorted) {
    BlockAggregator ba(0, AggregationMethod::SUM);
    ba.addPoint(100, 1.0);
    ba.addPoint(200, 2.0);
    ba.addPoint(300, 3.0);

    ba.sortTimestamps();  // Should be a no-op

    auto ts = ba.takeTimestamps();
    auto vals = ba.takeValues();
    EXPECT_EQ(ts[0], 100u);
    EXPECT_EQ(ts[1], 200u);
    EXPECT_EQ(ts[2], 300u);
    EXPECT_DOUBLE_EQ(vals[0], 1.0);
    EXPECT_DOUBLE_EQ(vals[2], 3.0);
}

TEST(BlockAggregatorSort, SortOutOfOrder) {
    BlockAggregator ba(0, AggregationMethod::SUM);
    ba.addPoint(300, 30.0);
    ba.addPoint(100, 10.0);
    ba.addPoint(200, 20.0);

    ba.sortTimestamps();

    auto ts = ba.takeTimestamps();
    auto vals = ba.takeValues();
    EXPECT_EQ(ts[0], 100u);
    EXPECT_EQ(ts[1], 200u);
    EXPECT_EQ(ts[2], 300u);
    // Values must follow the sorted timestamp order
    EXPECT_DOUBLE_EQ(vals[0], 10.0);
    EXPECT_DOUBLE_EQ(vals[1], 20.0);
    EXPECT_DOUBLE_EQ(vals[2], 30.0);
}

TEST(BlockAggregatorSort, SortEmpty) {
    BlockAggregator ba(0, AggregationMethod::SUM);
    ba.sortTimestamps();  // Should not crash
    EXPECT_EQ(ba.takeTimestamps().size(), 0u);
}

TEST(BlockAggregatorSort, SortSingleElement) {
    BlockAggregator ba(0, AggregationMethod::SUM);
    ba.addPoint(500, 5.0);
    ba.sortTimestamps();

    auto ts = ba.takeTimestamps();
    ASSERT_EQ(ts.size(), 1u);
    EXPECT_EQ(ts[0], 500u);
}

// ============================================================================
// Pre-allocation / MAX_PREALLOCATED_BUCKETS guard
// ============================================================================

TEST(BlockAggregatorPrealloc, PreallocatesReasonableBucketCount) {
    // 10 buckets: range=1000, interval=100
    uint64_t interval = 100;
    BlockAggregator ba(interval, 0, 1000, AggregationMethod::SUM, true);

    // Should work fine — points go into expected buckets
    ba.addPoint(50, 1.0);
    ba.addPoint(150, 2.0);

    auto buckets = ba.takeBucketStates();
    EXPECT_EQ(buckets.size(), 2u);
    EXPECT_DOUBLE_EQ(buckets[0].getValue(AggregationMethod::SUM), 1.0);
    EXPECT_DOUBLE_EQ(buckets[100].getValue(AggregationMethod::SUM), 2.0);
}

// ============================================================================
// SIMD fold paths must track firstTimestamp / latestTimestamp
// ============================================================================

// Helper: build a BlockAggregator in fold-to-single-state mode, feed it
// enough points to hit the SIMD path (>= 8), then verify the state has
// valid first/latest timestamps and values.
static void verifySIMDTimestamps(AggregationMethod method) {
    BlockAggregator ba(0, method);
    ba.enableFoldToSingleState();

    // 16 points — enough to trigger the SIMD fold path.
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (size_t i = 0; i < 16; ++i) {
        ts.push_back(1000 + i * 100);   // 1000, 1100, ..., 2500
        vals.push_back(10.0 + i);        // 10.0, 11.0, ..., 25.0
    }

    ba.addPoints(ts, vals);
    auto state = ba.takeSingleState();

    EXPECT_EQ(state.count, 16u);
    // firstTimestamp must be the earliest, not UINT64_MAX default.
    EXPECT_EQ(state.firstTimestamp, 1000u)
        << "SIMD path for " << static_cast<int>(method)
        << " did not set firstTimestamp";
    EXPECT_DOUBLE_EQ(state.first, 10.0);
    // latestTimestamp must be the latest, not 0 default.
    EXPECT_EQ(state.latestTimestamp, 2500u)
        << "SIMD path for " << static_cast<int>(method)
        << " did not set latestTimestamp";
    EXPECT_DOUBLE_EQ(state.latest, 25.0);
}

TEST(BlockAggregatorSIMD, SumTracksTimestamps) {
    verifySIMDTimestamps(AggregationMethod::SUM);
}

TEST(BlockAggregatorSIMD, AvgTracksTimestamps) {
    verifySIMDTimestamps(AggregationMethod::AVG);
}

TEST(BlockAggregatorSIMD, MinTracksTimestamps) {
    verifySIMDTimestamps(AggregationMethod::MIN);
}

TEST(BlockAggregatorSIMD, MaxTracksTimestamps) {
    verifySIMDTimestamps(AggregationMethod::MAX);
}

TEST(BlockAggregatorSIMD, CountTracksTimestamps) {
    verifySIMDTimestamps(AggregationMethod::COUNT);
}

TEST(BlockAggregatorSIMD, SpreadTracksTimestamps) {
    verifySIMDTimestamps(AggregationMethod::SPREAD);
}

TEST(BlockAggregatorSIMD, LatestTracksTimestamps) {
    BlockAggregator ba(0, AggregationMethod::LATEST);
    ba.enableFoldToSingleState();

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (size_t i = 0; i < 16; ++i) {
        ts.push_back(1000 + i * 100);
        vals.push_back(10.0 + i);
    }
    ba.addPoints(ts, vals);
    auto state = ba.takeSingleState();

    EXPECT_EQ(state.count, 16u);
    EXPECT_EQ(state.latestTimestamp, 2500u);
    EXPECT_DOUBLE_EQ(state.latest, 25.0);
}

TEST(BlockAggregatorSIMD, FirstTracksTimestamps) {
    BlockAggregator ba(0, AggregationMethod::FIRST);
    ba.enableFoldToSingleState();

    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (size_t i = 0; i < 16; ++i) {
        ts.push_back(1000 + i * 100);
        vals.push_back(10.0 + i);
    }
    ba.addPoints(ts, vals);
    auto state = ba.takeSingleState();

    EXPECT_EQ(state.count, 16u);
    EXPECT_EQ(state.firstTimestamp, 1000u);
    EXPECT_DOUBLE_EQ(state.first, 10.0);
}

// ── STDDEV/STDVAR SIMD two-pass fold vs scalar Welford ──────────────────────
// The SIMD batch fold computes batch mean + M2 and merges via Chan's parallel
// formula; results must match the per-point Welford path to high precision.

TEST(BlockAggregatorSIMD, StddevMatchesScalarWelford) {
    std::mt19937_64 rng(2026);
    std::uniform_real_distribution<double> dist(-50.0, 150.0);

    constexpr size_t N = 10000;
    constexpr size_t BATCH = 997;  // deliberately not a lane multiple
    std::vector<uint64_t> ts(N);
    std::vector<double> vals(N);
    for (size_t i = 0; i < N; ++i) {
        ts[i] = 1'000'000 + i * 1000;
        vals[i] = dist(rng);
    }

    // SIMD path: batched addPointsRange (methodAware, n >= 4)
    BlockAggregator simdAgg(0, AggregationMethod::STDDEV);
    simdAgg.enableFoldToSingleState();
    for (size_t off = 0; off < N; off += BATCH) {
        size_t end = std::min(off + BATCH, N);
        simdAgg.addPointsRange(ts, vals, off, end);
    }
    auto simdState = simdAgg.takeSingleState();

    // Scalar path: per-point Welford via addValueForMethod
    timestar::AggregationState scalarState;
    for (size_t i = 0; i < N; ++i) {
        scalarState.addValueForMethod(vals[i], ts[i], AggregationMethod::STDDEV);
    }

    const double simdStddev = simdState.getValue(AggregationMethod::STDDEV);
    const double scalarStddev = scalarState.getValue(AggregationMethod::STDDEV);
    EXPECT_EQ(simdState.count, scalarState.count);
    EXPECT_NEAR(simdStddev, scalarStddev, scalarStddev * 1e-10);

    const double simdVar = simdState.getValue(AggregationMethod::STDVAR);
    const double scalarVar = scalarState.getValue(AggregationMethod::STDVAR);
    EXPECT_NEAR(simdVar, scalarVar, scalarVar * 1e-10);
}

TEST(BlockAggregatorSIMD, StddevBucketedMatchesScalar) {
    // Multi-bucket STDDEV: run batching + SIMD fold per bucket segment.
    constexpr uint64_t INTERVAL = 10'000;
    constexpr size_t N = 1000;
    std::vector<uint64_t> ts(N);
    std::vector<double> vals(N);
    for (size_t i = 0; i < N; ++i) {
        ts[i] = i * 100;  // 100 points per bucket
        vals[i] = static_cast<double>((i * 37) % 101);
    }

    BlockAggregator simdAgg(INTERVAL, 0, N * 100, AggregationMethod::STDDEV, true);
    simdAgg.addPoints(ts, vals);
    auto simdStates = simdAgg.takeBucketStates();

    // Scalar reference
    std::unordered_map<uint64_t, timestar::AggregationState> refStates;
    for (size_t i = 0; i < N; ++i) {
        uint64_t bucket = (ts[i] / INTERVAL) * INTERVAL;
        refStates[bucket].addValueForMethod(vals[i], ts[i], AggregationMethod::STDDEV);
    }

    ASSERT_EQ(simdStates.size(), refStates.size());
    for (auto& [bucket, refState] : refStates) {
        auto it = simdStates.find(bucket);
        ASSERT_NE(it, simdStates.end()) << "missing bucket " << bucket;
        const double refV = refState.getValue(AggregationMethod::STDDEV);
        EXPECT_NEAR(it->second.getValue(AggregationMethod::STDDEV), refV, std::max(refV * 1e-10, 1e-12))
            << "bucket " << bucket;
        EXPECT_EQ(it->second.count, refState.count);
    }
}
