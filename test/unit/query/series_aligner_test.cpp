#include "series_aligner.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <limits>
#include <numeric>

using namespace timestar;

class SeriesAlignerTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    SubQueryResult makeResult(const std::string& name, std::vector<uint64_t> ts, std::vector<double> vals) {
        SubQueryResult result;
        result.queryName = name;
        result.timestamps = std::move(ts);
        result.values = std::move(vals);
        return result;
    }
};

// ==================== Basic Alignment Tests ====================

TEST_F(SeriesAlignerTest, AlignEmptySeries) {
    SeriesAligner aligner;
    std::map<std::string, SubQueryResult> series;

    auto result = aligner.align(series);

    EXPECT_TRUE(result.empty());
}

TEST_F(SeriesAlignerTest, AlignSingleSeries) {
    SeriesAligner aligner;
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000, 3000}, {1.0, 2.0, 3.0});

    auto result = aligner.align(series);

    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(result.count("a"));
    EXPECT_EQ(result["a"].size(), 3);
    EXPECT_DOUBLE_EQ(result["a"].values[0], 1.0);
    EXPECT_DOUBLE_EQ(result["a"].values[2], 3.0);
}

TEST_F(SeriesAlignerTest, AlignIdenticalTimestamps) {
    SeriesAligner aligner;
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000, 3000}, {1.0, 2.0, 3.0});
    series["b"] = makeResult("b", {1000, 2000, 3000}, {10.0, 20.0, 30.0});

    auto result = aligner.align(series);

    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result["a"].size(), 3);
    EXPECT_EQ(result["b"].size(), 3);
    EXPECT_EQ(*result["a"].timestamps, *result["b"].timestamps);
}

// ==================== Inner Join Tests ====================

TEST_F(SeriesAlignerTest, InnerJoinPartialOverlap) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000, 3000, 4000}, {1.0, 2.0, 3.0, 4.0});
    series["b"] = makeResult("b", {2000, 3000, 4000, 5000}, {20.0, 30.0, 40.0, 50.0});

    auto result = aligner.align(series);

    // Only timestamps 2000, 3000, 4000 are common
    ASSERT_EQ(result["a"].size(), 3);
    EXPECT_EQ((*result["a"].timestamps)[0], 2000);
    EXPECT_EQ((*result["a"].timestamps)[2], 4000);
}

TEST_F(SeriesAlignerTest, InnerJoinNoOverlap) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000}, {1.0, 2.0});
    series["b"] = makeResult("b", {3000, 4000}, {3.0, 4.0});

    auto result = aligner.align(series);

    // No common timestamps
    EXPECT_TRUE(result.empty() || result["a"].empty());
}

TEST_F(SeriesAlignerTest, InnerJoinThreeSeries) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000, 3000, 4000}, {1.0, 2.0, 3.0, 4.0});
    series["b"] = makeResult("b", {2000, 3000, 4000, 5000}, {20.0, 30.0, 40.0, 50.0});
    series["c"] = makeResult("c", {3000, 4000, 5000, 6000}, {300.0, 400.0, 500.0, 600.0});

    auto result = aligner.align(series);

    // Only timestamps 3000, 4000 are common to all three
    ASSERT_EQ(result["a"].size(), 2);
    EXPECT_EQ((*result["a"].timestamps)[0], 3000);
    EXPECT_EQ((*result["a"].timestamps)[1], 4000);
}

// ==================== Outer Join / Union Tests ====================

TEST_F(SeriesAlignerTest, OuterJoinWithInterpolation) {
    SeriesAligner aligner(AlignmentStrategy::OUTER, InterpolationMethod::LINEAR);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 3000}, {10.0, 30.0});
    series["b"] = makeResult("b", {1000, 2000, 3000}, {100.0, 200.0, 300.0});

    auto result = aligner.align(series);

    // Union: {1000, 2000, 3000}
    ASSERT_EQ(result["a"].size(), 3);
    EXPECT_DOUBLE_EQ(result["a"].values[0], 10.0);
    // Interpolated: (10 + 30) / 2 = 20
    EXPECT_DOUBLE_EQ(result["a"].values[1], 20.0);
    EXPECT_DOUBLE_EQ(result["a"].values[2], 30.0);

    EXPECT_EQ(result["b"].size(), 3);
    EXPECT_DOUBLE_EQ(result["b"].values[0], 100.0);
    EXPECT_DOUBLE_EQ(result["b"].values[1], 200.0);
    EXPECT_DOUBLE_EQ(result["b"].values[2], 300.0);
}

TEST_F(SeriesAlignerTest, UnionWithNaNFill) {
    SeriesAligner aligner(AlignmentStrategy::UNION, InterpolationMethod::NAN_FILL);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 3000}, {10.0, 30.0});
    series["b"] = makeResult("b", {2000}, {200.0});

    auto result = aligner.align(series);

    // Union: {1000, 2000, 3000}
    ASSERT_EQ(result["a"].size(), 3);
    EXPECT_DOUBLE_EQ(result["a"].values[0], 10.0);
    // NaN fill for missing
    EXPECT_TRUE(std::isnan(result["a"].values[1]));
    EXPECT_DOUBLE_EQ(result["a"].values[2], 30.0);
}

// ==================== Interpolation Method Tests ====================

TEST_F(SeriesAlignerTest, LinearInterpolation) {
    SeriesAligner aligner(AlignmentStrategy::OUTER, InterpolationMethod::LINEAR);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 5000}, {10.0, 50.0});
    series["b"] = makeResult("b", {1000, 3000, 5000}, {0.0, 0.0, 0.0});

    auto result = aligner.align(series);

    // At t=3000, linear interpolation: 10 + (50-10) * (3000-1000)/(5000-1000) = 30
    EXPECT_DOUBLE_EQ(result["a"].values[1], 30.0);
}

TEST_F(SeriesAlignerTest, PreviousValueInterpolation) {
    SeriesAligner aligner(AlignmentStrategy::OUTER, InterpolationMethod::PREVIOUS);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 5000}, {10.0, 50.0});
    series["b"] = makeResult("b", {1000, 3000, 5000}, {0.0, 0.0, 0.0});

    auto result = aligner.align(series);

    // At t=3000, use previous value (at t=1000) = 10
    EXPECT_DOUBLE_EQ(result["a"].values[1], 10.0);
}

TEST_F(SeriesAlignerTest, NextValueInterpolation) {
    SeriesAligner aligner(AlignmentStrategy::OUTER, InterpolationMethod::NEXT);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 5000}, {10.0, 50.0});
    series["b"] = makeResult("b", {1000, 3000, 5000}, {0.0, 0.0, 0.0});

    auto result = aligner.align(series);

    // At t=3000, use next value (at t=5000) = 50
    EXPECT_DOUBLE_EQ(result["a"].values[1], 50.0);
}

TEST_F(SeriesAlignerTest, ZeroFillInterpolation) {
    SeriesAligner aligner(AlignmentStrategy::OUTER, InterpolationMethod::ZERO);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 5000}, {10.0, 50.0});
    series["b"] = makeResult("b", {1000, 3000, 5000}, {0.0, 0.0, 0.0});

    auto result = aligner.align(series);

    // At t=3000, fill with zero
    EXPECT_DOUBLE_EQ(result["a"].values[1], 0.0);
}

// ==================== Resampling Tests ====================

TEST_F(SeriesAlignerTest, ResampleToInterval) {
    SeriesAligner aligner(AlignmentStrategy::INNER, InterpolationMethod::LINEAR);
    aligner.setTargetInterval(1000);

    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 1500, 2000, 2500, 3000}, {10.0, 15.0, 20.0, 25.0, 30.0});

    auto result = aligner.align(series);

    // Should resample to 1000, 2000, 3000
    ASSERT_EQ(result["a"].size(), 3);
    EXPECT_EQ((*result["a"].timestamps)[0], 1000);
    EXPECT_EQ((*result["a"].timestamps)[1], 2000);
    EXPECT_EQ((*result["a"].timestamps)[2], 3000);
}

TEST_F(SeriesAlignerTest, ResampleWithInterpolation) {
    SeriesAligner aligner(AlignmentStrategy::INNER, InterpolationMethod::LINEAR);
    aligner.setTargetInterval(2000);

    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {0, 1000, 2000, 3000, 4000}, {0.0, 10.0, 20.0, 30.0, 40.0});

    auto result = aligner.align(series);

    // Should resample to 0, 2000, 4000
    ASSERT_EQ(result["a"].size(), 3);
    EXPECT_DOUBLE_EQ(result["a"].values[0], 0.0);
    EXPECT_DOUBLE_EQ(result["a"].values[1], 20.0);
    EXPECT_DOUBLE_EQ(result["a"].values[2], 40.0);
}

// ==================== Statistics Tests ====================

TEST_F(SeriesAlignerTest, StatsRecorded) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000, 3000, 4000}, {1.0, 2.0, 3.0, 4.0});
    series["b"] = makeResult("b", {2000, 3000}, {20.0, 30.0});

    aligner.align(series);
    auto stats = aligner.getStats();

    EXPECT_EQ(stats.inputSeriesCount, 2);
    EXPECT_EQ(stats.outputPointCount, 2);  // Only 2000, 3000 are common
    EXPECT_EQ(stats.inputPointCounts.size(), 2);
    EXPECT_EQ(stats.inputPointCounts[0], 4);  // Series 'a' had 4 points
    EXPECT_EQ(stats.inputPointCounts[1], 2);  // Series 'b' had 2 points
}

// ==================== Edge Cases ====================

TEST_F(SeriesAlignerTest, EmptySourceSeries) {
    SeriesAligner aligner(AlignmentStrategy::OUTER, InterpolationMethod::NAN_FILL);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {}, {});
    series["b"] = makeResult("b", {1000, 2000}, {10.0, 20.0});

    auto result = aligner.align(series);

    // 'a' should be all NaN
    ASSERT_EQ(result["a"].size(), 2);
    EXPECT_TRUE(std::isnan(result["a"].values[0]));
    EXPECT_TRUE(std::isnan(result["a"].values[1]));
}

TEST_F(SeriesAlignerTest, SinglePointSeries) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000}, {10.0});
    series["b"] = makeResult("b", {1000}, {20.0});

    auto result = aligner.align(series);

    ASSERT_EQ(result["a"].size(), 1);
    EXPECT_DOUBLE_EQ(result["a"].values[0], 10.0);
}

// ==================== Utility Function Tests ====================

TEST_F(SeriesAlignerTest, FindCommonTimeRange) {
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000, 3000}, {1.0, 2.0, 3.0});
    series["b"] = makeResult("b", {2000, 3000, 4000}, {20.0, 30.0, 40.0});
    series["c"] = makeResult("c", {2500, 3500}, {250.0, 350.0});

    auto range = findCommonTimeRange(series);

    EXPECT_TRUE(range.valid);
    EXPECT_EQ(range.start, 2500);  // max of all starts
    EXPECT_EQ(range.end, 3000);    // min of all ends
}

TEST_F(SeriesAlignerTest, FindCommonTimeRangeNoOverlap) {
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {1000, 2000}, {1.0, 2.0});
    series["b"] = makeResult("b", {3000, 4000}, {30.0, 40.0});

    auto range = findCommonTimeRange(series);

    // start (3000) > end (2000), so invalid
    EXPECT_FALSE(range.valid);
}

TEST_F(SeriesAlignerTest, GenerateTimestamps) {
    auto timestamps = generateTimestamps(1000, 5000, 1000);

    ASSERT_EQ(timestamps.size(), 5);
    EXPECT_EQ(timestamps[0], 1000);
    EXPECT_EQ(timestamps[1], 2000);
    EXPECT_EQ(timestamps[4], 5000);
}

TEST_F(SeriesAlignerTest, GenerateTimestampsZeroInterval) {
    auto timestamps = generateTimestamps(1000, 5000, 0);
    EXPECT_TRUE(timestamps.empty());
}

TEST_F(SeriesAlignerTest, GenerateTimestampsInvalidRange) {
    auto timestamps = generateTimestamps(5000, 1000, 1000);  // start > end
    EXPECT_TRUE(timestamps.empty());
}

// ==================== Intersection Performance Tests ====================

TEST_F(SeriesAlignerTest, IntersectionTwoSeriesPartialOverlap) {
    // Two series with partial overlap: verify correct timestamps and values
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;

    // Series a: timestamps 100..900 (even hundreds)
    // Series b: timestamps 500..1300 (even hundreds)
    // Overlap: 500, 600, 700, 800, 900
    std::vector<uint64_t> tsA, tsB;
    std::vector<double> valsA, valsB;
    for (uint64_t t = 100; t <= 900; t += 100) {
        tsA.push_back(t);
        valsA.push_back(static_cast<double>(t));
    }
    for (uint64_t t = 500; t <= 1300; t += 100) {
        tsB.push_back(t);
        valsB.push_back(static_cast<double>(t) * 10.0);
    }

    series["a"] = makeResult("a", tsA, valsA);
    series["b"] = makeResult("b", tsB, valsB);

    auto result = aligner.align(series);

    // Intersection should contain exactly {500, 600, 700, 800, 900}
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result["a"].size(), 5);
    ASSERT_EQ(result["b"].size(), 5);

    std::vector<uint64_t> expected = {500, 600, 700, 800, 900};
    EXPECT_EQ(*result["a"].timestamps, expected);
    EXPECT_EQ(*result["b"].timestamps, expected);

    // Verify values are correctly aligned
    EXPECT_DOUBLE_EQ(result["a"].values[0], 500.0);
    EXPECT_DOUBLE_EQ(result["a"].values[4], 900.0);
    EXPECT_DOUBLE_EQ(result["b"].values[0], 5000.0);
    EXPECT_DOUBLE_EQ(result["b"].values[4], 9000.0);

    // Verify stats: a had 9 points, b had 9 points, output 5
    auto stats = aligner.getStats();
    EXPECT_EQ(stats.outputPointCount, 5);
    // a dropped 4, b dropped 4
    EXPECT_EQ(stats.pointsDropped, 8);
}

TEST_F(SeriesAlignerTest, IntersectionThreeOrMoreSeries) {
    // Three series with progressively narrowing overlap
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;

    // a: 1000, 2000, 3000, 4000, 5000, 6000
    // b:       2000, 3000, 4000, 5000, 6000, 7000
    // c:             3000, 4000, 5000
    // Intersection: 3000, 4000, 5000

    series["a"] = makeResult("a", {1000, 2000, 3000, 4000, 5000, 6000}, {1.0, 2.0, 3.0, 4.0, 5.0, 6.0});
    series["b"] = makeResult("b", {2000, 3000, 4000, 5000, 6000, 7000}, {20.0, 30.0, 40.0, 50.0, 60.0, 70.0});
    series["c"] = makeResult("c", {3000, 4000, 5000}, {300.0, 400.0, 500.0});

    auto result = aligner.align(series);

    ASSERT_EQ(result.size(), 3);
    std::vector<uint64_t> expected = {3000, 4000, 5000};

    EXPECT_EQ(*result["a"].timestamps, expected);
    EXPECT_EQ(*result["b"].timestamps, expected);
    EXPECT_EQ(*result["c"].timestamps, expected);

    // Check values are correctly selected
    EXPECT_DOUBLE_EQ(result["a"].values[0], 3.0);
    EXPECT_DOUBLE_EQ(result["a"].values[1], 4.0);
    EXPECT_DOUBLE_EQ(result["a"].values[2], 5.0);
    EXPECT_DOUBLE_EQ(result["b"].values[0], 30.0);
    EXPECT_DOUBLE_EQ(result["b"].values[1], 40.0);
    EXPECT_DOUBLE_EQ(result["b"].values[2], 50.0);
    EXPECT_DOUBLE_EQ(result["c"].values[0], 300.0);
    EXPECT_DOUBLE_EQ(result["c"].values[1], 400.0);
    EXPECT_DOUBLE_EQ(result["c"].values[2], 500.0);

    // Now test with 5 series where intersection is a single point
    SeriesAligner aligner5(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series5;

    series5["s1"] = makeResult("s1", {1000, 2000, 3000, 4000, 5000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    series5["s2"] = makeResult("s2", {2000, 3000, 4000, 5000, 6000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    series5["s3"] = makeResult("s3", {3000, 4000, 5000, 6000, 7000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    series5["s4"] = makeResult("s4", {4000, 5000, 6000, 7000, 8000}, {1.0, 2.0, 3.0, 4.0, 5.0});
    series5["s5"] = makeResult("s5", {5000, 6000, 7000, 8000, 9000}, {1.0, 2.0, 3.0, 4.0, 5.0});

    auto result5 = aligner5.align(series5);

    // Only timestamp 5000 is common to all 5 series
    ASSERT_EQ(result5["s1"].size(), 1);
    EXPECT_EQ((*result5["s1"].timestamps)[0], 5000);
}

TEST_F(SeriesAlignerTest, IntersectionLargeDatasetPerformance) {
    // Test with 10K+ timestamps to verify the O(n) algorithm is fast enough
    constexpr size_t N = 50000;

    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;

    // Series a: timestamps 0, 10, 20, ..., (N-1)*10
    // Series b: timestamps 5*10, 5*10+10, ..., (N+4)*10  (shifted by 50)
    // Overlap: 50, 60, 70, ..., (N-1)*10
    // Overlap size: N - 5

    std::vector<uint64_t> tsA(N), tsB(N);
    std::vector<double> valsA(N), valsB(N);

    for (size_t i = 0; i < N; ++i) {
        tsA[i] = i * 10;
        valsA[i] = static_cast<double>(i);
        tsB[i] = (i + 5) * 10;
        valsB[i] = static_cast<double>(i + 5);
    }

    series["a"] = makeResult("a", tsA, valsA);
    series["b"] = makeResult("b", tsB, valsB);

    auto start = std::chrono::high_resolution_clock::now();
    auto result = aligner.align(series);
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Verify correctness
    size_t expectedSize = N - 5;
    ASSERT_EQ(result["a"].size(), expectedSize);
    ASSERT_EQ(result["b"].size(), expectedSize);

    // First common timestamp should be 50
    EXPECT_EQ((*result["a"].timestamps)[0], 50);
    // Last common timestamp should be (N-1)*10
    EXPECT_EQ((*result["a"].timestamps)[expectedSize - 1], (N - 1) * 10);

    // Verify all timestamps match between the two aligned series
    EXPECT_EQ(*result["a"].timestamps, *result["b"].timestamps);

    // Verify values are correct
    EXPECT_DOUBLE_EQ(result["a"].values[0], 5.0);  // tsA[5] = 50 -> valsA[5] = 5.0
    EXPECT_DOUBLE_EQ(result["b"].values[0], 5.0);  // tsB[0] = 50 -> valsB[0] = 5.0

    // Performance: with the O(n) algorithm using set_intersection on sorted
    // vectors, 50K elements should complete well under 1 second.
    // The old O(n log n) std::set approach would be noticeably slower.
    EXPECT_LT(elapsed.count(), 1000) << "Intersection of 50K-element series took " << elapsed.count()
                                     << "ms, expected < 1000ms";

    // Test with 3 large series
    SeriesAligner aligner3(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series3;

    std::vector<uint64_t> tsC(N);
    std::vector<double> valsC(N);
    for (size_t i = 0; i < N; ++i) {
        tsC[i] = (i + 3) * 10;  // shifted by 30
        valsC[i] = static_cast<double>(i + 3);
    }
    series3["a"] = makeResult("a", tsA, valsA);
    series3["b"] = makeResult("b", tsB, valsB);
    series3["c"] = makeResult("c", tsC, valsC);

    auto start3 = std::chrono::high_resolution_clock::now();
    auto result3 = aligner3.align(series3);
    auto end3 = std::chrono::high_resolution_clock::now();
    auto elapsed3 = std::chrono::duration_cast<std::chrono::milliseconds>(end3 - start3);

    // Intersection of a (0..N*10-10), b (50..N*10+40), c (30..N*10+20)
    // Common range: max(0,50,30)=50 to min(N*10-10, N*10+40, N*10+20)=N*10-10
    // With step 10: 50, 60, ..., N*10-10 => N - 5 points
    ASSERT_EQ(result3["a"].size(), N - 5);
    EXPECT_EQ((*result3["a"].timestamps)[0], 50);

    EXPECT_LT(elapsed3.count(), 1000) << "Intersection of three 50K-element series took " << elapsed3.count()
                                      << "ms, expected < 1000ms";
}

// ==================== projectedOutputSize (Phase 4.5) ====================
//
// The multi-series /derived bound is enforced from this projection BEFORE a
// group is materialised, so what matters is: it never under-states what align()
// would actually emit (nothing escapes the bound), and it is exact in the
// ordinary shapes (nothing is refused that costs nothing).
//
// Phase 4.6 made the second half unconditional for INNER and LEFT: the answer
// EQUALS align()'s output size, because the intersection is counted rather than
// bounded.  UNION/OUTER still over-state a count (never a grid, and never
// under).

TEST_F(SeriesAlignerTest, ProjectedSizeIsExactForASharedAxis) {
    SeriesAligner aligner;
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {10, 20, 30, 40}, {1.0, 2.0, 3.0, 4.0});
    series["b"] = makeResult("b", {10, 20, 30, 40}, {5.0, 6.0, 7.0, 8.0});

    EXPECT_EQ(aligner.projectedOutputSize(series), 4u);
    EXPECT_EQ(aligner.align(series)["a"].size(), 4u);
}

TEST_F(SeriesAlignerTest, ProjectedSizeIsAnUpperBoundOnAPartialIntersection) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {10, 20, 30, 40}, {1.0, 2.0, 3.0, 4.0});
    series["b"] = makeResult("b", {20, 30}, {5.0, 6.0});

    const size_t projected = aligner.projectedOutputSize(series);
    const size_t actual = aligner.align(series)["a"].size();
    EXPECT_EQ(actual, 2u);
    EXPECT_GE(projected, actual) << "an under-estimate would let a result escape the bound built on it";
    EXPECT_EQ(projected, 2u) << "the shortest input bounds an intersection";
}

TEST_F(SeriesAlignerTest, ProjectedSizeIsZeroWhenAnInputIsEmptyOrTheInputsDoNotOverlap) {
    SeriesAligner aligner(AlignmentStrategy::INNER);

    std::map<std::string, SubQueryResult> withEmpty;
    withEmpty["a"] = makeResult("a", {10, 20}, {1.0, 2.0});
    withEmpty["b"] = makeResult("b", {}, {});
    EXPECT_EQ(aligner.projectedOutputSize(withEmpty), 0u);

    std::map<std::string, SubQueryResult> disjoint;
    disjoint["a"] = makeResult("a", {10, 20}, {1.0, 2.0});
    disjoint["b"] = makeResult("b", {90, 100}, {3.0, 4.0});
    EXPECT_EQ(aligner.projectedOutputSize(disjoint), 0u);
    EXPECT_TRUE(aligner.align(disjoint).empty());

    // Overlapping SPANS, no shared TIMESTAMP -- the gap the span-disjoint case
    // above does not cover, and the one that let an over-statement through:
    // [max(first), min(last)] is 20..80 here and perfectly plausible, but the
    // intersection is still empty, so sizing anything over that interval claims
    // points this alignment cannot produce.
    std::map<std::string, SubQueryResult> interleaved;
    interleaved["a"] = makeResult("a", {10, 30, 50, 70, 90}, {1.0, 2.0, 3.0, 4.0, 5.0});
    interleaved["b"] = makeResult("b", {20, 40, 60, 80, 100}, {6.0, 7.0, 8.0, 9.0, 10.0});
    EXPECT_EQ(aligner.projectedOutputSize(interleaved), 0u);
    EXPECT_TRUE(aligner.align(interleaved).empty());

    // And with an interval, where the grid rather than the axis would decide:
    // 20..80 at a step of 1 is 61 claimed points for an answer of none.
    SeriesAligner resampling(AlignmentStrategy::INNER);
    resampling.setTargetInterval(1);
    EXPECT_EQ(resampling.projectedOutputSize(interleaved), 0u);
    EXPECT_TRUE(resampling.align(interleaved).empty());

    EXPECT_EQ(aligner.projectedOutputSize({}), 0u);
}

// The case the bound exists for: four stored points over a wide window with a
// fine interval.  align() would emit the whole dense grid; the projection says
// so WITHOUT building it.
TEST_F(SeriesAlignerTest, ProjectedSizeCountsTheResampledGridWithoutBuildingIt) {
    SeriesAligner aligner;
    aligner.setTargetInterval(1000);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {0, 1000000}, {1.0, 2.0});
    series["b"] = makeResult("b", {0, 1000000}, {3.0, 4.0});

    EXPECT_EQ(aligner.projectedOutputSize(series), 1001u);
    EXPECT_EQ(aligner.align(series)["a"].size(), 1001u);
}

TEST_F(SeriesAlignerTest, ProjectedSizeAlignsTheGridStartDownLikeTheResampler) {
    SeriesAligner aligner;
    aligner.setTargetInterval(1000);
    std::map<std::string, SubQueryResult> series;
    // 1500 aligns down to 1000, so the grid is 1000..5000 == 5 slots.
    series["a"] = makeResult("a", {1500, 5000}, {1.0, 2.0});
    series["b"] = makeResult("b", {1500, 5000}, {3.0, 4.0});

    EXPECT_EQ(aligner.projectedOutputSize(series), 5u);
    EXPECT_EQ(aligner.align(series)["a"].size(), 5u);
}

// Past kMaxResamplePoints align() abandons the grid and keeps the axis; the
// projection follows it there rather than refusing a query that costs nothing.
TEST_F(SeriesAlignerTest, ProjectedSizeFollowsTheResampleFallbackPastTheGridCeiling) {
    SeriesAligner aligner;
    aligner.setTargetInterval(1);
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {0, SeriesAligner::kMaxResamplePoints * 10}, {1.0, 2.0});
    series["b"] = makeResult("b", {0, SeriesAligner::kMaxResamplePoints * 10}, {3.0, 4.0});

    EXPECT_EQ(aligner.projectedOutputSize(series), 2u);
    EXPECT_EQ(aligner.align(series)["a"].size(), 2u);
}

TEST_F(SeriesAlignerTest, ProjectedSizeBoundsTheUnionAndLeftStrategiesToo) {
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {10, 20, 30}, {1.0, 2.0, 3.0});
    series["b"] = makeResult("b", {20, 40}, {4.0, 5.0});

    SeriesAligner unionAligner(AlignmentStrategy::UNION);
    const size_t unionProjected = unionAligner.projectedOutputSize(series);
    const size_t unionActual = unionAligner.align(series)["a"].size();
    EXPECT_EQ(unionActual, 4u);  // 10, 20, 30, 40
    EXPECT_GE(unionProjected, unionActual);
    EXPECT_EQ(unionProjected, 5u) << "the sum of the inputs bounds a union";

    SeriesAligner leftAligner(AlignmentStrategy::LEFT);
    EXPECT_EQ(leftAligner.projectedOutputSize(series), 3u) << "LEFT is exactly the first input";
    EXPECT_EQ(leftAligner.align(series)["a"].size(), 3u);
}

TEST_F(SeriesAlignerTest, ProjectedSizeDoesNotDisturbTheReportedStats) {
    SeriesAligner aligner;
    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", {10, 20, 30, 40}, {1.0, 2.0, 3.0, 4.0});
    series["b"] = makeResult("b", {20, 30}, {5.0, 6.0});

    aligner.projectedOutputSize(series);
    EXPECT_EQ(aligner.getStats().pointsDropped, 0u) << "the projection is a query, not an alignment";
    aligner.align(series);
    const size_t dropped = aligner.getStats().pointsDropped;
    aligner.projectedOutputSize(series);
    EXPECT_EQ(aligner.getStats().pointsDropped, dropped);
}

// ==================== projectedOutputSize (Phase 4.6) ====================
//
// The intersection is COUNTED, not bounded by the shortest input and the
// interval that contains it.  These pin the shapes where the two differ.

// The false-refusal shape: a shared dense band with unshared extremes.  Sized
// over [max(first), min(last)] this claimed 2,999 points at an interval of 1;
// the real answer is 100, and the /derived budget refused the query for it.
TEST_F(SeriesAlignerTest, ProjectedSizeIsExactWhenTheExtremeTimestampsAreNotShared) {
    std::vector<uint64_t> a{0};
    std::vector<uint64_t> b{1};
    std::vector<double> av{0.0};
    std::vector<double> bv{0.0};
    for (uint64_t i = 0; i < 100; ++i) {
        a.push_back(1000 + i);
        b.push_back(1000 + i);
        av.push_back(static_cast<double>(i));
        bv.push_back(static_cast<double>(i));
    }
    a.push_back(3000);
    b.push_back(2999);
    av.push_back(1.0);
    bv.push_back(1.0);

    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", a, av);
    series["b"] = makeResult("b", b, bv);

    SeriesAligner plain(AlignmentStrategy::INNER);
    EXPECT_EQ(plain.projectedOutputSize(series), 100u);
    EXPECT_EQ(plain.align(series)["a"].size(), 100u);

    SeriesAligner resampling(AlignmentStrategy::INNER);
    resampling.setTargetInterval(1);
    EXPECT_EQ(resampling.projectedOutputSize(series), 100u) << "the grid spans the SHARED timestamps, 1000..1099";
    EXPECT_EQ(resampling.align(series)["a"].size(), 100u);
}

// The other direction, and the reason a merge beats a wider-but-cheaper bound:
// the containing interval's grid runs past kMaxResamplePoints (so the projection
// fell back to the axis, 1,002) while the grid over the REAL intersection stays
// just under it and IS built -- 9,990,001 points, admitted by a budget of
// 250,000 and paid for in full before the after-the-fact backstop caught it.
//
// align() is deliberately not called: materialising those 9,990,001 points is
// the 200 ms and two reactor stalls this exists to avoid.
TEST_F(SeriesAlignerTest, ProjectedSizeDoesNotUnderStateWhenTheContainingGridExceedsTheCeiling) {
    constexpr uint64_t kStep = 10000;
    constexpr uint64_t kBand = 1000;
    std::vector<uint64_t> a{0};
    std::vector<uint64_t> b{1};
    std::vector<double> values{0.0};
    for (uint64_t i = 0; i < kBand; ++i) {
        a.push_back(1000000 + i * kStep);
        b.push_back(1000000 + i * kStep);
        values.push_back(static_cast<double>(i));
    }
    a.push_back(100000000000000ULL);
    b.push_back(99999999999999ULL);
    values.push_back(1.0);

    std::map<std::string, SubQueryResult> series;
    series["a"] = makeResult("a", a, values);
    series["b"] = makeResult("b", b, values);

    SeriesAligner aligner(AlignmentStrategy::INNER);
    aligner.setTargetInterval(1);
    // The band is (kBand - 1) * kStep wide, so the grid over it is that + 1 --
    // just under the 10M ceiling, hence really built by align().
    const uint64_t realGrid = (kBand - 1) * kStep + 1;
    EXPECT_LE(realGrid, SeriesAligner::kMaxResamplePoints);
    EXPECT_EQ(aligner.projectedOutputSize(series), realGrid);
}

// Three legs, and duplicates: the walk must agree with the set_intersection
// chain computeIntersection() uses, which is multiset intersection (a value
// repeated in every input contributes min(counts)).
TEST_F(SeriesAlignerTest, ProjectedSizeMatchesTheIntersectionForThreeLegsAndRepeatedTimestamps) {
    SeriesAligner aligner(AlignmentStrategy::INNER);

    std::map<std::string, SubQueryResult> three;
    three["a"] = makeResult("a", {10, 20, 30, 40, 50}, {1.0, 2.0, 3.0, 4.0, 5.0});
    three["b"] = makeResult("b", {20, 30, 40, 60}, {1.0, 2.0, 3.0, 4.0});
    three["c"] = makeResult("c", {5, 30, 40, 70}, {1.0, 2.0, 3.0, 4.0});
    EXPECT_EQ(aligner.projectedOutputSize(three), 2u);  // 30, 40
    EXPECT_EQ(aligner.align(three)["a"].size(), 2u);

    std::map<std::string, SubQueryResult> repeated;
    repeated["a"] = makeResult("a", {10, 10, 10, 20}, {1.0, 2.0, 3.0, 4.0});
    repeated["b"] = makeResult("b", {10, 10, 30}, {1.0, 2.0, 3.0});
    EXPECT_EQ(aligner.projectedOutputSize(repeated), aligner.align(repeated)["a"].size());
}

// More legs than the inline cursor array holds: the spill is a correctness
// fallback, so it must reach the same answer.
TEST_F(SeriesAlignerTest, ProjectedSizeIsExactWithMoreLegsThanTheInlineCursors) {
    SeriesAligner aligner(AlignmentStrategy::INNER);
    std::map<std::string, SubQueryResult> series;
    for (size_t leg = 0; leg < 12; ++leg) {
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        // Every leg holds 100..109; leg n additionally holds a point of its own,
        // so no leg is trivially the intersection.
        for (uint64_t t = 100; t < 110; ++t) {
            timestamps.push_back(t);
            values.push_back(static_cast<double>(t));
        }
        timestamps.push_back(200 + leg);
        values.push_back(1.0);
        series["leg" + std::to_string(leg)] = makeResult("leg" + std::to_string(leg), timestamps, values);
    }

    EXPECT_EQ(aligner.projectedOutputSize(series), 10u);
    EXPECT_EQ(aligner.align(series)["leg0"].size(), 10u);
}
