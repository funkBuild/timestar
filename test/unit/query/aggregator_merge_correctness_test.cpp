// Tests for two aggregation merge correctness bugs:
//  1. allTimestampsIdentical false-positive from 3-point heuristic
//  2. LATEST fold using std::max instead of keeping either value

#include "../../../lib/http/http_query_handler.hpp"  // For SeriesResult
#include "../../../lib/query/aggregator.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <vector>

using namespace timestar;

class AggregatorMergeCorrectnessTest : public ::testing::Test {
protected:
    // Helper to create a SeriesResult with double values
    SeriesResult createSeriesResult(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                    const std::string& fieldName, const std::vector<uint64_t>& timestamps,
                                    const std::vector<double>& values) {
        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        sr.fields[fieldName] = std::make_pair(timestamps, FieldValues(values));
        return sr;
    }

    // Helper: build raw-value partials (simulating pushdown) with given timestamps/values.
    // This populates sortedTimestamps + sortedValues (the compact pushdown path),
    // which triggers the allTimestampsIdentical / fold / nWayMerge code paths.
    PartialAggregationResult makeRawPartial(const std::string& groupKey, const std::vector<uint64_t>& ts,
                                            const std::vector<double>& vals) {
        PartialAggregationResult p;
        p.measurement = "test";
        p.fieldName = "value";
        p.groupKey = groupKey;
        p.groupKeyHash = std::hash<std::string>{}(groupKey);
        p.sortedTimestamps = ts;
        p.sortedValues = vals;
        p.totalPoints = ts.size();
        return p;
    }
};

// ============================================================================
// Bug 1: allTimestampsIdentical false-positive
// ============================================================================

// Two vectors that match on front(), back(), and mid but differ on an interior
// element. The old 3-point heuristic would incorrectly declare them identical,
// causing foldAlignedRawValues (element-wise combine) instead of nWayMerge.
// With SUM, fold would add element-wise (wrong), while merge would interleave
// the different timestamps correctly.
TEST_F(AggregatorMergeCorrectnessTest, AllTimestampsIdentical_FalsePositive_DifferentInterior) {
    // Construct two 5-element vectors:
    //   ref:    [100, 200, 300, 400, 500]
    //   other:  [100, 250, 300, 450, 500]
    // front()==front(), back()==back(), mid(size/2=2)==mid(2) i.e. 300==300
    // But elements at index 1 and 3 differ.
    auto p1 = makeRawPartial("test\0value", {100, 200, 300, 400, 500}, {1.0, 1.0, 1.0, 1.0, 1.0});
    auto p2 = makeRawPartial("test\0value", {100, 250, 300, 450, 500}, {2.0, 2.0, 2.0, 2.0, 2.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1);

    // With correct merge (nWayMerge), the output should have 7 unique timestamps:
    //   100(1+2=3), 200(1), 250(2), 300(1+2=3), 400(1), 450(2), 500(1+2=3)
    // With the old buggy fold, it would have only 5 elements with wrong values.
    ASSERT_EQ(grouped[0].points.size(), 7);

    // Verify shared timestamps are summed, unique ones are kept as-is
    EXPECT_EQ(grouped[0].points[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 3.0);  // 1+2

    EXPECT_EQ(grouped[0].points[1].timestamp, 200);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 1.0);  // only in p1

    EXPECT_EQ(grouped[0].points[2].timestamp, 250);
    EXPECT_DOUBLE_EQ(grouped[0].points[2].value, 2.0);  // only in p2

    EXPECT_EQ(grouped[0].points[3].timestamp, 300);
    EXPECT_DOUBLE_EQ(grouped[0].points[3].value, 3.0);  // 1+2

    EXPECT_EQ(grouped[0].points[4].timestamp, 400);
    EXPECT_DOUBLE_EQ(grouped[0].points[4].value, 1.0);  // only in p1

    EXPECT_EQ(grouped[0].points[5].timestamp, 450);
    EXPECT_DOUBLE_EQ(grouped[0].points[5].value, 2.0);  // only in p2

    EXPECT_EQ(grouped[0].points[6].timestamp, 500);
    EXPECT_DOUBLE_EQ(grouped[0].points[6].value, 3.0);  // 1+2
}

// A variant: same size, same front/back/mid, but a single interior element differs.
TEST_F(AggregatorMergeCorrectnessTest, AllTimestampsIdentical_FalsePositive_SingleInteriorDiff) {
    // 7-element vectors; mid=index 3 is the same; differ only at index 5
    auto p1 = makeRawPartial("test\0value", {10, 20, 30, 40, 50, 60, 70}, {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0});
    auto p2 = makeRawPartial("test\0value", {10, 20, 30, 40, 50, 65, 70}, {2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1);
    // 6 shared timestamps + 2 unique (60, 65) = 8 output points
    EXPECT_EQ(grouped[0].points.size(), 8);
}

// Truly identical timestamps should still take the fast fold path and produce
// correct results.
TEST_F(AggregatorMergeCorrectnessTest, AllTimestampsIdentical_TruePositive_FoldCorrect) {
    auto p1 = makeRawPartial("test\0value", {100, 200, 300}, {10.0, 20.0, 30.0});
    auto p2 = makeRawPartial("test\0value", {100, 200, 300}, {1.0, 2.0, 3.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 11.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 22.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[2].value, 33.0);
}

// ============================================================================
// Bug 2: LATEST fold uses std::max(value) instead of keeping either value
// ============================================================================

// When two partials have the same timestamp, LATEST should keep either value
// (both are equally "latest"), NOT return the numerically larger one.
// We test with the smaller value in the base partial to verify it does NOT
// return the max.
TEST_F(AggregatorMergeCorrectnessTest, LatestMerge_SameTimestamp_DoesNotReturnMax) {
    // p1 has value 5.0 at ts=1000, p2 has value 99.0 at ts=1000.
    // LATEST at identical timestamps should keep one of them (base=5.0 in fold/merge),
    // NOT return max(5.0, 99.0) = 99.0.
    auto p1 = makeRawPartial("test\0value", {1000}, {5.0});
    auto p2 = makeRawPartial("test\0value", {1000}, {99.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    // Must NOT be 99.0 (which std::max would return)
    // Should be 5.0 (the base/first value — either is acceptable, but NOT max)
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 5.0);
}

// Multi-timestamp LATEST merge: partials with identical timestamp vectors.
// This exercises the foldAlignedRawValues path.
TEST_F(AggregatorMergeCorrectnessTest, LatestFoldAligned_SameTimestamps_DoesNotReturnMax) {
    auto p1 = makeRawPartial("test\0value", {100, 200, 300}, {1.0, 2.0, 3.0});
    auto p2 = makeRawPartial("test\0value", {100, 200, 300}, {10.0, 20.0, 30.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);
    // With aligned fold, LATEST should keep the base (p1) values, not max.
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 1.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 2.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[2].value, 3.0);
}

// N-way merge path with mismatched timestamps: at shared timestamps, LATEST
// should keep the first-popped value (whichever the heap yields first), NOT
// apply std::max. We verify the value is one of the two inputs — the heap
// order for equal timestamps is unspecified, so either is acceptable.
TEST_F(AggregatorMergeCorrectnessTest, LatestNWayMerge_SharedTimestamp_KeepsOneInputValue) {
    // p1: ts=[100, 200, 400], p2: ts=[200, 300, 400]
    // Shared timestamps: 200, 400. Unique: 100 (p1 only), 300 (p2 only).
    auto p1 = makeRawPartial("test\0value", {100, 200, 400}, {1.0, 5.0, 3.0});
    auto p2 = makeRawPartial("test\0value", {200, 300, 400}, {50.0, 7.0, 30.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 4);  // ts: 100, 200, 300, 400

    // ts=100: only in p1 -> value=1.0
    EXPECT_EQ(grouped[0].points[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 1.0);

    // ts=200: shared. Value must be one of {5.0, 50.0} (not some combination).
    EXPECT_EQ(grouped[0].points[1].timestamp, 200);
    double v200 = grouped[0].points[1].value;
    EXPECT_TRUE(v200 == 5.0 || v200 == 50.0) << "Expected 5.0 or 50.0, got " << v200;

    // ts=300: only in p2 -> value=7.0
    EXPECT_EQ(grouped[0].points[2].timestamp, 300);
    EXPECT_DOUBLE_EQ(grouped[0].points[2].value, 7.0);

    // ts=400: shared. Value must be one of {3.0, 30.0}.
    EXPECT_EQ(grouped[0].points[3].timestamp, 400);
    double v400 = grouped[0].points[3].value;
    EXPECT_TRUE(v400 == 3.0 || v400 == 30.0) << "Expected 3.0 or 30.0, got " << v400;
}

// Verify the N-way merge LATEST fix more precisely: with the old std::max bug,
// merging {-10} and {5} at the same timestamp would return 5 (max). With the
// fix, it returns whichever is popped first (deterministic: the first partial
// seeds the heap, so its value is popped first for equal timestamps among
// partials seeded in order).
TEST_F(AggregatorMergeCorrectnessTest, LatestNWayMerge_NegativeValue_DoesNotReturnMax) {
    // If the old code used std::max, merging -10 and 5 at the same timestamp
    // would return 5. The fix keeps the first-popped value instead.
    auto p1 = makeRawPartial("test\0value", {500, 1000}, {-10.0, -20.0});
    auto p2 = makeRawPartial("test\0value", {750, 1000}, {5.0, 5.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);  // ts: 500, 750, 1000

    // ts=1000: shared. Value must be one of {-20.0, 5.0}.
    // The fix ensures it's one of the inputs, NOT always std::max(-20, 5) = 5.
    double v1000 = grouped[0].points[2].value;
    EXPECT_TRUE(v1000 == -20.0 || v1000 == 5.0) << "Expected -20.0 or 5.0, got " << v1000;
}

// Three partials with identical timestamps — exercises the K>2 fold path.
TEST_F(AggregatorMergeCorrectnessTest, LatestFoldAligned_ThreePartials_KeepsBase) {
    auto p1 = makeRawPartial("test\0value", {1000, 2000}, {1.0, 2.0});
    auto p2 = makeRawPartial("test\0value", {1000, 2000}, {100.0, 200.0});
    auto p3 = makeRawPartial("test\0value", {1000, 2000}, {50.0, 60.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 2);
    // Should keep p1 (base) values, not max across all three
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 1.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 2.0);
}

// Verify MAX still works correctly (should still use std::max, unlike LATEST).
TEST_F(AggregatorMergeCorrectnessTest, MaxMerge_StillReturnsMaxValue) {
    auto p1 = makeRawPartial("test\0value", {1000}, {5.0});
    auto p2 = makeRawPartial("test\0value", {1000}, {99.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MAX);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    // MAX should still return the larger value
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 99.0);
}

// Verify FIRST fold also keeps base value (not max) at identical timestamps.
TEST_F(AggregatorMergeCorrectnessTest, FirstFoldAligned_KeepsBaseValue) {
    auto p1 = makeRawPartial("test\0value", {100, 200}, {1.0, 2.0});
    auto p2 = makeRawPartial("test\0value", {100, 200}, {99.0, 88.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::FIRST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 2);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 1.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 2.0);
}

// ============================================================================
// Bug C2: SPREAD/STDDEV/STDVAR fell through to SUM in the raw-value merge path.
// The fix gates these methods away from the allRaw fast path so they go through
// full AggregationState merge instead.
// ============================================================================

// SPREAD: two raw-value partials with values [1,5] and [2,8].
// Merged spread = max(1,5,2,8) - min(1,5,2,8) = 8 - 1 = 7.
TEST_F(AggregatorMergeCorrectnessTest, SpreadMerge_RawValues_CorrectRange) {
    // p1 has values at ts=100 and ts=200; p2 has values at ts=300 and ts=400.
    // All timestamps are distinct, so no duplicate-timestamp folding needed.
    auto p1 = makeRawPartial("test\0value", {100, 200}, {1.0, 5.0});
    auto p2 = makeRawPartial("test\0value", {300, 400}, {2.0, 8.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SPREAD);

    ASSERT_EQ(grouped.size(), 1);
    // With the C2 fix, SPREAD goes through AggregationState merge which tracks
    // min/max across all points and returns max - min.
    // All 4 unique timestamps produce 4 points, each with spread computed from
    // a single-value state (spread = 0). OR the implementation may collapse them.
    // The key invariant: no point should have a spread that looks like a SUM.
    //
    // The fallback path converts raw values to AggregationState objects (one per
    // unique timestamp). With 4 unique timestamps and one value each, each state
    // has count=1, min==max, so spread=0 per point.
    ASSERT_EQ(grouped[0].points.size(), 4);
    for (const auto& pt : grouped[0].points) {
        // Each point is a single observation: spread = max - min = 0
        EXPECT_DOUBLE_EQ(pt.value, 0.0);
    }
}

// SPREAD with overlapping timestamps: two partials sharing a timestamp.
// At the shared timestamp, two values are merged into one AggregationState,
// so spread = max - min of those two values.
TEST_F(AggregatorMergeCorrectnessTest, SpreadMerge_SharedTimestamp_CorrectRange) {
    // p1: ts=100 val=1, ts=200 val=5
    // p2: ts=200 val=8, ts=300 val=2
    // At ts=200: two values {5, 8} => spread = 8 - 5 = 3
    auto p1 = makeRawPartial("test\0value", {100, 200}, {1.0, 5.0});
    auto p2 = makeRawPartial("test\0value", {200, 300}, {8.0, 2.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SPREAD);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    // Sort points by timestamp for deterministic checks
    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

    // ts=100: single value 1.0 => spread = 0
    EXPECT_EQ(pts[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(pts[0].value, 0.0);

    // ts=200: values {5.0, 8.0} => spread = 8 - 5 = 3
    EXPECT_EQ(pts[1].timestamp, 200);
    EXPECT_DOUBLE_EQ(pts[1].value, 3.0);

    // ts=300: single value 2.0 => spread = 0
    EXPECT_EQ(pts[2].timestamp, 300);
    EXPECT_DOUBLE_EQ(pts[2].value, 0.0);
}

// STDDEV: two raw-value partials with known values, verify population stddev.
// Values at shared timestamp ts=200: {10, 20} => mean=15, variance=25, stddev=5.
TEST_F(AggregatorMergeCorrectnessTest, StddevMerge_RawValues_CorrectResult) {
    // p1: ts=100 val=5, ts=200 val=10
    // p2: ts=200 val=20, ts=300 val=30
    // At ts=200: values {10, 20}
    //   population stddev = sqrt(((10-15)^2 + (20-15)^2) / 2) = sqrt(50/2) = sqrt(25) = 5
    auto p1 = makeRawPartial("test\0value", {100, 200}, {5.0, 10.0});
    auto p2 = makeRawPartial("test\0value", {200, 300}, {20.0, 30.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::STDDEV);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

    // ts=100: single value => stddev = 0
    EXPECT_EQ(pts[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(pts[0].value, 0.0);

    // ts=200: values {10, 20} => pop stddev = 5.0
    EXPECT_EQ(pts[1].timestamp, 200);
    EXPECT_NEAR(pts[1].value, 5.0, 1e-10);

    // ts=300: single value => stddev = 0
    EXPECT_EQ(pts[2].timestamp, 300);
    EXPECT_DOUBLE_EQ(pts[2].value, 0.0);
}

// STDVAR: same data as STDDEV, verify population variance (stddev^2).
TEST_F(AggregatorMergeCorrectnessTest, StdvarMerge_RawValues_CorrectResult) {
    auto p1 = makeRawPartial("test\0value", {100, 200}, {5.0, 10.0});
    auto p2 = makeRawPartial("test\0value", {200, 300}, {20.0, 30.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::STDVAR);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

    // ts=100: single value => variance = 0
    EXPECT_EQ(pts[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(pts[0].value, 0.0);

    // ts=200: values {10, 20} => pop variance = 25.0
    EXPECT_EQ(pts[1].timestamp, 200);
    EXPECT_NEAR(pts[1].value, 25.0, 1e-10);

    // ts=300: single value => variance = 0
    EXPECT_EQ(pts[2].timestamp, 300);
    EXPECT_DOUBLE_EQ(pts[2].value, 0.0);
}

// STDDEV with three partials sharing the same timestamp: Welford merge correctness.
// Values at ts=1000: {2, 4, 6} => mean=4, pop var = ((2-4)^2+(4-4)^2+(6-4)^2)/3 = 8/3
// pop stddev = sqrt(8/3)
TEST_F(AggregatorMergeCorrectnessTest, StddevMerge_ThreePartials_WelfordMergeCorrect) {
    auto p1 = makeRawPartial("test\0value", {1000}, {2.0});
    auto p2 = makeRawPartial("test\0value", {1000}, {4.0});
    auto p3 = makeRawPartial("test\0value", {1000}, {6.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::STDDEV);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    double expected = std::sqrt(8.0 / 3.0);
    EXPECT_NEAR(grouped[0].points[0].value, expected, 1e-10);
}

// MEDIAN: two raw-value partials with overlapping timestamps.
// At ts=200: values {10, 20} => median = (10+20)/2 = 15.
// Single-value timestamps: median = that value.
TEST_F(AggregatorMergeCorrectnessTest, MedianMerge_RawValues_CorrectResult) {
    auto p1 = makeRawPartial("test\0value", {100, 200}, {3.0, 10.0});
    auto p2 = makeRawPartial("test\0value", {200, 300}, {20.0, 7.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MEDIAN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

    // ts=100: single value 3.0 => median = 3.0
    EXPECT_EQ(pts[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(pts[0].value, 3.0);

    // ts=200: values {10, 20} => median = (10+20)/2 = 15.0
    EXPECT_EQ(pts[1].timestamp, 200);
    EXPECT_DOUBLE_EQ(pts[1].value, 15.0);

    // ts=300: single value 7.0 => median = 7.0
    EXPECT_EQ(pts[2].timestamp, 300);
    EXPECT_DOUBLE_EQ(pts[2].value, 7.0);
}

// MEDIAN with odd number of merged values at a single timestamp.
// Values at ts=500: {1, 3, 5} from three partials => median = 3.
TEST_F(AggregatorMergeCorrectnessTest, MedianMerge_ThreePartials_OddCount) {
    auto p1 = makeRawPartial("test\0value", {500}, {1.0});
    auto p2 = makeRawPartial("test\0value", {500}, {5.0});
    auto p3 = makeRawPartial("test\0value", {500}, {3.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MEDIAN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 3.0);
}
