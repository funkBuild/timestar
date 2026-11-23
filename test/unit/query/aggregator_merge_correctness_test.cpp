// Tests for two aggregation merge correctness bugs:
//  1. allTimestampsIdentical false-positive from 3-point heuristic
//  2. LATEST fold using std::max instead of keeping either value

#include "../../../lib/http/http_query_handler.hpp"  // For SeriesResult
#include "../../../lib/query/aggregator.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <map>
#include <random>
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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MAX).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::FIRST).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SPREAD).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SPREAD).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::STDDEV).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::STDVAR).get();

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
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::STDDEV).get();

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    double expected = std::sqrt(8.0 / 3.0);
    EXPECT_NEAR(grouped[0].points[0].value, expected, 1e-10);
}

// MEDIAN (approximate via t-digest): two raw-value partials with overlapping timestamps.
// At ts=200: values {10, 20} => approximate median ~15.
// Single-value timestamps: t-digest returns the exact value.
TEST_F(AggregatorMergeCorrectnessTest, MedianMerge_RawValues_CorrectResult) {
    auto p1 = makeRawPartial("test\0value", {100, 200}, {3.0, 10.0});
    auto p2 = makeRawPartial("test\0value", {200, 300}, {20.0, 7.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MEDIAN).get();

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

    // ts=100: single value 3.0 => t-digest returns exact value
    EXPECT_EQ(pts[0].timestamp, 100);
    EXPECT_NEAR(pts[0].value, 3.0, 0.1);

    // ts=200: values {10, 20} => t-digest approximate median ~15
    EXPECT_EQ(pts[1].timestamp, 200);
    EXPECT_NEAR(pts[1].value, 15.0, 1.0);

    // ts=300: single value 7.0 => t-digest returns exact value
    EXPECT_EQ(pts[2].timestamp, 300);
    EXPECT_NEAR(pts[2].value, 7.0, 0.1);
}

// EXACT_MEDIAN: preserves the old exact behavior via rawValues.
TEST_F(AggregatorMergeCorrectnessTest, ExactMedianMerge_RawValues_CorrectResult) {
    auto p1 = makeRawPartial("test\0value", {100, 200}, {3.0, 10.0});
    auto p2 = makeRawPartial("test\0value", {200, 300}, {20.0, 7.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::EXACT_MEDIAN).get();

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(),
              [](const AggregatedPoint& a, const AggregatedPoint& b) { return a.timestamp < b.timestamp; });

    EXPECT_EQ(pts[0].timestamp, 100);
    EXPECT_DOUBLE_EQ(pts[0].value, 3.0);

    EXPECT_EQ(pts[1].timestamp, 200);
    EXPECT_DOUBLE_EQ(pts[1].value, 15.0);

    EXPECT_EQ(pts[2].timestamp, 300);
    EXPECT_DOUBLE_EQ(pts[2].value, 7.0);
}

// MEDIAN with odd number of merged values at a single timestamp.
// Values at ts=500: {1, 3, 5} from three partials => approximate median ~3.
TEST_F(AggregatorMergeCorrectnessTest, MedianMerge_ThreePartials_OddCount) {
    auto p1 = makeRawPartial("test\0value", {500}, {1.0});
    auto p2 = makeRawPartial("test\0value", {500}, {5.0});
    auto p3 = makeRawPartial("test\0value", {500}, {3.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MEDIAN).get();

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_NEAR(grouped[0].points[0].value, 3.0, 0.5);
}

// EXACT_MEDIAN: preserves exact behavior for odd count.
TEST_F(AggregatorMergeCorrectnessTest, ExactMedianMerge_ThreePartials_OddCount) {
    auto p1 = makeRawPartial("test\0value", {500}, {1.0});
    auto p2 = makeRawPartial("test\0value", {500}, {5.0});
    auto p3 = makeRawPartial("test\0value", {500}, {3.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::EXACT_MEDIAN).get();

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 3.0);
}

// ============================================================================
// NaN = missing data in the raw merge paths (docs/nan_policy.md)
// ============================================================================
//
// The raw (sortedValues) partial format carries NaN verbatim — raw passthrough
// reads must return it — so the MERGE is where NaN must be skipped: a NaN
// contribution never folds and never counts, exactly like
// AggregationState::addValue on the state path.  Pre-fix, one NaN poisoned the
// whole timestamp (NaN + 5.0 = NaN) and inflated AVG's divisor.

TEST_F(AggregatorMergeCorrectnessTest, NanNeverFolds_HeapMerge) {
    // Mismatched timestamp sets force the N-way heap merge.
    auto p1 = makeRawPartial("g", {100, 200, 350}, {std::nan(""), 3.0, 7.0});
    auto p2 = makeRawPartial("g", {100, 200, 300}, {5.0, std::nan(""), 9.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::AVG).get();

    ASSERT_EQ(grouped.size(), 1u);
    auto& pts = grouped[0].points;
    ASSERT_EQ(pts.size(), 4u);
    EXPECT_EQ(pts[0].timestamp, 100u);
    EXPECT_DOUBLE_EQ(pts[0].value, 5.0);  // NaN skipped, not folded
    EXPECT_EQ(pts[0].count, 1u);          // and not counted (AVG divisor)
    EXPECT_EQ(pts[1].timestamp, 200u);
    EXPECT_DOUBLE_EQ(pts[1].value, 3.0);
    EXPECT_EQ(pts[1].count, 1u);
    EXPECT_DOUBLE_EQ(pts[2].value, 9.0);  // 300: single non-NaN contributor
    EXPECT_DOUBLE_EQ(pts[3].value, 7.0);  // 350
}

TEST_F(AggregatorMergeCorrectnessTest, NanNeverFolds_IdenticalTimestamps) {
    // Identical timestamp vectors would take the branch-free aligned fold —
    // the NaN prescan must route this group to the NaN-aware heap merge.
    auto p1 = makeRawPartial("g", {100, 200, 300}, {std::nan(""), 2.0, 4.0});
    auto p2 = makeRawPartial("g", {100, 200, 300}, {6.0, std::nan(""), 8.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::AVG).get();

    ASSERT_EQ(grouped.size(), 1u);
    auto& pts = grouped[0].points;
    ASSERT_EQ(pts.size(), 3u);
    EXPECT_DOUBLE_EQ(pts[0].value, 6.0);
    EXPECT_EQ(pts[0].count, 1u);
    EXPECT_DOUBLE_EQ(pts[1].value, 2.0);
    EXPECT_EQ(pts[1].count, 1u);
    EXPECT_DOUBLE_EQ(pts[2].value, 6.0);  // avg(4, 8)
    EXPECT_EQ(pts[2].count, 2u);
}

TEST_F(AggregatorMergeCorrectnessTest, NanFirstContributionDoesNotPoisonMin) {
    // std::min(NaN, x) returns NaN when NaN is the accumulator — the merge
    // must replace a NaN-only slot with the first real value instead.
    auto p1 = makeRawPartial("g", {100}, {std::nan("")});
    auto p2 = makeRawPartial("g", {100}, {42.0});
    auto p3 = makeRawPartial("g", {100}, {17.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MIN).get();

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 17.0);
    EXPECT_EQ(grouped[0].points[0].count, 2u);
}

TEST_F(AggregatorMergeCorrectnessTest, AllNanTimestampIsNanWithZeroCount) {
    // Every contribution NaN → empty per-timestamp set: value NaN, count 0
    // (mirrors getValue() on a count-0 AggregationState).
    auto p1 = makeRawPartial("g", {100, 200}, {std::nan(""), 1.0});
    auto p2 = makeRawPartial("g", {100, 200}, {std::nan(""), 2.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM).get();

    ASSERT_EQ(grouped.size(), 1u);
    auto& pts = grouped[0].points;
    ASSERT_EQ(pts.size(), 2u);
    EXPECT_TRUE(std::isnan(pts[0].value));
    EXPECT_EQ(pts[0].count, 0u);
    EXPECT_DOUBLE_EQ(pts[1].value, 3.0);
    EXPECT_EQ(pts[1].count, 2u);
}

TEST_F(AggregatorMergeCorrectnessTest, CountSkipsNan) {
    // Multi-partial: COUNT counts only non-NaN contributors.
    auto p1 = makeRawPartial("g", {100, 200}, {std::nan(""), 1.0});
    auto p2 = makeRawPartial("g", {100, 200}, {5.0, 2.0});
    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::COUNT).get();
    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 2u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 1.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 2.0);

    // Single partial: a lone NaN point is an empty set → NaN, not 1.
    auto p3 = makeRawPartial("g", {100, 200}, {std::nan(""), 7.0});
    std::vector<PartialAggregationResult> single = {std::move(p3)};
    auto groupedSingle = Aggregator::mergePartialAggregationsGrouped(single, AggregationMethod::COUNT).get();
    ASSERT_EQ(groupedSingle.size(), 1u);
    ASSERT_EQ(groupedSingle[0].points.size(), 2u);
    EXPECT_TRUE(std::isnan(groupedSingle[0].points[0].value));
    EXPECT_EQ(groupedSingle[0].points[0].count, 0u);
    EXPECT_DOUBLE_EQ(groupedSingle[0].points[1].value, 1.0);
}

TEST_F(AggregatorMergeCorrectnessTest, NanSkippedByStateNeedingMethods_RawPartials) {
    // Non-foldable methods route raw partials through addValue (NaN-skipping)
    // inside the raw-aware state merge — pin that a NaN contributor changes
    // neither the value nor the count.
    auto p1 = makeRawPartial("g", {100}, {std::nan("")});
    auto p2 = makeRawPartial("g", {100}, {3.0});
    auto p3 = makeRawPartial("g", {100}, {7.0});

    std::vector<PartialAggregationResult> allPartials = {std::move(p1), std::move(p2), std::move(p3)};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SPREAD).get();

    ASSERT_EQ(grouped.size(), 1u);
    ASSERT_EQ(grouped[0].points.size(), 1u);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 4.0);  // spread(3, 7), NaN ignored
    EXPECT_EQ(grouped[0].points[0].count, 2u);
}

TEST_F(AggregatorMergeCorrectnessTest, ShortAlignedCompactionPreservesWeightedResultsAndNaNs) {
    for (auto method :
         {AggregationMethod::AVG, AggregationMethod::SUM, AggregationMethod::COUNT, AggregationMethod::MIN,
          AggregationMethod::MAX, AggregationMethod::SPREAD, AggregationMethod::STDDEV, AggregationMethod::STDVAR}) {
        std::vector<PartialAggregationResult> partials;
        const double nan = std::numeric_limits<double>::quiet_NaN();
        for (size_t i = 0; i < 32; ++i) {
            auto p = makeRawPartial("aligned", {10, 20, 30},
                                    {static_cast<double>(i), i % 3 == 0 ? nan : static_cast<double>(i), nan});
            p.totalPoints = 3;
            partials.push_back(std::move(p));
        }
        // Simulate an extra shard with different timestamps. It must merge
        // correctly with the compacted shard, including unequal AVG weights.
        auto other = makeRawPartial("aligned", {10, 25, 30}, {100.0, 200.0, nan});
        auto original = partials;
        original.push_back(other);
        auto expected = Aggregator::mergePartialAggregationsGrouped(original, method).get();
        Aggregator::compactAlignedPartialGroups(partials, method).get();
        ASSERT_EQ(partials.size(), 1u);
        EXPECT_EQ(partials[0].totalPoints, 96u);
        ASSERT_EQ(partials[0].sortedStates.size(), 3u);
        EXPECT_TRUE(partials[0].sortedValues.empty());
        partials.push_back(std::move(other));
        auto actual = Aggregator::mergePartialAggregationsGrouped(partials, method).get();
        ASSERT_EQ(actual.size(), 1u);
        ASSERT_EQ(actual[0].points.size(), expected[0].points.size());
        for (size_t i = 0; i < actual[0].points.size(); ++i) {
            const auto& a = actual[0].points[i];
            const auto& e = expected[0].points[i];
            EXPECT_EQ(a.timestamp, e.timestamp);
            EXPECT_EQ(a.count, e.count);
            if (std::isnan(e.value))
                EXPECT_TRUE(std::isnan(a.value));
            else
                EXPECT_NEAR(a.value, e.value, 1e-10) << static_cast<int>(method);
        }
    }
}

TEST_F(AggregatorMergeCorrectnessTest, ShortCompactionKeepsDistinctGroupsAndMisalignedSeries) {
    std::vector<PartialAggregationResult> partials;
    for (size_t i = 0; i < 16; ++i) {
        partials.push_back(makeRawPartial("aligned", {1, 2, 3}, {1, 2, 3}));
        partials.push_back(makeRawPartial("misaligned", {1, i + 2, 100}, {1, 2, 3}));
    }
    Aggregator::compactAlignedPartialGroups(partials, AggregationMethod::SUM).get();
    ASSERT_EQ(partials.size(), 17u);
    EXPECT_EQ(partials.front().groupKey, "aligned");
    EXPECT_EQ(partials.front().sortedStates.size(), 3u);
    for (size_t i = 1; i < partials.size(); ++i) {
        EXPECT_EQ(partials[i].groupKey, "misaligned");
        EXPECT_TRUE(partials[i].sortedStates.empty());
        EXPECT_EQ(partials[i].sortedTimestamps[1], i + 1);
    }
}

TEST_F(AggregatorMergeCorrectnessTest, CompactionLeavesLargeSeriesAndOrderSensitiveMethodsRaw) {
    for (auto method : {AggregationMethod::AVG, AggregationMethod::LATEST, AggregationMethod::FIRST,
                        AggregationMethod::MEDIAN, AggregationMethod::EXACT_MEDIAN}) {
        std::vector<PartialAggregationResult> partials;
        const size_t n = method == AggregationMethod::AVG ? 257 : 3;
        std::vector<uint64_t> ts(n);
        std::iota(ts.begin(), ts.end(), 1);
        for (size_t i = 0; i < 16; ++i)
            partials.push_back(makeRawPartial("group", ts, std::vector<double>(n, i)));
        Aggregator::compactAlignedPartialGroups(partials, method).get();
        ASSERT_EQ(partials.size(), 16u);
        for (const auto& p : partials) {
            EXPECT_TRUE(p.sortedStates.empty());
            EXPECT_EQ(p.sortedValues.size(), n);
        }
    }
}

TEST_F(AggregatorMergeCorrectnessTest, CompactResponseArraysMatchPointResults) {
    for (auto method : {AggregationMethod::AVG, AggregationMethod::SUM, AggregationMethod::COUNT,
                        AggregationMethod::MIN, AggregationMethod::MAX, AggregationMethod::FIRST,
                        AggregationMethod::LATEST, AggregationMethod::SPREAD, AggregationMethod::STDDEV,
                        AggregationMethod::STDVAR, AggregationMethod::MEDIAN, AggregationMethod::EXACT_MEDIAN}) {
        for (int shape = 0; shape < 6; ++shape) {
            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<PartialAggregationResult> partials;
            partials.push_back(makeRawPartial("g", {1, 2, 3, 5}, {nan, 4.0, -2.0, nan}));
            if (shape != 0) {
                partials.push_back(makeRawPartial(
                    "g", shape == 1 ? std::vector<uint64_t>{1, 2, 3, 5} : std::vector<uint64_t>{1, 2, 4, 5},
                    {nan, 8.0, 3.0, nan}));
            }
            if (shape == 5) {
                partials[0].sortedValues = {1, 4, -2, 3};
                partials[1].sortedTimestamps = partials[0].sortedTimestamps;
                partials[1].sortedValues = {2, 8, 3, 4};
            }
            if (shape == 3 || shape == 4) {
                auto& p = partials.front();
                for (size_t i = 0; i < p.sortedValues.size(); ++i) {
                    AggregationState state;
                    state.collectRaw = method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN;
                    state.addValue(p.sortedValues[i], p.sortedTimestamps[i]);
                    p.sortedStates.push_back(std::move(state));
                }
                p.sortedValues.clear();
                if (shape == 4)
                    partials.resize(1);
            }
            auto baseline = partials;
            auto expected = Aggregator::mergePartialAggregationsGrouped(baseline, method).get();
            auto compact = Aggregator::mergePartialAggregationsGrouped(partials, method, true).get();
            ASSERT_EQ(compact.size(), expected.size());
            ASSERT_EQ(compact.size(), 1u);
            EXPECT_TRUE(compact[0].points.empty());
            auto& e = expected[0];
            std::vector<uint64_t> ts = e.rawTimestamps;
            std::vector<double> vals = e.rawValues;
            if (ts.empty())
                for (const auto& p : e.points) {
                    ts.push_back(p.timestamp);
                    vals.push_back(p.value);
                }
            EXPECT_EQ(compact[0].rawTimestamps, ts);
            ASSERT_EQ(compact[0].rawValues.size(), vals.size());
            EXPECT_EQ(compact[0].fieldName, e.fieldName);
            EXPECT_EQ(compact[0].tags, e.tags);
            for (size_t i = 0; i < vals.size(); ++i) {
                if (std::isnan(vals[i]))
                    EXPECT_TRUE(std::isnan(compact[0].rawValues[i]));
                else
                    EXPECT_DOUBLE_EQ(compact[0].rawValues[i], vals[i]) << static_cast<int>(method) << ':' << shape;
            }
        }
    }
}

TEST_F(AggregatorMergeCorrectnessTest, HeapAdvanceAndCompactCountsMatchIndependentOracle) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (size_t streams : {1u, 2u, 3u, 17u, 64u}) {
        std::vector<PartialAggregationResult> partials;
        std::map<uint64_t, std::vector<double>> oracle;
        for (size_t s = 0; s < streams; ++s) {
            std::vector<uint64_t> ts;
            std::vector<double> vals;
            // Empty streams, uneven lengths and repeated cross-stream times.
            for (size_t i = 0; i < (s * 13 + 7) % 37; ++i) {
                uint64_t t = i * (s % 3 + 1) + s % 5;
                double v = t % 7 == 0 || (i + s) % 5 == 0 ? nan : static_cast<double>((i * 17 + s) % 100) * .25;
                ts.push_back(t);
                vals.push_back(v);
                oracle[t].push_back(v);
            }
            partials.push_back(makeRawPartial("oracle", ts, vals));
        }
        for (auto method : {AggregationMethod::AVG, AggregationMethod::SUM, AggregationMethod::COUNT,
                            AggregationMethod::MIN, AggregationMethod::MAX, AggregationMethod::SPREAD,
                            AggregationMethod::STDDEV, AggregationMethod::STDVAR}) {
            for (bool compact : {false, true}) {
                auto input = partials;
                auto groups = Aggregator::mergePartialAggregationsGrouped(input, method, compact).get();
                ASSERT_EQ(groups.size(), 1u);
                auto& g = groups[0];
                const bool arrays = !g.rawTimestamps.empty();
                ASSERT_EQ(arrays ? g.rawTimestamps.size() : g.points.size(), oracle.size());
                size_t out = 0;
                for (const auto& [ts, incoming] : oracle) {
                    std::vector<double> valid;
                    for (double v : incoming)
                        if (!std::isnan(v))
                            valid.push_back(v);
                    double expected = nan;
                    if (!valid.empty()) {
                        long double sum = 0;
                        for (double v : valid)
                            sum += v;
                        long double mean = sum / valid.size(), variance = 0;
                        for (double v : valid)
                            variance += (v - mean) * (v - mean);
                        variance /= valid.size();
                        switch (method) {
                            case AggregationMethod::AVG:
                                expected = static_cast<double>(mean);
                                break;
                            case AggregationMethod::SUM:
                                expected = static_cast<double>(sum);
                                break;
                            case AggregationMethod::COUNT:
                                expected = valid.size();
                                break;
                            case AggregationMethod::MIN:
                                expected = *std::min_element(valid.begin(), valid.end());
                                break;
                            case AggregationMethod::MAX:
                                expected = *std::max_element(valid.begin(), valid.end());
                                break;
                            case AggregationMethod::SPREAD:
                                expected = *std::max_element(valid.begin(), valid.end()) -
                                           *std::min_element(valid.begin(), valid.end());
                                break;
                            case AggregationMethod::STDDEV:
                                expected = std::sqrt(variance);
                                break;
                            case AggregationMethod::STDVAR:
                                expected = variance;
                                break;
                            default:
                                FAIL() << "unexpected method";
                        }
                    }
                    EXPECT_EQ(arrays ? g.rawTimestamps[out] : g.points[out].timestamp, ts);
                    const double actual = arrays ? g.rawValues[out] : g.points[out].value;
                    if (std::isnan(expected))
                        EXPECT_TRUE(std::isnan(actual));
                    else
                        EXPECT_NEAR(actual, expected, 1e-11) << streams << ':' << static_cast<int>(method);
                    if (!arrays)
                        EXPECT_EQ(g.points[out].count, valid.size());
                    ++out;
                }
            }
        }
    }
}

TEST_F(AggregatorMergeCorrectnessTest, CompactSumDoesNotConfuseArithmeticNaNWithMissingData) {
    const double inf = std::numeric_limits<double>::infinity();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (auto method : {AggregationMethod::SUM, AggregationMethod::AVG, AggregationMethod::COUNT}) {
        std::vector<PartialAggregationResult> partials;
        partials.push_back(makeRawPartial("inf", {1, 2}, {inf, nan}));
        partials.push_back(makeRawPartial("inf", {1, 3}, {-inf, 4}));
        partials.push_back(makeRawPartial("inf", {1, 4}, {7, 5}));
        auto groups = Aggregator::mergePartialAggregationsGrouped(partials, method, true).get();
        ASSERT_EQ(groups.size(), 1u);
        EXPECT_EQ(groups[0].rawTimestamps, (std::vector<uint64_t>{1, 2, 3, 4}));
        if (method == AggregationMethod::COUNT)
            EXPECT_DOUBLE_EQ(groups[0].rawValues[0], 3);
        else
            EXPECT_TRUE(std::isnan(groups[0].rawValues[0]));
        EXPECT_TRUE(std::isnan(groups[0].rawValues[1]));
    }
}

// The merge emits a stretch that only one stream covers straight from that
// stream, without going back to the heap for every point. Build timelines out
// of such stretches — long and short, some shared by several streams, with
// NaNs sprinkled in and enough points to cross the yield boundaries mid-run —
// and check every method against a map-based oracle.
TEST_F(AggregatorMergeCorrectnessTest, SingleStreamRunsMatchIndependentOracle) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    uint64_t rng = 0x9E3779B97F4A7C15ULL;
    auto next = [&rng] {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        return rng;
    };

    for (size_t streams : {1u, 2u, 5u, 12u}) {
        for (size_t maxRun : {1u, 40u, 30000u}) {
            std::vector<std::vector<uint64_t>> ts(streams);
            std::vector<std::vector<double>> vals(streams);
            std::map<uint64_t, std::vector<double>> oracle;
            uint64_t t = 1000;
            while (oracle.size() < 90000) {
                // One stretch: owned by one stream, or shared by a few.
                const size_t owners = (next() % 4 == 0) ? 1 + next() % std::min<size_t>(streams, 3) : 1;
                const size_t first = next() % streams;
                const size_t length = 1 + next() % maxRun;
                for (size_t i = 0; i < length; ++i) {
                    t += 1 + next() % 3;
                    for (size_t o = 0; o < owners; ++o) {
                        const size_t s = (first + o) % streams;
                        const double v = (next() % 97 == 0) ? nan : static_cast<double>(next() % 2000) * 0.5 - 300.0;
                        ts[s].push_back(t);
                        vals[s].push_back(v);
                        oracle[t].push_back(v);
                    }
                }
            }

            std::vector<PartialAggregationResult> partials;
            for (size_t s = 0; s < streams; ++s) {
                partials.push_back(makeRawPartial("runs", ts[s], vals[s]));
            }

            for (auto method : {AggregationMethod::AVG, AggregationMethod::SUM, AggregationMethod::COUNT,
                                AggregationMethod::MIN, AggregationMethod::MAX, AggregationMethod::FIRST,
                                AggregationMethod::LATEST, AggregationMethod::SPREAD, AggregationMethod::STDDEV}) {
                for (bool compact : {false, true}) {
                    auto input = partials;
                    auto groups = Aggregator::mergePartialAggregationsGrouped(input, method, compact).get();
                    ASSERT_EQ(groups.size(), 1u);
                    auto& g = groups[0];
                    const bool arrays = !g.rawTimestamps.empty();
                    ASSERT_EQ(arrays ? g.rawTimestamps.size() : g.points.size(), oracle.size())
                        << streams << ':' << maxRun << ':' << static_cast<int>(method) << ':' << compact;
                    size_t out = 0;
                    for (const auto& [stamp, incoming] : oracle) {
                        std::vector<double> valid;
                        for (double v : incoming)
                            if (!std::isnan(v))
                                valid.push_back(v);
                        ASSERT_EQ(arrays ? g.rawTimestamps[out] : g.points[out].timestamp, stamp);
                        const double actual = arrays ? g.rawValues[out] : g.points[out].value;
                        ++out;
                        if (method == AggregationMethod::FIRST || method == AggregationMethod::LATEST) {
                            // Equal timestamps across series: any contributing
                            // value is a valid pick; a lone point is itself.
                            if (valid.empty()) {
                                ASSERT_TRUE(std::isnan(actual));
                            } else if (incoming.size() == 1) {
                                ASSERT_EQ(actual, incoming[0]);
                            } else {
                                ASSERT_TRUE(std::isnan(actual) ||
                                            std::find(valid.begin(), valid.end(), actual) != valid.end());
                            }
                            continue;
                        }
                        if (valid.empty()) {
                            ASSERT_TRUE(std::isnan(actual));
                            continue;
                        }
                        long double sum = 0;
                        for (double v : valid)
                            sum += v;
                        const long double mean = sum / valid.size();
                        long double variance = 0;
                        for (double v : valid)
                            variance += (v - mean) * (v - mean);
                        variance /= valid.size();
                        double expected = 0;
                        switch (method) {
                            case AggregationMethod::AVG:
                                expected = static_cast<double>(mean);
                                break;
                            case AggregationMethod::SUM:
                                expected = static_cast<double>(sum);
                                break;
                            case AggregationMethod::COUNT:
                                expected = static_cast<double>(valid.size());
                                break;
                            case AggregationMethod::MIN:
                                expected = *std::min_element(valid.begin(), valid.end());
                                break;
                            case AggregationMethod::MAX:
                                expected = *std::max_element(valid.begin(), valid.end());
                                break;
                            case AggregationMethod::SPREAD:
                                expected = *std::max_element(valid.begin(), valid.end()) -
                                           *std::min_element(valid.begin(), valid.end());
                                break;
                            default:
                                expected = static_cast<double>(std::sqrt(variance));
                        }
                        ASSERT_NEAR(actual, expected, 1e-9)
                            << streams << ':' << maxRun << ':' << static_cast<int>(method) << ':' << compact;
                    }
                }
            }
        }
    }
}

// Bucketed partials (one hash map of bucket -> state per shard) are merged by
// one of three routes: a single partial is finalized directly, partials that
// share their buckets fold through the first partial's map, and partials with
// mostly different buckets fold through a sorted index. Which route runs is a
// performance decision only, so all of them are held to the same oracle: for
// each bucket, the first partial that has it absorbs the others in partial
// order, and points come out ordered by bucket — compared exactly, bits
// included, since the fold order decides the last bits of sums and variances.
TEST_F(AggregatorMergeCorrectnessTest, BucketedPartialsMatchLeftFoldOracleOnEveryRoute) {
    struct Shape {
        const char* name;
        size_t partials;
        size_t bucketsPerPartial;
        size_t stride;  // bucket-id step between a partial's consecutive buckets
        size_t offset;  // bucket-id shift from one partial to the next
        bool shuffle;   // insert in random order, so map iteration is not monotonic
    };
    // stride == partials, offset == 1: fully disjoint (interleaved) -> sorted index.
    // offset == 0: identical buckets everywhere -> map fold.
    // The others overlap partly, and land on either route.
    const std::vector<Shape> shapes = {
        {"single partial", 1, 5000, 1, 0, true},
        {"disjoint, time-ordered inserts", 4, 3000, 4, 1, false},
        {"disjoint, shuffled inserts", 4, 40000, 4, 1, true},
        {"disjoint blocks", 3, 2000, 1, 2000, false},
        {"fully shared", 5, 2000, 1, 0, true},
        {"half overlapping", 4, 3000, 1, 1500, true},
        {"mostly disjoint with a shared fringe", 6, 1000, 1, 990, false},
        {"two partials, one tiny", 2, 3, 1, 1, true},
    };
    const std::vector<AggregationMethod> methods = {
        AggregationMethod::AVG,    AggregationMethod::SUM,    AggregationMethod::MIN,
        AggregationMethod::MAX,    AggregationMethod::COUNT,  AggregationMethod::LATEST,
        AggregationMethod::FIRST,  AggregationMethod::STDDEV, AggregationMethod::STDVAR,
        AggregationMethod::SPREAD, AggregationMethod::MEDIAN, AggregationMethod::EXACT_MEDIAN};
    constexpr uint64_t kBucketNs = 60'000'000'000ULL;

    std::mt19937_64 rng(4242);
    for (const auto& shape : shapes) {
        for (AggregationMethod method : methods) {
            const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);

            // Build the inputs twice from the same recipe: one set to merge,
            // one for the oracle (merging consumes its input).
            auto build = [&](uint64_t seed) {
                std::mt19937_64 gen(seed);
                std::vector<PartialAggregationResult> partials(shape.partials);
                for (size_t p = 0; p < shape.partials; ++p) {
                    auto& partial = partials[p];
                    partial.measurement = "test";
                    partial.fieldName = "value";
                    partial.groupKey = "g";
                    partial.groupKeyHash = std::hash<std::string>{}("g");
                    std::vector<uint64_t> ids(shape.bucketsPerPartial);
                    for (size_t i = 0; i < ids.size(); ++i) {
                        ids[i] = i * shape.stride + p * shape.offset;
                    }
                    if (shape.shuffle) {
                        std::shuffle(ids.begin(), ids.end(), gen);
                    }
                    for (uint64_t id : ids) {
                        AggregationState state;
                        state.collectRaw = needsRaw;
                        const size_t n = 1 + gen() % 3;
                        for (size_t k = 0; k < n; ++k) {
                            // Values whose sums depend on the order they are added in.
                            const double v =
                                static_cast<double>(gen() % 1'000'003) * 0.1 + 1e-7 * static_cast<double>(p);
                            state.addValueForMethod(v, id * kBucketNs + (gen() % 1000) * 1'000'000ULL, method);
                        }
                        partial.bucketStates.emplace(id * kBucketNs, std::move(state));
                    }
                    partial.totalPoints = partial.bucketStates.size();
                }
                return partials;
            };
            const uint64_t seed = rng();

            // Oracle: left fold in partial order, then order by bucket.
            std::map<uint64_t, AggregationState> folded;
            {
                auto inputs = build(seed);
                for (auto& partial : inputs) {
                    for (auto& [bucket, state] : partial.bucketStates) {
                        auto [it, inserted] = folded.try_emplace(bucket, std::move(state));
                        if (!inserted) {
                            it->second.mergeForMethod(std::move(state), method);
                        }
                    }
                }
            }

            auto inputs = build(seed);
            auto grouped = Aggregator::mergePartialAggregationsGrouped(inputs, method).get();
            ASSERT_EQ(grouped.size(), 1u) << shape.name;
            const auto& points = grouped[0].points;
            ASSERT_EQ(points.size(), folded.size()) << shape.name << " method=" << static_cast<int>(method);

            size_t i = 0;
            for (const auto& [bucket, state] : folded) {
                const auto& point = points[i++];
                ASSERT_EQ(point.timestamp, bucket) << shape.name << " method=" << static_cast<int>(method);
                ASSERT_EQ(point.count, state.count) << shape.name << " bucket=" << bucket;
                const double expected = state.getValue(method);
                ASSERT_EQ(std::bit_cast<uint64_t>(point.value), std::bit_cast<uint64_t>(expected))
                    << shape.name << " method=" << static_cast<int>(method) << " bucket=" << bucket << " got "
                    << point.value << " want " << expected;
            }
        }
    }
}

// On a shard, each series' bucket states are folded into its group's. How the
// group's entry is located — in place, or by merging two ordered maps into a
// new one — is a performance choice; per bucket the operations must be exactly
// those of `group[bucket].mergeForMethod(state)`. Checked against that loop on
// std::map-ordered copies, comparing every method's value bit for bit after
// each series is folded.
TEST_F(AggregatorMergeCorrectnessTest, FoldingSeriesIntoAGroupMatchesThePlainLoop) {
    struct Series {
        size_t buckets;
        uint64_t first;  // first bucket id
        uint64_t stride;
        bool shuffle;
    };
    // Each case folds its series, in order, into one group.
    const std::vector<std::pair<const char*, std::vector<Series>>> cases = {
        {"same buckets every time", {{3000, 0, 1, false}, {3000, 0, 1, false}, {3000, 0, 1, false}}},
        {"interleaved, never shared", {{3000, 0, 3, false}, {3000, 1, 3, false}, {3000, 2, 3, false}}},
        {"disjoint stretches out of time order", {{2000, 4000, 1, false}, {2000, 0, 1, false}, {2000, 2000, 1, false}}},
        {"later series is all new buckets", {{2000, 0, 1, false}, {2000, 2000, 1, false}, {500, 9000, 1, false}}},
        {"partly shared", {{4000, 0, 1, false}, {4000, 2000, 1, false}, {4000, 1000, 2, false}}},
        {"small series into a large group", {{20000, 0, 2, false}, {50, 1, 400, false}, {5000, 1, 8, false}}},
        {"series not in bucket order", {{3000, 0, 2, true}, {3000, 1, 2, false}, {3000, 0, 1, true}}},
        {"group starts empty and tiny", {{1, 5, 1, false}, {3, 0, 4, false}, {2, 5, 1, false}}},
    };
    const std::vector<AggregationMethod> methods = {
        AggregationMethod::AVG,    AggregationMethod::SUM,    AggregationMethod::MIN,         AggregationMethod::MAX,
        AggregationMethod::COUNT,  AggregationMethod::LATEST, AggregationMethod::FIRST,       AggregationMethod::STDDEV,
        AggregationMethod::SPREAD, AggregationMethod::MEDIAN, AggregationMethod::EXACT_MEDIAN};
    constexpr uint64_t kBucketNs = 60'000'000'000ULL;

    std::mt19937_64 rng(77);
    for (const auto& [name, seriesList] : cases) {
        for (AggregationMethod method : methods) {
            const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
            for (bool consume : {false, true}) {
                BucketStateMap group;
                std::map<uint64_t, AggregationState> reference;
                size_t seriesIndex = 0;
                for (const auto& spec : seriesList) {
                    std::vector<uint64_t> ids(spec.buckets);
                    for (size_t i = 0; i < ids.size(); ++i) {
                        ids[i] = spec.first + i * spec.stride;
                    }
                    if (spec.shuffle) {
                        std::shuffle(ids.begin(), ids.end(), rng);
                    }
                    BucketStateMap series;
                    for (uint64_t id : ids) {
                        AggregationState state;
                        state.collectRaw = needsRaw;
                        const size_t n = 1 + rng() % 3;
                        for (size_t k = 0; k < n; ++k) {
                            const double v =
                                static_cast<double>(rng() % 1'000'003) * 0.1 + 1e-7 * static_cast<double>(k);
                            state.addValueForMethod(v, id * kBucketNs + (rng() % 1000) * 1'000'000ULL, method);
                        }
                        // The plain loop, on a copy.
                        if (consume) {
                            AggregationState copy = state;
                            reference[id * kBucketNs].mergeForMethod(std::move(copy), method);
                        } else {
                            reference[id * kBucketNs].mergeForMethod(state, method);
                        }
                        series.emplace(id * kBucketNs, std::move(state));
                    }

                    foldSeriesBucketsIntoGroup(group, series, method, consume);
                    ASSERT_TRUE(series.empty()) << name;

                    ASSERT_EQ(group.size(), reference.size()) << name << " after series " << seriesIndex;
                    for (const auto& [bucket, expected] : reference) {
                        auto it = group.find(bucket);
                        ASSERT_NE(it, group.end()) << name << " bucket " << bucket;
                        const AggregationState& got = it->second;
                        ASSERT_EQ(got.count, expected.count) << name;
                        ASSERT_EQ(std::bit_cast<uint64_t>(got.getValue(method)),
                                  std::bit_cast<uint64_t>(expected.getValue(method)))
                            << name << " method=" << static_cast<int>(method) << " consume=" << consume
                            << " after series " << seriesIndex << " bucket " << bucket;
                        // The accumulators later folds build on, not just this method's value.
                        ASSERT_EQ(std::bit_cast<uint64_t>(got.sum), std::bit_cast<uint64_t>(expected.sum)) << name;
                        ASSERT_EQ(std::bit_cast<uint64_t>(got.sumCompensation),
                                  std::bit_cast<uint64_t>(expected.sumCompensation))
                            << name;
                        ASSERT_EQ(std::bit_cast<uint64_t>(got.mean), std::bit_cast<uint64_t>(expected.mean)) << name;
                        ASSERT_EQ(std::bit_cast<uint64_t>(got.m2), std::bit_cast<uint64_t>(expected.m2)) << name;
                        ASSERT_EQ(got.latestTimestamp, expected.latestTimestamp) << name;
                        ASSERT_EQ(got.firstTimestamp, expected.firstTimestamp) << name;
                        ASSERT_EQ(got.rawValues.size(), expected.rawValues.size()) << name;
                    }
                    ++seriesIndex;
                }
            }
        }
    }
}

// A timestamp with one raw contributor is finalized from closed forms instead
// of a state. They must be exactly what the state reports — for every method,
// including the values the closed forms hand back to the state (NaN, the
// infinities, -0.0, the sentinel timestamp).
TEST_F(AggregatorMergeCorrectnessTest, LoneValueMatchesAStateOfOne) {
    const std::vector<AggregationMethod> methods = {
        AggregationMethod::AVG,    AggregationMethod::MIN,    AggregationMethod::MAX,
        AggregationMethod::SUM,    AggregationMethod::COUNT,  AggregationMethod::LATEST,
        AggregationMethod::FIRST,  AggregationMethod::SPREAD, AggregationMethod::STDDEV,
        AggregationMethod::STDVAR, AggregationMethod::MEDIAN, AggregationMethod::EXACT_MEDIAN};
    std::vector<double> values = {0.0,
                                  -0.0,
                                  1.0,
                                  -1.0,
                                  0.1,
                                  1e300,
                                  -1e300,
                                  5e-324,
                                  -5e-324,
                                  std::numeric_limits<double>::max(),
                                  std::numeric_limits<double>::lowest(),
                                  std::numeric_limits<double>::infinity(),
                                  -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::quiet_NaN()};
    std::mt19937_64 rng(31);
    for (int i = 0; i < 200'000; ++i) {
        values.push_back(std::bit_cast<double>(rng()));
        values.push_back(static_cast<double>(static_cast<int64_t>(rng() % 2'000'000) - 1'000'000) / 1000.0);
    }
    const std::vector<uint64_t> timestamps = {0, 1, 1'700'000'000'000'000'000ULL,
                                              std::numeric_limits<uint64_t>::max() - 1,
                                              std::numeric_limits<uint64_t>::max()};
    for (AggregationMethod method : methods) {
        const bool needsRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
        for (double v : values) {
            for (uint64_t ts : timestamps) {
                AggregationState state;
                state.collectRaw = needsRaw;
                state.addValue(v, ts);
                const double expected = state.getValue(method);
                const auto [value, count] = Aggregator::loneValueResult(v, ts, method);
                ASSERT_EQ(count, state.count) << "method=" << static_cast<int>(method) << " v=" << v << " ts=" << ts;
                ASSERT_EQ(std::bit_cast<uint64_t>(value), std::bit_cast<uint64_t>(expected))
                    << "method=" << static_cast<int>(method) << " v=" << std::hexfloat << v << " ts=" << ts;
            }
        }
    }
}
