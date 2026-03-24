// Tests for the streaming group-by aggregation path.
//
// The core coroutine (streamingGroupByAggregation) is a static function inside
// http_query_handler.cpp and cannot be called directly from tests.  Instead,
// we test the public building blocks it relies on:
//
//  1. isStreamableMethod()        — aggregator.hpp
//  2. canStreamGroupBy semantics  — same predicate logic
//  3. AggregationState mergeForMethod correctness for all streamable methods
//  4. Bucketed group accumulation via AggregationState maps
//  5. Non-bucketed collapsed-state accumulation

#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/utils/group_key.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

using namespace timestar;

// ============================================================================
// isStreamableMethod — shared free function in aggregator.hpp
// ============================================================================

TEST(StreamingGroupBy, IsStreamableMethod_AVG) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::AVG));
}

TEST(StreamingGroupBy, IsStreamableMethod_MIN) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::MIN));
}

TEST(StreamingGroupBy, IsStreamableMethod_MAX) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::MAX));
}

TEST(StreamingGroupBy, IsStreamableMethod_SUM) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::SUM));
}

TEST(StreamingGroupBy, IsStreamableMethod_COUNT) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::COUNT));
}

TEST(StreamingGroupBy, IsStreamableMethod_SPREAD) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::SPREAD));
}

TEST(StreamingGroupBy, IsStreamableMethod_STDDEV) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::STDDEV));
}

TEST(StreamingGroupBy, IsStreamableMethod_STDVAR) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::STDVAR));
}

TEST(StreamingGroupBy, IsStreamableMethod_LATEST) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::LATEST));
}

TEST(StreamingGroupBy, IsStreamableMethod_FIRST) {
    EXPECT_TRUE(isStreamableMethod(AggregationMethod::FIRST));
}

TEST(StreamingGroupBy, IsStreamableMethod_MEDIAN) {
    EXPECT_FALSE(isStreamableMethod(AggregationMethod::MEDIAN));
}

// ============================================================================
// canStreamAggregation — predicate logic
// ============================================================================

// Reproduce the canStreamAggregation logic inline since the static function
// is not exported from http_query_handler.cpp.
static bool canStreamAggregationLocal(AggregationMethod method,
                                       const std::vector<std::string>& groupByTags,
                                       uint64_t aggregationInterval) {
    if (!isStreamableMethod(method)) return false;
    return !groupByTags.empty() || aggregationInterval > 0;
}

TEST(StreamingGroupBy, CanStreamAggregation_AVG_WithGroupBy) {
    EXPECT_TRUE(canStreamAggregationLocal(AggregationMethod::AVG, {"host"}, 0));
}

TEST(StreamingGroupBy, CanStreamAggregation_MEDIAN_WithGroupBy) {
    EXPECT_FALSE(canStreamAggregationLocal(AggregationMethod::MEDIAN, {"host"}, 0));
}

TEST(StreamingGroupBy, CanStreamAggregation_AVG_NoGroupByNoBucket) {
    // No group-by AND no bucketing → false (raw points needed by derived queries)
    EXPECT_FALSE(canStreamAggregationLocal(AggregationMethod::AVG, {}, 0));
}

TEST(StreamingGroupBy, CanStreamAggregation_AVG_NoBucketWithGroupBy) {
    EXPECT_TRUE(canStreamAggregationLocal(AggregationMethod::AVG, {"host"}, 0));
}

TEST(StreamingGroupBy, CanStreamAggregation_AVG_BucketNoGroupBy) {
    // Bucketed without group-by → true (fold into per-bucket states)
    EXPECT_TRUE(canStreamAggregationLocal(AggregationMethod::AVG, {}, 3600000000000ULL));
}

TEST(StreamingGroupBy, CanStreamAggregation_LATEST_WithGroupBy) {
    EXPECT_TRUE(canStreamAggregationLocal(AggregationMethod::LATEST, {"region"}, 0));
}

// ============================================================================
// AggregationState mergeForMethod — correctness for all streamable methods
// ============================================================================
// Simulates what streamingGroupByAggregation does: fold per-series results
// into a shared AggregationState using mergeForMethod.

TEST(StreamingGroupBy, MergeForMethod_AVG) {
    // Series A: sum=30, count=3, mean=10
    AggregationState a;
    a.addValueForMethod(10.0, 100, AggregationMethod::AVG);
    a.addValueForMethod(10.0, 200, AggregationMethod::AVG);
    a.addValueForMethod(10.0, 300, AggregationMethod::AVG);

    // Series B: sum=60, count=3, mean=20
    AggregationState b;
    b.addValueForMethod(20.0, 400, AggregationMethod::AVG);
    b.addValueForMethod(20.0, 500, AggregationMethod::AVG);
    b.addValueForMethod(20.0, 600, AggregationMethod::AVG);

    a.mergeForMethod(b, AggregationMethod::AVG);
    // sum=90, count=6, avg=15
    EXPECT_EQ(a.count, 6u);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::AVG), 15.0);
}

TEST(StreamingGroupBy, MergeForMethod_SUM) {
    AggregationState a;
    a.addValueForMethod(10.0, 100, AggregationMethod::SUM);
    a.addValueForMethod(20.0, 200, AggregationMethod::SUM);

    AggregationState b;
    b.addValueForMethod(30.0, 300, AggregationMethod::SUM);

    a.mergeForMethod(b, AggregationMethod::SUM);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::SUM), 60.0);
    EXPECT_EQ(a.count, 3u);
}

TEST(StreamingGroupBy, MergeForMethod_MIN) {
    AggregationState a;
    a.addValueForMethod(5.0, 100, AggregationMethod::MIN);
    a.addValueForMethod(10.0, 200, AggregationMethod::MIN);

    AggregationState b;
    b.addValueForMethod(3.0, 300, AggregationMethod::MIN);
    b.addValueForMethod(8.0, 400, AggregationMethod::MIN);

    a.mergeForMethod(b, AggregationMethod::MIN);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MIN), 3.0);
    EXPECT_EQ(a.count, 4u);
}

TEST(StreamingGroupBy, MergeForMethod_MAX) {
    AggregationState a;
    a.addValueForMethod(5.0, 100, AggregationMethod::MAX);
    a.addValueForMethod(10.0, 200, AggregationMethod::MAX);

    AggregationState b;
    b.addValueForMethod(3.0, 300, AggregationMethod::MAX);
    b.addValueForMethod(12.0, 400, AggregationMethod::MAX);

    a.mergeForMethod(b, AggregationMethod::MAX);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MAX), 12.0);
}

TEST(StreamingGroupBy, MergeForMethod_COUNT) {
    AggregationState a;
    a.addValueForMethod(1.0, 100, AggregationMethod::COUNT);
    a.addValueForMethod(2.0, 200, AggregationMethod::COUNT);

    AggregationState b;
    b.addValueForMethod(3.0, 300, AggregationMethod::COUNT);

    a.mergeForMethod(b, AggregationMethod::COUNT);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::COUNT), 3.0);
}

TEST(StreamingGroupBy, MergeForMethod_SPREAD) {
    AggregationState a;
    a.addValueForMethod(5.0, 100, AggregationMethod::SPREAD);
    a.addValueForMethod(15.0, 200, AggregationMethod::SPREAD);

    AggregationState b;
    b.addValueForMethod(2.0, 300, AggregationMethod::SPREAD);
    b.addValueForMethod(20.0, 400, AggregationMethod::SPREAD);

    a.mergeForMethod(b, AggregationMethod::SPREAD);
    // min=2, max=20, spread=18
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::SPREAD), 18.0);
}

TEST(StreamingGroupBy, MergeForMethod_LATEST) {
    AggregationState a;
    a.addValueForMethod(10.0, 100, AggregationMethod::LATEST);
    a.addValueForMethod(20.0, 200, AggregationMethod::LATEST);

    AggregationState b;
    b.addValueForMethod(30.0, 300, AggregationMethod::LATEST);

    a.mergeForMethod(b, AggregationMethod::LATEST);
    // Latest is ts=300, value=30
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::LATEST), 30.0);
    EXPECT_EQ(a.latestTimestamp, 300u);
}

TEST(StreamingGroupBy, MergeForMethod_FIRST) {
    AggregationState a;
    a.addValueForMethod(10.0, 200, AggregationMethod::FIRST);
    a.addValueForMethod(20.0, 300, AggregationMethod::FIRST);

    AggregationState b;
    b.addValueForMethod(5.0, 100, AggregationMethod::FIRST);

    a.mergeForMethod(b, AggregationMethod::FIRST);
    // First is ts=100, value=5
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::FIRST), 5.0);
    EXPECT_EQ(a.firstTimestamp, 100u);
}

TEST(StreamingGroupBy, MergeForMethod_STDDEV) {
    // Values: 10, 20, 30, 40, 50
    // Mean=30, Variance=200, StdDev=sqrt(200)=14.142...
    AggregationState a;
    a.addValueForMethod(10.0, 100, AggregationMethod::STDDEV);
    a.addValueForMethod(20.0, 200, AggregationMethod::STDDEV);
    a.addValueForMethod(30.0, 300, AggregationMethod::STDDEV);

    AggregationState b;
    b.addValueForMethod(40.0, 400, AggregationMethod::STDDEV);
    b.addValueForMethod(50.0, 500, AggregationMethod::STDDEV);

    a.mergeForMethod(b, AggregationMethod::STDDEV);
    // Population stddev of {10,20,30,40,50} = sqrt(200) = 14.142...
    double expected = std::sqrt(200.0);
    EXPECT_NEAR(a.getValue(AggregationMethod::STDDEV), expected, 0.001);
}

TEST(StreamingGroupBy, MergeForMethod_STDVAR) {
    AggregationState a;
    a.addValueForMethod(10.0, 100, AggregationMethod::STDVAR);
    a.addValueForMethod(20.0, 200, AggregationMethod::STDVAR);
    a.addValueForMethod(30.0, 300, AggregationMethod::STDVAR);

    AggregationState b;
    b.addValueForMethod(40.0, 400, AggregationMethod::STDVAR);
    b.addValueForMethod(50.0, 500, AggregationMethod::STDVAR);

    a.mergeForMethod(b, AggregationMethod::STDVAR);
    // Population variance of {10,20,30,40,50} = 200
    EXPECT_NEAR(a.getValue(AggregationMethod::STDVAR), 200.0, 0.001);
}

// ============================================================================
// Multi-group correctness
// ============================================================================
// Simulates the streaming group-by accumulation pattern: different series
// (identified by different tags) produce separate group accumulators.

TEST(StreamingGroupBy, MultiGroup_SeparateAccumulators) {
    // Simulate two groups: host=A and host=B
    AggregationState groupA;
    AggregationState groupB;

    // Series 1 belongs to group A
    groupA.addValueForMethod(10.0, 100, AggregationMethod::AVG);
    groupA.addValueForMethod(20.0, 200, AggregationMethod::AVG);

    // Series 2 also belongs to group A (same host=A, different field or something)
    {
        AggregationState series2;
        series2.addValueForMethod(30.0, 300, AggregationMethod::AVG);
        groupA.mergeForMethod(series2, AggregationMethod::AVG);
    }

    // Series 3 belongs to group B
    groupB.addValueForMethod(100.0, 400, AggregationMethod::AVG);
    groupB.addValueForMethod(200.0, 500, AggregationMethod::AVG);

    // Group A: values {10, 20, 30}, avg=20
    EXPECT_DOUBLE_EQ(groupA.getValue(AggregationMethod::AVG), 20.0);
    EXPECT_EQ(groupA.count, 3u);

    // Group B: values {100, 200}, avg=150
    EXPECT_DOUBLE_EQ(groupB.getValue(AggregationMethod::AVG), 150.0);
    EXPECT_EQ(groupB.count, 2u);
}

TEST(StreamingGroupBy, MultiGroup_DifferentTags_ProduceSeparateGroupKeys) {
    std::map<std::string, std::string> tagsA = {{"host", "server-A"}, {"dc", "us-west"}};
    std::map<std::string, std::string> tagsB = {{"host", "server-B"}, {"dc", "us-west"}};
    std::vector<std::string> groupByTags = {"host"};

    auto gkA = buildGroupKeyDirect("cpu", "usage", tagsA, groupByTags);
    auto gkB = buildGroupKeyDirect("cpu", "usage", tagsB, groupByTags);

    // Different host tags produce different group keys
    EXPECT_NE(gkA.key, gkB.key);
    EXPECT_NE(gkA.hash, gkB.hash);

    // Same host tag produces same group key
    auto gkA2 = buildGroupKeyDirect("cpu", "usage", tagsA, groupByTags);
    EXPECT_EQ(gkA.key, gkA2.key);
    EXPECT_EQ(gkA.hash, gkA2.hash);
}

TEST(StreamingGroupBy, MultiGroup_SameHost_SameGroupKey) {
    // Different non-group-by tags should still produce the same group key
    std::map<std::string, std::string> tags1 = {{"host", "server-A"}, {"dc", "us-west"}};
    std::map<std::string, std::string> tags2 = {{"host", "server-A"}, {"dc", "us-east"}};
    std::vector<std::string> groupByTags = {"host"};

    auto gk1 = buildGroupKeyDirect("cpu", "usage", tags1, groupByTags);
    auto gk2 = buildGroupKeyDirect("cpu", "usage", tags2, groupByTags);

    EXPECT_EQ(gk1.key, gk2.key);
}

// ============================================================================
// Bucketed group-by accumulation
// ============================================================================

TEST(StreamingGroupBy, BucketedAccumulation_CorrectBucketAssignment) {
    // Simulate what streamingGroupByAggregation does for bucketed queries:
    // fold values into bucketStates[bucket].
    uint64_t interval = 1000;
    std::unordered_map<uint64_t, AggregationState> bucketStates;

    // Points at timestamps 100, 500, 1100, 1500, 2100
    // Bucket 0: {100, 500}, Bucket 1000: {1100, 1500}, Bucket 2000: {2100}
    auto foldPoint = [&](uint64_t ts, double val) {
        uint64_t bucket = (ts / interval) * interval;
        bucketStates[bucket].addValueForMethod(val, ts, AggregationMethod::SUM);
    };

    foldPoint(100, 10.0);
    foldPoint(500, 20.0);
    foldPoint(1100, 30.0);
    foldPoint(1500, 40.0);
    foldPoint(2100, 50.0);

    ASSERT_EQ(bucketStates.size(), 3u);
    EXPECT_DOUBLE_EQ(bucketStates[0].getValue(AggregationMethod::SUM), 30.0);     // 10 + 20
    EXPECT_DOUBLE_EQ(bucketStates[1000].getValue(AggregationMethod::SUM), 70.0);  // 30 + 40
    EXPECT_DOUBLE_EQ(bucketStates[2000].getValue(AggregationMethod::SUM), 50.0);
}

TEST(StreamingGroupBy, BucketedAccumulation_MergeBucketStatesFromTwoSeries) {
    uint64_t interval = 1000;

    // Series 1 produces bucket states
    std::unordered_map<uint64_t, AggregationState> series1Buckets;
    series1Buckets[0].addValueForMethod(10.0, 100, AggregationMethod::AVG);
    series1Buckets[0].addValueForMethod(20.0, 200, AggregationMethod::AVG);
    series1Buckets[1000].addValueForMethod(30.0, 1100, AggregationMethod::AVG);

    // Series 2 produces bucket states (overlap on bucket 0)
    std::unordered_map<uint64_t, AggregationState> series2Buckets;
    series2Buckets[0].addValueForMethod(40.0, 300, AggregationMethod::AVG);
    series2Buckets[2000].addValueForMethod(50.0, 2100, AggregationMethod::AVG);

    // Merge series 2 into series 1 (same as streaming group-by does)
    for (auto& [ts, state] : series2Buckets) {
        series1Buckets[ts].mergeForMethod(state, AggregationMethod::AVG);
    }

    ASSERT_EQ(series1Buckets.size(), 3u);
    // Bucket 0: {10, 20, 40}, avg=(10+20+40)/3 = 23.333...
    EXPECT_NEAR(series1Buckets[0].getValue(AggregationMethod::AVG), 23.333, 0.01);
    EXPECT_EQ(series1Buckets[0].count, 3u);
    // Bucket 1000: {30}, avg=30
    EXPECT_DOUBLE_EQ(series1Buckets[1000].getValue(AggregationMethod::AVG), 30.0);
    // Bucket 2000: {50}, avg=50
    EXPECT_DOUBLE_EQ(series1Buckets[2000].getValue(AggregationMethod::AVG), 50.0);
}

// ============================================================================
// Non-bucketed group-by (collapsed state)
// ============================================================================

TEST(StreamingGroupBy, NonBucketed_CollapsedState_MergeMultipleSeries) {
    AggregationState group;

    // Series 1: {10, 20}
    group.addValueForMethod(10.0, 100, AggregationMethod::SUM);
    group.addValueForMethod(20.0, 200, AggregationMethod::SUM);

    // Series 2 pushdown returns a collapsed state
    AggregationState series2State;
    series2State.addValueForMethod(30.0, 300, AggregationMethod::SUM);
    series2State.addValueForMethod(40.0, 400, AggregationMethod::SUM);

    group.mergeForMethod(series2State, AggregationMethod::SUM);

    // Total sum = 10 + 20 + 30 + 40 = 100
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::SUM), 100.0);
    EXPECT_EQ(group.count, 4u);
}

TEST(StreamingGroupBy, NonBucketed_CollapsedState_LATEST_KeepsNewest) {
    AggregationState group;

    // Series 1: latest is ts=200, value=20
    group.addValueForMethod(10.0, 100, AggregationMethod::LATEST);
    group.addValueForMethod(20.0, 200, AggregationMethod::LATEST);

    // Series 2 pushdown returns collapsed state with ts=500, value=50
    AggregationState series2State;
    series2State.addValueForMethod(50.0, 500, AggregationMethod::LATEST);

    group.mergeForMethod(series2State, AggregationMethod::LATEST);

    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::LATEST), 50.0);
    EXPECT_EQ(group.latestTimestamp, 500u);
}

TEST(StreamingGroupBy, NonBucketed_CollapsedState_FIRST_KeepsOldest) {
    AggregationState group;

    // Series 1: first is ts=200, value=20
    group.addValueForMethod(20.0, 200, AggregationMethod::FIRST);
    group.addValueForMethod(30.0, 300, AggregationMethod::FIRST);

    // Series 2 pushdown returns collapsed state with ts=100, value=10
    AggregationState series2State;
    series2State.addValueForMethod(10.0, 100, AggregationMethod::FIRST);

    group.mergeForMethod(series2State, AggregationMethod::FIRST);

    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::FIRST), 10.0);
    EXPECT_EQ(group.firstTimestamp, 100u);
}

// ============================================================================
// PushdownResult → group accumulator pattern
// ============================================================================
// Verifies the pattern used in streamingGroupByAggregation for merging
// PushdownResult into group accumulators.

TEST(StreamingGroupBy, MergePushdownResult_BucketStates) {
    std::unordered_map<uint64_t, AggregationState> groupBuckets;

    // Simulate a PushdownResult with bucket states
    std::unordered_map<uint64_t, AggregationState> pushdownBuckets;
    pushdownBuckets[0].addValueForMethod(10.0, 100, AggregationMethod::SUM);
    pushdownBuckets[1000].addValueForMethod(20.0, 1100, AggregationMethod::SUM);

    // Merge pushdown buckets into group (same pattern as streamingGroupByAggregation)
    for (auto& [bucketTs, state] : pushdownBuckets) {
        groupBuckets[bucketTs].mergeForMethod(state, AggregationMethod::SUM);
    }

    // Second pushdown from another series
    std::unordered_map<uint64_t, AggregationState> pushdownBuckets2;
    pushdownBuckets2[0].addValueForMethod(30.0, 200, AggregationMethod::SUM);
    pushdownBuckets2[2000].addValueForMethod(40.0, 2100, AggregationMethod::SUM);

    for (auto& [bucketTs, state] : pushdownBuckets2) {
        groupBuckets[bucketTs].mergeForMethod(state, AggregationMethod::SUM);
    }

    ASSERT_EQ(groupBuckets.size(), 3u);
    EXPECT_DOUBLE_EQ(groupBuckets[0].getValue(AggregationMethod::SUM), 40.0);     // 10 + 30
    EXPECT_DOUBLE_EQ(groupBuckets[1000].getValue(AggregationMethod::SUM), 20.0);
    EXPECT_DOUBLE_EQ(groupBuckets[2000].getValue(AggregationMethod::SUM), 40.0);
}

TEST(StreamingGroupBy, MergePushdownResult_CollapsedState) {
    AggregationState group;

    // Simulate a PushdownResult with aggregatedState
    AggregationState pushdownState;
    pushdownState.addValueForMethod(10.0, 100, AggregationMethod::AVG);
    pushdownState.addValueForMethod(20.0, 200, AggregationMethod::AVG);

    group.mergeForMethod(pushdownState, AggregationMethod::AVG);

    // Another pushdown result
    AggregationState pushdownState2;
    pushdownState2.addValueForMethod(30.0, 300, AggregationMethod::AVG);

    group.mergeForMethod(pushdownState2, AggregationMethod::AVG);

    // avg = (10 + 20 + 30) / 3 = 20
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::AVG), 20.0);
    EXPECT_EQ(group.count, 3u);
}

TEST(StreamingGroupBy, MergePushdownResult_SortedValues) {
    // When pushdown returns raw sorted values (not aggregatedState or bucketStates),
    // the streaming path folds them one-by-one via addValueForMethod.
    AggregationState group;

    std::vector<uint64_t> timestamps = {100, 200, 300};
    std::vector<double> values = {10.0, 20.0, 30.0};

    for (size_t i = 0; i < timestamps.size(); ++i) {
        group.addValueForMethod(values[i], timestamps[i], AggregationMethod::MAX);
    }

    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::MAX), 30.0);
    EXPECT_EQ(group.count, 3u);
}

// ============================================================================
// PartialAggregationResult construction from group accumulators
// ============================================================================

TEST(StreamingGroupBy, PartialResult_Bucketed_HasBucketStates) {
    std::unordered_map<uint64_t, AggregationState> groupBuckets;
    groupBuckets[0].addValueForMethod(10.0, 100, AggregationMethod::SUM);
    groupBuckets[1000].addValueForMethod(20.0, 1100, AggregationMethod::SUM);

    PartialAggregationResult partial;
    partial.measurement = "cpu";
    partial.fieldName = "usage";
    partial.groupKey = "cpu\0host=A\0usage";
    partial.totalPoints = 2;
    partial.bucketStates = std::move(groupBuckets);

    ASSERT_EQ(partial.bucketStates.size(), 2u);
    EXPECT_DOUBLE_EQ(partial.bucketStates[0].getValue(AggregationMethod::SUM), 10.0);
    EXPECT_DOUBLE_EQ(partial.bucketStates[1000].getValue(AggregationMethod::SUM), 20.0);
    EXPECT_FALSE(partial.collapsedState.has_value());
}

TEST(StreamingGroupBy, PartialResult_NonBucketed_HasCollapsedState) {
    AggregationState collapsed;
    collapsed.addValueForMethod(10.0, 100, AggregationMethod::AVG);
    collapsed.addValueForMethod(20.0, 200, AggregationMethod::AVG);

    PartialAggregationResult partial;
    partial.measurement = "cpu";
    partial.fieldName = "usage";
    partial.groupKey = "cpu\0host=A\0usage";
    partial.totalPoints = 2;
    partial.collapsedState = std::move(collapsed);

    EXPECT_TRUE(partial.collapsedState.has_value());
    EXPECT_EQ(partial.collapsedState->count, 2u);
    EXPECT_DOUBLE_EQ(partial.collapsedState->getValue(AggregationMethod::AVG), 15.0);
    EXPECT_TRUE(partial.bucketStates.empty());
}

// ============================================================================
// Edge cases
// ============================================================================

TEST(StreamingGroupBy, EmptyGroupProducesNoPartial) {
    AggregationState group;
    // No values added — count is 0
    EXPECT_EQ(group.count, 0u);
    EXPECT_TRUE(std::isnan(group.getValue(AggregationMethod::AVG)));
}

TEST(StreamingGroupBy, SinglePointGroup_AVG) {
    AggregationState group;
    group.addValueForMethod(42.0, 1000, AggregationMethod::AVG);
    EXPECT_EQ(group.count, 1u);
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::AVG), 42.0);
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::SUM), 42.0);
}

TEST(StreamingGroupBy, SinglePointGroup_MIN) {
    AggregationState group;
    group.addValueForMethod(42.0, 1000, AggregationMethod::MIN);
    EXPECT_EQ(group.count, 1u);
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::MIN), 42.0);
}

TEST(StreamingGroupBy, SinglePointGroup_MAX) {
    AggregationState group;
    group.addValueForMethod(42.0, 1000, AggregationMethod::MAX);
    EXPECT_EQ(group.count, 1u);
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::MAX), 42.0);
}

TEST(StreamingGroupBy, SinglePointGroup_COUNT) {
    AggregationState group;
    group.addValueForMethod(42.0, 1000, AggregationMethod::COUNT);
    EXPECT_EQ(group.count, 1u);
    EXPECT_DOUBLE_EQ(group.getValue(AggregationMethod::COUNT), 1.0);
}

TEST(StreamingGroupBy, WelfordMerge_LargeValueRange) {
    // Test that Welford merge handles values with large range without
    // catastrophic cancellation.
    AggregationState a;
    a.addValueForMethod(1e9, 100, AggregationMethod::STDDEV);
    a.addValueForMethod(1e9 + 1.0, 200, AggregationMethod::STDDEV);

    AggregationState b;
    b.addValueForMethod(1e9 + 2.0, 300, AggregationMethod::STDDEV);
    b.addValueForMethod(1e9 + 3.0, 400, AggregationMethod::STDDEV);

    a.mergeForMethod(b, AggregationMethod::STDDEV);

    // Values: 1e9, 1e9+1, 1e9+2, 1e9+3
    // Mean = 1e9 + 1.5
    // Variance = ((0-1.5)^2 + (1-1.5)^2 + (2-1.5)^2 + (3-1.5)^2) / 4
    //          = (2.25 + 0.25 + 0.25 + 2.25) / 4 = 5.0 / 4 = 1.25
    // StdDev = sqrt(1.25) = 1.118...
    EXPECT_NEAR(a.getValue(AggregationMethod::STDVAR), 1.25, 0.01);
    EXPECT_NEAR(a.getValue(AggregationMethod::STDDEV), std::sqrt(1.25), 0.01);
}

TEST(StreamingGroupBy, MultipleGroupBy_TagsSorted) {
    // Verify that group keys sort correctly with multiple group-by tags
    std::map<std::string, std::string> tags = {
        {"host", "server-A"}, {"dc", "us-west"}, {"rack", "r1"}};
    std::vector<std::string> groupByTags = {"dc", "host"};

    auto gk = buildGroupKeyDirect("cpu", "usage", tags, groupByTags);
    EXPECT_EQ(gk.tags.size(), 2u);
    EXPECT_EQ(gk.tags["dc"], "us-west");
    EXPECT_EQ(gk.tags["host"], "server-A");
}
