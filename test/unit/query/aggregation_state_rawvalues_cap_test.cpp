// Tests for the rawValues capacity cap in AggregationState.
//
// AggregationState::rawValues is only required for MEDIAN.  STDDEV and STDVAR
// use Welford's online M2 accumulator which is O(1) memory and does not need
// rawValues at all.
//
// Without a cap, merging many shards with large result sets would grow rawValues
// without bound, exhausting process memory.
//
// The cap (RAW_VALUES_HARD_LIMIT = 1,000,000) prevents this:
//  - addValue() stops appending to rawValues once the limit is reached and sets
//    rawValuesSaturated = true.
//  - merge() propagates the saturation flag and stops appending from the other
//    state's rawValues once the combined total would exceed the limit.
//  - getValue() returns NaN for MEDIAN when rawValuesSaturated is set, signalling
//    that the result would be statistically unreliable.
//  - STDDEV and STDVAR always return correct results via Welford's M2 accumulator,
//    regardless of rawValues saturation.
//  - All other aggregation methods (AVG, MIN, MAX, SUM, COUNT, LATEST, FIRST,
//    SPREAD) are unaffected because they compute from running scalar accumulators
//    that are always updated regardless of saturation.

#include <gtest/gtest.h>
#include "../../../lib/query/aggregator.hpp"
#include <cmath>
#include <vector>

using namespace timestar;

// ---------------------------------------------------------------------------
// Helper: confirm a double is NaN
// ---------------------------------------------------------------------------
static bool isNaN(double v) { return std::isnan(v); }

// ============================================================================
// addValue() — saturation via direct insertion
// ============================================================================

// Verify that rawValues does not grow past RAW_VALUES_HARD_LIMIT when values
// are added one at a time, and that the saturation flag is set.
TEST(AggregationStateCapTest, AddValue_CapEnforced) {
    AggregationState s;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    for (size_t i = 0; i <= limit; ++i) {
        s.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }

    // rawValues must not exceed the hard limit
    EXPECT_EQ(s.rawValues.size(), limit);

    // The saturation flag must be set
    EXPECT_TRUE(s.rawValuesSaturated);

    // count reflects all values including the overflow ones
    EXPECT_EQ(s.count, limit + 1);
}

// Scalar accumulators must remain correct even when rawValues is saturated.
TEST(AggregationStateCapTest, AddValue_ScalarAccumulatorsCorrectAfterCap) {
    AggregationState s;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    // Fill to cap with value 1.0, then add one extra with value 999.0
    for (size_t i = 0; i < limit; ++i) {
        s.addValue(1.0, static_cast<uint64_t>(i + 1));
    }
    s.addValue(999.0, static_cast<uint64_t>(limit + 1));

    EXPECT_TRUE(s.rawValuesSaturated);
    EXPECT_EQ(s.count, limit + 1);

    // min/max/sum/latest/first must account for the overflow value
    EXPECT_DOUBLE_EQ(s.min, 1.0);
    EXPECT_DOUBLE_EQ(s.max, 999.0);
    EXPECT_DOUBLE_EQ(s.sum, static_cast<double>(limit) * 1.0 + 999.0);
    EXPECT_DOUBLE_EQ(s.latest, 999.0);
    EXPECT_DOUBLE_EQ(s.first, 1.0);
}

// AVG, MIN, MAX, SUM, COUNT, LATEST, FIRST, SPREAD still return correct values
// when rawValues is saturated, because they rely on scalar accumulators.
TEST(AggregationStateCapTest, AddValue_NonRawMethodsCorrectAfterCap) {
    AggregationState s;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    // Alternate values: evens = 0.0, odds = 10.0
    for (size_t i = 0; i <= limit; ++i) {
        double v = (i % 2 == 0) ? 0.0 : 10.0;
        s.addValue(v, static_cast<uint64_t>(i));
    }

    EXPECT_TRUE(s.rawValuesSaturated);
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::MIN)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::MAX)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::SUM)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::AVG)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::COUNT)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::LATEST)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::FIRST)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::SPREAD)));

    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MIN), 0.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::MAX), 10.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::SPREAD), 10.0);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::COUNT), static_cast<double>(limit + 1));
}

// When rawValues is saturated, MEDIAN must return NaN.
// STDDEV/STDVAR use Welford's M2 accumulator and remain correct.
TEST(AggregationStateCapTest, AddValue_OrderSensitiveMethodsReturnNaNAfterCap) {
    AggregationState s;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    for (size_t i = 0; i <= limit; ++i) {
        s.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }

    EXPECT_TRUE(s.rawValuesSaturated);
    EXPECT_TRUE(isNaN(s.getValue(AggregationMethod::MEDIAN)));
    // STDDEV/STDVAR use Welford's online algorithm — always correct
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::STDDEV)));
    EXPECT_FALSE(isNaN(s.getValue(AggregationMethod::STDVAR)));
    EXPECT_GT(s.getValue(AggregationMethod::STDDEV), 0.0);
    EXPECT_GT(s.getValue(AggregationMethod::STDVAR), 0.0);
}

// Just under the limit: MEDIAN should still compute correctly.
TEST(AggregationStateCapTest, AddValue_JustUnderLimit_MedianCorrect) {
    AggregationState s;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    // Add exactly limit values: 1.0 through limit.0
    for (size_t i = 1; i <= limit; ++i) {
        s.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }

    EXPECT_FALSE(s.rawValuesSaturated);
    EXPECT_EQ(s.rawValues.size(), limit);

    // Median of 1..limit: midpoint value
    double median = s.getValue(AggregationMethod::MEDIAN);
    EXPECT_FALSE(isNaN(median));
    // For even count n the median is (n/2 + n/2+1) / 2 = (500000 + 500001)/2 = 500000.5
    EXPECT_DOUBLE_EQ(median, static_cast<double>(limit / 2) + 0.5);
}

// ============================================================================
// merge() — saturation propagation
// ============================================================================

// Merging two states where the combined rawValues would exceed the limit:
// rawValues must be capped and rawValuesSaturated set.
TEST(AggregationStateCapTest, Merge_CombinedExceedsLimit_SetsFlag) {
    AggregationState a, b;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;
    const size_t half = limit / 2;

    for (size_t i = 0; i < half + 1; ++i) {
        a.addValue(1.0, static_cast<uint64_t>(i));
    }
    for (size_t i = 0; i < half + 1; ++i) {
        b.addValue(2.0, static_cast<uint64_t>(i + half + 1));
    }

    a.merge(b);

    EXPECT_TRUE(a.rawValuesSaturated);
    EXPECT_LE(a.rawValues.size(), limit);
    EXPECT_EQ(a.count, (half + 1) * 2);

    // MEDIAN must be NaN (incomplete sample)
    EXPECT_TRUE(isNaN(a.getValue(AggregationMethod::MEDIAN)));
    // STDDEV/STDVAR use Welford's M2 — correct even when rawValues saturated
    EXPECT_FALSE(isNaN(a.getValue(AggregationMethod::STDDEV)));
    EXPECT_FALSE(isNaN(a.getValue(AggregationMethod::STDVAR)));
}

// When the source state already has rawValuesSaturated, the destination also
// becomes saturated even if its own rawValues vector is small.
TEST(AggregationStateCapTest, Merge_SourceAlreadySaturated_PropagatesFlag) {
    AggregationState a, b;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    // 'b' is saturated
    for (size_t i = 0; i <= limit; ++i) {
        b.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }
    ASSERT_TRUE(b.rawValuesSaturated);

    // 'a' has only 3 values — small
    a.addValue(1.0, 1);
    a.addValue(2.0, 2);
    a.addValue(3.0, 3);
    ASSERT_FALSE(a.rawValuesSaturated);

    a.merge(b);

    EXPECT_TRUE(a.rawValuesSaturated);
    EXPECT_TRUE(isNaN(a.getValue(AggregationMethod::MEDIAN)));
}

// When neither state is saturated and combined rawValues fit within the limit,
// merge must succeed without saturation.
TEST(AggregationStateCapTest, Merge_BothSmall_NoSaturation) {
    AggregationState a, b;

    for (int i = 0; i < 5; ++i) {
        a.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }
    for (int i = 5; i < 10; ++i) {
        b.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }

    a.merge(b);

    EXPECT_FALSE(a.rawValuesSaturated);
    EXPECT_EQ(a.rawValues.size(), 10u);
    EXPECT_EQ(a.count, 10u);

    // Median of 0..9 = 4.5
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MEDIAN), 4.5);
}

// Merging into a state that is already saturated must not grow rawValues
// further, must keep the flag set, and must still update scalar accumulators.
TEST(AggregationStateCapTest, Merge_DestinationAlreadySaturated_NoFurtherGrowth) {
    AggregationState a, b;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    // Saturate 'a' via addValue
    for (size_t i = 0; i <= limit; ++i) {
        a.addValue(1.0, static_cast<uint64_t>(i));
    }
    ASSERT_TRUE(a.rawValuesSaturated);
    size_t sizeBeforeMerge = a.rawValues.size();

    // 'b' contributes a new min and a few raw values
    b.addValue(-99.0, 0);
    b.addValue(500.0, 1);

    a.merge(b);

    // rawValues must not have grown
    EXPECT_EQ(a.rawValues.size(), sizeBeforeMerge);
    EXPECT_TRUE(a.rawValuesSaturated);

    // But scalar accumulators must reflect the new values
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MIN), -99.0);
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MAX), 500.0);
    EXPECT_EQ(a.count, limit + 1 + 2);
}

// ============================================================================
// Simulate large multi-shard merge
// ============================================================================

// Simulate 8 shards each contributing 200,000 points to the same bucket.
// Total = 1,600,000 which exceeds RAW_VALUES_HARD_LIMIT (1,000,000).
// After merging all shards:
//  - rawValuesSaturated must be true
//  - rawValues.size() must be exactly RAW_VALUES_HARD_LIMIT
//  - count must be 1,600,000 (scalar accumulator is not capped)
//  - SUM/AVG/MIN/MAX/COUNT must be correct
//  - MEDIAN must be NaN (incomplete sample)
//  - STDDEV/STDVAR must be correct (Welford's M2 accumulator)
TEST(AggregationStateCapTest, LargeMultiShardMerge_CorrectBehavior) {
    const size_t shardsCount = 8;
    const size_t pointsPerShard = 200'000;
    const size_t totalPoints = shardsCount * pointsPerShard;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;

    // Build one partial state per shard, all with value = 1.0
    std::vector<AggregationState> shards(shardsCount);
    for (size_t s = 0; s < shardsCount; ++s) {
        for (size_t i = 0; i < pointsPerShard; ++i) {
            shards[s].addValue(1.0, static_cast<uint64_t>(s * pointsPerShard + i));
        }
        // Each individual shard is below the limit
        ASSERT_FALSE(shards[s].rawValuesSaturated)
            << "Shard " << s << " pre-saturated before merge";
    }

    // Merge all shards into shard 0
    for (size_t s = 1; s < shardsCount; ++s) {
        shards[0].merge(shards[s]);
    }

    AggregationState& merged = shards[0];

    // count is the sum of all points (not capped)
    EXPECT_EQ(merged.count, totalPoints);

    // rawValues must be capped at the hard limit
    EXPECT_EQ(merged.rawValues.size(), limit);
    EXPECT_TRUE(merged.rawValuesSaturated);

    // Scalar methods must be correct
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::COUNT),
                     static_cast<double>(totalPoints));
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::SUM),
                     static_cast<double>(totalPoints));
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::AVG), 1.0);
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::MIN), 1.0);
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::MAX), 1.0);
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::SPREAD), 0.0);

    // MEDIAN must return NaN (incomplete sample)
    EXPECT_TRUE(isNaN(merged.getValue(AggregationMethod::MEDIAN)));
    // STDDEV/STDVAR use Welford's M2 — correct even when saturated
    // All values are 1.0, so variance = 0
    EXPECT_FALSE(isNaN(merged.getValue(AggregationMethod::STDDEV)));
    EXPECT_FALSE(isNaN(merged.getValue(AggregationMethod::STDVAR)));
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::STDDEV), 0.0);
    EXPECT_DOUBLE_EQ(merged.getValue(AggregationMethod::STDVAR), 0.0);
}

// ============================================================================
// Boundary: exactly RAW_VALUES_HARD_LIMIT across two merged states
// ============================================================================

// When two states each have exactly limit/2 values and limit is even,
// the merge fills rawValues to exactly limit with no saturation.
TEST(AggregationStateCapTest, Merge_ExactlyAtLimit_NoSaturation) {
    AggregationState a, b;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;
    const size_t half = limit / 2;

    for (size_t i = 0; i < half; ++i) {
        a.addValue(static_cast<double>(i), static_cast<uint64_t>(i));
    }
    for (size_t i = 0; i < half; ++i) {
        b.addValue(static_cast<double>(half + i), static_cast<uint64_t>(half + i));
    }

    ASSERT_FALSE(a.rawValuesSaturated);
    ASSERT_FALSE(b.rawValuesSaturated);

    a.merge(b);

    // Combined should be exactly at the limit — no saturation
    EXPECT_EQ(a.rawValues.size(), limit);
    EXPECT_FALSE(a.rawValuesSaturated);
    EXPECT_EQ(a.count, limit);

    // MEDIAN should be computable: values 0..limit-1
    double median = a.getValue(AggregationMethod::MEDIAN);
    EXPECT_FALSE(isNaN(median));
}

// One value over the boundary: merge of (limit/2 + 1) + (limit/2) exceeds limit.
TEST(AggregationStateCapTest, Merge_OnePastLimit_Saturated) {
    AggregationState a, b;
    const size_t limit = AggregationState::RAW_VALUES_HARD_LIMIT;
    const size_t half = limit / 2;

    for (size_t i = 0; i < half + 1; ++i) {
        a.addValue(1.0, static_cast<uint64_t>(i));
    }
    for (size_t i = 0; i < half; ++i) {
        b.addValue(2.0, static_cast<uint64_t>(half + 1 + i));
    }

    a.merge(b);

    EXPECT_EQ(a.rawValues.size(), limit);
    EXPECT_TRUE(a.rawValuesSaturated);
    EXPECT_TRUE(isNaN(a.getValue(AggregationMethod::MEDIAN)));
}
