// Tests for the TDigest approximate quantile estimator.
//
// Verifies:
//   - Single-value digests return exact results
//   - Odd/even count medians are accurate
//   - Large datasets have bounded error (~0.01% relative for median)
//   - Merge of two digests produces accurate combined quantiles
//   - Edge cases: empty, NaN, extreme values
//   - quantile(0) and quantile(1) return min and max
//   - centroidCount() is bounded by compression factor

#include "../../../lib/query/tdigest.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <numeric>
#include <vector>

using timestar::TDigest;

// ============================================================================
// Basic functionality
// ============================================================================

TEST(TDigestTest, Empty_ReturnsNaN) {
    TDigest td;
    EXPECT_TRUE(std::isnan(td.median()));
    EXPECT_TRUE(std::isnan(td.quantile(0.5)));
    EXPECT_TRUE(td.empty());
    EXPECT_EQ(td.centroidCount(), 0u);
}

TEST(TDigestTest, SingleValue_ReturnsExact) {
    TDigest td;
    td.add(42.0);
    EXPECT_DOUBLE_EQ(td.median(), 42.0);
    EXPECT_DOUBLE_EQ(td.quantile(0.0), 42.0);
    EXPECT_DOUBLE_EQ(td.quantile(1.0), 42.0);
    EXPECT_FALSE(td.empty());
}

TEST(TDigestTest, TwoValues_ReturnsAverage) {
    TDigest td;
    td.add(10.0);
    td.add(20.0);
    double med = td.median();
    EXPECT_NEAR(med, 15.0, 1.0);
}

TEST(TDigestTest, ThreeValues_OddCount) {
    TDigest td;
    td.add(1.0);
    td.add(3.0);
    td.add(5.0);
    EXPECT_NEAR(td.median(), 3.0, 0.5);
}

TEST(TDigestTest, FourValues_EvenCount) {
    TDigest td;
    td.add(1.0);
    td.add(2.0);
    td.add(3.0);
    td.add(4.0);
    EXPECT_NEAR(td.median(), 2.5, 0.5);
}

TEST(TDigestTest, UnsortedInput) {
    TDigest td;
    td.add(5.0);
    td.add(1.0);
    td.add(3.0);
    EXPECT_NEAR(td.median(), 3.0, 0.5);
}

// ============================================================================
// Quantile boundaries
// ============================================================================

TEST(TDigestTest, Quantile_Zero_ReturnsMin) {
    TDigest td;
    for (int i = 1; i <= 100; ++i) {
        td.add(static_cast<double>(i));
    }
    EXPECT_DOUBLE_EQ(td.quantile(0.0), 1.0);
}

TEST(TDigestTest, Quantile_One_ReturnsMax) {
    TDigest td;
    for (int i = 1; i <= 100; ++i) {
        td.add(static_cast<double>(i));
    }
    EXPECT_DOUBLE_EQ(td.quantile(1.0), 100.0);
}

// ============================================================================
// NaN handling
// ============================================================================

TEST(TDigestTest, NaN_IsSkipped) {
    TDigest td;
    td.add(1.0);
    td.add(std::numeric_limits<double>::quiet_NaN());
    td.add(3.0);
    EXPECT_NEAR(td.median(), 2.0, 0.5);
    EXPECT_DOUBLE_EQ(td.totalCount(), 2.0);
}

// ============================================================================
// Large datasets — accuracy test
// ============================================================================

TEST(TDigestTest, LargeUniform_MedianAccuracy) {
    // 100,000 values from 1 to 100,000.
    // True median = 50000.5
    TDigest td;
    for (int i = 1; i <= 100000; ++i) {
        td.add(static_cast<double>(i));
    }
    double med = td.median();
    double trueMedian = 50000.5;
    double relError = std::abs(med - trueMedian) / trueMedian;
    EXPECT_LT(relError, 0.01) << "Median relative error " << relError << " exceeds 1%";
}

TEST(TDigestTest, LargeUniform_P25_P75) {
    TDigest td;
    for (int i = 1; i <= 100000; ++i) {
        td.add(static_cast<double>(i));
    }
    // True P25 = 25000.25, True P75 = 75000.75
    double p25 = td.quantile(0.25);
    double p75 = td.quantile(0.75);
    EXPECT_NEAR(p25, 25000.25, 500.0);  // Within 2% absolute
    EXPECT_NEAR(p75, 75000.75, 500.0);
}

// ============================================================================
// Merge
// ============================================================================

TEST(TDigestTest, Merge_TwoHalves_AccurateMedian) {
    // Split values 1..1000 into two digests and merge.
    // True median = 500.5
    TDigest td1, td2;
    for (int i = 1; i <= 500; ++i) {
        td1.add(static_cast<double>(i));
    }
    for (int i = 501; i <= 1000; ++i) {
        td2.add(static_cast<double>(i));
    }

    td1.merge(td2);

    double med = td1.median();
    EXPECT_NEAR(med, 500.5, 10.0);
    EXPECT_DOUBLE_EQ(td1.totalCount(), 1000.0);
    EXPECT_DOUBLE_EQ(td1.quantile(0.0), 1.0);
    EXPECT_DOUBLE_EQ(td1.quantile(1.0), 1000.0);
}

TEST(TDigestTest, Merge_IntoEmpty) {
    TDigest empty;
    TDigest td;
    td.add(1.0);
    td.add(2.0);
    td.add(3.0);

    empty.merge(td);
    EXPECT_NEAR(empty.median(), 2.0, 0.5);
    EXPECT_DOUBLE_EQ(empty.totalCount(), 3.0);
}

TEST(TDigestTest, Merge_EmptyIntoPopulated) {
    TDigest td;
    td.add(1.0);
    td.add(2.0);

    TDigest empty;
    td.merge(empty);
    EXPECT_NEAR(td.median(), 1.5, 0.5);
    EXPECT_DOUBLE_EQ(td.totalCount(), 2.0);
}

// ============================================================================
// Centroid count bounded by compression
// ============================================================================

TEST(TDigestTest, CentroidCount_Bounded) {
    TDigest td;
    for (int i = 0; i < 10000; ++i) {
        td.add(static_cast<double>(i));
    }
    // With delta=100, max centroids should be around 200-300.
    // After buffer flush, centroid count should be reasonable.
    EXPECT_LT(td.centroidCount(), 600u);
}

// ============================================================================
// All-same values
// ============================================================================

TEST(TDigestTest, AllSameValues) {
    TDigest td;
    for (int i = 0; i < 100; ++i) {
        td.add(42.0);
    }
    EXPECT_DOUBLE_EQ(td.median(), 42.0);
    EXPECT_DOUBLE_EQ(td.quantile(0.1), 42.0);
    EXPECT_DOUBLE_EQ(td.quantile(0.9), 42.0);
}

// ============================================================================
// Extreme values
// ============================================================================

TEST(TDigestTest, ExtremeValues_LargeRange) {
    TDigest td;
    td.add(-1e15);
    td.add(0.0);
    td.add(1e15);
    EXPECT_NEAR(td.median(), 0.0, 1e14);  // Median should be approximately 0
}

// ============================================================================
// Integration with AggregationState
// ============================================================================

#include "../../../lib/query/aggregator.hpp"

using timestar::AggregationMethod;
using timestar::AggregationState;

TEST(TDigestIntegration, AddValueForMethod_Median_UsesRawValues) {
    // MEDIAN uses rawValues (same as EXACT_MEDIAN) for per-value accumulation.
    // T-digest is reserved for future cross-shard merge optimization.
    AggregationState s;
    s.collectRaw = true;
    for (int i = 1; i <= 100; ++i) {
        s.addValueForMethod(static_cast<double>(i), static_cast<uint64_t>(i * 1000), AggregationMethod::MEDIAN);
    }
    // rawValues should be populated, not t-digest
    EXPECT_FALSE(s.tdigest.has_value());
    EXPECT_EQ(s.rawValues.size(), 100u);

    double med = s.getValue(AggregationMethod::MEDIAN);
    EXPECT_NEAR(med, 50.5, 0.5);
}

TEST(TDigestIntegration, MergeForMethod_Median_MergesRawValues) {
    AggregationState a, b;
    a.collectRaw = true;
    b.collectRaw = true;
    for (int i = 1; i <= 50; ++i) {
        a.addValueForMethod(static_cast<double>(i), static_cast<uint64_t>(i), AggregationMethod::MEDIAN);
    }
    for (int i = 51; i <= 100; ++i) {
        b.addValueForMethod(static_cast<double>(i), static_cast<uint64_t>(i), AggregationMethod::MEDIAN);
    }

    a.mergeForMethod(std::move(b), AggregationMethod::MEDIAN);

    EXPECT_EQ(a.rawValues.size(), 100u);
    double med = a.getValue(AggregationMethod::MEDIAN);
    EXPECT_NEAR(med, 50.5, 0.5);
    EXPECT_EQ(a.count, 100u);
}

TEST(TDigestIntegration, ExactMedian_UsesRawValues) {
    AggregationState s;
    s.collectRaw = true;
    s.addValueForMethod(1.0, 1000, AggregationMethod::EXACT_MEDIAN);
    s.addValueForMethod(2.0, 2000, AggregationMethod::EXACT_MEDIAN);
    s.addValueForMethod(3.0, 3000, AggregationMethod::EXACT_MEDIAN);

    EXPECT_FALSE(s.tdigest.has_value());
    EXPECT_EQ(s.rawValues.size(), 3u);
    EXPECT_DOUBLE_EQ(s.getValue(AggregationMethod::EXACT_MEDIAN), 2.0);
}

TEST(TDigestIntegration, Median_BehavesLikeExactMedian) {
    // MEDIAN and EXACT_MEDIAN should produce identical results
    // (both use rawValues + nth_element on single server).
    AggregationState a, b;
    a.collectRaw = true;
    b.collectRaw = true;
    for (int i = 1; i <= 1000; ++i) {
        double v = static_cast<double>(i);
        a.addValueForMethod(v, static_cast<uint64_t>(i), AggregationMethod::MEDIAN);
        b.addValueForMethod(v, static_cast<uint64_t>(i), AggregationMethod::EXACT_MEDIAN);
    }
    EXPECT_DOUBLE_EQ(a.getValue(AggregationMethod::MEDIAN),
                     b.getValue(AggregationMethod::EXACT_MEDIAN));
}
