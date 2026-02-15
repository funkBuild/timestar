#include <gtest/gtest.h>
#include "../../../lib/query/simd_aggregator.hpp"
#include <vector>
#include <cmath>
#include <random>
#include <limits>
#include <numeric>
#include <algorithm>

using tsdb::simd::SimdAggregator;
namespace scalar = tsdb::simd::scalar;

// ============================================================================
// SimdAggregator Correctness Tests
// ============================================================================

class SimdAggregatorCorrectnessTest : public ::testing::Test {
protected:
    bool avx2Available_ = false;
    bool avx512Available_ = false;

    void SetUp() override {
        avx2Available_ = SimdAggregator::isAvx2Available();
        avx512Available_ = SimdAggregator::isAvx512Available();
    }
};

// ==================== Sum ====================

TEST_F(SimdAggregatorCorrectnessTest, Sum_Empty) {
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateSum(nullptr, 0), 0.0);
    EXPECT_DOUBLE_EQ(scalar::calculateSum(nullptr, 0), 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_SingleValue) {
    double val = 42.5;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateSum(&val, 1), 42.5);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_SmallArray) {
    // Fewer elements than any SIMD width
    std::vector<double> values = {1.0, 2.0, 3.0};
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    double scl = scalar::calculateSum(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
    EXPECT_DOUBLE_EQ(simd, 6.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_ExactAVX2Width) {
    // Exactly 4 elements
    std::vector<double> values = {1.5, 2.5, 3.5, 4.5};
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    double scl = scalar::calculateSum(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_ExactAVX512Width) {
    // Exactly 8 elements
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    double scl = scalar::calculateSum(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
    EXPECT_DOUBLE_EQ(simd, 36.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_LargeArray) {
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

    std::vector<double> values(10000);
    for (auto& v : values) v = dist(rng);

    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    double scl = scalar::calculateSum(values.data(), values.size());

    // Allow small relative error due to different summation order
    double tolerance = std::abs(scl) * 1e-10 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance)
        << "Sum differs for large array (SIMD=" << simd << ", scalar=" << scl << ")";
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_AllZeros) {
    std::vector<double> values(100, 0.0);
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_AllOnes) {
    std::vector<double> values(1000, 1.0);
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, 1000.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_NegativeValues) {
    std::vector<double> values = {-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -7.0, -8.0, -9.0, -10.0};
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    double scl = scalar::calculateSum(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
    EXPECT_DOUBLE_EQ(simd, -55.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Sum_MixedSignCancellation) {
    // Values that should cancel out
    std::vector<double> values;
    for (int i = 0; i < 100; i++) {
        values.push_back(static_cast<double>(i + 1));
        values.push_back(-static_cast<double>(i + 1));
    }
    double simd = SimdAggregator::calculateSum(values.data(), values.size());
    EXPECT_NEAR(simd, 0.0, 1e-10);
}

// Test various sizes to exercise SIMD boundary conditions
TEST_F(SimdAggregatorCorrectnessTest, Sum_VariousSizes) {
    for (size_t n = 1; n <= 50; n++) {
        std::vector<double> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = static_cast<double>(i + 1);
        }
        double simd = SimdAggregator::calculateSum(values.data(), n);
        double expected = static_cast<double>(n * (n + 1)) / 2.0;
        EXPECT_DOUBLE_EQ(simd, expected)
            << "Sum mismatch for n=" << n;
    }
}

// ==================== Avg ====================

TEST_F(SimdAggregatorCorrectnessTest, Avg_Empty) {
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateAvg(nullptr, 0), 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Avg_SingleValue) {
    double val = 7.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateAvg(&val, 1), 7.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Avg_MatchesScalar) {
    std::mt19937_64 rng(99);
    std::uniform_real_distribution<double> dist(-500.0, 500.0);

    std::vector<double> values(1000);
    for (auto& v : values) v = dist(rng);

    double simd = SimdAggregator::calculateAvg(values.data(), values.size());
    double scl = scalar::calculateAvg(values.data(), values.size());

    double tolerance = std::abs(scl) * 1e-10 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance);
}

TEST_F(SimdAggregatorCorrectnessTest, Avg_AllSame) {
    std::vector<double> values(100, 5.5);
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateAvg(values.data(), values.size()), 5.5);
}

// ==================== Min ====================

TEST_F(SimdAggregatorCorrectnessTest, Min_Empty) {
    double result = SimdAggregator::calculateMin(nullptr, 0);
    EXPECT_TRUE(std::isnan(result));
}

TEST_F(SimdAggregatorCorrectnessTest, Min_SingleValue) {
    double val = 42.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMin(&val, 1), 42.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Min_SmallArray) {
    std::vector<double> values = {5.0, 3.0, 7.0};
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMin(values.data(), values.size()), 3.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Min_NegativeValues) {
    std::vector<double> values = {-1.0, -5.0, -3.0, -2.0, -4.0, -6.0, -8.0, -7.0, -9.0, -10.0};
    double simd = SimdAggregator::calculateMin(values.data(), values.size());
    double scl = scalar::calculateMin(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
    EXPECT_DOUBLE_EQ(simd, -10.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Min_MinAtStart) {
    std::vector<double> values(100);
    values[0] = -999.0;
    for (size_t i = 1; i < values.size(); i++) {
        values[i] = static_cast<double>(i);
    }
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMin(values.data(), values.size()), -999.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Min_MinAtEnd) {
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size() - 1; i++) {
        values[i] = static_cast<double>(i + 1);
    }
    values[99] = -999.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMin(values.data(), values.size()), -999.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Min_VariousSizes) {
    for (size_t n = 1; n <= 50; n++) {
        std::vector<double> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = static_cast<double>(n - i);  // descending
        }
        double simd = SimdAggregator::calculateMin(values.data(), n);
        double scl = scalar::calculateMin(values.data(), n);
        EXPECT_DOUBLE_EQ(simd, scl)
            << "Min mismatch for n=" << n;
    }
}

TEST_F(SimdAggregatorCorrectnessTest, Min_LargeRandom) {
    std::mt19937_64 rng(123);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    std::vector<double> values(10000);
    for (auto& v : values) v = dist(rng);

    double simd = SimdAggregator::calculateMin(values.data(), values.size());
    double scl = scalar::calculateMin(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
}

// ==================== Max ====================

TEST_F(SimdAggregatorCorrectnessTest, Max_Empty) {
    double result = SimdAggregator::calculateMax(nullptr, 0);
    EXPECT_TRUE(std::isnan(result));
}

TEST_F(SimdAggregatorCorrectnessTest, Max_SingleValue) {
    double val = 42.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMax(&val, 1), 42.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Max_SmallArray) {
    std::vector<double> values = {5.0, 7.0, 3.0};
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMax(values.data(), values.size()), 7.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Max_MaxAtStart) {
    std::vector<double> values(100);
    values[0] = 999.0;
    for (size_t i = 1; i < values.size(); i++) {
        values[i] = -static_cast<double>(i);
    }
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMax(values.data(), values.size()), 999.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Max_MaxAtEnd) {
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size() - 1; i++) {
        values[i] = -static_cast<double>(i + 1);
    }
    values[99] = 999.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateMax(values.data(), values.size()), 999.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Max_VariousSizes) {
    for (size_t n = 1; n <= 50; n++) {
        std::vector<double> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = static_cast<double>(i + 1);  // ascending
        }
        double simd = SimdAggregator::calculateMax(values.data(), n);
        double scl = scalar::calculateMax(values.data(), n);
        EXPECT_DOUBLE_EQ(simd, scl)
            << "Max mismatch for n=" << n;
    }
}

TEST_F(SimdAggregatorCorrectnessTest, Max_LargeRandom) {
    std::mt19937_64 rng(456);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    std::vector<double> values(10000);
    for (auto& v : values) v = dist(rng);

    double simd = SimdAggregator::calculateMax(values.data(), values.size());
    double scl = scalar::calculateMax(values.data(), values.size());
    EXPECT_DOUBLE_EQ(simd, scl);
}

// ==================== Variance ====================

TEST_F(SimdAggregatorCorrectnessTest, Variance_Empty) {
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateVariance(nullptr, 0, 0.0), 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Variance_SingleValue) {
    double val = 5.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::calculateVariance(&val, 1, 5.0), 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Variance_TwoValues) {
    std::vector<double> values = {1.0, 3.0};
    double mean = 2.0;
    double simd = SimdAggregator::calculateVariance(values.data(), values.size(), mean);
    double scl = scalar::calculateVariance(values.data(), values.size(), mean);
    EXPECT_DOUBLE_EQ(simd, scl);
    // Variance of {1,3} with mean 2: ((1-2)^2 + (3-2)^2) / (2-1) = 2.0
    EXPECT_DOUBLE_EQ(simd, 2.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Variance_AllSame) {
    std::vector<double> values(100, 5.0);
    double simd = SimdAggregator::calculateVariance(values.data(), values.size(), 5.0);
    EXPECT_DOUBLE_EQ(simd, 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, Variance_MatchesScalar) {
    std::mt19937_64 rng(789);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> values(1000);
    for (auto& v : values) v = dist(rng);

    double mean = scalar::calculateAvg(values.data(), values.size());
    double simd = SimdAggregator::calculateVariance(values.data(), values.size(), mean);
    double scl = scalar::calculateVariance(values.data(), values.size(), mean);

    double tolerance = std::abs(scl) * 1e-9 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance)
        << "Variance differs (SIMD=" << simd << ", scalar=" << scl << ")";
}

TEST_F(SimdAggregatorCorrectnessTest, Variance_VariousSizes) {
    for (size_t n = 2; n <= 50; n++) {
        std::vector<double> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = static_cast<double>(i);
        }
        double mean = scalar::calculateAvg(values.data(), n);
        double simd = SimdAggregator::calculateVariance(values.data(), n, mean);
        double scl = scalar::calculateVariance(values.data(), n, mean);

        double tolerance = std::abs(scl) * 1e-12 + 1e-12;
        EXPECT_NEAR(simd, scl, tolerance)
            << "Variance mismatch for n=" << n;
    }
}

// ==================== Dot Product ====================

TEST_F(SimdAggregatorCorrectnessTest, DotProduct_Empty) {
    EXPECT_DOUBLE_EQ(SimdAggregator::dotProduct(nullptr, nullptr, 0), 0.0);
}

TEST_F(SimdAggregatorCorrectnessTest, DotProduct_SingleValue) {
    double a = 3.0, b = 4.0;
    EXPECT_DOUBLE_EQ(SimdAggregator::dotProduct(&a, &b, 1), 12.0);
}

TEST_F(SimdAggregatorCorrectnessTest, DotProduct_SmallArray) {
    std::vector<double> a = {1.0, 2.0, 3.0};
    std::vector<double> b = {4.0, 5.0, 6.0};
    double result = SimdAggregator::dotProduct(a.data(), b.data(), a.size());
    EXPECT_DOUBLE_EQ(result, 32.0);  // 1*4 + 2*5 + 3*6 = 32
}

TEST_F(SimdAggregatorCorrectnessTest, DotProduct_LargeArray) {
    std::mt19937_64 rng(321);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> a(1000), b(1000);
    for (size_t i = 0; i < 1000; i++) {
        a[i] = dist(rng);
        b[i] = dist(rng);
    }

    double simd = SimdAggregator::dotProduct(a.data(), b.data(), a.size());

    // Compute scalar dot product
    double scl = 0.0;
    for (size_t i = 0; i < 1000; i++) {
        scl += a[i] * b[i];
    }

    double tolerance = std::abs(scl) * 1e-10 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance);
}

TEST_F(SimdAggregatorCorrectnessTest, DotProduct_Orthogonal) {
    // Orthogonal vectors: dot product should be zero
    std::vector<double> a = {1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0};
    std::vector<double> b = {0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0};
    EXPECT_DOUBLE_EQ(SimdAggregator::dotProduct(a.data(), b.data(), a.size()), 0.0);
}

// ==================== Bucket Sums ====================

TEST_F(SimdAggregatorCorrectnessTest, BucketSums_BasicCase) {
    // 12 values split into 3 buckets of 4
    std::vector<double> values = {1, 2, 3, 4, 10, 20, 30, 40, 100, 200, 300, 400};
    std::vector<size_t> bucket_indices = {0, 4, 8};
    std::vector<double> bucket_sums(3, 0.0);

    SimdAggregator::calculateBucketSums(
        values.data(), bucket_indices.data(), 3, 4, bucket_sums.data());

    EXPECT_DOUBLE_EQ(bucket_sums[0], 10.0);    // 1+2+3+4
    EXPECT_DOUBLE_EQ(bucket_sums[1], 100.0);   // 10+20+30+40
    EXPECT_DOUBLE_EQ(bucket_sums[2], 1000.0);  // 100+200+300+400
}

TEST_F(SimdAggregatorCorrectnessTest, BucketSums_UnevenBuckets) {
    std::vector<double> values = {1, 2, 3, 4, 5, 10, 20, 30};
    std::vector<size_t> bucket_indices = {0, 5};
    std::vector<double> bucket_sums(2, 0.0);

    SimdAggregator::calculateBucketSums(
        values.data(), bucket_indices.data(), 2, 3, bucket_sums.data());

    EXPECT_DOUBLE_EQ(bucket_sums[0], 15.0);  // 1+2+3+4+5
    EXPECT_DOUBLE_EQ(bucket_sums[1], 60.0);  // 10+20+30
}

// ==================== Histogram ====================

TEST_F(SimdAggregatorCorrectnessTest, Histogram_BasicCase) {
    std::vector<double> values = {0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5};
    std::vector<uint32_t> histogram(5, 0);

    SimdAggregator::computeHistogram(
        values.data(), values.size(), 0.0, 4.5, 5, histogram.data());

    // Every value should be counted
    uint32_t total = 0;
    for (auto h : histogram) total += h;
    EXPECT_EQ(total, 10u);
}

TEST_F(SimdAggregatorCorrectnessTest, Histogram_AllSameBin) {
    std::vector<double> values(100, 5.0);
    std::vector<uint32_t> histogram(10, 0);

    SimdAggregator::computeHistogram(
        values.data(), values.size(), 0.0, 10.0, 10, histogram.data());

    uint32_t total = 0;
    for (auto h : histogram) total += h;
    EXPECT_EQ(total, 100u);
}

TEST_F(SimdAggregatorCorrectnessTest, Histogram_LargeRandom) {
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> dist(0.0, 100.0);

    std::vector<double> values(10000);
    for (auto& v : values) v = dist(rng);

    std::vector<uint32_t> histogram(100, 0);

    SimdAggregator::computeHistogram(
        values.data(), values.size(), 0.0, 100.0, 100, histogram.data());

    // All values should be counted
    uint32_t total = 0;
    for (auto h : histogram) total += h;
    EXPECT_EQ(total, 10000u);

    // Most bins should be non-empty in a uniform distribution with 10k samples
    uint32_t nonEmptyBins = 0;
    for (size_t i = 0; i < 100; i++) {
        if (histogram[i] > 0) nonEmptyBins++;
    }
    EXPECT_GE(nonEmptyBins, 90u)
        << "Expected at least 90% of bins non-empty in uniform distribution";
}

// ==================== Multiple Aggregations Consistency ====================

TEST_F(SimdAggregatorCorrectnessTest, ConsistencyCheck_MinLEAvgLEMax) {
    std::mt19937_64 rng(555);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

    for (int trial = 0; trial < 10; trial++) {
        size_t n = 50 + trial * 100;
        std::vector<double> values(n);
        for (auto& v : values) v = dist(rng);

        double minVal = SimdAggregator::calculateMin(values.data(), n);
        double maxVal = SimdAggregator::calculateMax(values.data(), n);
        double avgVal = SimdAggregator::calculateAvg(values.data(), n);

        EXPECT_LE(minVal, avgVal)
            << "Min (" << minVal << ") > Avg (" << avgVal << ") for n=" << n;
        EXPECT_LE(avgVal, maxVal)
            << "Avg (" << avgVal << ") > Max (" << maxVal << ") for n=" << n;
        EXPECT_LE(minVal, maxVal)
            << "Min (" << minVal << ") > Max (" << maxVal << ") for n=" << n;
    }
}

TEST_F(SimdAggregatorCorrectnessTest, ConsistencyCheck_SumEqualsAvgTimesCount) {
    std::mt19937_64 rng(666);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> values(500);
    for (auto& v : values) v = dist(rng);

    double sum = SimdAggregator::calculateSum(values.data(), values.size());
    double avg = SimdAggregator::calculateAvg(values.data(), values.size());

    double expectedSum = avg * static_cast<double>(values.size());
    double tolerance = std::abs(sum) * 1e-12 + 1e-10;
    EXPECT_NEAR(sum, expectedSum, tolerance);
}

TEST_F(SimdAggregatorCorrectnessTest, ConsistencyCheck_VarianceNonNegative) {
    std::mt19937_64 rng(777);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    for (int trial = 0; trial < 10; trial++) {
        size_t n = 10 + trial * 50;
        std::vector<double> values(n);
        for (auto& v : values) v = dist(rng);

        double mean = SimdAggregator::calculateAvg(values.data(), n);
        double var = SimdAggregator::calculateVariance(values.data(), n, mean);

        EXPECT_GE(var, 0.0)
            << "Negative variance (" << var << ") for n=" << n;
    }
}
