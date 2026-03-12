#include <gtest/gtest.h>
#include "../../../lib/query/anomaly/simd_anomaly.hpp"
#include <vector>
#include <cmath>
#include <random>
#include <limits>
#include <numeric>
#include <algorithm>

namespace simd_ns = timestar::anomaly::simd;

// ============================================================================
// Local scalar reference implementations for comparison.
// These replicate the logic of the scalar fallbacks in simd_anomaly.cpp,
// which are not exposed in the public header.
// ============================================================================

namespace ref {

void vectorSubtract(const double* a, const double* b, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) result[i] = a[i] - b[i];
}

void vectorAdd(const double* a, const double* b, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) result[i] = a[i] + b[i];
}

void vectorMultiply(const double* a, const double* b, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) result[i] = a[i] * b[i];
}

void vectorScalarMultiply(const double* a, double scalar, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) result[i] = a[i] * scalar;
}

void vectorFMA(const double* a, const double* b, double scalar, double* result, size_t count) {
    for (size_t i = 0; i < count; ++i) result[i] = a[i] + b[i] * scalar;
}

double vectorSum(const double* values, size_t count) {
    // Kahan summation for reference precision
    double sum = 0.0, c = 0.0;
    for (size_t i = 0; i < count; ++i) {
        double y = values[i] - c;
        double t = sum + y;
        c = (t - sum) - y;
        sum = t;
    }
    return sum;
}

double vectorMean(const double* values, size_t count) {
    if (count == 0) return 0.0;
    return vectorSum(values, count) / static_cast<double>(count);
}

double vectorSumSquaredDiff(const double* values, size_t count, double mean) {
    double result = 0.0;
    for (size_t i = 0; i < count; ++i) {
        double diff = values[i] - mean;
        result += diff * diff;
    }
    return result;
}

double vectorVariance(const double* values, size_t count, double mean) {
    if (count <= 1) return 0.0;
    return vectorSumSquaredDiff(values, count, mean) / static_cast<double>(count - 1);
}

void computeBounds(const double* predictions, const double* scale, double bounds,
                   double* upper, double* lower, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        double margin = bounds * scale[i];
        upper[i] = predictions[i] + margin;
        lower[i] = predictions[i] - margin;
    }
}

void computeAnomalyScores(const double* values, const double* upper,
                           const double* lower, double* scores, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        double above = std::max(0.0, values[i] - upper[i]);
        double below = std::max(0.0, lower[i] - values[i]);
        scores[i] = above + below;
    }
}

void computeTricubeWeights(const double* distances, double* weights, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        double u = distances[i];
        if (u >= 1.0) {
            weights[i] = 0.0;
        } else {
            double t = 1.0 - u * u * u;
            weights[i] = t * t * t;
        }
    }
}

double weightedSum(const double* values, const double* weights, size_t count) {
    double sum = 0.0;
    for (size_t i = 0; i < count; ++i) sum += values[i] * weights[i];
    return sum;
}

void computeMovingAverage(const double* values, size_t count, size_t windowSize, double* result) {
    if (count == 0 || windowSize == 0) return;
    size_t halfWindow = windowSize / 2;
    double windowSum = 0.0;
    size_t windowCount = 0;
    size_t initEnd = std::min(halfWindow + 1, count);
    for (size_t i = 0; i < initEnd; ++i) {
        if (!std::isnan(values[i])) { windowSum += values[i]; ++windowCount; }
    }
    for (size_t i = 0; i < count; ++i) {
        size_t addIdx = i + halfWindow + 1;
        if (addIdx < count && !std::isnan(values[addIdx])) { windowSum += values[addIdx]; ++windowCount; }
        if (i > halfWindow) {
            size_t removeIdx = i - halfWindow - 1;
            if (!std::isnan(values[removeIdx])) { windowSum -= values[removeIdx]; --windowCount; }
        }
        result[i] = (windowCount > 0) ? windowSum / static_cast<double>(windowCount) : 0.0;
    }
}

} // namespace ref

// ============================================================================
// SIMD Anomaly Detection Correctness Tests
// ============================================================================

class SimdAnomalyCorrectnessTest : public ::testing::Test {
protected:
    bool avx2Available_ = false;
    bool avx512Available_ = false;

    void SetUp() override {
        avx2Available_ = simd_ns::isAvx2Available();
        avx512Available_ = simd_ns::isAvx512Available();
    }
};

// ==================== Vector Operations ====================

TEST_F(SimdAnomalyCorrectnessTest, VectorSubtract_SmallArray) {
    std::vector<double> a = {5.0, 10.0, 15.0};
    std::vector<double> b = {1.0, 2.0, 3.0};
    std::vector<double> simdResult(3), refResult(3);

    simd_ns::vectorSubtract(a.data(), b.data(), simdResult.data(), 3);
    ref::vectorSubtract(a.data(), b.data(), refResult.data(), 3);

    for (size_t i = 0; i < 3; i++) {
        EXPECT_DOUBLE_EQ(simdResult[i], refResult[i]) << "subtract index " << i;
    }
    EXPECT_DOUBLE_EQ(simdResult[0], 4.0);
    EXPECT_DOUBLE_EQ(simdResult[1], 8.0);
    EXPECT_DOUBLE_EQ(simdResult[2], 12.0);
}

TEST_F(SimdAnomalyCorrectnessTest, VectorSubtract_LargeArray) {
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    const size_t n = 1000;
    std::vector<double> a(n), b(n), simdResult(n), refResult(n);
    for (size_t i = 0; i < n; i++) { a[i] = dist(rng); b[i] = dist(rng); }

    simd_ns::vectorSubtract(a.data(), b.data(), simdResult.data(), n);
    ref::vectorSubtract(a.data(), b.data(), refResult.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_DOUBLE_EQ(simdResult[i], refResult[i]) << "subtract index " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, VectorAdd_MatchesRef) {
    std::mt19937_64 rng(43);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    const size_t n = 1000;
    std::vector<double> a(n), b(n), simdResult(n), refResult(n);
    for (size_t i = 0; i < n; i++) { a[i] = dist(rng); b[i] = dist(rng); }

    simd_ns::vectorAdd(a.data(), b.data(), simdResult.data(), n);
    ref::vectorAdd(a.data(), b.data(), refResult.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_DOUBLE_EQ(simdResult[i], refResult[i]) << "add index " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, VectorMultiply_MatchesRef) {
    std::mt19937_64 rng(44);
    std::uniform_real_distribution<double> dist(-1e3, 1e3);

    const size_t n = 1000;
    std::vector<double> a(n), b(n), simdResult(n), refResult(n);
    for (size_t i = 0; i < n; i++) { a[i] = dist(rng); b[i] = dist(rng); }

    simd_ns::vectorMultiply(a.data(), b.data(), simdResult.data(), n);
    ref::vectorMultiply(a.data(), b.data(), refResult.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_DOUBLE_EQ(simdResult[i], refResult[i]) << "multiply index " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, VectorScalarMultiply_MatchesRef) {
    std::mt19937_64 rng(45);
    std::uniform_real_distribution<double> dist(-1e3, 1e3);

    const size_t n = 500;
    std::vector<double> a(n), simdResult(n), refResult(n);
    double s = 3.14159;
    for (size_t i = 0; i < n; i++) a[i] = dist(rng);

    simd_ns::vectorScalarMultiply(a.data(), s, simdResult.data(), n);
    ref::vectorScalarMultiply(a.data(), s, refResult.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_DOUBLE_EQ(simdResult[i], refResult[i]) << "scalar multiply index " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, VectorFMA_MatchesRef) {
    std::mt19937_64 rng(46);
    std::uniform_real_distribution<double> dist(-1e3, 1e3);

    const size_t n = 500;
    std::vector<double> a(n), b(n), simdResult(n), refResult(n);
    double s = 2.71828;
    for (size_t i = 0; i < n; i++) { a[i] = dist(rng); b[i] = dist(rng); }

    simd_ns::vectorFMA(a.data(), b.data(), s, simdResult.data(), n);
    ref::vectorFMA(a.data(), b.data(), s, refResult.data(), n);

    for (size_t i = 0; i < n; i++) {
        // FMA may have slight differences due to fused multiply-add precision
        double tolerance = std::abs(refResult[i]) * 1e-14 + 1e-14;
        EXPECT_NEAR(simdResult[i], refResult[i], tolerance) << "FMA index " << i;
    }
}

// Test all vector operations at boundary sizes
TEST_F(SimdAnomalyCorrectnessTest, VectorOps_BoundarySizes) {
    for (size_t n : {1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17}) {
        std::vector<double> a(n), b(n), simdResult(n), refResult(n);
        for (size_t i = 0; i < n; i++) {
            a[i] = static_cast<double>(i + 1);
            b[i] = static_cast<double>(i + 1) * 0.5;
        }

        simd_ns::vectorSubtract(a.data(), b.data(), simdResult.data(), n);
        ref::vectorSubtract(a.data(), b.data(), refResult.data(), n);
        for (size_t i = 0; i < n; i++) {
            EXPECT_DOUBLE_EQ(simdResult[i], refResult[i])
                << "subtract boundary n=" << n << " i=" << i;
        }

        simd_ns::vectorAdd(a.data(), b.data(), simdResult.data(), n);
        ref::vectorAdd(a.data(), b.data(), refResult.data(), n);
        for (size_t i = 0; i < n; i++) {
            EXPECT_DOUBLE_EQ(simdResult[i], refResult[i])
                << "add boundary n=" << n << " i=" << i;
        }

        simd_ns::vectorMultiply(a.data(), b.data(), simdResult.data(), n);
        ref::vectorMultiply(a.data(), b.data(), refResult.data(), n);
        for (size_t i = 0; i < n; i++) {
            EXPECT_DOUBLE_EQ(simdResult[i], refResult[i])
                << "multiply boundary n=" << n << " i=" << i;
        }
    }
}

// ==================== Sum and Mean ====================

TEST_F(SimdAnomalyCorrectnessTest, VectorSum_Empty) {
    EXPECT_DOUBLE_EQ(simd_ns::vectorSum(nullptr, 0), 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, VectorSum_MatchesRef) {
    std::mt19937_64 rng(47);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

    for (size_t n : {1, 3, 4, 7, 8, 15, 16, 50, 100, 1000}) {
        std::vector<double> values(n);
        for (auto& v : values) v = dist(rng);

        double simd = simd_ns::vectorSum(values.data(), n);
        double scl = ref::vectorSum(values.data(), n);

        double tolerance = std::abs(scl) * 1e-10 + 1e-10;
        EXPECT_NEAR(simd, scl, tolerance) << "Sum mismatch for n=" << n;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, VectorMean_Empty) {
    EXPECT_DOUBLE_EQ(simd_ns::vectorMean(nullptr, 0), 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, VectorMean_MatchesRef) {
    std::mt19937_64 rng(48);
    std::uniform_real_distribution<double> dist(-500.0, 500.0);

    std::vector<double> values(1000);
    for (auto& v : values) v = dist(rng);

    double simd = simd_ns::vectorMean(values.data(), values.size());
    double scl = ref::vectorMean(values.data(), values.size());

    double tolerance = std::abs(scl) * 1e-10 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance);
}

TEST_F(SimdAnomalyCorrectnessTest, VectorMean_AllSame) {
    std::vector<double> values(200, 7.5);
    EXPECT_DOUBLE_EQ(simd_ns::vectorMean(values.data(), values.size()), 7.5);
}

// ==================== Variance ====================

TEST_F(SimdAnomalyCorrectnessTest, VectorVariance_MatchesRef) {
    std::mt19937_64 rng(49);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> values(500);
    for (auto& v : values) v = dist(rng);

    double mean = simd_ns::vectorMean(values.data(), values.size());

    double simd = simd_ns::vectorVariance(values.data(), values.size(), mean);
    double scl = ref::vectorVariance(values.data(), values.size(), mean);

    double tolerance = std::abs(scl) * 1e-9 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance)
        << "Variance mismatch (SIMD=" << simd << ", ref=" << scl << ")";
}

TEST_F(SimdAnomalyCorrectnessTest, VectorVariance_AllSame) {
    std::vector<double> values(100, 5.0);
    double var = simd_ns::vectorVariance(values.data(), values.size(), 5.0);
    EXPECT_DOUBLE_EQ(var, 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, VectorVariance_NonNegative) {
    std::mt19937_64 rng(50);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    for (int trial = 0; trial < 20; trial++) {
        size_t n = 10 + trial * 25;
        std::vector<double> values(n);
        for (auto& v : values) v = dist(rng);

        double mean = simd_ns::vectorMean(values.data(), n);
        double var = simd_ns::vectorVariance(values.data(), n, mean);

        EXPECT_GE(var, 0.0) << "Negative variance for n=" << n;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, VectorSumSquaredDiff_MatchesRef) {
    std::mt19937_64 rng(51);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    for (size_t n : {1, 3, 7, 8, 15, 16, 50, 500}) {
        std::vector<double> values(n);
        for (auto& v : values) v = dist(rng);

        double mean = ref::vectorMean(values.data(), n);
        double simd = simd_ns::vectorSumSquaredDiff(values.data(), n, mean);
        double scl = ref::vectorSumSquaredDiff(values.data(), n, mean);

        double tolerance = std::abs(scl) * 1e-9 + 1e-10;
        EXPECT_NEAR(simd, scl, tolerance) << "SumSquaredDiff mismatch for n=" << n;
    }
}

// ==================== Incremental Rolling Stats ====================

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_Basic) {
    simd_ns::IncrementalRollingStats stats(5);

    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    for (double v : values) stats.update(v);

    EXPECT_DOUBLE_EQ(stats.mean(), 3.0);
    EXPECT_EQ(stats.count(), 5u);
    EXPECT_NEAR(stats.variance(), 2.5, 1e-10);
    EXPECT_NEAR(stats.stddev(), std::sqrt(2.5), 1e-10);
}

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_WindowSliding) {
    simd_ns::IncrementalRollingStats stats(3);

    stats.update(1.0);
    stats.update(2.0);
    stats.update(3.0);
    EXPECT_NEAR(stats.mean(), 2.0, 1e-10);

    stats.update(4.0);
    EXPECT_NEAR(stats.mean(), 3.0, 1e-10);

    stats.update(5.0);
    EXPECT_NEAR(stats.mean(), 4.0, 1e-10);
}

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_MatchesBruteForce) {
    const size_t windowSize = 20;
    simd_ns::IncrementalRollingStats stats(windowSize);

    std::mt19937_64 rng(52);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> allValues;

    for (int i = 0; i < 200; i++) {
        double v = dist(rng);
        stats.update(v);
        allValues.push_back(v);

        size_t start = allValues.size() > windowSize ? allValues.size() - windowSize : 0;
        size_t count = allValues.size() - start;

        double bfMean = 0.0;
        for (size_t j = start; j < allValues.size(); j++) bfMean += allValues[j];
        bfMean /= static_cast<double>(count);

        double bfVar = 0.0;
        if (count > 1) {
            for (size_t j = start; j < allValues.size(); j++) {
                double diff = allValues[j] - bfMean;
                bfVar += diff * diff;
            }
            bfVar /= static_cast<double>(count - 1);
        }

        double meanTolerance = std::abs(bfMean) * 1e-8 + 1e-8;
        EXPECT_NEAR(stats.mean(), bfMean, meanTolerance)
            << "Mean mismatch at iteration " << i;

        if (count > 1) {
            double varTolerance = std::abs(bfVar) * 1e-6 + 1e-8;
            EXPECT_NEAR(stats.variance(), bfVar, varTolerance)
                << "Variance mismatch at iteration " << i;
        }
    }
}

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_StddevNonNegative) {
    simd_ns::IncrementalRollingStats stats(10);
    std::mt19937_64 rng(53);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

    for (int i = 0; i < 500; i++) {
        stats.update(dist(rng));
        EXPECT_GE(stats.stddev(), 0.0) << "Negative stddev at iteration " << i;
        EXPECT_GE(stats.variance(), 0.0) << "Negative variance at iteration " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_Reset) {
    simd_ns::IncrementalRollingStats stats(5);
    stats.update(10.0);
    stats.update(20.0);
    stats.update(30.0);

    stats.reset();

    EXPECT_DOUBLE_EQ(stats.mean(), 0.0);
    EXPECT_EQ(stats.count(), 0u);
    EXPECT_DOUBLE_EQ(stats.variance(), 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_NaNIgnored) {
    simd_ns::IncrementalRollingStats stats(5);

    stats.update(1.0);
    stats.update(std::numeric_limits<double>::quiet_NaN());
    stats.update(3.0);

    EXPECT_EQ(stats.count(), 2u);
    EXPECT_NEAR(stats.mean(), 2.0, 1e-10);
}

TEST_F(SimdAnomalyCorrectnessTest, IncrementalRollingStats_ConstantValues) {
    simd_ns::IncrementalRollingStats stats(10);

    for (int i = 0; i < 50; i++) stats.update(42.0);

    EXPECT_NEAR(stats.mean(), 42.0, 1e-10);
    EXPECT_NEAR(stats.variance(), 0.0, 1e-10);
    EXPECT_NEAR(stats.stddev(), 0.0, 1e-10);
}

// ==================== Bounds Computation ====================

TEST_F(SimdAnomalyCorrectnessTest, ComputeBounds_MatchesRef) {
    const size_t n = 100;
    std::mt19937_64 rng(54);
    std::uniform_real_distribution<double> dist(-50.0, 50.0);
    std::uniform_real_distribution<double> scaleDist(0.1, 5.0);

    std::vector<double> predictions(n), scale(n);
    std::vector<double> simdUpper(n), simdLower(n);
    std::vector<double> refUpper(n), refLower(n);

    for (size_t i = 0; i < n; i++) {
        predictions[i] = dist(rng);
        scale[i] = scaleDist(rng);
    }

    double bounds = 2.5;

    simd_ns::computeBounds(predictions.data(), scale.data(), bounds,
                            simdUpper.data(), simdLower.data(), n);
    ref::computeBounds(predictions.data(), scale.data(), bounds,
                        refUpper.data(), refLower.data(), n);

    // simd_anomaly.cpp is compiled with -mfma, allowing the compiler to contract
    // mul+add into a single FMA instruction (1 rounding vs 2). This can produce
    // results that differ by ~1 ULP from non-FMA scalar code. With catastrophic
    // cancellation the error in the intermediate product (bounds * scale, up to
    // 12.5) is ~1 ULP ≈ 2.8e-15, not 1 ULP of the near-zero result. Use an
    // absolute tolerance of 1e-10 which is ~1e5× the worst-case FMA error yet
    // tight enough to catch any real indexing or logic bugs.
    constexpr double kAbsTol = 1e-10;
    for (size_t i = 0; i < n; i++) {
        EXPECT_NEAR(simdUpper[i], refUpper[i], kAbsTol) << "upper mismatch at " << i;
        EXPECT_NEAR(simdLower[i], refLower[i], kAbsTol) << "lower mismatch at " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, ComputeBounds_BoundarySizes) {
    for (size_t n : {1, 3, 4, 7, 8, 9, 15, 16, 17, 50}) {
        std::vector<double> predictions(n, 10.0);
        std::vector<double> scale(n, 2.0);
        std::vector<double> simdUpper(n), simdLower(n);
        std::vector<double> refUpper(n), refLower(n);

        double bounds = 3.0;

        simd_ns::computeBounds(predictions.data(), scale.data(), bounds,
                                simdUpper.data(), simdLower.data(), n);
        ref::computeBounds(predictions.data(), scale.data(), bounds,
                            refUpper.data(), refLower.data(), n);

        for (size_t i = 0; i < n; i++) {
            EXPECT_DOUBLE_EQ(simdUpper[i], refUpper[i])
                << "upper at n=" << n << " i=" << i;
            EXPECT_DOUBLE_EQ(simdLower[i], refLower[i])
                << "lower at n=" << n << " i=" << i;
            EXPECT_DOUBLE_EQ(simdUpper[i], 16.0);
            EXPECT_DOUBLE_EQ(simdLower[i], 4.0);
        }
    }
}

// ==================== Anomaly Scores ====================

TEST_F(SimdAnomalyCorrectnessTest, AnomalyScores_MatchesRef) {
    const size_t n = 100;
    std::mt19937_64 rng(55);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> values(n), upper(n), lower(n);
    std::vector<double> simdScores(n), refScores(n);

    for (size_t i = 0; i < n; i++) {
        values[i] = dist(rng);
        double center = dist(rng);
        upper[i] = center + 10.0;
        lower[i] = center - 10.0;
    }

    simd_ns::computeAnomalyScores(values.data(), upper.data(), lower.data(),
                                    simdScores.data(), n);
    ref::computeAnomalyScores(values.data(), upper.data(), lower.data(),
                               refScores.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_DOUBLE_EQ(simdScores[i], refScores[i])
            << "anomaly score mismatch at " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, AnomalyScores_WithinBounds) {
    std::vector<double> values = {5.0, 6.0, 7.0, 8.0, 5.5, 6.5, 7.5, 8.5, 6.0, 7.0};
    std::vector<double> upper(10, 10.0);
    std::vector<double> lower(10, 0.0);
    std::vector<double> scores(10);

    simd_ns::computeAnomalyScores(values.data(), upper.data(), lower.data(),
                                    scores.data(), 10);

    for (size_t i = 0; i < 10; i++) {
        EXPECT_DOUBLE_EQ(scores[i], 0.0) << "Expected zero score at " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, AnomalyScores_AboveBounds) {
    std::vector<double> values = {15.0, 20.0, 25.0, 30.0, 12.0, 18.0, 22.0, 28.0, 11.0, 16.0};
    std::vector<double> upper(10, 10.0);
    std::vector<double> lower(10, 0.0);
    std::vector<double> scores(10);

    simd_ns::computeAnomalyScores(values.data(), upper.data(), lower.data(),
                                    scores.data(), 10);

    for (size_t i = 0; i < 10; i++) {
        EXPECT_EQ(scores[i], values[i] - upper[i]) << "Wrong score above bound at " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, AnomalyScores_BelowBounds) {
    std::vector<double> values = {-5.0, -10.0, -3.0, -8.0, -1.0, -2.0, -7.0, -6.0, -4.0, -9.0};
    std::vector<double> upper(10, 10.0);
    std::vector<double> lower(10, 0.0);
    std::vector<double> scores(10);

    simd_ns::computeAnomalyScores(values.data(), upper.data(), lower.data(),
                                    scores.data(), 10);

    for (size_t i = 0; i < 10; i++) {
        EXPECT_EQ(scores[i], lower[i] - values[i]) << "Wrong score below bound at " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, AnomalyScores_NonNegative) {
    std::mt19937_64 rng(56);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    const size_t n = 500;
    std::vector<double> values(n), upper(n), lower(n), scores(n);
    for (size_t i = 0; i < n; i++) {
        values[i] = dist(rng);
        upper[i] = dist(rng) + 10.0;
        lower[i] = upper[i] - 20.0;
    }

    simd_ns::computeAnomalyScores(values.data(), upper.data(), lower.data(),
                                    scores.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_GE(scores[i], 0.0) << "Negative anomaly score at " << i;
    }
}

// ==================== Tricube Weights ====================

TEST_F(SimdAnomalyCorrectnessTest, TricubeWeights_MatchesRef) {
    const size_t n = 100;
    std::mt19937_64 rng(57);
    std::uniform_real_distribution<double> dist(0.0, 2.0);

    std::vector<double> distances(n), simdWeights(n), refWeights(n);
    for (auto& d : distances) d = dist(rng);

    simd_ns::computeTricubeWeights(distances.data(), simdWeights.data(), n);
    ref::computeTricubeWeights(distances.data(), refWeights.data(), n);

    for (size_t i = 0; i < n; i++) {
        double tolerance = std::abs(refWeights[i]) * 1e-14 + 1e-14;
        EXPECT_NEAR(simdWeights[i], refWeights[i], tolerance)
            << "tricube weight mismatch at " << i << " (distance=" << distances[i] << ")";
    }
}

TEST_F(SimdAnomalyCorrectnessTest, TricubeWeights_ZeroDistance) {
    std::vector<double> distances(10, 0.0);
    std::vector<double> weights(10);
    simd_ns::computeTricubeWeights(distances.data(), weights.data(), 10);

    for (size_t i = 0; i < 10; i++) {
        EXPECT_DOUBLE_EQ(weights[i], 1.0) << "Expected weight 1.0 at distance 0";
    }
}

TEST_F(SimdAnomalyCorrectnessTest, TricubeWeights_AtOrBeyondOne) {
    std::vector<double> distances = {1.0, 1.1, 1.5, 2.0, 10.0, 1.0, 0.99, 1.0, 1.01, 100.0};
    std::vector<double> weights(10);
    simd_ns::computeTricubeWeights(distances.data(), weights.data(), 10);

    EXPECT_DOUBLE_EQ(weights[0], 0.0);
    EXPECT_DOUBLE_EQ(weights[1], 0.0);
    EXPECT_DOUBLE_EQ(weights[2], 0.0);
    EXPECT_DOUBLE_EQ(weights[3], 0.0);
    EXPECT_DOUBLE_EQ(weights[4], 0.0);
    EXPECT_DOUBLE_EQ(weights[5], 0.0);
    EXPECT_GT(weights[6], 0.0);  // 0.99 < 1.0
    EXPECT_DOUBLE_EQ(weights[7], 0.0);
    EXPECT_DOUBLE_EQ(weights[8], 0.0);
    EXPECT_DOUBLE_EQ(weights[9], 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, TricubeWeights_BoundarySizes) {
    for (size_t n : {1, 3, 4, 7, 8, 9, 15, 16, 17}) {
        std::vector<double> distances(n), simdWeights(n), refWeights(n);
        for (size_t i = 0; i < n; i++) {
            distances[i] = static_cast<double>(i) / static_cast<double>(n);
        }

        simd_ns::computeTricubeWeights(distances.data(), simdWeights.data(), n);
        ref::computeTricubeWeights(distances.data(), refWeights.data(), n);

        for (size_t i = 0; i < n; i++) {
            double tolerance = std::abs(refWeights[i]) * 1e-14 + 1e-14;
            EXPECT_NEAR(simdWeights[i], refWeights[i], tolerance)
                << "tricube boundary n=" << n << " i=" << i;
        }
    }
}

// ==================== Weighted Operations ====================

TEST_F(SimdAnomalyCorrectnessTest, WeightedSum_MatchesRef) {
    std::mt19937_64 rng(58);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);
    std::uniform_real_distribution<double> wDist(0.0, 1.0);

    for (size_t n : {1, 3, 7, 8, 15, 16, 50, 500}) {
        std::vector<double> values(n), weights(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = dist(rng);
            weights[i] = wDist(rng);
        }

        double simd = simd_ns::weightedSum(values.data(), weights.data(), n);
        double scl = ref::weightedSum(values.data(), weights.data(), n);

        double tolerance = std::abs(scl) * 1e-10 + 1e-10;
        EXPECT_NEAR(simd, scl, tolerance) << "weighted sum mismatch for n=" << n;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, WeightedSum_Empty) {
    EXPECT_DOUBLE_EQ(simd_ns::weightedSum(nullptr, nullptr, 0), 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, WeightedMean_MatchesRef) {
    std::mt19937_64 rng(59);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);
    std::uniform_real_distribution<double> wDist(0.1, 1.0);

    std::vector<double> values(200), weights(200);
    for (size_t i = 0; i < 200; i++) {
        values[i] = dist(rng);
        weights[i] = wDist(rng);
    }

    double simd = simd_ns::weightedMean(values.data(), weights.data(), 200);

    double sumW = 0.0, sumVW = 0.0;
    for (size_t i = 0; i < 200; i++) {
        sumW += weights[i];
        sumVW += values[i] * weights[i];
    }
    double scl = sumVW / sumW;

    double tolerance = std::abs(scl) * 1e-9 + 1e-10;
    EXPECT_NEAR(simd, scl, tolerance);
}

// ==================== Moving Average ====================

TEST_F(SimdAnomalyCorrectnessTest, MovingAverage_MatchesRef) {
    const size_t n = 100;
    const size_t windowSize = 5;

    std::mt19937_64 rng(60);
    std::uniform_real_distribution<double> dist(-50.0, 50.0);

    std::vector<double> values(n);
    for (auto& v : values) v = dist(rng);

    std::vector<double> simdResult(n), refResult(n);

    simd_ns::computeMovingAverage(values.data(), n, windowSize, simdResult.data());
    ref::computeMovingAverage(values.data(), n, windowSize, refResult.data());

    for (size_t i = 0; i < n; i++) {
        double tolerance = std::abs(refResult[i]) * 1e-10 + 1e-10;
        EXPECT_NEAR(simdResult[i], refResult[i], tolerance)
            << "moving average mismatch at " << i;
    }
}

TEST_F(SimdAnomalyCorrectnessTest, MovingAverage_WindowSizeOne) {
    // With window size 1, halfWindow = 0, so window spans [i, i+1) effectively.
    // Verify SIMD matches our reference implementation.
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<double> simdResult(5), refResult(5);

    simd_ns::computeMovingAverage(values.data(), 5, 1, simdResult.data());
    ref::computeMovingAverage(values.data(), 5, 1, refResult.data());

    for (size_t i = 0; i < 5; i++) {
        EXPECT_NEAR(simdResult[i], refResult[i], 1e-10) << "window=1 mismatch at " << i;
    }
}

// ==================== Weighted Linear Regression ====================

TEST_F(SimdAnomalyCorrectnessTest, WeightedLinearRegression_PerfectLine) {
    std::vector<double> x = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<double> y = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
    std::vector<double> w(10, 1.0);

    auto fit = simd_ns::weightedLinearRegression(x.data(), y.data(), w.data(), 10);

    EXPECT_NEAR(fit.slope, 2.0, 1e-10);
    EXPECT_NEAR(fit.intercept, 1.0, 1e-10);
}

TEST_F(SimdAnomalyCorrectnessTest, WeightedLinearRegression_Empty) {
    auto fit = simd_ns::weightedLinearRegression(nullptr, nullptr, nullptr, 0);
    EXPECT_DOUBLE_EQ(fit.slope, 0.0);
    EXPECT_DOUBLE_EQ(fit.intercept, 0.0);
}

TEST_F(SimdAnomalyCorrectnessTest, WeightedLinearRegression_WithWeights) {
    std::vector<double> x = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<double> y = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
    std::vector<double> w = {10.0, 10.0, 10.0, 10.0, 10.0, 0.1, 0.1, 0.1, 0.1, 0.1};

    auto fit = simd_ns::weightedLinearRegression(x.data(), y.data(), w.data(), 10);

    EXPECT_NEAR(fit.slope, 2.0, 1e-6);
}

// ==================== End-to-End Consistency ====================

TEST_F(SimdAnomalyCorrectnessTest, EndToEnd_BoundsAndScores) {
    const size_t n = 200;
    std::mt19937_64 rng(61);
    std::uniform_real_distribution<double> predDist(-10.0, 10.0);
    std::uniform_real_distribution<double> scaleDist(0.5, 3.0);
    std::uniform_real_distribution<double> valueDist(-20.0, 20.0);

    std::vector<double> predictions(n), scale(n), values(n);
    for (size_t i = 0; i < n; i++) {
        predictions[i] = predDist(rng);
        scale[i] = scaleDist(rng);
        values[i] = valueDist(rng);
    }

    double bounds = 2.0;

    std::vector<double> simdUpper(n), simdLower(n), simdScores(n);
    simd_ns::computeBounds(predictions.data(), scale.data(), bounds,
                            simdUpper.data(), simdLower.data(), n);
    simd_ns::computeAnomalyScores(values.data(), simdUpper.data(), simdLower.data(),
                                    simdScores.data(), n);

    std::vector<double> refUpper(n), refLower(n), refScores(n);
    ref::computeBounds(predictions.data(), scale.data(), bounds,
                        refUpper.data(), refLower.data(), n);
    ref::computeAnomalyScores(values.data(), refUpper.data(), refLower.data(),
                               refScores.data(), n);

    for (size_t i = 0; i < n; i++) {
        EXPECT_DOUBLE_EQ(simdUpper[i], refUpper[i]) << "upper at " << i;
        EXPECT_DOUBLE_EQ(simdLower[i], refLower[i]) << "lower at " << i;
        EXPECT_DOUBLE_EQ(simdScores[i], refScores[i]) << "score at " << i;
    }
}
