#include <gtest/gtest.h>
#include "anomaly/anomaly_result.hpp"
#include "anomaly/anomaly_detector.hpp"
#include "anomaly/anomaly_executor.hpp"
#include "anomaly/basic_detector.hpp"
#include "anomaly/robust_detector.hpp"
#include "anomaly/agile_detector.hpp"
#include "anomaly/stl_decomposition.hpp"
#include "anomaly/simd_anomaly.hpp"

#include <vector>
#include <set>
#include <cmath>
#include <random>

using namespace tsdb::anomaly;

class AnomalyDetectionTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    // Generate timestamps with 1-minute intervals
    std::vector<uint64_t> generateTimestamps(size_t count, uint64_t startNs = 1704067200000000000ULL) {
        std::vector<uint64_t> timestamps;
        timestamps.reserve(count);
        uint64_t interval = 60000000000ULL; // 1 minute in nanoseconds
        for (size_t i = 0; i < count; ++i) {
            timestamps.push_back(startNs + i * interval);
        }
        return timestamps;
    }

    // Generate constant values
    std::vector<double> generateConstant(size_t count, double value) {
        return std::vector<double>(count, value);
    }

    // Generate linear trend
    std::vector<double> generateLinearTrend(size_t count, double start, double slope) {
        std::vector<double> values;
        values.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            values.push_back(start + i * slope);
        }
        return values;
    }

    // Generate sine wave (for seasonal patterns)
    std::vector<double> generateSinusoidal(size_t count, double baseline, double amplitude, size_t period) {
        std::vector<double> values;
        values.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            values.push_back(baseline + amplitude * std::sin(2.0 * M_PI * i / period));
        }
        return values;
    }

    // Add noise to values
    void addNoise(std::vector<double>& values, double stddev, unsigned seed = 42) {
        std::mt19937 gen(seed);
        std::normal_distribution<> dist(0.0, stddev);
        for (double& v : values) {
            v += dist(gen);
        }
    }

    // Insert anomalies at specific positions
    void insertAnomalies(std::vector<double>& values, const std::vector<size_t>& positions, double deviation) {
        for (size_t pos : positions) {
            if (pos < values.size()) {
                values[pos] += deviation;
            }
        }
    }
};

// ==================== Algorithm Creation Tests ====================

TEST_F(AnomalyDetectionTest, CreateBasicDetector) {
    auto detector = createDetector(Algorithm::BASIC);
    ASSERT_NE(detector, nullptr);
    EXPECT_EQ(detector->algorithmName(), "basic");
    EXPECT_FALSE(detector->supportsSeasonality());
}

TEST_F(AnomalyDetectionTest, CreateAgileDetector) {
    auto detector = createDetector(Algorithm::AGILE);
    ASSERT_NE(detector, nullptr);
    EXPECT_EQ(detector->algorithmName(), "agile");
    EXPECT_TRUE(detector->supportsSeasonality());
}

TEST_F(AnomalyDetectionTest, CreateRobustDetector) {
    auto detector = createDetector(Algorithm::ROBUST);
    ASSERT_NE(detector, nullptr);
    EXPECT_EQ(detector->algorithmName(), "robust");
    EXPECT_TRUE(detector->supportsSeasonality());
}

// ==================== Basic Detector Tests ====================

TEST_F(AnomalyDetectionTest, BasicDetectorConstantSeries) {
    BasicDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;
    config.windowSize = 30;

    AnomalyInput input;
    input.timestamps = generateTimestamps(100);
    input.values = generateConstant(100, 50.0);

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.size(), 100);
    // All values should be within bounds for constant series
    size_t anomaliesAfterWarmup = 0;
    for (size_t i = config.minDataPoints; i < output.scores.size(); ++i) {
        if (output.scores[i] > 0) {
            ++anomaliesAfterWarmup;
        }
    }
    EXPECT_EQ(anomaliesAfterWarmup, 0);
}

TEST_F(AnomalyDetectionTest, BasicDetectorWithOutlier) {
    BasicDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;
    config.windowSize = 30;
    config.minDataPoints = 20;

    AnomalyInput input;
    input.timestamps = generateTimestamps(100);
    input.values = generateConstant(100, 50.0);
    addNoise(input.values, 1.0);

    // Insert a large outlier
    input.values[75] = 100.0;  // ~50 stddevs away

    auto output = detector.detect(input, config);

    EXPECT_GT(output.scores[75], 0);  // The outlier should have a positive score
    EXPECT_GE(output.anomalyCount, 1);
}

TEST_F(AnomalyDetectionTest, BasicDetectorLinearTrend) {
    BasicDetector detector;
    AnomalyConfig config;
    config.bounds = 3.0;
    config.windowSize = 20;
    config.minDataPoints = 15;

    AnomalyInput input;
    input.timestamps = generateTimestamps(100);
    input.values = generateLinearTrend(100, 0.0, 1.0);
    addNoise(input.values, 2.0);

    auto output = detector.detect(input, config);

    // Linear trend with noise shouldn't produce many anomalies with wide bounds
    EXPECT_LE(output.anomalyCount, 10);  // Allow for some edge cases
}

// ==================== Robust Detector Tests ====================

TEST_F(AnomalyDetectionTest, RobustDetectorConstantSeries) {
    RobustDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;

    AnomalyInput input;
    input.timestamps = generateTimestamps(100);
    input.values = generateConstant(100, 50.0);

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.size(), 100);
    EXPECT_EQ(output.anomalyCount, 0);
}

TEST_F(AnomalyDetectionTest, RobustDetectorWithSeasonality) {
    RobustDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;
    config.seasonality = Seasonality::HOURLY;  // 60 points = 1 hour

    AnomalyInput input;
    input.timestamps = generateTimestamps(300);
    input.values = generateSinusoidal(300, 50.0, 10.0, 60);  // 60-point period
    addNoise(input.values, 1.0);

    auto output = detector.detect(input, config);

    // Most points should be within bounds for well-behaved seasonal data
    EXPECT_LE(output.anomalyCount, 30);  // Allow some noise
}

// ==================== Agile Detector Tests ====================

TEST_F(AnomalyDetectionTest, AgileDetectorConstantSeries) {
    AgileDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;

    AnomalyInput input;
    input.timestamps = generateTimestamps(100);
    input.values = generateConstant(100, 50.0);

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.size(), 100);
    EXPECT_EQ(output.anomalyCount, 0);
}

TEST_F(AnomalyDetectionTest, AgileDetectorAdaptsToLevelShift) {
    AgileDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;
    config.minDataPoints = 20;
    config.windowSize = 30;

    AnomalyInput input;
    input.timestamps = generateTimestamps(200);

    // First half at level 50, second half at level 100
    input.values.resize(200);
    for (size_t i = 0; i < 100; ++i) {
        input.values[i] = 50.0;
    }
    for (size_t i = 100; i < 200; ++i) {
        input.values[i] = 100.0;
    }
    addNoise(input.values, 2.0);

    auto output = detector.detect(input, config);

    // Should detect the level shift but then adapt
    // After adaptation, anomaly count should stabilize
    size_t anomaliesInSecondHalf = 0;
    for (size_t i = 150; i < 200; ++i) {  // Last 50 points
        if (output.scores[i] > 0) {
            ++anomaliesInSecondHalf;
        }
    }
    // After adapting, there should be few anomalies
    EXPECT_LE(anomaliesInSecondHalf, 10);
}

// ==================== STL Decomposition Tests ====================

TEST_F(AnomalyDetectionTest, STLDecompositionBasic) {
    std::vector<double> values = generateSinusoidal(120, 100.0, 20.0, 12);  // Monthly pattern
    addNoise(values, 2.0);

    STLConfig config;
    config.seasonalPeriod = 12;
    config.seasonalWindow = 7;

    auto stl = STLDecomposition::decompose(values, config);

    EXPECT_EQ(stl.size(), 120);
    EXPECT_FALSE(stl.empty());

    // Verify decomposition: original ≈ trend + seasonal + residual
    for (size_t i = 0; i < values.size(); ++i) {
        double reconstructed = stl.trend[i] + stl.seasonal[i] + stl.residual[i];
        EXPECT_NEAR(reconstructed, values[i], 0.01);
    }
}

TEST_F(AnomalyDetectionTest, STLDecompositionConstant) {
    std::vector<double> values(100, 50.0);

    STLConfig config;
    config.seasonalPeriod = 0;  // No seasonality

    auto stl = STLDecomposition::decompose(values, config);

    EXPECT_EQ(stl.size(), 100);

    // For constant series, trend should be close to the constant value
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_NEAR(stl.trend[i], 50.0, 1.0);
    }
}

// ==================== Anomaly Executor Tests ====================

TEST_F(AnomalyDetectionTest, ExecutorBasicAlgorithm) {
    AnomalyExecutor executor;

    auto timestamps = generateTimestamps(100);
    auto values = generateConstant(100, 50.0);
    values[75] = 200.0;  // Insert anomaly

    std::vector<std::string> groupTags = {"host=server01"};

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto result = executor.execute(timestamps, values, groupTags, config);

    EXPECT_TRUE(result.success);
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.times.size(), 100);

    // Should have multiple series pieces
    EXPECT_GE(result.series.size(), 4);  // raw, upper, lower, scores

    // Check that we have the expected pieces
    EXPECT_NE(result.getPiece("raw"), nullptr);
    EXPECT_NE(result.getPiece("upper"), nullptr);
    EXPECT_NE(result.getPiece("lower"), nullptr);
    EXPECT_NE(result.getPiece("scores"), nullptr);

    // Statistics should be populated
    EXPECT_EQ(result.statistics.algorithm, "basic");
    EXPECT_EQ(result.statistics.totalPoints, 100);
}

TEST_F(AnomalyDetectionTest, ExecutorEmptyInput) {
    AnomalyExecutor executor;

    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::vector<std::string> groupTags;

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;

    auto result = executor.execute(timestamps, values, groupTags, config);

    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.empty());
}

TEST_F(AnomalyDetectionTest, ExecutorMultiSeries) {
    AnomalyExecutor executor;

    auto timestamps = generateTimestamps(100);
    std::vector<std::vector<double>> seriesValues = {
        generateConstant(100, 50.0),
        generateConstant(100, 100.0)
    };
    std::vector<std::vector<std::string>> seriesGroupTags = {
        {"host=server01"},
        {"host=server02"}
    };

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto result = executor.executeMulti(timestamps, seriesValues, seriesGroupTags, config);

    EXPECT_TRUE(result.success);
    // Should have pieces for both series
    EXPECT_GE(result.series.size(), 8);  // 4 pieces × 2 series
}

// ==================== Algorithm Config Tests ====================

TEST_F(AnomalyDetectionTest, ParseAlgorithmStrings) {
    EXPECT_EQ(parseAlgorithm("basic"), Algorithm::BASIC);
    EXPECT_EQ(parseAlgorithm("agile"), Algorithm::AGILE);
    EXPECT_EQ(parseAlgorithm("robust"), Algorithm::ROBUST);

    EXPECT_THROW(parseAlgorithm("invalid"), std::invalid_argument);
}

TEST_F(AnomalyDetectionTest, ParseSeasonalityStrings) {
    EXPECT_EQ(parseSeasonality(""), Seasonality::NONE);
    EXPECT_EQ(parseSeasonality("none"), Seasonality::NONE);
    EXPECT_EQ(parseSeasonality("hourly"), Seasonality::HOURLY);
    EXPECT_EQ(parseSeasonality("daily"), Seasonality::DAILY);
    EXPECT_EQ(parseSeasonality("weekly"), Seasonality::WEEKLY);

    EXPECT_THROW(parseSeasonality("monthly"), std::invalid_argument);
}

TEST_F(AnomalyDetectionTest, SeasonalityToPeriod) {
    // With 1-minute intervals
    uint64_t oneMinuteNs = 60000000000ULL;

    EXPECT_EQ(seasonalityToPeriod(Seasonality::NONE, oneMinuteNs), 0);
    EXPECT_EQ(seasonalityToPeriod(Seasonality::HOURLY, oneMinuteNs), 60);   // 60 mins
    EXPECT_EQ(seasonalityToPeriod(Seasonality::DAILY, oneMinuteNs), 1440);  // 24 * 60
    EXPECT_EQ(seasonalityToPeriod(Seasonality::WEEKLY, oneMinuteNs), 10080); // 7 * 24 * 60
}

TEST_F(AnomalyDetectionTest, AlgorithmToString) {
    EXPECT_EQ(algorithmToString(Algorithm::BASIC), "basic");
    EXPECT_EQ(algorithmToString(Algorithm::AGILE), "agile");
    EXPECT_EQ(algorithmToString(Algorithm::ROBUST), "robust");
}

// ==================== Numerical Robustness Tests ====================

TEST_F(AnomalyDetectionTest, IncrementalRollingStatsM2ClampingNoNaN) {
    // Test that M2 clamping prevents NaN from negative M2 values.
    // The inverse Welford update (removing oldest value from a sliding window)
    // can cause M2 to go slightly negative due to floating-point rounding.
    // This test constructs a scenario that stresses the M2 accumulator.

    simd::IncrementalRollingStats stats(5);  // Small window to stress removal path

    // Feed in values that are designed to create floating-point rounding issues.
    // Alternating between very similar values can cause M2 to drift negative
    // when the oldest value is removed from the window.
    double baseValue = 1e15;  // Large base value amplifies rounding errors
    std::vector<double> testValues;
    for (int i = 0; i < 100; ++i) {
        // Tiny perturbations around a large base value
        testValues.push_back(baseValue + (i % 3) * 1e-5);
    }

    for (double v : testValues) {
        stats.update(v);

        // The key assertion: stddev should never be NaN
        double sd = stats.stddev();
        EXPECT_FALSE(std::isnan(sd))
            << "stddev() returned NaN after update with value " << v;

        // variance should also never be negative
        double var = stats.variance();
        EXPECT_GE(var, 0.0)
            << "variance() returned negative value " << var << " after update with value " << v;

        // mean should always be finite
        EXPECT_TRUE(std::isfinite(stats.mean()))
            << "mean() is not finite after update with value " << v;
    }
}

TEST_F(AnomalyDetectionTest, IncrementalRollingStatsM2ClampingConstantValues) {
    // Constant values should produce exactly zero variance/stddev.
    // This is another edge case where M2 can go slightly negative during
    // the window sliding due to floating-point arithmetic.

    simd::IncrementalRollingStats stats(10);

    for (int i = 0; i < 50; ++i) {
        stats.update(42.0);

        double sd = stats.stddev();
        EXPECT_FALSE(std::isnan(sd))
            << "stddev() returned NaN for constant value series at iteration " << i;
        EXPECT_GE(stats.variance(), 0.0)
            << "variance() negative for constant value series at iteration " << i;
    }

    // After enough constant values, variance should be very close to zero
    EXPECT_NEAR(stats.variance(), 0.0, 1e-10);
    EXPECT_NEAR(stats.stddev(), 0.0, 1e-5);
}

TEST_F(AnomalyDetectionTest, IncrementalRollingStatsWindowSlidingProducesFinite) {
    // Comprehensive test: after the window is full and values start getting
    // removed (buffer full), all stats should remain finite.

    simd::IncrementalRollingStats stats(20);

    std::mt19937 gen(42);
    std::normal_distribution<> dist(100.0, 5.0);

    for (int i = 0; i < 200; ++i) {
        stats.update(dist(gen));

        EXPECT_FALSE(std::isnan(stats.mean()));
        EXPECT_FALSE(std::isnan(stats.stddev()));
        EXPECT_FALSE(std::isinf(stats.stddev()));
        EXPECT_GE(stats.variance(), 0.0);
    }
}

TEST_F(AnomalyDetectionTest, BasicDetectorProducesNoNaN) {
    // End-to-end test: BasicDetector should never produce NaN values
    // in its output, even with edge-case input data.

    BasicDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;
    config.windowSize = 10;
    config.minDataPoints = 5;

    AnomalyInput input;
    input.timestamps = generateTimestamps(100);
    // Use values designed to stress the rolling stats
    input.values.resize(100);
    for (size_t i = 0; i < 100; ++i) {
        input.values[i] = 1e10 + (i % 2) * 1e-8;
    }

    auto output = detector.detect(input, config);

    for (size_t i = 0; i < output.size(); ++i) {
        EXPECT_FALSE(std::isnan(output.predictions[i]))
            << "prediction is NaN at index " << i;
        EXPECT_FALSE(std::isnan(output.scores[i]))
            << "score is NaN at index " << i;
        EXPECT_FALSE(std::isnan(output.upper[i]))
            << "upper bound is NaN at index " << i;
        EXPECT_FALSE(std::isnan(output.lower[i]))
            << "lower bound is NaN at index " << i;
    }
}

TEST_F(AnomalyDetectionTest, AgileDetectorShortDataFallback) {
    // Test that the agile detector handles short data gracefully
    // when the seasonal period is larger than the data length.
    // It should fall back to non-seasonal detection instead of
    // producing garbage bounds.

    AgileDetector detector;
    AnomalyConfig config;
    config.bounds = 2.0;
    config.seasonality = Seasonality::WEEKLY;  // Weekly = 10080 for 1-min data
    config.minDataPoints = 10;
    config.windowSize = 20;

    AnomalyInput input;
    input.timestamps = generateTimestamps(50);  // Only 50 points, far less than weekly period
    input.values = generateConstant(50, 100.0);
    addNoise(input.values, 2.0);
    input.values[40] = 200.0;  // Insert obvious anomaly

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.size(), 50);

    // Should still detect anomalies (fell back to non-seasonal)
    // rather than producing all-infinity bounds
    bool hasFiniteBounds = false;
    for (size_t i = 0; i < output.size(); ++i) {
        if (std::isfinite(output.upper[i]) && std::isfinite(output.lower[i])) {
            hasFiniteBounds = true;
            break;
        }
        // No NaN anywhere
        EXPECT_FALSE(std::isnan(output.upper[i]));
        EXPECT_FALSE(std::isnan(output.lower[i]));
        EXPECT_FALSE(std::isnan(output.scores[i]));
        EXPECT_FALSE(std::isnan(output.predictions[i]));
    }
    EXPECT_TRUE(hasFiniteBounds)
        << "Agile detector should produce finite bounds for short data by falling back to non-seasonal";

    // The anomaly at index 40 should still be detected
    EXPECT_GT(output.scores[40], 0.0)
        << "Should detect the outlier even with short data";
}

// ==================== Welford Drift Accuracy Test ====================

TEST_F(AnomalyDetectionTest, IncrementalRollingStatsNoDriftOnLongSeries) {
    // Test that periodic recomputation prevents drift over a long series.
    // We run a 15K-point series through IncrementalRollingStats and compare
    // its rolling mean/variance against a brute-force recomputation from
    // the window buffer at multiple checkpoints. Without periodic
    // recomputation, the inverse Welford update accumulates floating-point
    // errors that grow with the number of updates.

    const size_t windowSize = 100;
    const size_t totalPoints = 15000;

    simd::IncrementalRollingStats stats(windowSize);

    // Generate a series with known anomalies embedded in noisy data.
    // Use a deterministic seed for reproducibility.
    std::mt19937 gen(12345);
    std::normal_distribution<> noise(0.0, 5.0);

    std::vector<double> allValues;
    allValues.reserve(totalPoints);

    // Known anomaly positions
    std::vector<size_t> anomalyPositions = {
        500, 2000, 5000, 7500, 10000, 12000, 14000
    };
    std::set<size_t> anomalySet(anomalyPositions.begin(), anomalyPositions.end());

    for (size_t i = 0; i < totalPoints; ++i) {
        double value = 100.0 + noise(gen);
        if (anomalySet.count(i)) {
            value += 500.0;  // Large spike anomaly
        }
        allValues.push_back(value);
    }

    // Feed all values and check accuracy at intervals
    for (size_t i = 0; i < totalPoints; ++i) {
        stats.update(allValues[i]);

        // Check at every 1000th point after the window is full
        if (i >= windowSize && (i % 1000 == 0)) {
            // Brute-force compute mean and variance from the last windowSize values
            size_t start = i + 1 - windowSize;
            double bruteSum = 0.0;
            for (size_t j = start; j <= i; ++j) {
                bruteSum += allValues[j];
            }
            double bruteMean = bruteSum / static_cast<double>(windowSize);

            double bruteM2 = 0.0;
            for (size_t j = start; j <= i; ++j) {
                double diff = allValues[j] - bruteMean;
                bruteM2 += diff * diff;
            }
            double bruteVariance = bruteM2 / static_cast<double>(windowSize - 1);

            // The incremental stats should closely match the brute-force values.
            // With periodic recomputation, relative error should stay tiny.
            double meanErr = std::abs(stats.mean() - bruteMean);
            double varErr = std::abs(stats.variance() - bruteVariance);

            // Mean should be accurate to within 1e-9 relative error
            double meanRelErr = (bruteMean != 0.0) ? meanErr / std::abs(bruteMean) : meanErr;
            EXPECT_LT(meanRelErr, 1e-9)
                << "Mean drift too large at point " << i
                << ": incremental=" << stats.mean() << " brute=" << bruteMean;

            // Variance should be accurate to within 1e-6 relative error
            double varRelErr = (bruteVariance > 1e-10) ? varErr / bruteVariance : varErr;
            EXPECT_LT(varRelErr, 1e-6)
                << "Variance drift too large at point " << i
                << ": incremental=" << stats.variance() << " brute=" << bruteVariance;

            // stddev should never be NaN
            EXPECT_FALSE(std::isnan(stats.stddev()));
        }
    }

    // Final check: variance should be positive and finite
    EXPECT_GT(stats.variance(), 0.0);
    EXPECT_TRUE(std::isfinite(stats.variance()));
    EXPECT_TRUE(std::isfinite(stats.mean()));
}

TEST_F(AnomalyDetectionTest, BasicDetectorAccurateOnLongSeriesWithKnownAnomalies) {
    // End-to-end test: run BasicDetector on 10K+ points with known anomalies
    // and verify that all anomalies are detected without false negatives
    // growing over time (which would indicate Welford drift).

    BasicDetector detector;
    AnomalyConfig config;
    config.bounds = 3.0;
    config.windowSize = 50;
    config.minDataPoints = 30;

    const size_t totalPoints = 12000;
    AnomalyInput input;
    input.timestamps = generateTimestamps(totalPoints);

    // Stable series with small noise
    std::mt19937 gen(99);
    std::normal_distribution<> noise(0.0, 2.0);
    input.values.resize(totalPoints);
    for (size_t i = 0; i < totalPoints; ++i) {
        input.values[i] = 50.0 + noise(gen);
    }

    // Insert large anomalies at known positions spread across the series.
    // If drift is occurring, later anomalies would be missed.
    std::vector<size_t> anomalyPositions = {
        200, 1000, 3000, 5000, 7000, 9000, 11000
    };
    for (size_t pos : anomalyPositions) {
        input.values[pos] = 200.0;  // ~75 stddevs above mean
    }

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.size(), totalPoints);

    // Every injected anomaly should be detected (positive score)
    for (size_t pos : anomalyPositions) {
        EXPECT_GT(output.scores[pos], 0.0)
            << "Failed to detect known anomaly at position " << pos
            << " (may indicate Welford drift degrading accuracy over time)";
    }

    // No NaN values anywhere in the output
    for (size_t i = 0; i < output.size(); ++i) {
        EXPECT_FALSE(std::isnan(output.predictions[i]))
            << "NaN prediction at index " << i;
        EXPECT_FALSE(std::isnan(output.scores[i]))
            << "NaN score at index " << i;
    }
}

// ==================== CPUID Caching Test ====================

TEST_F(AnomalyDetectionTest, CpuidCachingReturnsConsistentResults) {
    // Verify that isAvx2Available() and isAvx512Available() return the
    // same result on repeated calls. This validates that the static
    // caching works correctly and the functions are idempotent.

    bool avx2_first = simd::isAvx2Available();
    bool avx2_second = simd::isAvx2Available();
    EXPECT_EQ(avx2_first, avx2_second)
        << "isAvx2Available() returned inconsistent results";

    bool avx512_first = simd::isAvx512Available();
    bool avx512_second = simd::isAvx512Available();
    EXPECT_EQ(avx512_first, avx512_second)
        << "isAvx512Available() returned inconsistent results";

    // Call many more times to be thorough
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(simd::isAvx2Available(), avx2_first);
        EXPECT_EQ(simd::isAvx512Available(), avx512_first);
    }
}
