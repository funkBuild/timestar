#include "anomaly/agile_detector.hpp"
#include "anomaly/anomaly_detector.hpp"
#include "anomaly/anomaly_executor.hpp"
#include "anomaly/anomaly_result.hpp"
#include "anomaly/basic_detector.hpp"
#include "anomaly/robust_detector.hpp"
#include "anomaly/simd_anomaly.hpp"
#include "anomaly/stl_decomposition.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <random>
#include <set>
#include <vector>

using namespace timestar::anomaly;

class AnomalyDetectionTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    // Generate timestamps with 1-minute intervals
    std::vector<uint64_t> generateTimestamps(size_t count, uint64_t startNs = 1704067200000000000ULL) {
        std::vector<uint64_t> timestamps;
        timestamps.reserve(count);
        uint64_t interval = 60000000000ULL;  // 1 minute in nanoseconds
        for (size_t i = 0; i < count; ++i) {
            timestamps.push_back(startNs + i * interval);
        }
        return timestamps;
    }

    // Generate constant values
    std::vector<double> generateConstant(size_t count, double value) { return std::vector<double>(count, value); }

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
    std::vector<std::vector<double>> seriesValues = {generateConstant(100, 50.0), generateConstant(100, 100.0)};
    std::vector<std::vector<std::string>> seriesGroupTags = {{"host=server01"}, {"host=server02"}};

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto result = executor.executeMulti(timestamps, seriesValues, seriesGroupTags, config);

    EXPECT_TRUE(result.success);
    // Should have pieces for both series
    EXPECT_GE(result.series.size(), 8);  // 4 pieces × 2 series
}

TEST_F(AnomalyDetectionTest, ExecutorMultiSeriesMismatchedSizesThrows) {
    // seriesValues has 3 entries but seriesGroupTags has only 2.
    // executeMulti must throw std::invalid_argument rather than silently
    // using empty tags for the third series, which would lose tag data.
    AnomalyExecutor executor;

    auto timestamps = generateTimestamps(100);
    std::vector<std::vector<double>> seriesValues = {generateConstant(100, 50.0), generateConstant(100, 75.0),
                                                     generateConstant(100, 100.0)};
    // Intentionally only 2 tag sets for 3 series
    std::vector<std::vector<std::string>> seriesGroupTags = {{"host=server01"}, {"host=server02"}};

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    EXPECT_THROW(executor.executeMulti(timestamps, seriesValues, seriesGroupTags, config), std::invalid_argument);
}

// R4: the two entry points must agree.  execute() reported totalPoints as the
// ROW WIDTH while executeMulti() counted non-NaN observations, so the same
// series answered through the two doors reported different totals -- and the
// finite-point gate executeMulti grew was missing from execute() entirely.
// execute() is now defined as the one-series case of executeMulti, so any
// future divergence has to be introduced deliberately.
TEST_F(AnomalyDetectionTest, ExecuteAgreesWithExecuteMultiOnASingleSeries) {
    AnomalyExecutor executor;

    auto timestamps = generateTimestamps(60);
    auto values = generateConstant(60, 50.0);
    values[40] = 250.0;  // an anomaly, so anomalyCount is non-trivial
    // ...and genuine gaps, which is where the two used to disagree.
    for (size_t i : {5u, 6u, 17u, 33u}) {
        values[i] = std::numeric_limits<double>::quiet_NaN();
    }

    std::vector<std::string> groupTags = {"host=server01"};

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto single = executor.execute(timestamps, values, groupTags, config);
    auto multi = executor.executeMulti(timestamps, {values}, {groupTags}, config);

    ASSERT_TRUE(single.success);
    ASSERT_TRUE(multi.success);
    EXPECT_EQ(single.statistics.totalPoints, multi.statistics.totalPoints);
    EXPECT_EQ(single.statistics.totalPoints, 56u) << "60 slots, 4 of them missing";
    EXPECT_EQ(single.statistics.anomalyCount, multi.statistics.anomalyCount);
    EXPECT_EQ(single.statistics.declinedSeriesCount, multi.statistics.declinedSeriesCount);
    EXPECT_EQ(single.statistics.algorithm, multi.statistics.algorithm);
    EXPECT_EQ(single.series.size(), multi.series.size());
    EXPECT_EQ(single.times, multi.times);

    for (size_t i = 0; i < single.series.size(); ++i) {
        EXPECT_EQ(single.series[i].piece, multi.series[i].piece);
        EXPECT_EQ(single.series[i].groupTags, multi.series[i].groupTags);
        EXPECT_EQ(single.series[i].values.size(), multi.series[i].values.size());
    }
}

// R3: a series with fewer OBSERVATIONS than minDataPoints is declined rather
// than given an envelope derived from a handful of samples.  The warm-up is
// indexed by slot, so on a mostly-NaN row it stops protecting anything.
TEST_F(AnomalyDetectionTest, ExecutorDeclinesASeriesWithTooFewObservations) {
    AnomalyExecutor executor;

    auto timestamps = generateTimestamps(40);
    std::vector<double> oneObservation(40, std::numeric_limits<double>::quiet_NaN());
    oneObservation[3] = 900.0;
    auto dense = generateConstant(40, 50.0);

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto result = executor.executeMulti(timestamps, {dense, oneObservation}, {{"dev=FULL"}, {"dev=SPARSE"}}, config);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.statistics.declinedSeriesCount, 1u);
    EXPECT_EQ(result.statistics.totalPoints, 40u) << "only the answered group's points count";
    for (const auto& piece : result.series) {
        ASSERT_FALSE(piece.groupTags.empty());
        EXPECT_EQ(piece.groupTags[0], "dev=FULL")
            << "REGRESSION: a group with one observation was given a confidence envelope";
    }
}

// Phase 3.7 / S1: the gate above does not fire below the warm-up length.  A
// SHORT series is answered however short it is, because it is the shape the
// ordinary single-series path produces, where the axis IS the series' own
// timestamps.
//
// Gating on length instead threw data away: a dense 5-point series used to come
// back with `raw` and `predictions` carrying the STORED VALUES (under BASIC,
// used here, with an honest all-null envelope since it never leaves warm-up;
// ROBUST has no warm-up and produces a finite band even at n = 3), and Phase
// 3.6 answered `"series": []` for it.  A client plotting a 5-minute panel at
// one-minute resolution, or any series younger than minDataPoints intervals,
// lost its data to a guard aimed at fan-out.
TEST_F(AnomalyDetectionTest, ADenseSeriesShorterThanMinDataPointsIsAnsweredNotDeclined) {
    AnomalyExecutor executor;

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 3.0;
    ASSERT_EQ(config.minDataPoints, 10u) << "this test is about lengths BELOW the default warm-up";

    for (size_t n : {1u, 5u, 8u, 9u}) {
        auto timestamps = generateTimestamps(n);
        std::vector<double> values;
        for (size_t i = 0; i < n; ++i) {
            values.push_back(10.0 + static_cast<double>(i));
        }

        auto result = executor.executeMulti(timestamps, {values}, {{"dev=SHORT"}}, config);

        ASSERT_TRUE(result.success) << result.errorMessage;
        EXPECT_EQ(result.statistics.declinedSeriesCount, 0u) << "n=" << n << ": a dense series must not be declined";
        EXPECT_EQ(result.statistics.totalPoints, n) << "n=" << n;
        ASSERT_FALSE(result.series.empty()) << "n=" << n << ": REGRESSION: the stored values were dropped";

        // The stored values come back verbatim under `raw`.
        const auto* raw = [&]() -> const AnomalySeriesPiece* {
            for (const auto& piece : result.series) {
                if (piece.piece == "raw") {
                    return &piece;
                }
            }
            return nullptr;
        }();
        ASSERT_NE(raw, nullptr) << "n=" << n;
        ASSERT_EQ(raw->values.size(), n);
        for (size_t i = 0; i < n; ++i) {
            EXPECT_DOUBLE_EQ(raw->values[i], 10.0 + static_cast<double>(i)) << "n=" << n << " i=" << i;
        }
    }
}

// Phase 3.8 / F1: the BOUNDARY the two tests above jump straight over.
//
// Phase 3.7's gate was `finitePoints < values.size() && finitePoints <
// minDataPoints` -- ANY padding at all, however short the row.  So the same
// nine observations were answered in full on a 9-slot axis and declined
// outright (`"series": []`, total_points 0, declined 1) on a 10-slot one.  One
// missing slot flipped the answer from the data to nothing, and it bought
// nothing: a 10-slot row is entirely inside BASIC's warm-up, so its envelope is
// all-null and its scores all zero either way.  MEASURED against a pre-campaign
// build, which answered every row in this loop.
//
// The gate is now `values.size() > minDataPoints && finitePoints <
// minDataPoints`: a row no longer than the warm-up is never declined, whatever
// it holds.
TEST_F(AnomalyDetectionTest, ARowNoLongerThanTheWarmUpIsNeverDeclinedHoweverPaddedItIs) {
    AnomalyExecutor executor;

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 3.0;
    ASSERT_EQ(config.minDataPoints, 10u) << "the boundary under test is minDataPoints itself";

    // slots x gaps, walked across the boundary at slots == minDataPoints.
    struct Case {
        size_t slots;
        size_t gaps;
        bool declined;
    };
    const Case cases[] = {
        {9, 0, false},   // dense 9 -- answered before and after
        {9, 1, false},   // 8 of 9   -- 3.7 declined this
        {10, 1, false},  // 9 of 10  -- 3.7 declined this: the reported bug
        {10, 0, false},  // dense 10 -- at the warm-up length
        {11, 1, false},  // 10 of 11 -- enough observations on its own
        {12, 3, true},   // 9 of 12  -- past the warm-up AND short of it
        {40, 31, true},  // 9 of 40  -- the fan-out case the gate exists for
        {40, 0, false},  // dense 40
    };

    for (const auto& c : cases) {
        auto timestamps = generateTimestamps(c.slots);
        std::vector<double> values(c.slots, std::numeric_limits<double>::quiet_NaN());
        // Put the gaps first so the finite points are contiguous at the end.
        for (size_t i = c.gaps; i < c.slots; ++i) {
            values[i] = 10.0 + static_cast<double>(i);
        }

        auto result = executor.executeMulti(timestamps, {values}, {{"dev=D1"}}, config);

        ASSERT_TRUE(result.success) << result.errorMessage;
        EXPECT_EQ(result.statistics.declinedSeriesCount, c.declined ? 1u : 0u)
            << "slots=" << c.slots << " gaps=" << c.gaps;
        EXPECT_EQ(result.series.empty(), c.declined) << "slots=" << c.slots << " gaps=" << c.gaps;
        if (!c.declined) {
            EXPECT_EQ(result.statistics.totalPoints, c.slots - c.gaps)
                << "slots=" << c.slots << " gaps=" << c.gaps << ": only the real observations count";
        }
    }
}

// The same rule from the other side: past the warm-up length a row IS gated on
// its observation count, so a long sparse row is declined even though it is
// longer than a dense row that is answered.  9 dense points are answered; 9
// observations spread over 40 slots are not.
TEST_F(AnomalyDetectionTest, PaddingNotLengthDecidesWhetherASeriesIsDeclined) {
    AnomalyExecutor executor;

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 3.0;

    auto shortDense = generateTimestamps(9);
    std::vector<double> dense(9, 42.0);
    auto denseResult = executor.executeMulti(shortDense, {dense}, {{"dev=DENSE"}}, config);
    ASSERT_TRUE(denseResult.success);
    EXPECT_EQ(denseResult.statistics.declinedSeriesCount, 0u);
    EXPECT_FALSE(denseResult.series.empty());

    auto longAxis = generateTimestamps(40);
    std::vector<double> sparse(40, std::numeric_limits<double>::quiet_NaN());
    for (size_t i = 0; i < 9; ++i) {
        sparse[i * 4] = 42.0;
    }
    auto sparseResult = executor.executeMulti(longAxis, {sparse}, {{"dev=SPARSE"}}, config);
    ASSERT_TRUE(sparseResult.success);
    EXPECT_EQ(sparseResult.statistics.declinedSeriesCount, 1u)
        << "REGRESSION: 9 observations on a 40-slot axis were given an envelope";
    EXPECT_TRUE(sparseResult.series.empty());
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
    EXPECT_EQ(seasonalityToPeriod(Seasonality::HOURLY, oneMinuteNs), 60);     // 60 mins
    EXPECT_EQ(seasonalityToPeriod(Seasonality::DAILY, oneMinuteNs), 1440);    // 24 * 60
    EXPECT_EQ(seasonalityToPeriod(Seasonality::WEEKLY, oneMinuteNs), 10080);  // 7 * 24 * 60
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
        EXPECT_FALSE(std::isnan(sd)) << "stddev() returned NaN after update with value " << v;

        // variance should also never be negative
        double var = stats.variance();
        EXPECT_GE(var, 0.0) << "variance() returned negative value " << var << " after update with value " << v;

        // mean should always be finite
        EXPECT_TRUE(std::isfinite(stats.mean())) << "mean() is not finite after update with value " << v;
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
        EXPECT_FALSE(std::isnan(sd)) << "stddev() returned NaN for constant value series at iteration " << i;
        EXPECT_GE(stats.variance(), 0.0) << "variance() negative for constant value series at iteration " << i;
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
        EXPECT_FALSE(std::isnan(output.predictions[i])) << "prediction is NaN at index " << i;
        EXPECT_FALSE(std::isnan(output.scores[i])) << "score is NaN at index " << i;
        EXPECT_FALSE(std::isnan(output.upper[i])) << "upper bound is NaN at index " << i;
        EXPECT_FALSE(std::isnan(output.lower[i])) << "lower bound is NaN at index " << i;
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
    EXPECT_GT(output.scores[40], 0.0) << "Should detect the outlier even with short data";
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
    std::vector<size_t> anomalyPositions = {500, 2000, 5000, 7500, 10000, 12000, 14000};
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
            EXPECT_LT(meanRelErr, 1e-9) << "Mean drift too large at point " << i << ": incremental=" << stats.mean()
                                        << " brute=" << bruteMean;

            // Variance should be accurate to within 1e-6 relative error
            double varRelErr = (bruteVariance > 1e-10) ? varErr / bruteVariance : varErr;
            EXPECT_LT(varRelErr, 1e-6) << "Variance drift too large at point " << i
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
    std::vector<size_t> anomalyPositions = {200, 1000, 3000, 5000, 7000, 9000, 11000};
    for (size_t pos : anomalyPositions) {
        input.values[pos] = 200.0;  // ~75 stddevs above mean
    }

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.size(), totalPoints);

    // Every injected anomaly should be detected (positive score)
    for (size_t pos : anomalyPositions) {
        EXPECT_GT(output.scores[pos], 0.0) << "Failed to detect known anomaly at position " << pos
                                           << " (may indicate Welford drift degrading accuracy over time)";
    }

    // No NaN values anywhere in the output
    for (size_t i = 0; i < output.size(); ++i) {
        EXPECT_FALSE(std::isnan(output.predictions[i])) << "NaN prediction at index " << i;
        EXPECT_FALSE(std::isnan(output.scores[i])) << "NaN score at index " << i;
    }
}

TEST_F(AnomalyDetectionTest, RobustIsCausalAndDoesNotFlagNeighboursOfSpike) {
    RobustDetector detector;
    AnomalyConfig config;
    AnomalyInput input;
    input.timestamps = generateTimestamps(180);
    input.values = generateConstant(180, 60);
    for (size_t i = 0; i < 180; ++i)
        input.values[i] += 0.3 * std::sin(i * 2.7);
    auto prefix = input;
    prefix.timestamps.resize(120);
    prefix.values.resize(120);
    auto before = detector.detect(prefix, config);
    for (size_t i = 120; i < 124; ++i)
        input.values[i] += 25;
    auto after = detector.detect(input, config);
    for (size_t i = 0; i < 120; ++i) {
        EXPECT_DOUBLE_EQ(before.predictions[i], after.predictions[i]);
        EXPECT_DOUBLE_EQ(before.upper[i], after.upper[i]);
        EXPECT_DOUBLE_EQ(before.lower[i], after.lower[i]);
        EXPECT_DOUBLE_EQ(before.scores[i], after.scores[i]);
    }
    for (size_t i = 0; i < 180; ++i) {
        if (i >= 120 && i < 124)
            EXPECT_GT(after.scores[i], 0) << i;
        else
            EXPECT_EQ(after.scores[i], 0) << i;
    }
}

TEST_F(AnomalyDetectionTest, RobustSeasonalOutputIsPrefixInvariant) {
    RobustDetector detector;
    AnomalyConfig config;
    config.seasonality = Seasonality::HOURLY;
    AnomalyInput input;
    input.timestamps = generateTimestamps(360);
    input.values = generateSinusoidal(360, 50, 10, 60);
    auto prefix = input;
    prefix.timestamps.resize(210);
    prefix.values.resize(210);
    auto before = detector.detect(prefix, config);
    input.values[210] += 50;
    auto after = detector.detect(input, config);
    for (size_t i = 0; i < 210; ++i) {
        EXPECT_DOUBLE_EQ(before.predictions[i], after.predictions[i]);
        EXPECT_DOUBLE_EQ(before.upper[i], after.upper[i]);
        EXPECT_DOUBLE_EQ(before.scores[i], after.scores[i]);
    }
    EXPECT_GT(after.scores[210], 0);
}

TEST_F(AnomalyDetectionTest, AgileScoresBeforeLearningSpike) {
    AgileDetector detector;
    AnomalyConfig config;
    AnomalyInput input;
    input.timestamps = generateTimestamps(180);
    input.values = generateConstant(180, 60);
    input.values[100] = 1000;
    auto result = detector.detect(input, config);
    EXPECT_NEAR(result.upper[100], 61.2, 1e-8);
    EXPECT_GT(result.scores[100], 900);
    EXPECT_LT(result.predictions[101], 62);
}

TEST_F(AnomalyDetectionTest, BasicWarmupCountsFiniteObservations) {
    BasicDetector detector;
    AnomalyConfig config;
    config.minDataPoints = 10;
    std::vector<double> values(30, std::numeric_limits<double>::quiet_NaN());
    for (size_t i = 15; i < 25; ++i)
        values[i] = 60;
    values[25] = std::numeric_limits<double>::infinity();
    values[26] = 1000;
    values[27] = 60;
    AnomalyInput input{generateTimestamps(values.size()), values};
    auto result = detector.detect(input, config);
    for (size_t i = 0; i < 26; ++i)
        EXPECT_EQ(result.scores[i], 0);
    EXPECT_TRUE(std::isinf(result.upper[24]));
    EXPECT_TRUE(std::isnan(result.upper[25]));
    EXPECT_GT(result.scores[26], 900);
    EXPECT_TRUE(std::isfinite(result.upper[27]));
}

TEST_F(AnomalyDetectionTest, AgileDoesNotUseFutureValuesDuringInitialization) {
    AgileDetector detector;
    AnomalyConfig config;
    std::vector<double> values(100, 60);
    values[25] = 1000;
    AnomalyInput full{generateTimestamps(values.size()), values};
    AnomalyInput prefix{generateTimestamps(20), std::vector<double>(values.begin(), values.begin() + 20)};
    const auto before = detector.detect(prefix, config);
    const auto after = detector.detect(full, config);
    for (size_t i = 0; i < prefix.size(); ++i) {
        EXPECT_EQ(before.predictions[i], after.predictions[i]);
        EXPECT_EQ(before.upper[i], after.upper[i]);
        EXPECT_EQ(before.scores[i], after.scores[i]);
    }
}
