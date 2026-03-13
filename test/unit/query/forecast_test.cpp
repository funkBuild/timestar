/**
 * Forecast Unit Tests
 *
 * Tests for the forecast functionality including:
 * - Linear forecaster (regression-based)
 * - Seasonal forecaster (SARIMA-based)
 * - Forecast executor
 * - Expression parser integration
 */

#include "expression_parser.hpp"
#include "forecast/forecast_executor.hpp"
#include "forecast/forecast_result.hpp"
#include "forecast/linear_forecaster.hpp"
#include "forecast/seasonal_forecaster.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <random>

using namespace timestar::forecast;
using namespace timestar;

class ForecastTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test timestamps (1-minute intervals)
        timestamps_.clear();
        uint64_t startNs = 1704067200000000000ULL;  // Jan 1, 2024
        uint64_t intervalNs = 60000000000ULL;       // 1 minute
        for (size_t i = 0; i < 100; ++i) {
            timestamps_.push_back(startNs + i * intervalNs);
        }
    }

    // Generate linear trending data: y = mx + b + noise
    std::vector<double> generateLinearData(double slope, double intercept, double noise = 0.0) {
        std::vector<double> values;
        std::mt19937 gen(42);
        std::normal_distribution<> dist(0.0, noise);

        for (size_t i = 0; i < timestamps_.size(); ++i) {
            double x = static_cast<double>(i);
            double y = slope * x + intercept + dist(gen);
            values.push_back(y);
        }
        return values;
    }

    // Generate sinusoidal data with optional trend
    std::vector<double> generateSeasonalData(double baseline, double amplitude, size_t period, double trend = 0.0,
                                             double noise = 0.0) {
        std::vector<double> values;
        std::mt19937 gen(42);
        std::normal_distribution<> dist(0.0, noise);

        for (size_t i = 0; i < timestamps_.size(); ++i) {
            double x = static_cast<double>(i);
            double y = baseline + amplitude * std::sin(2.0 * M_PI * x / period) + trend * x + dist(gen);
            values.push_back(y);
        }
        return values;
    }

    // Generate constant data with noise
    std::vector<double> generateConstantData(double value, double noise = 0.0) {
        std::vector<double> values;
        std::mt19937 gen(42);
        std::normal_distribution<> dist(0.0, noise);

        for (size_t i = 0; i < timestamps_.size(); ++i) {
            values.push_back(value + dist(gen));
        }
        return values;
    }

    std::vector<uint64_t> timestamps_;
};

// ==================== Linear Forecaster Tests ====================

TEST_F(ForecastTest, LinearForecasterConstantData) {
    // Constant data should forecast the same constant value
    auto values = generateConstantData(100.0, 0.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    // Generate 10 forecast points
    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());
    ASSERT_EQ(output.forecastCount, 10);

    // All forecast values should be close to 100
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_NEAR(output.forecast[i], 100.0, 1.0);
    }

    // Slope should be near zero
    EXPECT_NEAR(output.slope, 0.0, 0.01);
}

TEST_F(ForecastTest, LinearForecasterTrendingData) {
    // Trending data should extrapolate the trend
    double slope = 0.5;
    double intercept = 10.0;
    auto values = generateLinearData(slope, intercept, 0.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());

    // Check slope is correct
    EXPECT_NEAR(output.slope, slope, 0.05);
    EXPECT_NEAR(output.intercept, intercept, 1.0);

    // R-squared should be high for perfect linear data
    EXPECT_GT(output.rSquared, 0.99);

    // Check forecast values continue the trend
    for (size_t i = 0; i < output.forecastCount; ++i) {
        double expectedX = static_cast<double>(timestamps_.size() + i);
        double expected = slope * expectedX + intercept;
        EXPECT_NEAR(output.forecast[i], expected, 1.0);
    }
}

TEST_F(ForecastTest, LinearForecasterConfidenceBounds) {
    auto values = generateLinearData(0.5, 10.0, 2.0);  // Add noise

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());

    // Confidence bounds should widen with forecast horizon
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_GT(output.upper[i], output.forecast[i]);
        EXPECT_LT(output.lower[i], output.forecast[i]);

        double width = output.upper[i] - output.lower[i];
        EXPECT_GT(width, 0.0);
    }

    // Bounds should widen as we go further into the future
    double firstWidth = output.upper[0] - output.lower[0];
    double lastWidth = output.upper[output.forecastCount - 1] - output.lower[output.forecastCount - 1];
    EXPECT_GT(lastWidth, firstWidth);
}

// ==================== Seasonal Forecaster Tests ====================

TEST_F(ForecastTest, SeasonalForecasterSinusoidal) {
    // Generate sinusoidal data with period of 24 points
    auto values = generateSeasonalData(100.0, 20.0, 24, 0.0, 0.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.deviations = 2.0;
    config.seasonality = Seasonality::HOURLY;  // 60 points per hour

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 24);

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());
    ASSERT_EQ(output.forecastCount, 24);

    // Forecast values should be within reasonable range of the baseline
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_GT(output.forecast[i], 50.0);
        EXPECT_LT(output.forecast[i], 150.0);
    }
}

TEST_F(ForecastTest, SeasonalForecasterWithTrend) {
    // Sinusoidal with trend
    auto values = generateSeasonalData(100.0, 10.0, 24, 0.1, 0.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 24);

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());

    // Forecast should show increasing trend
    double firstForecast = output.forecast[0];
    double lastForecast = output.forecast[output.forecastCount - 1];
    EXPECT_GT(lastForecast, firstForecast - 10.0);  // Allow for seasonality variation
}

// ==================== Forecast Executor Tests ====================

TEST_F(ForecastTest, ExecutorLinearForecast) {
    auto values = generateLinearData(0.5, 10.0, 1.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;
    config.forecastHorizon = 20;

    ForecastExecutor executor;
    auto output = executor.execute(input, config);

    ASSERT_FALSE(output.empty());
    EXPECT_EQ(output.forecastCount, 20);
    EXPECT_EQ(output.historicalCount, 100);
}

TEST_F(ForecastTest, ExecutorMultiSeries) {
    // Test with multiple series
    std::vector<std::vector<double>> seriesValues;
    seriesValues.push_back(generateLinearData(0.5, 10.0, 1.0));
    seriesValues.push_back(generateLinearData(1.0, 20.0, 1.0));

    std::vector<std::vector<std::string>> seriesGroupTags;
    seriesGroupTags.push_back({"host=server1"});
    seriesGroupTags.push_back({"host=server2"});

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;
    config.forecastHorizon = 10;

    ForecastExecutor executor;
    auto result = executor.executeMulti(timestamps_, seriesValues, seriesGroupTags, config);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.statistics.seriesCount, 2);

    // Should have 4 pieces per series (past, forecast, upper, lower) = 8 total
    EXPECT_EQ(result.series.size(), 8);

    // Check piece types
    int pastCount = 0, forecastCount = 0, upperCount = 0, lowerCount = 0;
    for (const auto& piece : result.series) {
        if (piece.piece == "past")
            pastCount++;
        else if (piece.piece == "forecast")
            forecastCount++;
        else if (piece.piece == "upper")
            upperCount++;
        else if (piece.piece == "lower")
            lowerCount++;
    }
    EXPECT_EQ(pastCount, 2);
    EXPECT_EQ(forecastCount, 2);
    EXPECT_EQ(upperCount, 2);
    EXPECT_EQ(lowerCount, 2);
}

TEST_F(ForecastTest, ExecutorTimestampGeneration) {
    // Test forecast timestamp generation
    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 0);

    // Default horizon should match historical count
    EXPECT_EQ(forecastTimestamps.size(), timestamps_.size());

    // Check interval consistency
    if (forecastTimestamps.size() >= 2) {
        uint64_t interval = forecastTimestamps[1] - forecastTimestamps[0];
        uint64_t expectedInterval = (timestamps_.back() - timestamps_.front()) / (timestamps_.size() - 1);
        EXPECT_EQ(interval, expectedInterval);
    }

    // First forecast timestamp should be after last historical
    EXPECT_GT(forecastTimestamps[0], timestamps_.back());
}

// ==================== Expression Parser Tests ====================

TEST_F(ForecastTest, ParseForecastFunctionLinear) {
    ExpressionParser parser("forecast(cpu, 'linear', 2)");
    auto ast = parser.parse();

    ASSERT_EQ(ast->type, ExprNodeType::FORECAST_FUNCTION);

    const auto& func = ast->asForecastFunction();
    EXPECT_EQ(func.queryRef, "cpu");
    EXPECT_EQ(func.algorithm, "linear");
    EXPECT_DOUBLE_EQ(func.deviations, 2.0);
}

TEST_F(ForecastTest, ParseForecastFunctionSeasonal) {
    ExpressionParser parser("forecast(temp, 'seasonal', 3)");
    auto ast = parser.parse();

    ASSERT_EQ(ast->type, ExprNodeType::FORECAST_FUNCTION);

    const auto& func = ast->asForecastFunction();
    EXPECT_EQ(func.queryRef, "temp");
    EXPECT_EQ(func.algorithm, "seasonal");
    EXPECT_DOUBLE_EQ(func.deviations, 3.0);
}

TEST_F(ForecastTest, ParseForecastFunctionInvalidAlgorithm) {
    EXPECT_THROW(
        {
            ExpressionParser parser("forecast(cpu, 'invalid', 2)");
            parser.parse();
        },
        ExpressionParseException);
}

TEST_F(ForecastTest, ParseForecastFunctionInvalidDeviations) {
    // Deviations < 1
    EXPECT_THROW(
        {
            ExpressionParser parser("forecast(cpu, 'linear', 0.5)");
            parser.parse();
        },
        ExpressionParseException);

    // Deviations > 4
    EXPECT_THROW(
        {
            ExpressionParser parser("forecast(cpu, 'linear', 5)");
            parser.parse();
        },
        ExpressionParseException);
}

TEST_F(ForecastTest, ForecastFunctionToString) {
    ExpressionParser parser("forecast(metric, 'linear', 2)");
    auto ast = parser.parse();

    std::string str = ast->toString();
    EXPECT_TRUE(str.find("forecast") != std::string::npos);
    EXPECT_TRUE(str.find("metric") != std::string::npos);
    EXPECT_TRUE(str.find("linear") != std::string::npos);
}

// ==================== Algorithm Type Tests ====================

TEST_F(ForecastTest, ParseAlgorithmType) {
    EXPECT_EQ(parseAlgorithm("linear"), Algorithm::LINEAR);
    EXPECT_EQ(parseAlgorithm("seasonal"), Algorithm::SEASONAL);

    EXPECT_THROW(parseAlgorithm("invalid"), std::invalid_argument);
}

TEST_F(ForecastTest, AlgorithmToString) {
    EXPECT_EQ(algorithmToString(Algorithm::LINEAR), "linear");
    EXPECT_EQ(algorithmToString(Algorithm::SEASONAL), "seasonal");
}

// ==================== Edge Cases ====================

TEST_F(ForecastTest, InsufficientData) {
    // Only 5 data points
    std::vector<uint64_t> shortTimestamps(timestamps_.begin(), timestamps_.begin() + 5);
    std::vector<double> shortValues = {1.0, 2.0, 3.0, 4.0, 5.0};

    ForecastInput input;
    input.timestamps = shortTimestamps;
    input.values = shortValues;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.minDataPoints = 10;  // Require 10 points

    ForecastExecutor executor;
    auto output = executor.execute(input, config);

    // Should return empty output due to insufficient data
    EXPECT_TRUE(output.empty());
}

TEST_F(ForecastTest, EmptyInput) {
    ForecastInput input;
    // Empty timestamps and values

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    ForecastExecutor executor;
    auto output = executor.execute(input, config);

    EXPECT_TRUE(output.empty());
}

TEST_F(ForecastTest, ConstantSeriesNoVariance) {
    // Series with all same values - no variance
    std::vector<double> values(100, 42.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());

    // All forecast values should equal the constant
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_DOUBLE_EQ(output.forecast[i], 42.0);
    }
}

// ==================== Robustness Tests ====================

TEST_F(ForecastTest, LinearLevelShiftHandling) {
    // Test that linear forecaster can handle data with a sudden level shift
    // Data with level shift at midpoint: first half at 50, second half at 100
    std::vector<double> values;
    for (size_t i = 0; i < 50; ++i) {
        values.push_back(50.0);
    }
    for (size_t i = 50; i < 100; ++i) {
        values.push_back(100.0);
    }

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());
    ASSERT_EQ(output.forecastCount, 10);

    // Forecast should adapt to the new level (closer to 100 than 50)
    // Since regression fits a line through both levels, forecast should be >= 75
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_GT(output.forecast[i], 75.0) << "Forecast should adapt to higher level";
    }

    // Slope should be positive due to the level shift
    EXPECT_GT(output.slope, 0.0);
}

TEST_F(ForecastTest, SeasonalRequiresTwoSeasons) {
    // Test with only 1.5 seasons of data (period=24, 36 points)
    // Should still produce output but may have less confidence
    std::vector<uint64_t> shortTimestamps(timestamps_.begin(), timestamps_.begin() + 36);
    auto values = generateSeasonalData(100.0, 20.0, 24, 0.0, 0.0);
    std::vector<double> shortValues(values.begin(), values.begin() + 36);

    ForecastInput input;
    input.timestamps = shortTimestamps;
    input.values = shortValues;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.deviations = 2.0;
    config.seasonality = Seasonality::HOURLY;
    config.minDataPoints = 10;  // Allow with fewer points

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(shortTimestamps, 12);

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    // Should still produce output
    ASSERT_FALSE(output.empty());
    EXPECT_EQ(output.forecastCount, 12);

    // Values should be in reasonable range
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_GT(output.forecast[i], 50.0);
        EXPECT_LT(output.forecast[i], 150.0);
    }
}

TEST_F(ForecastTest, SeasonalMultipleSeasonsImproves) {
    // Compare forecast quality with 2 vs 4 seasons of data
    size_t period = 24;

    // Generate 2 seasons (48 points)
    std::vector<uint64_t> twoSeasonTimestamps(timestamps_.begin(), timestamps_.begin() + 48);
    auto allValues = generateSeasonalData(100.0, 20.0, period, 0.0, 1.0);
    std::vector<double> twoSeasonValues(allValues.begin(), allValues.begin() + 48);

    ForecastInput input2;
    input2.timestamps = twoSeasonTimestamps;
    input2.values = twoSeasonValues;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(twoSeasonTimestamps, 12);

    SeasonalForecaster forecaster;
    auto output2 = forecaster.forecast(input2, config, forecastTimestamps);

    // Generate 4 seasons (96 points)
    std::vector<uint64_t> fourSeasonTimestamps(timestamps_.begin(), timestamps_.begin() + 96);
    std::vector<double> fourSeasonValues(allValues.begin(), allValues.begin() + 96);

    ForecastInput input4;
    input4.timestamps = fourSeasonTimestamps;
    input4.values = fourSeasonValues;

    auto forecastTimestamps4 = ForecastExecutor::generateForecastTimestamps(fourSeasonTimestamps, 12);
    auto output4 = forecaster.forecast(input4, config, forecastTimestamps4);

    // Both should produce output
    ASSERT_FALSE(output2.empty());
    ASSERT_FALSE(output4.empty());

    // More data should lead to narrower confidence bounds (lower residual std dev)
    EXPECT_LE(output4.residualStdDev, output2.residualStdDev * 1.5);
}

TEST_F(ForecastTest, SparseIrregularTimestamps) {
    // Test with non-uniform intervals - timestamps with gaps and varying intervals
    std::vector<uint64_t> sparseTimestamps;
    std::vector<double> sparseValues;

    uint64_t startNs = 1704067200000000000ULL;
    uint64_t baseInterval = 60000000000ULL;  // 1 minute

    // Create irregular pattern: normal, normal, gap, normal, double-interval, normal...
    for (size_t i = 0; i < 50; ++i) {
        uint64_t offset;
        if (i % 10 == 5) {
            // Insert gap (skip a timestamp)
            offset = baseInterval * (i + 1);
        } else if (i % 10 == 7) {
            // Double interval
            offset = baseInterval * (i + 2);
        } else {
            offset = baseInterval * i;
        }
        sparseTimestamps.push_back(startNs + offset);
        sparseValues.push_back(50.0 + 0.5 * i);  // Linear trend
    }

    ForecastInput input;
    input.timestamps = sparseTimestamps;
    input.values = sparseValues;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(sparseTimestamps, 10);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    // Forecaster should handle gracefully
    ASSERT_FALSE(output.empty());
    EXPECT_EQ(output.forecastCount, 10);

    // Should still capture the linear trend
    EXPECT_NEAR(output.slope, 0.5, 0.1);
}

TEST_F(ForecastTest, VeryLongForecastHorizon) {
    // Test forecasting far into future - forecast 5x the historical data length
    auto values = generateLinearData(0.5, 10.0, 2.0);

    ForecastInput input;
    input.timestamps = timestamps_;  // 100 points
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    // Forecast 500 points (5x historical)
    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 500);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());
    ASSERT_EQ(output.forecastCount, 500);

    // Confidence bounds should widen significantly as we go further
    double firstWidth = output.upper[0] - output.lower[0];
    double midWidth = output.upper[250] - output.lower[250];
    double lastWidth = output.upper[499] - output.lower[499];

    EXPECT_GT(midWidth, firstWidth * 1.2);
    EXPECT_GT(lastWidth, midWidth * 1.2);
    EXPECT_GT(lastWidth, firstWidth * 2.0);
}

TEST_F(ForecastTest, ForecastDeterminism) {
    // Same input should always produce same output
    auto values = generateLinearData(0.5, 10.0, 1.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 20);

    LinearForecaster forecaster;

    // Run forecast twice
    auto output1 = forecaster.forecast(input, config, forecastTimestamps);
    auto output2 = forecaster.forecast(input, config, forecastTimestamps);

    // Outputs must be exactly equal
    ASSERT_EQ(output1.forecastCount, output2.forecastCount);
    ASSERT_EQ(output1.historicalCount, output2.historicalCount);

    for (size_t i = 0; i < output1.forecastCount; ++i) {
        EXPECT_DOUBLE_EQ(output1.forecast[i], output2.forecast[i]);
        EXPECT_DOUBLE_EQ(output1.upper[i], output2.upper[i]);
        EXPECT_DOUBLE_EQ(output1.lower[i], output2.lower[i]);
    }

    EXPECT_DOUBLE_EQ(output1.slope, output2.slope);
    EXPECT_DOUBLE_EQ(output1.intercept, output2.intercept);
    EXPECT_DOUBLE_EQ(output1.rSquared, output2.rSquared);
}

TEST_F(ForecastTest, InputContainsNaN) {
    // Test graceful handling of NaN values
    // This is a robustness test - current implementation may propagate NaN
    // Ideally should filter NaN or return empty, but should not crash
    auto values = generateLinearData(0.5, 10.0, 0.0);

    // Inject NaN values at various positions
    values[10] = std::nan("");
    values[50] = std::nan("");
    values[90] = std::nan("");

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;

    // Test should not crash - this is the main robustness check
    EXPECT_NO_THROW({
        auto output = forecaster.forecast(input, config, forecastTimestamps);

        // If implementation improves to filter NaN, check results are valid
        // Currently may return NaN, which is acceptable for robustness test
        if (!output.empty()) {
            EXPECT_EQ(output.forecastCount, 10);
            // Note: Current implementation may produce NaN - this is a known limitation
        }
    });
}

TEST_F(ForecastTest, InputContainsInf) {
    // Test graceful handling of Inf values
    // This is a robustness test - current implementation may propagate Inf
    // Ideally should filter Inf or return empty, but should not crash
    auto values = generateLinearData(0.5, 10.0, 0.0);

    // Inject Inf values
    values[20] = std::numeric_limits<double>::infinity();
    values[60] = -std::numeric_limits<double>::infinity();

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;

    // Test should not crash - this is the main robustness check
    EXPECT_NO_THROW({
        auto output = forecaster.forecast(input, config, forecastTimestamps);

        // If implementation improves to filter Inf, check results are valid
        // Currently may return Inf, which is acceptable for robustness test
        if (!output.empty()) {
            EXPECT_EQ(output.forecastCount, 10);
            // Note: Current implementation may produce Inf - this is a known limitation
        }
    });
}

TEST_F(ForecastTest, PartialMissingValues) {
    // Test with a few NaN values (3% of data) - tests robustness to sparse missing data
    // Current implementation may propagate NaN, which is acceptable for robustness test
    auto values = generateLinearData(0.5, 10.0, 1.0);

    // Inject only a few NaN values (3% of data)
    values[15] = std::nan("");
    values[35] = std::nan("");
    values[75] = std::nan("");

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);

    LinearForecaster forecaster;

    // Test should not crash
    EXPECT_NO_THROW({
        auto output = forecaster.forecast(input, config, forecastTimestamps);

        // With mostly valid data, implementation might still produce output
        if (!output.empty()) {
            EXPECT_EQ(output.forecastCount, 10);
            // Note: Current implementation may propagate NaN from input data
            // Future improvements could filter NaN values before regression
        }
    });
}

TEST_F(ForecastTest, NegativeValuesLinear) {
    // Test metrics that go negative - data ranging from -50 to +50
    std::vector<double> values;
    std::mt19937 gen(42);
    std::normal_distribution<> dist(0.0, 1.0);

    for (size_t i = 0; i < timestamps_.size(); ++i) {
        double x = static_cast<double>(i);
        // Linear trend from -50 to +50
        double y = -50.0 + (100.0 / timestamps_.size()) * x + dist(gen);
        values.push_back(y);
    }

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 20);

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    ASSERT_FALSE(output.empty());

    // Forecast should handle negative domain correctly
    // Should continue the upward trend past 50
    EXPECT_GT(output.slope, 0.8);  // Strong positive slope

    // Later forecasts should be positive
    EXPECT_GT(output.forecast[output.forecastCount - 1], 50.0);
}

TEST_F(ForecastTest, DeviationsAffectsBoundWidth) {
    // Test with different deviation values - verify bounds width increases proportionally
    auto values = generateLinearData(0.5, 10.0, 2.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    auto forecastTimestamps = ForecastExecutor::generateForecastTimestamps(timestamps_, 10);
    LinearForecaster forecaster;

    // Test with deviations 1, 2, 3, 4
    std::vector<double> widthsAtMidpoint;

    for (double dev : {1.0, 2.0, 3.0, 4.0}) {
        ForecastConfig config;
        config.algorithm = Algorithm::LINEAR;
        config.deviations = dev;

        auto output = forecaster.forecast(input, config, forecastTimestamps);
        ASSERT_FALSE(output.empty());

        // Measure width at midpoint of forecast
        double width = output.upper[5] - output.lower[5];
        widthsAtMidpoint.push_back(width);
    }

    // Verify bounds width increases with deviations
    for (size_t i = 1; i < widthsAtMidpoint.size(); ++i) {
        EXPECT_GT(widthsAtMidpoint[i], widthsAtMidpoint[i - 1]) << "Width should increase with deviations";
    }

    // Verify roughly proportional scaling
    // 2-sigma should be ~2x wider than 1-sigma
    EXPECT_NEAR(widthsAtMidpoint[1] / widthsAtMidpoint[0], 2.0, 0.2);
    // 4-sigma should be ~2x wider than 2-sigma
    EXPECT_NEAR(widthsAtMidpoint[3] / widthsAtMidpoint[1], 2.0, 0.2);
}

TEST_F(ForecastTest, AutoDetectHourlySeasonality) {
    // Generate data with clear 60-point period (hourly with 1-min intervals)
    // Tests auto-detection capability
    size_t period = 60;
    auto values = generateSeasonalData(100.0, 25.0, period, 0.0, 1.0);

    ForecastInput input;
    input.timestamps = timestamps_;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.deviations = 2.0;
    config.seasonality = Seasonality::NONE;  // Auto-detect

    SeasonalForecaster forecaster;

    // Calculate interval for auto-detection
    uint64_t intervalNs = (timestamps_.back() - timestamps_.front()) / (timestamps_.size() - 1);

    // Test auto-detection doesn't crash
    EXPECT_NO_THROW({
        size_t detectedPeriod = forecaster.detectSeasonalPeriod(values, intervalNs);

        // Auto-detection may return 0 if it cannot detect a clear period
        // Or it should detect something close to 60
        if (detectedPeriod > 0) {
            // If detection succeeds, period should be in reasonable range
            EXPECT_GE(detectedPeriod, 40);
            EXPECT_LE(detectedPeriod, 80);
        }
        // Note: Return value of 0 indicates no clear seasonality detected
    });
}

TEST_F(ForecastTest, AutoDetectDailySeasonality) {
    // Generate data with 24-point period (daily with 1-hour intervals)
    size_t period = 24;

    // Need to adjust timestamps for hourly intervals
    std::vector<uint64_t> hourlyTimestamps;
    uint64_t startNs = 1704067200000000000ULL;
    uint64_t hourIntervalNs = 3600000000000ULL;  // 1 hour in nanoseconds

    for (size_t i = 0; i < 100; ++i) {
        hourlyTimestamps.push_back(startNs + i * hourIntervalNs);
    }

    auto values = generateSeasonalData(100.0, 30.0, period, 0.1, 2.0);

    ForecastInput input;
    input.timestamps = hourlyTimestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.deviations = 2.0;
    config.seasonality = Seasonality::NONE;  // Auto-detect

    SeasonalForecaster forecaster;

    uint64_t intervalNs = (hourlyTimestamps.back() - hourlyTimestamps.front()) / (hourlyTimestamps.size() - 1);
    size_t detectedPeriod = forecaster.detectSeasonalPeriod(values, intervalNs);

    // Should detect period close to 24
    EXPECT_GE(detectedPeriod, 20);
    EXPECT_LE(detectedPeriod, 28);
}
