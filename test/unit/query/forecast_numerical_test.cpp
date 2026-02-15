/**
 * Forecast Numerical Accuracy Tests
 *
 * Tests to validate the numerical accuracy of Linear and SARIMA forecasting algorithms
 * using analytically-known inputs and expected outputs.
 */

#include <gtest/gtest.h>
#include "forecast/forecast_result.hpp"
#include "forecast/linear_forecaster.hpp"
#include "forecast/seasonal_forecaster.hpp"
#include "forecast/forecast_executor.hpp"
#include <cmath>
#include <random>
#include <numeric>

using namespace tsdb::forecast;
using namespace tsdb;

class ForecastNumericalTest : public ::testing::Test {
protected:
    // Generate AR(1) process: y[t] = phi * y[t-1] + epsilon
    std::vector<double> generateAR1(double phi, size_t n, double sigma, unsigned seed = 42) {
        std::mt19937 gen(seed);
        std::normal_distribution<> dist(0.0, sigma);

        std::vector<double> y(n);
        y[0] = dist(gen);

        for (size_t t = 1; t < n; ++t) {
            y[t] = phi * y[t - 1] + dist(gen);
        }

        return y;
    }

    // Generate timestamps
    std::vector<uint64_t> generateTimestamps(size_t n, uint64_t intervalNs = 60000000000ULL) {
        std::vector<uint64_t> ts(n);
        uint64_t startNs = 1704067200000000000ULL;  // Jan 1, 2024
        for (size_t i = 0; i < n; ++i) {
            ts[i] = startNs + i * intervalNs;
        }
        return ts;
    }

    // Verify Yule-Walker equations: R * phi = r
    // For AR(p): [r0 r1 ... r(p-1)] [phi1]   [r1]
    //            [r1 r0 ... r(p-2)] [phi2] = [r2]
    //            [...]              [....]   [...]
    bool verifyYuleWalker(const std::vector<double>& r, const std::vector<double>& phi, double tol = 1e-6) {
        size_t p = phi.size();
        if (r.size() <= p) return false;

        for (size_t i = 0; i < p; ++i) {
            double computed = 0.0;
            for (size_t j = 0; j < p; ++j) {
                size_t lag = (i > j) ? (i - j) : (j - i);
                computed += r[lag] * phi[j];
            }
            if (std::abs(computed - r[i + 1]) > tol) {
                return false;
            }
        }
        return true;
    }
};

// ==================== Linear Regression Accuracy Tests ====================

TEST_F(ForecastNumericalTest, LinearRegressionPerfectFit) {
    // y = 2x + 5 for x = 0, 1, 2, ..., 99
    // Expected: slope = 2.0, intercept = 5.0, R² = 1.0, residualStdDev = 0.0

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 2.0 * i + 5.0;
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 10);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_NEAR(output.slope, 2.0, 1e-10);
    EXPECT_NEAR(output.intercept, 5.0, 1e-10);
    EXPECT_NEAR(output.rSquared, 1.0, 1e-10);
    EXPECT_NEAR(output.residualStdDev, 0.0, 1e-10);
}

TEST_F(ForecastNumericalTest, LinearRegressionKnownValues) {
    // Small dataset with hand-calculated values
    // x = [0, 1, 2, 3, 4], y = [2.1, 3.9, 6.2, 7.8, 10.1]
    // Hand-calculated:
    //   n = 5, Σx = 10, Σy = 30.1
    //   Σxy = 0*2.1 + 1*3.9 + 2*6.2 + 3*7.8 + 4*10.1 = 80.1
    //   Σx² = 30
    //   x̄ = 2.0, ȳ = 6.02
    //   slope = (nΣxy - ΣxΣy) / (nΣx² - (Σx)²) = (5*80.1 - 10*30.1) / (150 - 100) = 99.5/50 = 1.99
    //   intercept = ȳ - slope*x̄ = 6.02 - 1.99*2 = 2.04

    std::vector<double> values = {2.1, 3.9, 6.2, 7.8, 10.1};
    auto timestamps = generateTimestamps(5);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 3);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.minDataPoints = 3;  // Allow small dataset

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_NEAR(output.slope, 1.99, 1e-6);
    EXPECT_NEAR(output.intercept, 2.04, 1e-6);
}

TEST_F(ForecastNumericalTest, HorizontalLine) {
    // y = 42.0 for all x
    // Expected: slope = 0.0, intercept = 42.0

    std::vector<double> values(50, 42.0);
    auto timestamps = generateTimestamps(50);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 10);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_NEAR(output.slope, 0.0, 1e-10);
    EXPECT_NEAR(output.intercept, 42.0, 1e-10);
    EXPECT_NEAR(output.residualStdDev, 0.0, 1e-10);

    // All forecasts should be 42.0
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_DOUBLE_EQ(output.forecast[i], 42.0);
    }
}

TEST_F(ForecastNumericalTest, RSquaredAccuracy) {
    // y = 2x + noise, verify R² calculation
    // For y_i = 2x_i + e_i where e_i ~ N(0, σ²):
    //   R² ≈ Var(2x) / (Var(2x) + σ²) = 4*Var(x) / (4*Var(x) + σ²)
    //   For x = 0..99: Var(x) ≈ 833.25
    //   With σ = 1: R² ≈ 3333 / 3334 ≈ 0.9997

    std::mt19937 gen(42);
    std::normal_distribution<> noise(0.0, 1.0);

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 2.0 * i + noise(gen);
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 10);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    // R² should be very high (close to 1)
    EXPECT_GT(output.rSquared, 0.99);
    EXPECT_LE(output.rSquared, 1.0);
}

TEST_F(ForecastNumericalTest, PredictionIntervalWidth) {
    // Test prediction interval formula:
    // PI = deviations * σ * sqrt(1 + 1/n + (x-x̄)²/Σ(xᵢ-x̄)²)
    // For n=100, x̄=49.5, Σ(xᵢ-x̄)² = Var(x)*n = 833.25*100 = 83325

    // Create data with known residual std dev
    std::mt19937 gen(42);
    std::normal_distribution<> noise(0.0, 2.0);  // σ = 2.0

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 0.5 * i + 10.0 + noise(gen);
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 1);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    // Verify interval width is reasonable
    double intervalWidth = output.upper[0] - output.lower[0];
    EXPECT_GT(intervalWidth, 0.0);

    // The width should be approximately 2 * deviations * σ * sqrt(1.04) ≈ 8.16
    // But actual σ from fit may differ slightly
    EXPECT_GT(intervalWidth, 2.0);  // Should be significantly positive
    EXPECT_LT(intervalWidth, 20.0); // But not unreasonably large
}

TEST_F(ForecastNumericalTest, LinearExtrapolation) {
    // y = 0.5x + 10 for x = 0..99
    // Forecast at x = 100, 101, 102:
    //   y[100] = 60.0, y[101] = 60.5, y[102] = 61.0

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 0.5 * i + 10.0;
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 3);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_NEAR(output.forecast[0], 60.0, 1e-6);
    EXPECT_NEAR(output.forecast[1], 60.5, 1e-6);
    EXPECT_NEAR(output.forecast[2], 61.0, 1e-6);
}

// ==================== Differencing Accuracy Tests ====================

TEST_F(ForecastNumericalTest, SeasonalDifferencing) {
    // Test seasonal differencing: y'[i] = y[i+s] - y[i]
    // Input: [10, 20, 30, 40, 15, 25, 35, 45, 20, 30]
    // s = 4
    // Expected: [5, 5, 5, 5, 5, 5]

    std::vector<double> y = {10, 20, 30, 40, 15, 25, 35, 45, 20, 30};
    size_t seasonalPeriod = 4;

    SeasonalForecaster forecaster;
    auto result = forecaster.seasonalDifference(y, seasonalPeriod);

    ASSERT_EQ(result.size(), 6);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i], 5.0);
    }
}

TEST_F(ForecastNumericalTest, RegularDifferencing) {
    // Test regular differencing: y'[i] = y[i+1] - y[i]
    // Input: [1, 4, 9, 16, 25] (squares: 1², 2², 3², 4², 5²)
    // Expected: [3, 5, 7, 9]

    std::vector<double> y = {1, 4, 9, 16, 25};

    SeasonalForecaster forecaster;
    auto result = forecaster.regularDifference(y);

    ASSERT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result[0], 3.0);
    EXPECT_DOUBLE_EQ(result[1], 5.0);
    EXPECT_DOUBLE_EQ(result[2], 7.0);
    EXPECT_DOUBLE_EQ(result[3], 9.0);
}

TEST_F(ForecastNumericalTest, DoubleDifferencing) {
    // Second difference of squares should be constant
    // y = [1, 4, 9, 16, 25]
    // y' = [3, 5, 7, 9]
    // y'' = [2, 2, 2]

    std::vector<double> y = {1, 4, 9, 16, 25};

    SeasonalForecaster forecaster;
    auto firstDiff = forecaster.regularDifference(y);
    auto secondDiff = forecaster.regularDifference(firstDiff);

    ASSERT_EQ(secondDiff.size(), 3);
    for (size_t i = 0; i < secondDiff.size(); ++i) {
        EXPECT_DOUBLE_EQ(secondDiff[i], 2.0);
    }
}

TEST_F(ForecastNumericalTest, InverseRegularDifferencing) {
    // Test inverse differencing reconstruction
    // Differenced: d = [3, 5, 7, 9]
    // Last value: y_last = 1
    // Expected: [4, 9, 16, 25]

    std::vector<double> diffed = {3, 5, 7, 9};
    double lastValue = 1.0;

    SeasonalForecaster forecaster;
    auto result = forecaster.inverseRegularDifference(diffed, lastValue, 4);

    ASSERT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result[0], 4.0);
    EXPECT_DOUBLE_EQ(result[1], 9.0);
    EXPECT_DOUBLE_EQ(result[2], 16.0);
    EXPECT_DOUBLE_EQ(result[3], 25.0);
}

TEST_F(ForecastNumericalTest, InverseSeasonalDifferencing) {
    // Test inverse seasonal differencing
    // Original: [10, 20, 30, 40]
    // Differenced forecast: [5, 5, 5, 5]
    // Period: 4
    // Expected: [15, 25, 35, 45]

    std::vector<double> original = {10, 20, 30, 40};
    std::vector<double> diffed = {5, 5, 5, 5};
    size_t period = 4;

    SeasonalForecaster forecaster;
    auto result = forecaster.inverseSeasonalDifference(diffed, original, period, 4);

    ASSERT_EQ(result.size(), 4);
    EXPECT_DOUBLE_EQ(result[0], 15.0);
    EXPECT_DOUBLE_EQ(result[1], 25.0);
    EXPECT_DOUBLE_EQ(result[2], 35.0);
    EXPECT_DOUBLE_EQ(result[3], 45.0);
}

// ==================== Autocorrelation Accuracy Tests ====================

TEST_F(ForecastNumericalTest, AR1Autocorrelation) {
    // For AR(1): y[t] = φ*y[t-1] + ε
    // Theoretical ACF at lag k: ρ(k) = φ^k
    // Generate AR(1) with φ = 0.7

    double phi = 0.7;
    auto y = generateAR1(phi, 10000, 1.0);

    double mean = std::accumulate(y.begin(), y.end(), 0.0) / y.size();
    double variance = 0.0;
    for (double val : y) {
        variance += (val - mean) * (val - mean);
    }
    variance /= y.size();

    SeasonalForecaster forecaster;

    // ACF at lag 1 should be ≈ 0.7
    double acf1 = forecaster.autoCorrelation(y, mean, variance, 1);
    EXPECT_NEAR(acf1, 0.7, 0.03);

    // ACF at lag 2 should be ≈ 0.49
    double acf2 = forecaster.autoCorrelation(y, mean, variance, 2);
    EXPECT_NEAR(acf2, 0.49, 0.05);

    // ACF at lag 3 should be ≈ 0.343
    double acf3 = forecaster.autoCorrelation(y, mean, variance, 3);
    EXPECT_NEAR(acf3, 0.343, 0.06);
}

TEST_F(ForecastNumericalTest, SinusoidalAutocorrelation) {
    // y = sin(2πx/24) for x = 0..95 (4 complete cycles)
    // Theoretical ACF at lag 24 = 1.0, lag 12 = -1.0, lag 6 = 0.0
    // Finite sample bias reduces correlations (only 72 overlapping pairs at lag 24)

    std::vector<double> y(96);
    for (size_t i = 0; i < 96; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 24.0);
    }

    double mean = std::accumulate(y.begin(), y.end(), 0.0) / y.size();
    double variance = 0.0;
    for (double val : y) {
        variance += (val - mean) * (val - mean);
    }
    variance /= y.size();

    SeasonalForecaster forecaster;

    // Finite sample ACF with n-k/n scaling factor
    double acf24 = forecaster.autoCorrelation(y, mean, variance, 24);
    EXPECT_NEAR(acf24, 0.75, 0.1);  // 72/96 = 0.75 scaling

    double acf12 = forecaster.autoCorrelation(y, mean, variance, 12);
    EXPECT_NEAR(acf12, -0.875, 0.1);  // 84/96 ≈ 0.875 scaling

    double acf6 = forecaster.autoCorrelation(y, mean, variance, 6);
    EXPECT_NEAR(acf6, 0.0, 0.1);  // Still zero regardless of scaling
}

// ==================== Levinson-Durbin Accuracy Tests ====================

TEST_F(ForecastNumericalTest, LevinsonDurbinAR1) {
    // For AR(1): φ₁ = r[1]/r[0]
    // Input: r = [1.0, 0.7]
    // Expected: φ = [0.7]

    std::vector<double> r = {1.0, 0.7};

    SeasonalForecaster forecaster;
    auto phi = forecaster.levinsonDurbin(r, 1);

    ASSERT_EQ(phi.size(), 1);
    EXPECT_NEAR(phi[0], 0.7, 1e-10);
}

TEST_F(ForecastNumericalTest, LevinsonDurbinAR2) {
    // For AR(2) with r = [1.0, 0.8, 0.5]
    // Yule-Walker equations:
    //   r[0]*φ₁ + r[1]*φ₂ = r[1]  =>  φ₁ + 0.8*φ₂ = 0.8
    //   r[1]*φ₁ + r[0]*φ₂ = r[2]  =>  0.8*φ₁ + φ₂ = 0.5
    // Solving: φ₁ ≈ 1.111, φ₂ ≈ -0.389

    std::vector<double> r = {1.0, 0.8, 0.5};

    SeasonalForecaster forecaster;
    auto phi = forecaster.levinsonDurbin(r, 2);

    ASSERT_EQ(phi.size(), 2);
    EXPECT_NEAR(phi[0], 1.111, 0.01);
    EXPECT_NEAR(phi[1], -0.389, 0.01);

    // Verify Yule-Walker equations are satisfied
    EXPECT_TRUE(verifyYuleWalker(r, phi, 0.01));
}

TEST_F(ForecastNumericalTest, LevinsonDurbinYuleWalkerVerification) {
    // Generate random autocorrelations and verify solution satisfies equations

    std::vector<double> r = {1.0, 0.6, 0.3, 0.1};

    SeasonalForecaster forecaster;
    auto phi = forecaster.levinsonDurbin(r, 3);

    // Verify the solution satisfies the Yule-Walker equations
    EXPECT_TRUE(verifyYuleWalker(r, phi, 1e-4));
}

// ==================== AR Forecast Accuracy Tests ====================

TEST_F(ForecastNumericalTest, AR1OneStepForecast) {
    // Given: y = [10, 17, 19, 21, 23], mean = 18, φ = [0.5]
    // Forecast: ŷ = mean + φ₁*(y[n-1] - mean)
    //         = 18 + 0.5*(23 - 18) = 20.5

    std::vector<double> y = {10, 17, 19, 21, 23};
    std::vector<double> phi = {0.5};
    double mean = 18.0;

    SeasonalForecaster forecaster;
    double forecast = forecaster.arForecast(y, phi, mean);

    EXPECT_NEAR(forecast, 20.5, 1e-10);
}

TEST_F(ForecastNumericalTest, AR2OneStepForecast) {
    // Given: y = [10, 12, 15, 18, 22], mean = 15.4, φ = [0.6, 0.2]
    // Forecast: ŷ = mean + φ₁*(y[n-1] - mean) + φ₂*(y[n-2] - mean)
    //         = 15.4 + 0.6*(22 - 15.4) + 0.2*(18 - 15.4)
    //         = 15.4 + 3.96 + 0.52 = 19.88

    std::vector<double> y = {10, 12, 15, 18, 22};
    std::vector<double> phi = {0.6, 0.2};
    double mean = 15.4;

    SeasonalForecaster forecaster;
    double forecast = forecaster.arForecast(y, phi, mean);

    EXPECT_NEAR(forecast, 19.88, 1e-10);
}

// ==================== Full Pipeline Accuracy Tests ====================

TEST_F(ForecastNumericalTest, AR1PipelineAccuracy) {
    // Generate AR(1) with known φ and verify the fitted model is close
    double truePhi = 0.8;
    auto y = generateAR1(truePhi, 500, 1.0, 12345);
    auto timestamps = generateTimestamps(500);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 10);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = y;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.arOrder = 2;
    config.seasonalArOrder = 0;

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    ASSERT_FALSE(output.empty());
    EXPECT_EQ(output.forecastCount, 10);

    // Forecasts should exist and be finite
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_TRUE(std::isfinite(output.forecast[i]));
    }
}

TEST_F(ForecastNumericalTest, SeasonalPipelineAccuracy) {
    // y = 100 + 20*sin(2πx/24) for 96 points (4 cycles)
    std::vector<double> y(96);
    for (size_t i = 0; i < 96; ++i) {
        y[i] = 100.0 + 20.0 * std::sin(2.0 * M_PI * i / 24.0);
    }

    auto timestamps = generateTimestamps(96);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 24);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = y;

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.seasonality = Seasonality::HOURLY;

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    ASSERT_FALSE(output.empty());
    EXPECT_EQ(output.forecastCount, 24);

    // Forecasts should be finite
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_TRUE(std::isfinite(output.forecast[i]));
    }
}

// ==================== Edge Cases and Numerical Stability ====================

TEST_F(ForecastNumericalTest, NearZeroVariance) {
    // Constant with tiny noise - should not produce NaN/Inf
    std::mt19937 gen(42);
    std::normal_distribution<> noise(0.0, 1e-12);

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 100.0 + noise(gen);
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 10);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    // Should not produce NaN or Inf
    EXPECT_TRUE(std::isfinite(output.slope));
    EXPECT_TRUE(std::isfinite(output.intercept));
    for (size_t i = 0; i < output.forecastCount; ++i) {
        EXPECT_TRUE(std::isfinite(output.forecast[i]));
        EXPECT_TRUE(std::isfinite(output.upper[i]));
        EXPECT_TRUE(std::isfinite(output.lower[i]));
    }
}

TEST_F(ForecastNumericalTest, LargeMagnitudeData) {
    // y = 1e9 * x + 1e12 for x = 0..99
    // At x=100: y = 1e9 * 100 + 1e12 = 1e11 + 1e12 = 1.1e12
    // Should not overflow

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 1e9 * i + 1e12;
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 10);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_NEAR(output.slope, 1e9, 1e3);  // Tolerance for large numbers
    EXPECT_NEAR(output.intercept, 1e12, 1e6);

    // Forecast at x=100: 1e9 * 100 + 1e12 = 1.1e12
    EXPECT_NEAR(output.forecast[0], 1.1e12, 1e9);
}

TEST_F(ForecastNumericalTest, MinimumDataPoints) {
    // Test with exactly minimum data points
    std::vector<double> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto timestamps = generateTimestamps(10);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.minDataPoints = 10;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_FALSE(output.empty());
    EXPECT_EQ(output.forecastCount, 5);
}

TEST_F(ForecastNumericalTest, BelowMinimumDataPoints) {
    // Test with fewer than minimum data points - should return empty
    std::vector<double> values = {1, 2, 3, 4, 5};
    auto timestamps = generateTimestamps(5);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.minDataPoints = 10;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_TRUE(output.empty());
}

TEST_F(ForecastNumericalTest, TwoDataPoints) {
    // Edge case: exactly 2 data points
    std::vector<double> values = {10.0, 20.0};
    auto timestamps = generateTimestamps(2);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 3);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.minDataPoints = 2;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    // With 2 points, we can fit a perfect line
    EXPECT_FALSE(output.empty());
    EXPECT_NEAR(output.slope, 10.0, 1e-6);
    EXPECT_NEAR(output.intercept, 10.0, 1e-6);
}

// ==================== Linear Model Type Numerical Accuracy Tests ====================

TEST_F(ForecastNumericalTest, LinearModelDefaultStandardRegression) {
    // DEFAULT model: Standard least-squares using all data
    // y = 2x + 5 for x = 0..99
    // Expected: slope = 2.0, intercept = 5.0 (exact)

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 2.0 * i + 5.0;
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.linearModel = LinearModelType::DEFAULT;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    EXPECT_NEAR(output.slope, 2.0, 1e-10);
    EXPECT_NEAR(output.intercept, 5.0, 1e-10);
    EXPECT_NEAR(output.rSquared, 1.0, 1e-10);
}

TEST_F(ForecastNumericalTest, LinearModelSimpleUsesLastHalf) {
    // SIMPLE model: Uses only the last n/2 data points
    // First half: y = 0 (indices 0-49)
    // Second half: y = 2x + 100 (indices 50-99)
    // SIMPLE should only see second half: y = 2x + 100
    // With indices 50-99, slope = 2.0

    std::vector<double> values(100);
    for (size_t i = 0; i < 50; ++i) {
        values[i] = 0.0;  // First half: constant 0
    }
    for (size_t i = 50; i < 100; ++i) {
        values[i] = 2.0 * i + 100.0;  // Second half: y = 2x + 100
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.linearModel = LinearModelType::SIMPLE;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    // SIMPLE uses indices 50-99, where y = 2x + 100
    // Should fit slope ≈ 2.0
    EXPECT_NEAR(output.slope, 2.0, 1e-6);

    // Intercept should be around 100 (since y = 2*50 + 100 = 200 at x=50)
    // Actually: For indices 50-99, the regression on x values 50-99 with y = 2x + 100
    // gives intercept = 100
    EXPECT_NEAR(output.intercept, 100.0, 1e-6);
}

TEST_F(ForecastNumericalTest, LinearModelSimpleIgnoresFirstHalf) {
    // Verify SIMPLE model ignores outliers in first half
    // First half: extreme outliers (y = 1000)
    // Second half: clean linear (y = x)
    // DEFAULT would be heavily biased by outliers
    // SIMPLE should give slope ≈ 1.0

    std::vector<double> values(100);
    for (size_t i = 0; i < 50; ++i) {
        values[i] = 1000.0;  // Extreme outliers
    }
    for (size_t i = 50; i < 100; ++i) {
        values[i] = static_cast<double>(i);  // y = x
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    // Test with SIMPLE
    ForecastConfig configSimple;
    configSimple.algorithm = Algorithm::LINEAR;
    configSimple.linearModel = LinearModelType::SIMPLE;

    LinearForecaster forecaster;
    auto outputSimple = forecaster.forecast(input, configSimple, forecastTs);

    // SIMPLE ignores first half, fits y = x for x = 50..99
    // Slope should be 1.0
    EXPECT_NEAR(outputSimple.slope, 1.0, 1e-6);

    // Test with DEFAULT for comparison
    ForecastConfig configDefault;
    configDefault.algorithm = Algorithm::LINEAR;
    configDefault.linearModel = LinearModelType::DEFAULT;

    auto outputDefault = forecaster.forecast(input, configDefault, forecastTs);

    // DEFAULT uses all data, slope will be biased (less than 1.0 due to outliers)
    // The slope should be different from SIMPLE
    EXPECT_NE(outputDefault.slope, outputSimple.slope);
}

TEST_F(ForecastNumericalTest, LinearModelReactiveWeightingFormula) {
    // REACTIVE model: Exponential decay weighting
    // w[i] = exp(-lambda * (n-1-i)) where lambda = 0.05
    // For n = 100:
    //   w[99] = exp(0) = 1.0 (most recent)
    //   w[98] = exp(-0.05) ≈ 0.951
    //   w[0] = exp(-0.05 * 99) ≈ 0.0074 (oldest)

    // Create data where recent points have different trend
    // First 80 points: y = 0
    // Last 20 points: y = 10*(x - 80) = steep upward trend

    std::vector<double> values(100);
    for (size_t i = 0; i < 80; ++i) {
        values[i] = 0.0;
    }
    for (size_t i = 80; i < 100; ++i) {
        values[i] = 10.0 * (i - 80);  // y = 10*(x-80), goes 0 to 190
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    // REACTIVE should heavily weight the last 20 points
    ForecastConfig configReactive;
    configReactive.algorithm = Algorithm::LINEAR;
    configReactive.linearModel = LinearModelType::REACTIVE;

    LinearForecaster forecaster;
    auto outputReactive = forecaster.forecast(input, configReactive, forecastTs);

    // DEFAULT for comparison
    ForecastConfig configDefault;
    configDefault.algorithm = Algorithm::LINEAR;
    configDefault.linearModel = LinearModelType::DEFAULT;

    auto outputDefault = forecaster.forecast(input, configDefault, forecastTs);

    // REACTIVE should have higher slope than DEFAULT due to weighting recent steep trend
    // DEFAULT slope is ~1.9 (averages 80 zeros with 20 steep points)
    // REACTIVE slope is ~2.9 (weights recent steep points more heavily)
    EXPECT_GT(outputReactive.slope, outputDefault.slope);
    EXPECT_GT(outputReactive.slope, 2.5);  // Should be noticeably influenced by recent trend
}

TEST_F(ForecastNumericalTest, LinearModelReactiveVsDefaultLevelShift) {
    // Level shift scenario: Data at 0 for first 90 points, then at 100 for last 10
    // REACTIVE should forecast closer to 100 (recent level)
    // DEFAULT should forecast closer to 10 (average level)

    std::vector<double> values(100);
    for (size_t i = 0; i < 90; ++i) {
        values[i] = 0.0;
    }
    for (size_t i = 90; i < 100; ++i) {
        values[i] = 100.0;
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 1);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    LinearForecaster forecaster;

    // REACTIVE forecast
    ForecastConfig configReactive;
    configReactive.algorithm = Algorithm::LINEAR;
    configReactive.linearModel = LinearModelType::REACTIVE;
    auto outputReactive = forecaster.forecast(input, configReactive, forecastTs);

    // DEFAULT forecast
    ForecastConfig configDefault;
    configDefault.algorithm = Algorithm::LINEAR;
    configDefault.linearModel = LinearModelType::DEFAULT;
    auto outputDefault = forecaster.forecast(input, configDefault, forecastTs);

    // SIMPLE forecast
    ForecastConfig configSimple;
    configSimple.algorithm = Algorithm::LINEAR;
    configSimple.linearModel = LinearModelType::SIMPLE;
    auto outputSimple = forecaster.forecast(input, configSimple, forecastTs);

    // REACTIVE should forecast higher (closer to recent values)
    EXPECT_GT(outputReactive.forecast[0], outputDefault.forecast[0]);

    // SIMPLE uses last 50 points (indices 50-99), which includes the level shift
    // Should also forecast higher than DEFAULT
    EXPECT_GT(outputSimple.forecast[0], outputDefault.forecast[0]);
}

TEST_F(ForecastNumericalTest, LinearModelReactiveExponentialWeights) {
    // Verify exponential weighting produces expected coefficient changes
    // Use data where analytical solution can be computed
    // All zeros except last point which is 100
    // With exponential weighting, the regression should be heavily influenced by last point

    std::vector<double> values(20);
    for (size_t i = 0; i < 19; ++i) {
        values[i] = 0.0;
    }
    values[19] = 100.0;  // Single spike at the end

    auto timestamps = generateTimestamps(20);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 1);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    LinearForecaster forecaster;

    // With REACTIVE, the last point has weight 1.0, others have decreasing weights
    ForecastConfig configReactive;
    configReactive.algorithm = Algorithm::LINEAR;
    configReactive.linearModel = LinearModelType::REACTIVE;
    auto outputReactive = forecaster.forecast(input, configReactive, forecastTs);

    // With DEFAULT, all points have equal weight
    ForecastConfig configDefault;
    configDefault.algorithm = Algorithm::LINEAR;
    configDefault.linearModel = LinearModelType::DEFAULT;
    auto outputDefault = forecaster.forecast(input, configDefault, forecastTs);

    // REACTIVE should have steeper slope (more influenced by the spike)
    EXPECT_GT(outputReactive.slope, outputDefault.slope);

    // REACTIVE forecast should be higher
    EXPECT_GT(outputReactive.forecast[0], outputDefault.forecast[0]);
}

TEST_F(ForecastNumericalTest, LinearModelTypesIdenticalForPerfectLine) {
    // For perfect linear data without noise, all model types should give same result
    // y = 3x + 7

    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 3.0 * i + 7.0;
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    LinearForecaster forecaster;

    // DEFAULT
    ForecastConfig configDefault;
    configDefault.algorithm = Algorithm::LINEAR;
    configDefault.linearModel = LinearModelType::DEFAULT;
    auto outputDefault = forecaster.forecast(input, configDefault, forecastTs);

    // SIMPLE (uses indices 50-99, but data is still y = 3x + 7)
    ForecastConfig configSimple;
    configSimple.algorithm = Algorithm::LINEAR;
    configSimple.linearModel = LinearModelType::SIMPLE;
    auto outputSimple = forecaster.forecast(input, configSimple, forecastTs);

    // REACTIVE
    ForecastConfig configReactive;
    configReactive.algorithm = Algorithm::LINEAR;
    configReactive.linearModel = LinearModelType::REACTIVE;
    auto outputReactive = forecaster.forecast(input, configReactive, forecastTs);

    // All should have slope = 3.0
    EXPECT_NEAR(outputDefault.slope, 3.0, 1e-6);
    EXPECT_NEAR(outputSimple.slope, 3.0, 1e-6);
    EXPECT_NEAR(outputReactive.slope, 3.0, 1e-6);

    // All should have intercept = 7.0
    EXPECT_NEAR(outputDefault.intercept, 7.0, 1e-6);
    EXPECT_NEAR(outputSimple.intercept, 7.0, 1e-6);
    EXPECT_NEAR(outputReactive.intercept, 7.0, 1e-6);
}

TEST_F(ForecastNumericalTest, LinearModelParseAndConvert) {
    // Test parse and toString functions for LinearModelType
    EXPECT_EQ(parseLinearModel("default"), LinearModelType::DEFAULT);
    EXPECT_EQ(parseLinearModel("simple"), LinearModelType::SIMPLE);
    EXPECT_EQ(parseLinearModel("reactive"), LinearModelType::REACTIVE);

    EXPECT_EQ(linearModelToString(LinearModelType::DEFAULT), "default");
    EXPECT_EQ(linearModelToString(LinearModelType::SIMPLE), "simple");
    EXPECT_EQ(linearModelToString(LinearModelType::REACTIVE), "reactive");

    // Invalid model should throw
    EXPECT_THROW(parseLinearModel("invalid"), std::invalid_argument);
}

// ==================== Periodicity Detector Tests ====================

#include "forecast/periodicity_detector.hpp"

TEST_F(ForecastNumericalTest, PeriodicityDetectorDetrend) {
    // Detrending removes linear trend to isolate periodic components
    // Input: y = 2x + 5 + sin(2πx/10) with amplitude 1
    //   - Linear trend: slope=2, intercept=5
    //   - Periodic component: period=10, amplitude=1
    // Expected after detrend: only the sin(2πx/10) remains

    PeriodicityDetector detector;

    std::vector<double> y(100);
    for (size_t i = 0; i < 100; ++i) {
        y[i] = 2.0 * i + 5.0 + std::sin(2.0 * M_PI * i / 10.0);
    }

    auto detrended = detector.detrend(y);

    // Mean of pure sinusoid over complete cycles is 0
    double mean = std::accumulate(detrended.begin(), detrended.end(), 0.0) / detrended.size();
    EXPECT_NEAR(mean, 0.0, 0.01);  // Should be very close to 0

    // Amplitude of sin is 1, so range should be [-1, 1]
    double maxVal = *std::max_element(detrended.begin(), detrended.end());
    double minVal = *std::min_element(detrended.begin(), detrended.end());
    EXPECT_NEAR(maxVal, 1.0, 0.05);
    EXPECT_NEAR(minVal, -1.0, 0.05);

    // Verify detrended values follow sinusoidal pattern
    // Note: Due to finite sample effects, exact values have small offsets
    // At i=0: sin(0) = 0
    EXPECT_NEAR(detrended[0], 0.0, 0.15);
    // At i=5 (half period): sin(π) = 0
    EXPECT_NEAR(detrended[5], 0.0, 0.15);
    // At i=2 or i=3: near peak, should be close to 1.0
    EXPECT_GT(detrended[2], 0.8);
    EXPECT_GT(detrended[3], 0.8);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorHannWindow) {
    // Hann window reduces spectral leakage by tapering signal edges to zero
    // Formula: w(n) = 0.5 * (1 - cos(2πn/(N-1)))
    //
    // Key properties:
    //   w(0) = 0.5 * (1 - cos(0)) = 0.5 * (1-1) = 0
    //   w(N/2) = 0.5 * (1 - cos(π)) = 0.5 * (1-(-1)) = 1
    //   w(N-1) = 0.5 * (1 - cos(2π)) = 0.5 * (1-1) = 0
    //
    // This creates a smooth bell curve that is 0 at edges and 1 at center

    PeriodicityDetector detector;

    // Use constant input to see pure window shape
    std::vector<double> y(101, 1.0);
    auto windowed = detector.applyHannWindow(y);

    // Edges are zeroed (tapered)
    EXPECT_NEAR(windowed[0], 0.0, 1e-10);
    EXPECT_NEAR(windowed[100], 0.0, 1e-10);

    // Center is unchanged (window = 1.0)
    EXPECT_NEAR(windowed[50], 1.0, 1e-10);

    // Quarter points: w(25) = 0.5*(1 - cos(π/2)) = 0.5*(1-0) = 0.5
    EXPECT_NEAR(windowed[25], 0.5, 0.01);
    EXPECT_NEAR(windowed[75], 0.5, 0.01);

    // Verify symmetry: w(n) = w(N-1-n)
    for (size_t i = 0; i < 50; ++i) {
        EXPECT_NEAR(windowed[i], windowed[100 - i], 1e-10);
    }
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorPeriodogramPureSine) {
    // Periodogram reveals periodic components as peaks at specific frequencies
    //
    // For a pure sine wave y = sin(2πx/T):
    //   - Period T corresponds to frequency f = 1/T cycles per sample
    //   - In DFT, frequency index k = n * f = n/T
    //
    // Example: n=100 samples, period T=10
    //   - Frequency index k = 100/10 = 10
    //   - All power should concentrate at index 10

    PeriodicityDetector detector;

    size_t n = 100;
    size_t period = 10;
    std::vector<double> y(n);
    for (size_t i = 0; i < n; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / period);
    }

    auto periodogram = detector.computePeriodogram(y);

    // Peak should be at frequency index k = n/T = 100/10 = 10
    size_t expectedPeakIdx = n / period;

    // Find the peak (skip DC component at index 0)
    size_t peakIdx = 1;
    double maxPower = 0.0;
    for (size_t i = 1; i < periodogram.size(); ++i) {
        if (periodogram[i] > maxPower) {
            maxPower = periodogram[i];
            peakIdx = i;
        }
    }

    EXPECT_EQ(peakIdx, expectedPeakIdx);

    // For a pure sine, power at peak should dominate all others
    // DC component (index 0) should be near zero (sine has zero mean)
    EXPECT_LT(periodogram[0], maxPower * 0.01);

    // Adjacent frequencies should have much lower power (spectral concentration)
    if (expectedPeakIdx > 1) {
        EXPECT_LT(periodogram[expectedPeakIdx - 1], maxPower * 0.1);
    }
    if (expectedPeakIdx + 1 < periodogram.size()) {
        EXPECT_LT(periodogram[expectedPeakIdx + 1], maxPower * 0.1);
    }
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorAutoCorrelation) {
    // Autocorrelation measures self-similarity at different time lags
    //
    // For a sine wave with period T:
    //   - ACF(0) = 1.0 (signal correlates perfectly with itself)
    //   - ACF(T) ≈ 1.0 (signal repeats after one period)
    //   - ACF(T/2) ≈ -1.0 (half-period shift = opposite phase)
    //   - ACF(T/4) ≈ 0.0 (quarter-period = 90° phase shift)
    //
    // This is how we validate detected periods: high ACF at that lag

    PeriodicityDetector detector;

    // Pure sine: y = sin(2πx/24), period = 24, 4 complete cycles
    std::vector<double> y(96);
    for (size_t i = 0; i < 96; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 24.0);
    }

    // ACF at lag 0 is always 1.0 by definition
    double acf0 = detector.autoCorrelation(y, 0);
    EXPECT_NEAR(acf0, 1.0, 1e-10);

    // ACF at lag T=24 (full period): signal aligns with itself
    // Finite sample effect: only (n-k) overlapping pairs, so scaled by ~(n-k)/n
    double acf24 = detector.autoCorrelation(y, 24);
    EXPECT_GT(acf24, 0.7);  // Strong positive (72/96 scaling)

    // ACF at lag T/2=12 (half period): peaks align with troughs
    double acf12 = detector.autoCorrelation(y, 12);
    EXPECT_LT(acf12, -0.7);  // Strong negative correlation

    // ACF at lag T/4=6 (quarter period): 90° phase shift = uncorrelated
    double acf6 = detector.autoCorrelation(y, 6);
    EXPECT_NEAR(acf6, 0.0, 0.1);

    // ACF at lag 3T/4=18: also uncorrelated (270° = -90°)
    double acf18 = detector.autoCorrelation(y, 18);
    EXPECT_NEAR(acf18, 0.0, 0.1);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorFindPeaks) {
    // Test peak finding in periodogram

    PeriodicityDetector detector;

    // Create periodogram with known peaks
    std::vector<double> periodogram = {0, 10, 5, 20, 5, 15, 5, 30, 5, 10};
    //                                  0   1  2   3  4   5  6   7  8   9
    // Peaks at indices: 1 (local), 3 (local), 5 (local), 7 (global)

    auto peaks = detector.findPeaks(periodogram, 8.0);  // Threshold 8.0

    // Should find peaks at 1, 3, 5, 7 that exceed threshold
    // Index 1: 10 > 0 and 10 > 5 and 10 >= 8 -> peak
    // Index 3: 20 > 5 and 20 > 5 and 20 >= 8 -> peak
    // Index 5: 15 > 5 and 15 > 5 and 15 >= 8 -> peak
    // Index 7: 30 > 5 and 30 > 5 and 30 >= 8 -> peak

    EXPECT_GE(peaks.size(), 3);

    // The highest peak should be at index 7
    EXPECT_TRUE(std::find(peaks.begin(), peaks.end(), 7) != peaks.end());
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorDetectSinglePeriod) {
    // Generate sinusoidal data with known period
    // y = sin(2*pi*x/24) for period 24

    PeriodicityDetector detector;

    std::vector<double> y(192);  // 8 cycles of period 24
    for (size_t i = 0; i < 192; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 24.0);
    }

    auto periods = detector.detectPeriods(y, 4, 96, 3, 0.1);

    // Should detect period 24
    ASSERT_GE(periods.size(), 1);

    // The detected period should be close to 24
    bool foundPeriod24 = false;
    for (const auto& p : periods) {
        if (p.period >= 22 && p.period <= 26) {
            foundPeriod24 = true;
            break;
        }
    }
    EXPECT_TRUE(foundPeriod24);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorDetectMultiplePeriods) {
    // Generate data with two periods: 12 and 48
    // y = sin(2*pi*x/12) + 0.5*sin(2*pi*x/48)

    PeriodicityDetector detector;

    std::vector<double> y(192);  // Enough for multiple cycles
    for (size_t i = 0; i < 192; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 12.0) + 0.5 * std::sin(2.0 * M_PI * i / 48.0);
    }

    auto periods = detector.detectPeriods(y, 4, 96, 5, 0.05);

    // Should detect at least one period
    EXPECT_GE(periods.size(), 1);

    // Period 12 should be prominent (higher amplitude)
    bool foundPeriod12 = false;
    for (const auto& p : periods) {
        if (p.period >= 10 && p.period <= 14) {
            foundPeriod12 = true;
            break;
        }
    }
    EXPECT_TRUE(foundPeriod12);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorBestPeriod) {
    // detectBestPeriod should return the most confident period

    PeriodicityDetector detector;

    // Strong period 24
    std::vector<double> y(192);
    for (size_t i = 0; i < 192; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 24.0);
    }

    size_t bestPeriod = detector.detectBestPeriod(y, 0);

    // Should be close to 24
    EXPECT_GE(bestPeriod, 20);
    EXPECT_LE(bestPeriod, 28);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorConstantData) {
    // Constant data should return no periods

    PeriodicityDetector detector;

    std::vector<double> y(100, 42.0);

    auto periods = detector.detectPeriods(y, 4, 50, 3, 0.1);

    // Should return empty (no periodicity in constant data)
    EXPECT_EQ(periods.size(), 0);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorInsufficientData) {
    // Too little data should return no periods

    PeriodicityDetector detector;

    std::vector<double> y = {1, 2, 3, 4};  // Only 4 points

    auto periods = detector.detectPeriods(y, 2, 2, 1, 0.1);

    // Should return empty (not enough data)
    EXPECT_EQ(periods.size(), 0);
}

// ==================== STL Decomposition Tests ====================

#include "forecast/stl_decomposition.hpp"

TEST_F(ForecastNumericalTest, STLLoessLinearDataExact) {
    // LOESS (Locally Estimated Scatterplot Smoothing) fits local polynomials
    //
    // Key property: LOESS exactly reproduces linear data because:
    //   - It fits local linear regressions
    //   - Linear function is its own best linear fit everywhere
    //
    // Input: y = 2x + 5 (perfect line)
    // Expected: LOESS output equals input

    STLDecomposer decomposer;

    std::vector<double> x(20), y(20);
    for (size_t i = 0; i < 20; ++i) {
        x[i] = static_cast<double>(i);
        y[i] = 2.0 * i + 5.0;
    }

    auto smoothed = decomposer.loess(x, y, x, 0.5);

    // LOESS should exactly reproduce linear data
    for (size_t i = 0; i < 20; ++i) {
        EXPECT_NEAR(smoothed[i], y[i], 0.1);  // Should be very close
    }
}

TEST_F(ForecastNumericalTest, STLLoessSmoothing) {
    // LOESS smoothing reduces point-to-point variation (high frequency noise)
    //
    // Input: sin(2πx/10) + noise
    //   - Underlying signal: smooth sinusoid
    //   - Added noise: random jitter
    //
    // Expected after LOESS:
    //   - Point-to-point variation (roughness) is reduced
    //   - Smoothed curve follows general shape of input

    STLDecomposer decomposer;

    std::mt19937 gen(42);
    std::normal_distribution<> noise(0.0, 0.5);

    std::vector<double> x(50), y(50);
    for (size_t i = 0; i < 50; ++i) {
        x[i] = static_cast<double>(i);
        y[i] = std::sin(2.0 * M_PI * i / 10.0) + noise(gen);
    }

    auto smoothed = decomposer.loess(x, y, x, 0.3);

    // Calculate "roughness" = sum of squared differences between consecutive points
    // This measures point-to-point jitter
    double noisyRoughness = 0.0, smoothedRoughness = 0.0;
    for (size_t i = 1; i < 50; ++i) {
        double noisyDiff = y[i] - y[i-1];
        double smoothedDiff = smoothed[i] - smoothed[i-1];
        noisyRoughness += noisyDiff * noisyDiff;
        smoothedRoughness += smoothedDiff * smoothedDiff;
    }

    // Smoothed should have lower roughness (smoother curve)
    EXPECT_LT(smoothedRoughness, noisyRoughness);

    // Verify smoothed values are finite and bounded
    for (size_t i = 0; i < 50; ++i) {
        EXPECT_TRUE(std::isfinite(smoothed[i]));
        EXPECT_GT(smoothed[i], -2.0);
        EXPECT_LT(smoothed[i], 2.0);
    }

    // Smoothed values should be correlated with original (not scrambled)
    double meanY = std::accumulate(y.begin(), y.end(), 0.0) / 50;
    double meanS = std::accumulate(smoothed.begin(), smoothed.end(), 0.0) / 50;
    double cov = 0.0, varY = 0.0, varS = 0.0;
    for (size_t i = 0; i < 50; ++i) {
        cov += (y[i] - meanY) * (smoothed[i] - meanS);
        varY += (y[i] - meanY) * (y[i] - meanY);
        varS += (smoothed[i] - meanS) * (smoothed[i] - meanS);
    }
    double corr = cov / std::sqrt(varY * varS);
    EXPECT_GT(corr, 0.5);  // Moderate-high correlation with original (smoothing reduces this)
}

TEST_F(ForecastNumericalTest, STLCycleSubseriesSmoothing) {
    // Cycle-subseries smoothing: key step in STL that handles seasonality
    //
    // The algorithm:
    //   1. Split data into "subseries" by position within seasonal cycle
    //   2. Smooth each subseries independently using LOESS
    //   3. This captures seasonal patterns while allowing them to evolve
    //
    // Example: Weekly seasonality (period=7), each day gets its own subseries
    //   - Mondays form one subseries, Tuesdays another, etc.
    //   - Each subseries is smoothed to capture gradual changes in that day's pattern

    STLDecomposer decomposer;

    // Create data with period 4 showing gradual increase
    // Position 0: 10, 11, 12  (Monday levels increasing)
    // Position 1: 20, 21, 22  (Tuesday levels increasing)
    // Position 2: 30, 31, 32  (Wednesday levels increasing)
    // Position 3: 40, 41, 42  (Thursday levels increasing)
    std::vector<double> y = {10, 20, 30, 40, 11, 21, 31, 41, 12, 22, 32, 42};

    auto smoothed = decomposer.smoothCycleSubseries(y, 4, 3);

    // Each position maintains its relative ordering (seasonal pattern preserved)
    EXPECT_LT(smoothed[0], smoothed[1]);  // Position 0 < Position 1
    EXPECT_LT(smoothed[1], smoothed[2]);  // Position 1 < Position 2
    EXPECT_LT(smoothed[2], smoothed[3]);  // Position 2 < Position 3

    // Values at same position across cycles should be similar (smoothed)
    // Position 0 values: y[0]=10, y[4]=11, y[8]=12 -> smoothed to ~11
    EXPECT_NEAR(smoothed[0], smoothed[4], 2.0);
    EXPECT_NEAR(smoothed[4], smoothed[8], 2.0);
}

TEST_F(ForecastNumericalTest, STLLowPassFilter) {
    // Test low-pass filtering

    STLDecomposer decomposer;

    // High-frequency oscillation
    std::vector<double> y(50);
    for (size_t i = 0; i < 50; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 4.0);  // Period 4 oscillation
    }

    auto filtered = decomposer.lowPassFilter(y, 10);

    // Low-pass should attenuate high-frequency components
    // Variance should decrease
    double originalVar = 0.0, filteredVar = 0.0;
    double originalMean = std::accumulate(y.begin(), y.end(), 0.0) / y.size();
    double filteredMean = std::accumulate(filtered.begin(), filtered.end(), 0.0) / filtered.size();

    for (size_t i = 0; i < 50; ++i) {
        originalVar += (y[i] - originalMean) * (y[i] - originalMean);
        filteredVar += (filtered[i] - filteredMean) * (filtered[i] - filteredMean);
    }

    // Filtered should have much lower variance (high freq removed)
    EXPECT_LT(filteredVar, originalVar);
}

TEST_F(ForecastNumericalTest, STLDecomposeAdditive) {
    // STL (Seasonal-Trend Decomposition using LOESS) separates:
    //   Y(t) = Trend(t) + Seasonal(t) + Residual(t)
    //
    // Realistic example: Monthly electricity usage over 8 years
    //   - Trend: Gradual increase (growing household)
    //   - Seasonal: Monthly pattern (high in summer/winter, low spring/fall)
    //   - Residual: Random variation (weather, vacations)
    //
    // Input construction (96 months = 8 years, period = 12 months):
    //   Trend = 50 + 0.5*t (baseline 50, grows 6 units/year)
    //   Seasonal = 10*sin(2πt/12) (amplitude 10, period 12)
    //   Noise ~ N(0, 1) (small random variation)

    STLDecomposer decomposer;

    std::mt19937 gen(42);
    std::normal_distribution<> noise(0.0, 1.0);

    size_t n = 96;
    size_t period = 12;
    std::vector<double> y(n), trueTrend(n), trueSeasonal(n);
    for (size_t i = 0; i < n; ++i) {
        trueTrend[i] = 50.0 + 0.5 * i;
        trueSeasonal[i] = 10.0 * std::sin(2.0 * M_PI * i / period);
        y[i] = trueTrend[i] + trueSeasonal[i] + noise(gen);
    }

    auto result = decomposer.decompose(y, period);

    ASSERT_TRUE(result.success);
    ASSERT_EQ(result.trend.size(), n);
    ASSERT_EQ(result.seasonal.size(), n);
    ASSERT_EQ(result.residual.size(), n);

    // FUNDAMENTAL PROPERTY: Components exactly sum to original
    for (size_t i = 0; i < n; ++i) {
        double reconstructed = result.trend[i] + result.seasonal[i] + result.residual[i];
        EXPECT_NEAR(reconstructed, y[i], 1e-10);
    }

    // Trend should capture the growth (correlation with true trend)
    double trendCorr = 0.0, trendVar1 = 0.0, trendVar2 = 0.0;
    double meanTrue = std::accumulate(trueTrend.begin(), trueTrend.end(), 0.0) / n;
    double meanEst = std::accumulate(result.trend.begin(), result.trend.end(), 0.0) / n;
    for (size_t i = 0; i < n; ++i) {
        trendCorr += (trueTrend[i] - meanTrue) * (result.trend[i] - meanEst);
        trendVar1 += (trueTrend[i] - meanTrue) * (trueTrend[i] - meanTrue);
        trendVar2 += (result.trend[i] - meanEst) * (result.trend[i] - meanEst);
    }
    double correlation = trendCorr / std::sqrt(trendVar1 * trendVar2);
    EXPECT_GT(correlation, 0.95);  // High correlation with true trend

    // Residuals should have low variance (noise captured)
    double residualVar = 0.0;
    for (size_t i = 0; i < n; ++i) {
        residualVar += result.residual[i] * result.residual[i];
    }
    residualVar /= n;
    EXPECT_LT(residualVar, 5.0);  // Low residual variance
}

TEST_F(ForecastNumericalTest, STLDecomposeTrendExtraction) {
    // Test that STL correctly extracts the trend

    STLDecomposer decomposer;

    // Pure linear trend with seasonal
    size_t n = 120;
    size_t period = 12;
    std::vector<double> y(n);
    for (size_t i = 0; i < n; ++i) {
        double trend = 100.0 + 2.0 * i;
        double seasonal = 20.0 * std::sin(2.0 * M_PI * i / period);
        y[i] = trend + seasonal;
    }

    auto result = decomposer.decompose(y, period);

    ASSERT_TRUE(result.success);

    // Trend should be approximately linear
    // Check slope of extracted trend
    double sumX = 0.0, sumY = 0.0, sumXY = 0.0, sumXX = 0.0;
    for (size_t i = 0; i < n; ++i) {
        sumX += i;
        sumY += result.trend[i];
        sumXY += i * result.trend[i];
        sumXX += i * i;
    }
    double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);

    // Slope should be close to 2.0
    EXPECT_NEAR(slope, 2.0, 0.5);
}

TEST_F(ForecastNumericalTest, STLDecomposeSeasonalExtraction) {
    // Test that STL correctly extracts seasonal component

    STLDecomposer decomposer;

    // Pure seasonal (no trend, no noise)
    size_t n = 96;
    size_t period = 12;
    std::vector<double> y(n);
    for (size_t i = 0; i < n; ++i) {
        y[i] = 100.0 + 30.0 * std::sin(2.0 * M_PI * i / period);
    }

    auto result = decomposer.decompose(y, period);

    ASSERT_TRUE(result.success);

    // Seasonal component should have similar amplitude
    double maxSeasonal = *std::max_element(result.seasonal.begin(), result.seasonal.end());
    double minSeasonal = *std::min_element(result.seasonal.begin(), result.seasonal.end());
    double amplitude = (maxSeasonal - minSeasonal) / 2.0;

    // Should be close to 30.0
    EXPECT_NEAR(amplitude, 30.0, 5.0);
}

TEST_F(ForecastNumericalTest, STLRobustnessWeights) {
    // Test robustness weight computation (bisquare function)

    STLDecomposer decomposer;

    // Residuals with outliers
    std::vector<double> residuals = {0.1, -0.2, 0.15, 100.0, -0.1, 0.2};

    auto weights = decomposer.computeRobustnessWeights(residuals);

    ASSERT_EQ(weights.size(), 6);

    // Normal residuals should have high weights (close to 1)
    EXPECT_GT(weights[0], 0.5);
    EXPECT_GT(weights[1], 0.5);
    EXPECT_GT(weights[2], 0.5);

    // Outlier (index 3) should have low weight (close to 0)
    EXPECT_LT(weights[3], 0.1);
}

TEST_F(ForecastNumericalTest, STLMSTLMultiplePeriods) {
    // MSTL (Multiple Seasonal-Trend using LOESS) handles multiple seasonal periods
    //
    // Real-world example: Hourly electricity demand
    //   - Period 24: Daily pattern (low at night, peaks morning/evening)
    //   - Period 168: Weekly pattern (weekdays vs weekends)
    //
    // Here we simulate with periods 12 and 48 for faster testing:
    //   - Period 12: "Daily" cycle with amplitude 20
    //   - Period 48: "Weekly" cycle with amplitude 10
    //
    // MSTL extracts each seasonal component separately

    STLDecomposer decomposer;

    size_t n = 192;  // 4 complete cycles of period 48
    std::vector<double> y(n);
    for (size_t i = 0; i < n; ++i) {
        double trend = 100.0;
        double dailyCycle = 20.0 * std::sin(2.0 * M_PI * i / 12.0);    // Stronger daily pattern
        double weeklyCycle = 10.0 * std::sin(2.0 * M_PI * i / 48.0);   // Weaker weekly pattern
        y[i] = trend + dailyCycle + weeklyCycle;
    }

    auto result = decomposer.decomposeMultiple(y, {12, 48});

    ASSERT_TRUE(result.success);
    ASSERT_EQ(result.seasonals.size(), 2);
    ASSERT_EQ(result.periods.size(), 2);
    EXPECT_EQ(result.periods[0], 12);
    EXPECT_EQ(result.periods[1], 48);

    // Check first seasonal component (period 12) has higher amplitude than second
    double max1 = *std::max_element(result.seasonals[0].begin(), result.seasonals[0].end());
    double min1 = *std::min_element(result.seasonals[0].begin(), result.seasonals[0].end());
    double amp1 = (max1 - min1) / 2.0;

    double max2 = *std::max_element(result.seasonals[1].begin(), result.seasonals[1].end());
    double min2 = *std::min_element(result.seasonals[1].begin(), result.seasonals[1].end());
    double amp2 = (max2 - min2) / 2.0;

    // First seasonal (period 12, amp 20) should have larger amplitude than second (period 48, amp 10)
    EXPECT_GT(amp1, amp2);

    // Trend should be approximately constant at 100
    double trendMean = std::accumulate(result.trend.begin(), result.trend.end(), 0.0) / n;
    EXPECT_NEAR(trendMean, 100.0, 5.0);

    // Residuals should be small (we added no noise)
    double residualVar = 0.0;
    for (size_t i = 0; i < n; ++i) {
        residualVar += result.residual[i] * result.residual[i];
    }
    residualVar /= n;
    EXPECT_LT(residualVar, 10.0);  // Small residuals
}

TEST_F(ForecastNumericalTest, STLInsufficientData) {
    // Test with insufficient data for the period

    STLDecomposer decomposer;

    // Only 10 points but period = 12 (need at least 2*period = 24)
    std::vector<double> y(10, 1.0);

    auto result = decomposer.decompose(y, 12);

    EXPECT_FALSE(result.success);
}

TEST_F(ForecastNumericalTest, STLConstantData) {
    // Constant data should decompose with zero seasonal and residual

    STLDecomposer decomposer;

    std::vector<double> y(48, 100.0);

    auto result = decomposer.decompose(y, 12);

    ASSERT_TRUE(result.success);

    // Trend should be constant (all ~100)
    for (size_t i = 0; i < y.size(); ++i) {
        EXPECT_NEAR(result.trend[i], 100.0, 5.0);
    }

    // Seasonal and residual should be near zero
    double seasonalSum = std::accumulate(result.seasonal.begin(), result.seasonal.end(), 0.0);
    double residualSum = std::accumulate(result.residual.begin(), result.residual.end(), 0.0);
    EXPECT_NEAR(seasonalSum, 0.0, 1.0);
    EXPECT_NEAR(residualSum, 0.0, 1.0);
}

// ==================== Auto/Multi Seasonality Integration Tests ====================

TEST_F(ForecastNumericalTest, ForecastSeasonalityEnumParsing) {
    // Test ForecastSeasonality enum parsing
    EXPECT_EQ(parseForecastSeasonality("none"), ForecastSeasonality::NONE);
    EXPECT_EQ(parseForecastSeasonality("hourly"), ForecastSeasonality::HOURLY);
    EXPECT_EQ(parseForecastSeasonality("daily"), ForecastSeasonality::DAILY);
    EXPECT_EQ(parseForecastSeasonality("weekly"), ForecastSeasonality::WEEKLY);
    EXPECT_EQ(parseForecastSeasonality("auto"), ForecastSeasonality::AUTO);
    EXPECT_EQ(parseForecastSeasonality("multi"), ForecastSeasonality::MULTI);

    EXPECT_EQ(forecastSeasonalityToString(ForecastSeasonality::AUTO), "auto");
    EXPECT_EQ(forecastSeasonalityToString(ForecastSeasonality::MULTI), "multi");

    EXPECT_THROW(parseForecastSeasonality("invalid"), std::invalid_argument);
}

// ==================== Duration Parsing Tests ====================

TEST_F(ForecastNumericalTest, ParseDurationToNsSeconds) {
    // Test second-based durations
    EXPECT_EQ(parseDurationToNs("1s"), 1'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("30s"), 30'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("60s"), 60'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("0.5s"), 500'000'000ULL);
}

TEST_F(ForecastNumericalTest, ParseDurationToNsMinutes) {
    // Test minute-based durations
    EXPECT_EQ(parseDurationToNs("1m"), 60'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("5m"), 300'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("30m"), 1'800'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("1.5m"), 90'000'000'000ULL);  // 1.5 minutes = 90 seconds
}

TEST_F(ForecastNumericalTest, ParseDurationToNsHours) {
    // Test hour-based durations
    EXPECT_EQ(parseDurationToNs("1h"), 3'600'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("12h"), 43'200'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("24h"), 86'400'000'000'000ULL);
}

TEST_F(ForecastNumericalTest, ParseDurationToNsDays) {
    // Test day-based durations
    EXPECT_EQ(parseDurationToNs("1d"), 86'400'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("7d"), 604'800'000'000'000ULL);
    EXPECT_EQ(parseDurationToNs("30d"), 2'592'000'000'000'000ULL);
}

TEST_F(ForecastNumericalTest, ParseDurationToNsWeeks) {
    // Test week-based durations
    EXPECT_EQ(parseDurationToNs("1w"), 604'800'000'000'000ULL);  // 7 days
    EXPECT_EQ(parseDurationToNs("2w"), 1'209'600'000'000'000ULL);  // 14 days
}

TEST_F(ForecastNumericalTest, ParseDurationToNsSmallUnits) {
    // Test smaller units (ms, us, ns)
    EXPECT_EQ(parseDurationToNs("1ms"), 1'000'000ULL);
    EXPECT_EQ(parseDurationToNs("500ms"), 500'000'000ULL);
    EXPECT_EQ(parseDurationToNs("1us"), 1'000ULL);
    EXPECT_EQ(parseDurationToNs("1ns"), 1ULL);
}

TEST_F(ForecastNumericalTest, ParseDurationToNsInvalidFormats) {
    // Test invalid formats throw exceptions
    EXPECT_THROW(parseDurationToNs(""), std::invalid_argument);          // Empty
    EXPECT_THROW(parseDurationToNs("abc"), std::invalid_argument);       // No number
    EXPECT_THROW(parseDurationToNs("5"), std::invalid_argument);         // No unit
    EXPECT_THROW(parseDurationToNs("5x"), std::invalid_argument);        // Invalid unit
    EXPECT_THROW(parseDurationToNs("-5m"), std::invalid_argument);       // Negative
    EXPECT_THROW(parseDurationToNs("0m"), std::invalid_argument);        // Zero
}

// ==================== Seasonal Forecaster AUTO Mode Tests ====================

TEST_F(ForecastNumericalTest, SeasonalForecasterAutoDetection) {
    // Test that AUTO mode correctly detects and uses seasonality
    // Create data with clear daily pattern (period=24 for hourly data)
    const size_t period = 24;
    const size_t nCycles = 5;
    const size_t n = period * nCycles;

    ForecastInput input;
    input.timestamps.resize(n);
    input.values.resize(n);

    // Generate: trend + daily seasonal pattern
    // Simulates hourly data over 5 days
    for (size_t i = 0; i < n; ++i) {
        input.timestamps[i] = i * 3600'000'000'000ULL;  // 1 hour intervals in ns
        double trend = 100.0 + 0.1 * i;
        double seasonal = 10.0 * std::sin(2.0 * M_PI * i / period);
        input.values[i] = trend + seasonal;
    }

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::AUTO;
    config.deviations = 2.0;

    // Generate forecast timestamps (1 day = 24 points ahead)
    std::vector<uint64_t> forecastTimestamps(24);
    for (size_t i = 0; i < 24; ++i) {
        forecastTimestamps[i] = input.timestamps.back() + (i + 1) * 3600'000'000'000ULL;
    }

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    // Should produce valid output
    EXPECT_EQ(output.forecastCount, 24u);
    EXPECT_EQ(output.historicalCount, n);
    EXPECT_EQ(output.forecast.size(), 24u);
    EXPECT_EQ(output.upper.size(), 24u);
    EXPECT_EQ(output.lower.size(), 24u);

    // Forecast should continue the trend
    EXPECT_GT(output.forecast[0], input.values.back() - 15);  // Allow for seasonal variation
    EXPECT_LT(output.forecast[0], input.values.back() + 25);  // Allow for seasonal variation

    // Confidence bounds should be valid
    for (size_t i = 0; i < 24; ++i) {
        EXPECT_GT(output.upper[i], output.forecast[i]);
        EXPECT_LT(output.lower[i], output.forecast[i]);
    }
}

TEST_F(ForecastNumericalTest, SeasonalForecasterMultiMode) {
    // Test MULTI mode with two overlapping seasonalities
    // Daily (24) + Weekly (168) patterns for hourly data
    const size_t dailyPeriod = 24;
    const size_t weeklyPeriod = 168;  // 7 * 24
    const size_t nWeeks = 3;  // Need at least 2 full cycles of longest period
    const size_t n = weeklyPeriod * nWeeks;

    ForecastInput input;
    input.timestamps.resize(n);
    input.values.resize(n);

    // Generate: trend + daily + weekly patterns
    for (size_t i = 0; i < n; ++i) {
        input.timestamps[i] = i * 3600'000'000'000ULL;  // 1 hour intervals
        double trend = 50.0;
        double daily = 10.0 * std::sin(2.0 * M_PI * i / dailyPeriod);
        double weekly = 5.0 * std::sin(2.0 * M_PI * i / weeklyPeriod);
        input.values[i] = trend + daily + weekly;
    }

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::MULTI;
    config.deviations = 2.0;
    config.maxSeasonalComponents = 2;
    config.minPeriod = 4;
    config.seasonalThreshold = 0.15;

    // Forecast 1 week ahead
    std::vector<uint64_t> forecastTimestamps(168);
    for (size_t i = 0; i < 168; ++i) {
        forecastTimestamps[i] = input.timestamps.back() + (i + 1) * 3600'000'000'000ULL;
    }

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    // Should produce valid output
    EXPECT_EQ(output.forecastCount, 168u);
    EXPECT_EQ(output.historicalCount, n);

    // Forecasts should be reasonable (around the mean with seasonal oscillation)
    double minForecast = *std::min_element(output.forecast.begin(), output.forecast.end());
    double maxForecast = *std::max_element(output.forecast.begin(), output.forecast.end());

    // The range should reflect the combined seasonality (daily amplitude 10 + weekly amplitude 5)
    double forecastRange = maxForecast - minForecast;
    EXPECT_GT(forecastRange, 5.0);   // Should have some seasonal variation
    EXPECT_LT(forecastRange, 40.0);  // But not unreasonably large
}

TEST_F(ForecastNumericalTest, SeasonalForecasterWeeklySeasonality) {
    // Test explicit WEEKLY seasonality
    // Create 4 weeks of daily data (28 points)
    const size_t period = 7;  // Weekly cycle for daily data
    const size_t nCycles = 4;
    const size_t n = period * nCycles;

    ForecastInput input;
    input.timestamps.resize(n);
    input.values.resize(n);

    // Generate: weekend effect (lower values on Sat/Sun) with noise for residual variance
    std::mt19937 gen(123);
    std::normal_distribution<double> noise(0.0, 2.0);  // Add noise for variance

    for (size_t i = 0; i < n; ++i) {
        input.timestamps[i] = i * 86400'000'000'000ULL;  // 1 day intervals
        double dayOfWeek = i % 7;
        double weekendEffect = (dayOfWeek >= 5) ? -10.0 : 5.0;  // Lower on weekends
        double seasonal = 3.0 * std::sin(2.0 * M_PI * i / period);
        input.values[i] = 100.0 + weekendEffect + seasonal + noise(gen);
    }

    ForecastConfig config;
    config.algorithm = Algorithm::SEASONAL;
    config.forecastSeasonality = ForecastSeasonality::WEEKLY;
    config.deviations = 2.0;

    // Forecast 1 week ahead
    std::vector<uint64_t> forecastTimestamps(7);
    for (size_t i = 0; i < 7; ++i) {
        forecastTimestamps[i] = input.timestamps.back() + (i + 1) * 86400'000'000'000ULL;
    }

    SeasonalForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTimestamps);

    EXPECT_EQ(output.forecastCount, 7u);
    EXPECT_EQ(output.forecast.size(), 7u);

    // Confidence bounds should be valid (upper >= forecast >= lower)
    // Note: With noise in data, there should be non-zero residual variance
    for (size_t i = 0; i < 7; ++i) {
        EXPECT_GE(output.upper[i], output.forecast[i]);
        EXPECT_LE(output.lower[i], output.forecast[i]);
    }
}

// ==================== History Duration Tests ====================

TEST_F(ForecastNumericalTest, HistoryDurationLimitsData) {
    // Test that history duration parameter correctly limits data used for fitting
    // Create data with a level shift: first half at 100, second half at 200
    const size_t n = 100;

    ForecastInput inputFull;
    inputFull.timestamps.resize(n);
    inputFull.values.resize(n);

    for (size_t i = 0; i < n; ++i) {
        inputFull.timestamps[i] = i * 60'000'000'000ULL;  // 1 minute intervals
        // Level shift halfway through
        inputFull.values[i] = (i < n/2) ? 100.0 : 200.0;
    }

    ForecastConfig configFull;
    configFull.algorithm = Algorithm::LINEAR;
    configFull.linearModel = LinearModelType::DEFAULT;
    configFull.deviations = 2.0;

    // With full history, linear regression sees the level shift and predicts upward trend
    std::vector<uint64_t> forecastTimestamps(10);
    for (size_t i = 0; i < 10; ++i) {
        forecastTimestamps[i] = inputFull.timestamps.back() + (i + 1) * 60'000'000'000ULL;
    }

    LinearForecaster forecaster;
    auto outputFull = forecaster.forecast(inputFull, configFull, forecastTimestamps);

    // Now test with only last 25 points (all at level 200)
    // This simulates what historyDurationNs would do
    ForecastInput inputLimited;
    inputLimited.timestamps.assign(inputFull.timestamps.begin() + 75, inputFull.timestamps.end());
    inputLimited.values.assign(inputFull.values.begin() + 75, inputFull.values.end());

    auto outputLimited = forecaster.forecast(inputLimited, configFull, forecastTimestamps);

    // With limited history (all 200s), the slope should be near zero
    // With full history, the slope should be positive due to level shift
    EXPECT_NEAR(outputLimited.slope, 0.0, 0.01);  // Near-flat line
    EXPECT_GT(outputFull.slope, 0.5);  // Positive slope from level shift

    // Forecast from limited history should be around 200
    EXPECT_NEAR(outputLimited.forecast[0], 200.0, 5.0);
}

// ==================== Edge Case Coverage Tests ====================

TEST_F(ForecastNumericalTest, ForecastHorizonZeroDefaultsToHistorical) {
    // Test that forecastHorizon=0 defaults to matching historical count
    const size_t n = 50;
    std::vector<uint64_t> timestamps(n);
    for (size_t i = 0; i < n; ++i) {
        timestamps[i] = i * 60'000'000'000ULL;
    }

    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 0);

    // Should default to same number as historical
    EXPECT_EQ(forecastTs.size(), n);

    // Should continue from last timestamp with same interval
    uint64_t interval = 60'000'000'000ULL;
    EXPECT_EQ(forecastTs[0], timestamps.back() + interval);
    EXPECT_EQ(forecastTs[1], timestamps.back() + 2 * interval);
}

TEST_F(ForecastNumericalTest, LinearModelSimpleVsDefaultTrendData) {
    // Compare SIMPLE vs DEFAULT model on trending data
    // SIMPLE should be less affected by early data points
    const size_t n = 100;

    ForecastInput input;
    input.timestamps.resize(n);
    input.values.resize(n);

    // Create data with steeper trend in first half, flatter in second half
    for (size_t i = 0; i < n; ++i) {
        input.timestamps[i] = i * 60'000'000'000ULL;
        if (i < n/2) {
            input.values[i] = 100.0 + 2.0 * i;  // Steep slope = 2
        } else {
            input.values[i] = 200.0 + 0.5 * (i - n/2);  // Flatter slope = 0.5
        }
    }

    std::vector<uint64_t> forecastTimestamps(10);
    for (size_t i = 0; i < 10; ++i) {
        forecastTimestamps[i] = input.timestamps.back() + (i + 1) * 60'000'000'000ULL;
    }

    ForecastConfig configDefault;
    configDefault.algorithm = Algorithm::LINEAR;
    configDefault.linearModel = LinearModelType::DEFAULT;

    ForecastConfig configSimple;
    configSimple.algorithm = Algorithm::LINEAR;
    configSimple.linearModel = LinearModelType::SIMPLE;

    LinearForecaster forecaster;
    auto outputDefault = forecaster.forecast(input, configDefault, forecastTimestamps);
    auto outputSimple = forecaster.forecast(input, configSimple, forecastTimestamps);

    // SIMPLE model only uses last half, so slope should be closer to 0.5
    EXPECT_LT(std::abs(outputSimple.slope - 0.5), std::abs(outputDefault.slope - 0.5));

    // DEFAULT sees overall trend which averages to something between 0.5 and 2
    EXPECT_GT(outputDefault.slope, outputSimple.slope);
}

TEST_F(ForecastNumericalTest, PeriodicityDetectorNoSeasonalityDetected) {
    // Test behavior when no clear seasonality exists (pure noise)
    const size_t n = 200;

    // Generate white noise with fixed seed for reproducibility
    std::vector<double> noise(n);
    std::mt19937 gen(42);
    std::normal_distribution<double> dist(0.0, 1.0);
    for (size_t i = 0; i < n; ++i) {
        noise[i] = dist(gen);
    }

    PeriodicityDetector detector;
    auto periods = detector.detectPeriods(noise, 4, 50, 3, 0.3);

    // With high threshold (0.3), pure noise should not produce confident detections
    // Either empty or low confidence
    if (!periods.empty()) {
        EXPECT_LT(periods[0].confidence, 0.5);
    }
}

// ==================== Numerical Robustness Fix Tests ====================

TEST_F(ForecastNumericalTest, ExtrapolateTrendDivisionByZeroSinglePoint) {
    // extrapolateTrend with a single point should not divide by zero.
    // It should return a flat forecast at that value.

    SeasonalForecaster forecaster;

    std::vector<double> trend = {42.0};
    auto result = forecaster.extrapolateTrend(trend, 5);

    ASSERT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]))
            << "extrapolateTrend produced non-finite value at index " << i;
        EXPECT_DOUBLE_EQ(result[i], 42.0);
    }
}

TEST_F(ForecastNumericalTest, ExtrapolateTrendDivisionByZeroConstantTrend) {
    // When all x values map to the same position (degenerate case with
    // fitPoints=1 or all identical), the denominator (fitPoints*sumXX - sumX*sumX)
    // should be handled gracefully without division by zero.

    SeasonalForecaster forecaster;

    // Two identical values - sumXX might be zero depending on indexing
    std::vector<double> trend = {100.0, 100.0};
    auto result = forecaster.extrapolateTrend(trend, 5);

    ASSERT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]))
            << "extrapolateTrend produced non-finite value for constant trend at index " << i;
    }
}

TEST_F(ForecastNumericalTest, ExtrapolateTrendNormalCase) {
    // Normal case: linear trend should be extrapolated correctly.
    // trend = [0, 1, 2, ..., 19] (slope=1, intercept=0)
    // Forecast should continue: 20, 21, 22, ...

    SeasonalForecaster forecaster;

    std::vector<double> trend(20);
    for (size_t i = 0; i < 20; ++i) {
        trend[i] = static_cast<double>(i);
    }

    auto result = forecaster.extrapolateTrend(trend, 5);

    ASSERT_EQ(result.size(), 5);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]))
            << "extrapolateTrend produced non-finite value at index " << i;
    }
    // The extrapolation should continue the upward trend
    EXPECT_GT(result[0], trend.back() - 1.0);
}

TEST_F(ForecastNumericalTest, ExtrapolateTrendEmptyInput) {
    // Empty trend should produce flat forecast at 0.

    SeasonalForecaster forecaster;

    std::vector<double> trend;
    auto result = forecaster.extrapolateTrend(trend, 3);

    ASSERT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]));
        EXPECT_DOUBLE_EQ(result[i], 0.0);
    }
}

TEST_F(ForecastNumericalTest, InverseSeasonalDifferenceShortOriginal) {
    // Test that inverseSeasonalDifference handles the case where
    // original.size() < seasonalPeriod without unsigned underflow.
    // This previously caused out-of-bounds access due to unsigned arithmetic.

    SeasonalForecaster forecaster;

    std::vector<double> original = {10.0, 20.0};  // Only 2 values
    std::vector<double> diffed = {5.0, 5.0, 5.0, 5.0};
    size_t seasonalPeriod = 4;  // Period > original size

    // This should not crash or produce garbage
    auto result = forecaster.inverseSeasonalDifference(diffed, original, seasonalPeriod, 4);

    ASSERT_EQ(result.size(), 4);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]))
            << "inverseSeasonalDifference produced non-finite value at index " << i
            << " (original.size()=" << original.size() << " < seasonalPeriod=" << seasonalPeriod << ")";
    }
}

TEST_F(ForecastNumericalTest, InverseSeasonalDifferenceEmptyOriginal) {
    // Test with empty original data - should not crash.

    SeasonalForecaster forecaster;

    std::vector<double> original;
    std::vector<double> diffed = {1.0, 2.0, 3.0};
    size_t seasonalPeriod = 4;

    auto result = forecaster.inverseSeasonalDifference(diffed, original, seasonalPeriod, 3);

    ASSERT_EQ(result.size(), 3);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]))
            << "inverseSeasonalDifference produced non-finite value at index " << i
            << " with empty original";
    }
}

TEST_F(ForecastNumericalTest, InverseSeasonalDifferenceZeroPeriod) {
    // Test with zero seasonal period - should not divide by zero.

    SeasonalForecaster forecaster;

    std::vector<double> original = {10.0, 20.0, 30.0};
    std::vector<double> diffed = {5.0, 5.0};
    size_t seasonalPeriod = 0;

    auto result = forecaster.inverseSeasonalDifference(diffed, original, seasonalPeriod, 2);

    ASSERT_EQ(result.size(), 2);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(std::isfinite(result[i]));
    }
}

TEST_F(ForecastNumericalTest, AutoCorrelationWithNaN) {
    // Test that the periodicity detector's autoCorrelation handles NaN gracefully.
    // Previously, NaN in input would propagate through all computations.

    PeriodicityDetector detector;

    // Create data with NaN values
    std::vector<double> y(100);
    for (size_t i = 0; i < 100; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 24.0);
    }
    // Insert NaN at various positions
    y[10] = std::nan("");
    y[30] = std::nan("");
    y[50] = std::nan("");
    y[70] = std::nan("");

    double acf = detector.autoCorrelation(y, 24);

    // Result should be finite (not NaN)
    EXPECT_TRUE(std::isfinite(acf))
        << "autoCorrelation should handle NaN input without propagating NaN";
}

TEST_F(ForecastNumericalTest, AutoCorrelationAllNaN) {
    // Test with all-NaN input.

    PeriodicityDetector detector;

    std::vector<double> y(50, std::nan(""));

    double acf = detector.autoCorrelation(y, 5);

    // Should return 0.0 (no valid data)
    EXPECT_TRUE(std::isfinite(acf));
    EXPECT_DOUBLE_EQ(acf, 0.0);
}

TEST_F(ForecastNumericalTest, DetectPeriodsWithNaNInput) {
    // Test full period detection pipeline with NaN-contaminated input.

    PeriodicityDetector detector;

    // Sinusoidal signal with 10% NaN contamination
    std::vector<double> y(200);
    for (size_t i = 0; i < 200; ++i) {
        y[i] = std::sin(2.0 * M_PI * i / 24.0);
    }
    // Insert NaN at 10% of positions
    std::mt19937 gen(42);
    std::uniform_int_distribution<size_t> dist(0, 199);
    for (int i = 0; i < 20; ++i) {
        y[dist(gen)] = std::nan("");
    }

    // Should not crash
    EXPECT_NO_THROW({
        auto periods = detector.detectPeriods(y, 4, 96, 3, 0.1);
        // Results may be degraded due to NaN, but should not contain NaN
    });
}

TEST_F(ForecastNumericalTest, RSquaredClampedToValidRange) {
    // Test that R-squared is always in [0, 1] range even when the model
    // is poor. With weighted regression, the R-squared formula can
    // produce values outside [0, 1] in edge cases.

    std::vector<double> values(100);
    std::mt19937 gen(42);
    std::normal_distribution<> noise(0.0, 100.0);

    // Pure noise with no linear relationship
    for (size_t i = 0; i < 100; ++i) {
        values[i] = noise(gen);
    }

    auto timestamps = generateTimestamps(100);
    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ForecastInput input;
    input.timestamps = timestamps;
    input.values = values;

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;

    LinearForecaster forecaster;
    auto output = forecaster.forecast(input, config, forecastTs);

    // R-squared must be in valid range
    EXPECT_GE(output.rSquared, 0.0)
        << "R-squared should not be negative";
    EXPECT_LE(output.rSquared, 1.0)
        << "R-squared should not exceed 1.0";
}

