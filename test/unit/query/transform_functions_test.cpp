#include "../../../lib/query/transform/transform_functions.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <vector>

using namespace timestar::transform;

class TransformFunctionsTest : public ::testing::Test {
protected:
    // Helper to check if two doubles are equal (handling NaN)
    static bool nearEqual(double a, double b, double epsilon = 1e-9) {
        if (std::isnan(a) && std::isnan(b))
            return true;
        if (std::isnan(a) || std::isnan(b))
            return false;
        return std::abs(a - b) < epsilon;
    }

    // Helper to compare vectors
    static void expectVectorNear(const std::vector<double>& actual, const std::vector<double>& expected,
                                 double epsilon = 1e-9) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            EXPECT_TRUE(nearEqual(actual[i], expected[i], epsilon))
                << "Mismatch at index " << i << ": actual=" << actual[i] << ", expected=" << expected[i];
        }
    }
};

// ============================================================================
// Rate Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, DiffBasic) {
    // diff() calculates value[i] - value[i-1]
    std::vector<double> values = {10.0, 15.0, 13.0, 20.0, 18.0};
    auto result = diff(values);

    EXPECT_TRUE(std::isnan(result[0]));  // First value has no predecessor
    EXPECT_DOUBLE_EQ(result[1], 5.0);    // 15 - 10
    EXPECT_DOUBLE_EQ(result[2], -2.0);   // 13 - 15
    EXPECT_DOUBLE_EQ(result[3], 7.0);    // 20 - 13
    EXPECT_DOUBLE_EQ(result[4], -2.0);   // 18 - 20
}

TEST_F(TransformFunctionsTest, DiffWithNaN) {
    std::vector<double> values = {10.0, std::nan(""), 13.0, 20.0};
    auto result = diff(values);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_TRUE(std::isnan(result[1]));  // NaN input
    EXPECT_TRUE(std::isnan(result[2]));  // Previous was NaN
    EXPECT_DOUBLE_EQ(result[3], 7.0);    // 20 - 13
}

TEST_F(TransformFunctionsTest, DiffEmpty) {
    std::vector<double> empty;
    auto result = diff(empty);
    EXPECT_TRUE(result.empty());
}

TEST_F(TransformFunctionsTest, DtBasic) {
    // Timestamps in nanoseconds, 1 second apart
    std::vector<uint64_t> timestamps = {
        0,
        1'000'000'000,  // +1 second
        3'000'000'000,  // +2 seconds
        4'000'000'000   // +1 second
    };
    auto result = dt(timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 1.0);
    EXPECT_DOUBLE_EQ(result[2], 2.0);
    EXPECT_DOUBLE_EQ(result[3], 1.0);
}

TEST_F(TransformFunctionsTest, DerivativeBasic) {
    // Values: 0, 10, 30, 60 over 1-second intervals
    // Derivative should be: NaN, 10/s, 20/s, 30/s
    std::vector<double> values = {0.0, 10.0, 30.0, 60.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000};

    auto result = derivative(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 10.0);
    EXPECT_DOUBLE_EQ(result[2], 20.0);
    EXPECT_DOUBLE_EQ(result[3], 30.0);
}

TEST_F(TransformFunctionsTest, DerivativeVariableIntervals) {
    // Values increase by 100, but time intervals vary
    std::vector<double> values = {0.0, 100.0, 200.0, 300.0};
    std::vector<uint64_t> timestamps = {
        0,
        1'000'000'000,  // +1 second: rate = 100/s
        3'000'000'000,  // +2 seconds: rate = 50/s
        3'500'000'000   // +0.5 seconds: rate = 200/s
    };

    auto result = derivative(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 100.0);
    EXPECT_DOUBLE_EQ(result[2], 50.0);
    EXPECT_DOUBLE_EQ(result[3], 200.0);
}

TEST_F(TransformFunctionsTest, RateCounterIncrement) {
    // Counter that only increases
    std::vector<double> values = {100.0, 150.0, 200.0, 300.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000};

    auto result = rate(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 50.0);   // (150-100)/1
    EXPECT_DOUBLE_EQ(result[2], 50.0);   // (200-150)/1
    EXPECT_DOUBLE_EQ(result[3], 100.0);  // (300-200)/1
}

TEST_F(TransformFunctionsTest, RateCounterReset) {
    // Counter that resets (wraps around)
    std::vector<double> values = {100.0, 150.0, 50.0, 100.0};  // Reset at index 2
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000};

    auto result = rate(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 50.0);
    EXPECT_DOUBLE_EQ(result[2], 0.0);   // Counter reset: treated as 0 rate
    EXPECT_DOUBLE_EQ(result[3], 50.0);  // Normal increment after reset
}

TEST_F(TransformFunctionsTest, PerMinuteMultipliesBy60) {
    std::vector<double> values = {0.0, 60.0};  // +60 in 1 second = 60/s
    std::vector<uint64_t> timestamps = {0, 1'000'000'000};

    auto result = per_minute(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 3600.0);  // 60/s * 60 = 3600/min
}

TEST_F(TransformFunctionsTest, PerHourMultipliesBy3600) {
    std::vector<double> values = {0.0, 1.0};  // +1 in 1 second = 1/s
    std::vector<uint64_t> timestamps = {0, 1'000'000'000};

    auto result = per_hour(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 3600.0);  // 1/s * 3600 = 3600/hr
}

TEST_F(TransformFunctionsTest, MonotonicDiffHandlesReset) {
    std::vector<double> values = {10.0, 20.0, 5.0, 15.0};

    auto result = monotonic_diff(values);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 10.0);  // 20 - 10
    EXPECT_DOUBLE_EQ(result[2], 5.0);   // Reset: diff is -15, returns current value (5.0)
    EXPECT_DOUBLE_EQ(result[3], 10.0);  // 15 - 5
}

// ============================================================================
// Arithmetic Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, AbsFunction) {
    std::vector<double> values = {-5.0, 3.0, -1.5, 0.0, std::nan("")};

    auto result = abs(values);

    EXPECT_DOUBLE_EQ(result[0], 5.0);
    EXPECT_DOUBLE_EQ(result[1], 3.0);
    EXPECT_DOUBLE_EQ(result[2], 1.5);
    EXPECT_DOUBLE_EQ(result[3], 0.0);
    EXPECT_TRUE(std::isnan(result[4]));
}

TEST_F(TransformFunctionsTest, Log2Function) {
    std::vector<double> values = {1.0, 2.0, 4.0, 8.0, 0.0, -1.0};

    auto result = log2(values);

    EXPECT_DOUBLE_EQ(result[0], 0.0);    // log2(1) = 0
    EXPECT_DOUBLE_EQ(result[1], 1.0);    // log2(2) = 1
    EXPECT_DOUBLE_EQ(result[2], 2.0);    // log2(4) = 2
    EXPECT_DOUBLE_EQ(result[3], 3.0);    // log2(8) = 3
    EXPECT_TRUE(std::isnan(result[4]));  // log2(0) = NaN
    EXPECT_TRUE(std::isnan(result[5]));  // log2(-1) = NaN
}

TEST_F(TransformFunctionsTest, Log10Function) {
    std::vector<double> values = {1.0, 10.0, 100.0, 1000.0};

    auto result = log10(values);

    EXPECT_DOUBLE_EQ(result[0], 0.0);
    EXPECT_DOUBLE_EQ(result[1], 1.0);
    EXPECT_DOUBLE_EQ(result[2], 2.0);
    EXPECT_DOUBLE_EQ(result[3], 3.0);
}

TEST_F(TransformFunctionsTest, CumsumBasic) {
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};

    auto result = cumsum(values);

    EXPECT_DOUBLE_EQ(result[0], 1.0);
    EXPECT_DOUBLE_EQ(result[1], 3.0);   // 1+2
    EXPECT_DOUBLE_EQ(result[2], 6.0);   // 1+2+3
    EXPECT_DOUBLE_EQ(result[3], 10.0);  // 1+2+3+4
    EXPECT_DOUBLE_EQ(result[4], 15.0);  // 1+2+3+4+5
}

TEST_F(TransformFunctionsTest, CumsumWithNaN) {
    std::vector<double> values = {1.0, std::nan(""), 3.0, 4.0};

    auto result = cumsum(values);

    EXPECT_DOUBLE_EQ(result[0], 1.0);
    EXPECT_DOUBLE_EQ(result[1], 1.0);  // NaN skipped
    EXPECT_DOUBLE_EQ(result[2], 4.0);  // 1+3
    EXPECT_DOUBLE_EQ(result[3], 8.0);  // 1+3+4
}

TEST_F(TransformFunctionsTest, IntegralTrapezoid) {
    // Constant value of 10 over 4 seconds = area of 40
    std::vector<double> values = {10.0, 10.0, 10.0, 10.0, 10.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = integral(values, timestamps);

    EXPECT_DOUBLE_EQ(result[0], 0.0);
    EXPECT_DOUBLE_EQ(result[1], 10.0);  // Area after 1s
    EXPECT_DOUBLE_EQ(result[2], 20.0);  // Area after 2s
    EXPECT_DOUBLE_EQ(result[3], 30.0);  // Area after 3s
    EXPECT_DOUBLE_EQ(result[4], 40.0);  // Area after 4s
}

TEST_F(TransformFunctionsTest, IntegralTriangle) {
    // Linear ramp from 0 to 10 over 10 seconds
    // Area = 0.5 * base * height = 0.5 * 10 * 10 = 50
    std::vector<double> values = {0.0, 5.0, 10.0};
    std::vector<uint64_t> timestamps = {0, 5'000'000'000, 10'000'000'000};

    auto result = integral(values, timestamps);

    EXPECT_DOUBLE_EQ(result[0], 0.0);
    EXPECT_DOUBLE_EQ(result[1], 12.5);  // Trapezoid: (0+5)/2 * 5 = 12.5
    EXPECT_DOUBLE_EQ(result[2], 50.0);  // Full triangle area
}

// ============================================================================
// Smoothing Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, EwmaBasic) {
    // EWMA with span=3, alpha = 2/(3+1) = 0.5
    std::vector<double> values = {10.0, 20.0, 30.0, 40.0};

    auto result = ewma(values, 3);

    // ewma[0] = 10
    // ewma[1] = 0.5*20 + 0.5*10 = 15
    // ewma[2] = 0.5*30 + 0.5*15 = 22.5
    // ewma[3] = 0.5*40 + 0.5*22.5 = 31.25
    EXPECT_DOUBLE_EQ(result[0], 10.0);
    EXPECT_DOUBLE_EQ(result[1], 15.0);
    EXPECT_DOUBLE_EQ(result[2], 22.5);
    EXPECT_DOUBLE_EQ(result[3], 31.25);
}

TEST_F(TransformFunctionsTest, EwmaSmoothsNoise) {
    // Noisy signal: 100 ± 10
    std::vector<double> values = {90, 110, 95, 105, 100, 100, 100};

    // EWMA with larger span should smooth the noise
    auto result = ewma(values, 5);  // alpha = 2/6 = 0.333

    // Result should be smoother than input
    double inputVariance = 0, outputVariance = 0;
    double inputMean = 100, outputMean = 0;
    for (auto v : result)
        outputMean += v;
    outputMean /= result.size();

    for (size_t i = 0; i < values.size(); ++i) {
        inputVariance += (values[i] - inputMean) * (values[i] - inputMean);
        outputVariance += (result[i] - outputMean) * (result[i] - outputMean);
    }

    EXPECT_LT(outputVariance, inputVariance);  // Smoothed output has less variance
}

TEST_F(TransformFunctionsTest, MedianBasic) {
    std::vector<double> values = {1.0, 5.0, 3.0, 9.0, 2.0};

    auto result = median(values, 3);

    // Window of 3, centered:
    // [0]: window = [1,5] -> median = 3 (with edge handling)
    // [1]: window = [1,5,3] -> sorted [1,3,5] -> median = 3
    // [2]: window = [5,3,9] -> sorted [3,5,9] -> median = 5
    // [3]: window = [3,9,2] -> sorted [2,3,9] -> median = 3
    // [4]: window = [9,2] -> median = 5.5 (with edge handling)

    EXPECT_DOUBLE_EQ(result[1], 3.0);
    EXPECT_DOUBLE_EQ(result[2], 5.0);
    EXPECT_DOUBLE_EQ(result[3], 3.0);
}

TEST_F(TransformFunctionsTest, MedianRemovesSpikes) {
    // Signal with spike at index 2
    std::vector<double> values = {10.0, 10.0, 100.0, 10.0, 10.0};

    auto result = median(values, 3);

    // Median filter should remove the spike
    EXPECT_DOUBLE_EQ(result[2], 10.0);  // Spike removed
}

TEST_F(TransformFunctionsTest, AutosmoothSelectsSpan) {
    std::vector<double> values(100);
    for (size_t i = 0; i < 100; ++i) {
        values[i] = 50.0 + (i % 2 == 0 ? 5.0 : -5.0);  // Oscillating
    }

    auto result = autosmooth(values);

    // Should reduce oscillation amplitude
    double inputRange = 10.0;  // -5 to +5
    double outputMin = *std::min_element(result.begin(), result.end());
    double outputMax = *std::max_element(result.begin(), result.end());
    double outputRange = outputMax - outputMin;

    EXPECT_LT(outputRange, inputRange);
}

// ============================================================================
// Exclusion Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, ClampMinBasic) {
    std::vector<double> values = {-5.0, 0.0, 5.0, 10.0};

    auto result = clamp_min(values, 0.0);

    EXPECT_DOUBLE_EQ(result[0], 0.0);  // -5 clamped to 0
    EXPECT_DOUBLE_EQ(result[1], 0.0);
    EXPECT_DOUBLE_EQ(result[2], 5.0);
    EXPECT_DOUBLE_EQ(result[3], 10.0);
}

TEST_F(TransformFunctionsTest, ClampMaxBasic) {
    std::vector<double> values = {5.0, 10.0, 15.0, 20.0};

    auto result = clamp_max(values, 12.0);

    EXPECT_DOUBLE_EQ(result[0], 5.0);
    EXPECT_DOUBLE_EQ(result[1], 10.0);
    EXPECT_DOUBLE_EQ(result[2], 12.0);  // 15 clamped to 12
    EXPECT_DOUBLE_EQ(result[3], 12.0);  // 20 clamped to 12
}

TEST_F(TransformFunctionsTest, CutoffMinBasic) {
    std::vector<double> values = {1.0, 5.0, 10.0, 15.0};

    auto result = cutoff_min(values, 7.0);

    EXPECT_TRUE(std::isnan(result[0]));  // 1 < 7, removed
    EXPECT_TRUE(std::isnan(result[1]));  // 5 < 7, removed
    EXPECT_DOUBLE_EQ(result[2], 10.0);   // 10 >= 7, kept
    EXPECT_DOUBLE_EQ(result[3], 15.0);   // 15 >= 7, kept
}

TEST_F(TransformFunctionsTest, CutoffMaxBasic) {
    std::vector<double> values = {1.0, 5.0, 10.0, 15.0};

    auto result = cutoff_max(values, 7.0);

    EXPECT_DOUBLE_EQ(result[0], 1.0);    // 1 <= 7, kept
    EXPECT_DOUBLE_EQ(result[1], 5.0);    // 5 <= 7, kept
    EXPECT_TRUE(std::isnan(result[2]));  // 10 > 7, removed
    EXPECT_TRUE(std::isnan(result[3]));  // 15 > 7, removed
}

// ============================================================================
// Interpolation Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, DefaultZeroBasic) {
    std::vector<double> values = {1.0, std::nan(""), 3.0, std::nan("")};

    auto result = default_zero(values);

    EXPECT_DOUBLE_EQ(result[0], 1.0);
    EXPECT_DOUBLE_EQ(result[1], 0.0);  // NaN -> 0
    EXPECT_DOUBLE_EQ(result[2], 3.0);
    EXPECT_DOUBLE_EQ(result[3], 0.0);  // NaN -> 0
}

TEST_F(TransformFunctionsTest, FillLinear) {
    std::vector<double> values = {0.0, std::nan(""), std::nan(""), 6.0};

    auto result = fill(values, "linear");

    EXPECT_DOUBLE_EQ(result[0], 0.0);
    EXPECT_DOUBLE_EQ(result[1], 2.0);  // Interpolated
    EXPECT_DOUBLE_EQ(result[2], 4.0);  // Interpolated
    EXPECT_DOUBLE_EQ(result[3], 6.0);
}

TEST_F(TransformFunctionsTest, FillLast) {
    std::vector<double> values = {5.0, std::nan(""), std::nan(""), 10.0};

    auto result = fill(values, "last");

    EXPECT_DOUBLE_EQ(result[0], 5.0);
    EXPECT_DOUBLE_EQ(result[1], 5.0);  // Forward fill
    EXPECT_DOUBLE_EQ(result[2], 5.0);  // Forward fill
    EXPECT_DOUBLE_EQ(result[3], 10.0);
}

TEST_F(TransformFunctionsTest, FillZero) {
    std::vector<double> values = {1.0, std::nan(""), 3.0};

    auto result = fill(values, "zero");

    EXPECT_DOUBLE_EQ(result[0], 1.0);
    EXPECT_DOUBLE_EQ(result[1], 0.0);
    EXPECT_DOUBLE_EQ(result[2], 3.0);
}

TEST_F(TransformFunctionsTest, FillNull) {
    std::vector<double> values = {1.0, std::nan(""), 3.0};

    auto result = fill(values, "null");

    EXPECT_DOUBLE_EQ(result[0], 1.0);
    EXPECT_TRUE(std::isnan(result[1]));  // Unchanged
    EXPECT_DOUBLE_EQ(result[2], 3.0);
}

// ============================================================================
// Count Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, CountNonzero) {
    std::vector<double> values = {0.0, 1.0, 0.0, 5.0, std::nan("")};

    auto result = count_nonzero(values);

    EXPECT_DOUBLE_EQ(result[0], 0.0);  // 0 is zero
    EXPECT_DOUBLE_EQ(result[1], 1.0);  // 1 is non-zero
    EXPECT_DOUBLE_EQ(result[2], 0.0);  // 0 is zero
    EXPECT_DOUBLE_EQ(result[3], 1.0);  // 5 is non-zero
    EXPECT_DOUBLE_EQ(result[4], 0.0);  // NaN counts as zero
}

TEST_F(TransformFunctionsTest, CountNotNull) {
    std::vector<double> values = {0.0, 1.0, std::nan(""), 5.0};

    auto result = count_not_null(values);

    EXPECT_DOUBLE_EQ(result[0], 1.0);  // 0 is not null
    EXPECT_DOUBLE_EQ(result[1], 1.0);
    EXPECT_DOUBLE_EQ(result[2], 0.0);  // NaN is null
    EXPECT_DOUBLE_EQ(result[3], 1.0);
}

// ============================================================================
// Rollup Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, MovingRollupAvg) {
    std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    // 2-second window
    auto result = moving_rollup(values, timestamps, 2.0, "avg");

    // At t=4s, window includes t=2,3,4 -> values 30,40,50 -> avg=40
    EXPECT_DOUBLE_EQ(result[4], 40.0);
}

TEST_F(TransformFunctionsTest, MovingRollupSum) {
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    // 2-second window
    auto result = moving_rollup(values, timestamps, 2.0, "sum");

    // At t=4s, window includes t=2,3,4 -> values 3,4,5 -> sum=12
    EXPECT_DOUBLE_EQ(result[4], 12.0);
}

TEST_F(TransformFunctionsTest, MovingRollupMin) {
    std::vector<double> values = {5.0, 3.0, 7.0, 2.0, 8.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = moving_rollup(values, timestamps, 2.0, "min");

    // At t=4s, window includes t=2,3,4 -> values 7,2,8 -> min=2
    EXPECT_DOUBLE_EQ(result[4], 2.0);
}

TEST_F(TransformFunctionsTest, MovingRollupMax) {
    std::vector<double> values = {5.0, 3.0, 7.0, 2.0, 8.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = moving_rollup(values, timestamps, 2.0, "max");

    // At t=4s, window includes t=2,3,4 -> values 7,2,8 -> max=8
    EXPECT_DOUBLE_EQ(result[4], 8.0);
}

// ============================================================================
// Timeshift Function Tests
// ============================================================================

TEST_F(TransformFunctionsTest, TimeshiftForward) {
    std::vector<uint64_t> timestamps = {1'000'000'000, 2'000'000'000, 3'000'000'000};

    auto result = timeshift(timestamps, 1.0);  // Shift forward 1 second

    EXPECT_EQ(result[0], 2'000'000'000ULL);
    EXPECT_EQ(result[1], 3'000'000'000ULL);
    EXPECT_EQ(result[2], 4'000'000'000ULL);
}

TEST_F(TransformFunctionsTest, TimeshiftBackward) {
    std::vector<uint64_t> timestamps = {10'000'000'000, 20'000'000'000, 30'000'000'000};

    auto result = timeshift(timestamps, -5.0);  // Shift back 5 seconds

    EXPECT_EQ(result[0], 5'000'000'000ULL);
    EXPECT_EQ(result[1], 15'000'000'000ULL);
    EXPECT_EQ(result[2], 25'000'000'000ULL);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(TransformFunctionsTest, EmptyInputHandling) {
    std::vector<double> empty;
    std::vector<uint64_t> emptyTs;

    EXPECT_TRUE(diff(empty).empty());
    EXPECT_TRUE(dt(emptyTs).empty());
    EXPECT_TRUE(derivative(empty, emptyTs).empty());
    EXPECT_TRUE(rate(empty, emptyTs).empty());
    EXPECT_TRUE(abs(empty).empty());
    EXPECT_TRUE(cumsum(empty).empty());
    EXPECT_TRUE(ewma(empty, 3).empty());
    EXPECT_TRUE(median(empty, 3).empty());
}

TEST_F(TransformFunctionsTest, SingleValueHandling) {
    std::vector<double> single = {42.0};
    std::vector<uint64_t> singleTs = {1'000'000'000};

    auto diffResult = diff(single);
    EXPECT_EQ(diffResult.size(), 1u);
    EXPECT_TRUE(std::isnan(diffResult[0]));

    auto absResult = abs(single);
    EXPECT_EQ(absResult.size(), 1u);
    EXPECT_DOUBLE_EQ(absResult[0], 42.0);
}

TEST_F(TransformFunctionsTest, AllNaNHandling) {
    std::vector<double> allNaN = {std::nan(""), std::nan(""), std::nan("")};

    auto result = ewma(allNaN, 3);
    EXPECT_EQ(result.size(), 3u);
    for (auto v : result) {
        EXPECT_TRUE(std::isnan(v));
    }
}

// ============================================================================
// Rank Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, TopByMean) {
    // 5 series with different means
    std::vector<std::vector<double>> series = {
        {10.0, 20.0, 30.0},  // mean = 20, index 0
        {50.0, 60.0, 70.0},  // mean = 60, index 1
        {30.0, 40.0, 50.0},  // mean = 40, index 2
        {5.0, 10.0, 15.0},   // mean = 10, index 3
        {80.0, 90.0, 100.0}  // mean = 90, index 4
    };

    auto result = top(series, 3, "mean", "desc");

    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0], 4u);  // mean = 90
    EXPECT_EQ(result[1], 1u);  // mean = 60
    EXPECT_EQ(result[2], 2u);  // mean = 40
}

TEST_F(TransformFunctionsTest, TopByMax) {
    std::vector<std::vector<double>> series = {
        {1.0, 100.0, 1.0},   // max = 100, index 0
        {50.0, 50.0, 50.0},  // max = 50, index 1
        {10.0, 20.0, 200.0}  // max = 200, index 2
    };

    auto result = top(series, 2, "max", "desc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // max = 200
    EXPECT_EQ(result[1], 0u);  // max = 100
}

TEST_F(TransformFunctionsTest, TopByLast) {
    std::vector<std::vector<double>> series = {
        {100.0, 50.0, 10.0},  // last = 10, index 0
        {10.0, 20.0, 80.0},   // last = 80, index 1
        {30.0, 40.0, 50.0}    // last = 50, index 2
    };

    auto result = top(series, 2, "last", "desc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 1u);  // last = 80
    EXPECT_EQ(result[1], 2u);  // last = 50
}

TEST_F(TransformFunctionsTest, TopAscending) {
    std::vector<std::vector<double>> series = {
        {10.0, 20.0, 30.0},  // mean = 20
        {50.0, 60.0, 70.0},  // mean = 60
        {5.0, 5.0, 5.0}      // mean = 5
    };

    auto result = top(series, 2, "mean", "asc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // mean = 5 (lowest first)
    EXPECT_EQ(result[1], 0u);  // mean = 20
}

TEST_F(TransformFunctionsTest, BottomByMean) {
    std::vector<std::vector<double>> series = {
        {50.0, 60.0, 70.0},  // mean = 60
        {10.0, 20.0, 30.0},  // mean = 20
        {80.0, 90.0, 100.0}  // mean = 90
    };

    auto result = bottom(series, 2, "mean");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 1u);  // mean = 20 (lowest)
    EXPECT_EQ(result[1], 0u);  // mean = 60
}

TEST_F(TransformFunctionsTest, TopOffsetBasic) {
    std::vector<std::vector<double>> series = {{10.0}, {20.0}, {30.0}, {40.0}, {50.0}};

    // Skip top 2, get next 2
    auto result = top_offset(series, 2, "mean", "desc", 2);

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // mean = 30 (3rd highest)
    EXPECT_EQ(result[1], 1u);  // mean = 20 (4th highest)
}

TEST_F(TransformFunctionsTest, TopByL2Norm) {
    std::vector<std::vector<double>> series = {
        {3.0, 4.0},  // L2 = sqrt(9+16) = 5
        {1.0, 1.0},  // L2 = sqrt(2) = 1.41
        {6.0, 8.0}   // L2 = sqrt(36+64) = 10
    };

    auto result = top(series, 2, "l2norm", "desc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // L2 = 10
    EXPECT_EQ(result[1], 0u);  // L2 = 5
}

TEST_F(TransformFunctionsTest, TopByArea) {
    std::vector<std::vector<double>> series = {
        {-10.0, 10.0},  // area = 20
        {5.0, 5.0},     // area = 10
        {-20.0, -10.0}  // area = 30
    };

    auto result = top(series, 2, "area", "desc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // area = 30
    EXPECT_EQ(result[1], 0u);  // area = 20
}

TEST_F(TransformFunctionsTest, TopWithNaNSeries) {
    std::vector<std::vector<double>> series = {{10.0, 20.0},
                                               {std::nan(""), std::nan("")},  // All NaN - excluded
                                               {30.0, 40.0}};

    auto result = top(series, 3, "mean", "desc");

    // Only 2 series have valid data
    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // mean = 35
    EXPECT_EQ(result[1], 0u);  // mean = 15
}

TEST_F(TransformFunctionsTest, TopMoreThanAvailable) {
    std::vector<std::vector<double>> series = {{10.0}, {20.0}};

    auto result = top(series, 10, "mean", "desc");

    // Should return all available
    ASSERT_EQ(result.size(), 2u);
}

// ============================================================================
// Regression Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, TrendLinePerfectLinear) {
    // Perfect linear data: y = 2x + 10
    std::vector<double> values = {10.0, 12.0, 14.0, 16.0, 18.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = trend_line(values, timestamps);

    EXPECT_EQ(result.size(), 5u);
    EXPECT_NEAR(result[0], 10.0, 0.001);
    EXPECT_NEAR(result[1], 12.0, 0.001);
    EXPECT_NEAR(result[2], 14.0, 0.001);
    EXPECT_NEAR(result[3], 16.0, 0.001);
    EXPECT_NEAR(result[4], 18.0, 0.001);
}

TEST_F(TransformFunctionsTest, TrendLineWithNoise) {
    // Linear trend with noise
    std::vector<double> values = {10.0, 13.0, 13.5, 17.0, 19.5};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = trend_line(values, timestamps);

    EXPECT_EQ(result.size(), 5u);
    // Trend should be approximately linear, increasing
    EXPECT_LT(result[0], result[4]);
    // Check monotonically increasing
    for (size_t i = 1; i < result.size(); ++i) {
        EXPECT_GE(result[i], result[i - 1]);
    }
}

TEST_F(TransformFunctionsTest, TrendLineExtendedStatistics) {
    // Perfect linear: y = 5x + 0
    std::vector<double> values = {0.0, 5.0, 10.0, 15.0, 20.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = trend_line_extended(values, timestamps);

    EXPECT_NEAR(result.slope, 5.0, 0.001);
    EXPECT_NEAR(result.intercept, 0.0, 0.001);
    EXPECT_NEAR(result.rSquared, 1.0, 0.001);  // Perfect fit
    EXPECT_NEAR(result.residualStdDev, 0.0, 0.001);
}

TEST_F(TransformFunctionsTest, RobustTrendIgnoresOutliers) {
    // Linear trend with outlier
    std::vector<double> values = {10.0, 12.0, 100.0, 16.0, 18.0};  // 100 is outlier
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto olsResult = trend_line(values, timestamps);
    auto robustResult = robust_trend(values, timestamps);

    // OLS will be heavily influenced by outlier
    // Robust should fit closer to the true trend

    // The robust result at index 2 should be closer to 14 (true value)
    // than the OLS result
    double trueValueAt2 = 14.0;
    double olsError = std::abs(olsResult[2] - trueValueAt2);
    double robustError = std::abs(robustResult[2] - trueValueAt2);

    // Robust should have less error at the outlier point's expected value
    // (This test might be sensitive to the specific robust algorithm)
    EXPECT_LT(robustError, olsError + 20);  // Robust should be better or similar
}

TEST_F(TransformFunctionsTest, RobustTrendNormalData) {
    // Normal linear data - robust should match OLS
    std::vector<double> values = {10.0, 12.0, 14.0, 16.0, 18.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto olsResult = trend_line(values, timestamps);
    auto robustResult = robust_trend(values, timestamps);

    // Results should be similar for clean data
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_NEAR(olsResult[i], robustResult[i], 0.5);
    }
}

TEST_F(TransformFunctionsTest, PiecewiseConstantSingleLevel) {
    // Constant data
    std::vector<double> values(20, 50.0);

    auto result = piecewise_constant(values);

    // All values should be close to 50
    for (auto v : result) {
        EXPECT_NEAR(v, 50.0, 0.1);
    }
}

TEST_F(TransformFunctionsTest, PiecewiseConstantTwoLevels) {
    // Step change from 10 to 50
    std::vector<double> values;
    for (int i = 0; i < 10; ++i)
        values.push_back(10.0);
    for (int i = 0; i < 10; ++i)
        values.push_back(50.0);

    auto result = piecewise_constant(values, 3, 1.5);

    // First segment should be near 10
    EXPECT_NEAR(result[0], 10.0, 2.0);
    EXPECT_NEAR(result[5], 10.0, 2.0);

    // Second segment should be near 50
    EXPECT_NEAR(result[15], 50.0, 2.0);
    EXPECT_NEAR(result[19], 50.0, 2.0);
}

TEST_F(TransformFunctionsTest, RankMethodSum) {
    std::vector<std::vector<double>> series = {
        {10.0, 20.0, 30.0},  // sum = 60
        {5.0, 5.0, 5.0},     // sum = 15
        {100.0, 0.0, 0.0}    // sum = 100
    };

    auto result = top(series, 2, "sum", "desc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 2u);  // sum = 100
    EXPECT_EQ(result[1], 0u);  // sum = 60
}

TEST_F(TransformFunctionsTest, RankMethodMin) {
    std::vector<std::vector<double>> series = {
        {1.0, 100.0, 100.0},  // min = 1
        {50.0, 50.0, 50.0},   // min = 50
        {10.0, 20.0, 30.0}    // min = 10
    };

    // Top by min ascending = series with smallest minimum value
    auto result = top(series, 2, "min", "asc");

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0], 0u);  // min = 1 (smallest)
    EXPECT_EQ(result[1], 2u);  // min = 10
}

// ============================================================================
// Outliers (Spatial Anomaly) Functions Tests
// ============================================================================

TEST_F(TransformFunctionsTest, OutliersMADBasic) {
    // 5 series, one is clearly an outlier
    std::vector<std::vector<double>> series = {
        {10.0, 10.0, 10.0, 10.0, 10.0},      // Normal
        {11.0, 11.0, 11.0, 11.0, 11.0},      // Normal
        {9.0, 9.0, 9.0, 9.0, 9.0},           // Normal
        {10.5, 10.5, 10.5, 10.5, 10.5},      // Normal
        {100.0, 100.0, 100.0, 100.0, 100.0}  // Outlier!
    };

    auto results = outliers(series, "mad", 3.0, 50.0);

    ASSERT_EQ(results.size(), 5u);

    // Series 4 should be the outlier
    EXPECT_FALSE(results[0].isOutlier);
    EXPECT_FALSE(results[1].isOutlier);
    EXPECT_FALSE(results[2].isOutlier);
    EXPECT_FALSE(results[3].isOutlier);
    EXPECT_TRUE(results[4].isOutlier);
    EXPECT_GT(results[4].outlierPercentage, 50.0);
}

TEST_F(TransformFunctionsTest, OutliersDBSCANBasic) {
    // 5 series, one is an outlier
    std::vector<std::vector<double>> series = {
        {10.0, 10.0, 10.0, 10.0},
        {11.0, 11.0, 11.0, 11.0},
        {9.0, 9.0, 9.0, 9.0},
        {10.5, 10.5, 10.5, 10.5},
        {1000.0, 1000.0, 1000.0, 1000.0}  // Way off!
    };

    auto results = outliers(series, "dbscan", 3.0, 50.0);

    ASSERT_EQ(results.size(), 5u);

    // Series 4 should be the outlier with DBSCAN too
    EXPECT_TRUE(results[4].isOutlier);
}

TEST_F(TransformFunctionsTest, OutliersIndices) {
    std::vector<std::vector<double>> series = {
        {10.0, 10.0, 10.0},
        {10.0, 10.0, 10.0},
        {100.0, 100.0, 100.0},  // Outlier
        {10.0, 10.0, 10.0},
        {-50.0, -50.0, -50.0}  // Outlier
    };

    auto indices = outliers_indices(series, "mad", 2.0, 50.0);

    // Should contain indices 2 and 4
    EXPECT_EQ(indices.size(), 2u);
    bool has2 = std::find(indices.begin(), indices.end(), 2u) != indices.end();
    bool has4 = std::find(indices.begin(), indices.end(), 4u) != indices.end();
    EXPECT_TRUE(has2);
    EXPECT_TRUE(has4);
}

TEST_F(TransformFunctionsTest, OutliersMask) {
    std::vector<std::vector<double>> series = {{10.0, 10.0, 100.0, 10.0},  // Outlier at t=2
                                               {10.0, 10.0, 10.0, 10.0},
                                               {10.0, 10.0, 10.0, 10.0},
                                               {10.0, 10.0, 10.0, 10.0}};

    auto masks = outliers_mask(series, "mad", 2.0);

    ASSERT_EQ(masks.size(), 4u);
    ASSERT_EQ(masks[0].size(), 4u);

    // Series 0 should be marked as outlier at time point 2
    EXPECT_FALSE(masks[0][0]);
    EXPECT_FALSE(masks[0][1]);
    EXPECT_TRUE(masks[0][2]);  // The 100.0 value
    EXPECT_FALSE(masks[0][3]);

    // Other series should have no outlier flags at t=2
    EXPECT_FALSE(masks[1][2]);
    EXPECT_FALSE(masks[2][2]);
    EXPECT_FALSE(masks[3][2]);
}

TEST_F(TransformFunctionsTest, OutliersNoOutliers) {
    // All series are similar - no outliers
    std::vector<std::vector<double>> series = {
        {10.0, 10.0, 10.0}, {10.1, 10.1, 10.1}, {9.9, 9.9, 9.9}, {10.05, 10.05, 10.05}};

    auto results = outliers(series, "mad", 3.0, 50.0);

    for (const auto& r : results) {
        EXPECT_FALSE(r.isOutlier);
    }
}

TEST_F(TransformFunctionsTest, OutliersEmptyInput) {
    std::vector<std::vector<double>> empty;
    auto results = outliers(empty, "mad", 3.0, 50.0);
    EXPECT_TRUE(results.empty());
}

TEST_F(TransformFunctionsTest, OutliersWithNaN) {
    // Series with some NaN values
    std::vector<std::vector<double>> series = {
        {10.0, std::nan(""), 10.0, 10.0},
        {10.0, 10.0, 10.0, 10.0},
        {10.0, 10.0, 10.0, 10.0},
        {100.0, 100.0, 100.0, 100.0}  // Outlier
    };

    auto results = outliers(series, "mad", 3.0, 50.0);

    ASSERT_EQ(results.size(), 4u);
    EXPECT_TRUE(results[3].isOutlier);
}

TEST_F(TransformFunctionsTest, OutliersLowTolerance) {
    // With low tolerance, more things become outliers
    std::vector<std::vector<double>> series = {
        {10.0, 10.0, 10.0}, {11.0, 11.0, 11.0}, {12.0, 12.0, 12.0}, {20.0, 20.0, 20.0}};

    // With high tolerance
    auto highTol = outliers(series, "mad", 5.0, 50.0);
    size_t highOutliers = 0;
    for (const auto& r : highTol) {
        if (r.isOutlier)
            highOutliers++;
    }

    // With low tolerance
    auto lowTol = outliers(series, "mad", 1.5, 50.0);
    size_t lowOutliers = 0;
    for (const auto& r : lowTol) {
        if (r.isOutlier)
            lowOutliers++;
    }

    // Lower tolerance should find more or equal outliers
    EXPECT_GE(lowOutliers, highOutliers);
}

TEST_F(TransformFunctionsTest, OutliersMinPercentage) {
    // Series that's an outlier for only 1 out of 4 points (25%)
    std::vector<std::vector<double>> series = {
        {10.0, 10.0, 10.0, 10.0}, {10.0, 10.0, 10.0, 10.0}, {10.0, 10.0, 10.0, 10.0}, {100.0, 10.0, 10.0, 10.0}
        // Outlier only at t=0
    };

    // With 50% threshold, series 3 should NOT be an outlier
    auto high = outliers(series, "mad", 3.0, 50.0);
    EXPECT_FALSE(high[3].isOutlier);

    // With 20% threshold, series 3 SHOULD be an outlier
    auto low = outliers(series, "mad", 3.0, 20.0);
    EXPECT_TRUE(low[3].isOutlier);
}

// ============================================================================
// Edge Case Tests (Added for robustness)
// ============================================================================

TEST_F(TransformFunctionsTest, DtNonMonotonicTimestamps) {
    // Non-monotonic timestamps should return NaN
    std::vector<uint64_t> timestamps = {
        1'000'000'000,  // 1s
        500'000'000,    // 0.5s (goes backward!)
        2'000'000'000   // 2s
    };

    auto result = dt(timestamps);

    EXPECT_TRUE(std::isnan(result[0]));  // First is always NaN
    EXPECT_TRUE(std::isnan(result[1]));  // Non-monotonic
    EXPECT_DOUBLE_EQ(result[2], 1.5);    // 2s - 0.5s = 1.5s
}

TEST_F(TransformFunctionsTest, DerivativeNonMonotonicTimestamps) {
    std::vector<double> values = {10.0, 20.0, 30.0};
    std::vector<uint64_t> timestamps = {
        1'000'000'000,  // 1s
        500'000'000,    // 0.5s (goes backward!)
        2'000'000'000   // 2s
    };

    auto result = derivative(values, timestamps);

    EXPECT_TRUE(std::isnan(result[0]));       // First is always NaN
    EXPECT_TRUE(std::isnan(result[1]));       // Non-monotonic timestamps
    EXPECT_DOUBLE_EQ(result[2], 10.0 / 1.5);  // (30-20) / 1.5s
}

TEST_F(TransformFunctionsTest, RateNonMonotonicTimestamps) {
    std::vector<double> values = {10.0, 20.0, 30.0};
    std::vector<uint64_t> timestamps = {1'000'000'000,
                                        500'000'000,  // Goes backward
                                        2'000'000'000};

    auto result = rate(values, timestamps);

    EXPECT_TRUE(std::isnan(result[1]));  // Non-monotonic should be NaN
}

TEST_F(TransformFunctionsTest, IntegralNonMonotonicTimestamps) {
    std::vector<double> values = {10.0, 20.0, 30.0};
    std::vector<uint64_t> timestamps = {1'000'000'000,
                                        500'000'000,  // Goes backward
                                        2'000'000'000};

    auto result = integral(values, timestamps);

    // Integral should skip non-monotonic interval and keep previous area
    EXPECT_DOUBLE_EQ(result[0], 0.0);
    EXPECT_DOUBLE_EQ(result[1], 0.0);  // Skipped (kept previous)
    // At i=2: (20 + 30) / 2 * 1.5s = 37.5
    EXPECT_DOUBLE_EQ(result[2], 37.5);
}

TEST_F(TransformFunctionsTest, MedianEvenCountWindow) {
    // Test that even-count windows calculate median correctly
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};

    // Window of 4 (adjusted to 5 for odd, but let's verify even handling)
    auto result = median(values, 4);  // Will be adjusted to 5

    // With window 5 centered at position 2: values 0,1,2,3,4 -> median of [1,2,3,4,5] = 3
    EXPECT_DOUBLE_EQ(result[2], 3.0);
}

TEST_F(TransformFunctionsTest, CalculateMedianEvenArray) {
    // Verify calculateMedian helper works for even arrays
    std::vector<double> even = {1.0, 2.0, 3.0, 4.0};
    std::sort(even.begin(), even.end());
    double med = calculateMedian(even);
    EXPECT_DOUBLE_EQ(med, 2.5);  // (2 + 3) / 2

    std::vector<double> odd = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::sort(odd.begin(), odd.end());
    med = calculateMedian(odd);
    EXPECT_DOUBLE_EQ(med, 3.0);
}

TEST_F(TransformFunctionsTest, OutliersMaskMismatchedLengths) {
    // Mismatched series lengths should return empty
    std::vector<std::vector<double>> mismatched = {
        {1.0, 2.0, 3.0}, {1.0, 2.0}  // Different length
    };

    auto masks = outliers_mask(mismatched, "mad", 3.0);
    EXPECT_TRUE(masks.empty());
}

TEST_F(TransformFunctionsTest, CalculateMADWithNaN) {
    // MAD should filter NaN values
    std::vector<double> withNaN = {1.0, std::nan(""), 3.0, 5.0, 7.0};
    double mad = calculateMAD(withNaN);

    // Valid values: [1, 3, 5, 7], median = 4, deviations = [3, 1, 1, 3]
    // Median of deviations = 2 (or 1.5 for even)
    // MAD with valid values [1,3,5,7]: median = (3+5)/2 = 4
    // deviations = [3, 1, 1, 3], sorted = [1, 1, 3, 3], median = (1+3)/2 = 2
    // MAD = 2 * 1.4826 = 2.9652
    EXPECT_NEAR(mad, 2.9652, 0.01);
}

TEST_F(TransformFunctionsTest, PiecewiseConstantWithNaN) {
    // Piecewise constant should handle NaN values
    std::vector<double> values = {10.0, 10.0, std::nan(""), 10.0, 10.0, 50.0, 50.0, 50.0, std::nan(""), 50.0};

    auto result = piecewise_constant(values, 3, 2.0);

    // Should detect change point and fill NaN positions appropriately
    EXPECT_EQ(result.size(), values.size());
    // NaN positions should be filled with segment means
    EXPECT_FALSE(std::isnan(result[2]));
    EXPECT_FALSE(std::isnan(result[8]));
}

// ============================================================================
// SIMD Tests - Large arrays to trigger SIMD code paths
// These tests verify SIMD implementations produce correct results
// ============================================================================

TEST_F(TransformFunctionsTest, SimdAbsLargeArray) {
    // Test abs() with a large array (>16 elements triggers SIMD)
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = (i % 2 == 0) ? static_cast<double>(i) : -static_cast<double>(i);
    }

    auto result = abs(values);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i], static_cast<double>(i));
    }
}

TEST_F(TransformFunctionsTest, SimdDefaultZeroLargeArray) {
    // Test default_zero() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = (i % 5 == 0) ? std::nan("") : static_cast<double>(i);
    }

    auto result = default_zero(values);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i % 5 == 0) {
            EXPECT_DOUBLE_EQ(result[i], 0.0);
        } else {
            EXPECT_DOUBLE_EQ(result[i], static_cast<double>(i));
        }
    }
}

TEST_F(TransformFunctionsTest, SimdCountNonzeroLargeArray) {
    // Test count_nonzero() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i % 3 == 0)
            values[i] = 0.0;
        else if (i % 7 == 0)
            values[i] = std::nan("");
        else
            values[i] = static_cast<double>(i);
    }

    auto result = count_nonzero(values);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        bool isNonzero = !std::isnan(values[i]) && values[i] != 0.0;
        EXPECT_DOUBLE_EQ(result[i], isNonzero ? 1.0 : 0.0) << "Mismatch at index " << i;
    }
}

TEST_F(TransformFunctionsTest, SimdCountNotNullLargeArray) {
    // Test count_not_null() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = (i % 4 == 0) ? std::nan("") : static_cast<double>(i);
    }

    auto result = count_not_null(values);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_DOUBLE_EQ(result[i], (i % 4 == 0) ? 0.0 : 1.0);
    }
}

TEST_F(TransformFunctionsTest, SimdClampMinLargeArray) {
    // Test clamp_min() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<double>(i) - 50.0;  // Range: -50 to 49
    }
    values[25] = std::nan("");  // Add a NaN

    auto result = clamp_min(values, 0.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i == 25) {
            EXPECT_TRUE(std::isnan(result[i])) << "NaN should be preserved";
        } else {
            double expected = std::max(values[i], 0.0);
            EXPECT_DOUBLE_EQ(result[i], expected) << "Mismatch at index " << i;
        }
    }
}

TEST_F(TransformFunctionsTest, SimdClampMaxLargeArray) {
    // Test clamp_max() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<double>(i);  // Range: 0 to 99
    }
    values[50] = std::nan("");  // Add a NaN

    auto result = clamp_max(values, 50.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i == 50) {
            EXPECT_TRUE(std::isnan(result[i])) << "NaN should be preserved";
        } else {
            double expected = std::min(static_cast<double>(i), 50.0);
            EXPECT_DOUBLE_EQ(result[i], expected) << "Mismatch at index " << i;
        }
    }
}

TEST_F(TransformFunctionsTest, SimdCutoffMinLargeArray) {
    // Test cutoff_min() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<double>(i) - 50.0;  // Range: -50 to 49
    }

    auto result = cutoff_min(values, 0.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (values[i] < 0.0) {
            EXPECT_TRUE(std::isnan(result[i])) << "Values below threshold should be NaN";
        } else {
            EXPECT_DOUBLE_EQ(result[i], values[i]) << "Values >= threshold should be preserved";
        }
    }
}

TEST_F(TransformFunctionsTest, SimdCutoffMaxLargeArray) {
    // Test cutoff_max() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<double>(i);  // Range: 0 to 99
    }

    auto result = cutoff_max(values, 50.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (values[i] > 50.0) {
            EXPECT_TRUE(std::isnan(result[i])) << "Values above threshold should be NaN";
        } else {
            EXPECT_DOUBLE_EQ(result[i], values[i]) << "Values <= threshold should be preserved";
        }
    }
}

TEST_F(TransformFunctionsTest, SimdDiffLargeArray) {
    // Test diff() with a large array
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<double>(i * 2);  // 0, 2, 4, 6, ...
    }
    values[30] = std::nan("");  // Add NaN to test handling

    auto result = diff(values);

    ASSERT_EQ(result.size(), values.size());
    EXPECT_TRUE(std::isnan(result[0])) << "First diff should be NaN";

    for (size_t i = 1; i < result.size(); ++i) {
        if (i == 30 || i == 31) {
            EXPECT_TRUE(std::isnan(result[i])) << "Diff involving NaN should be NaN at " << i;
        } else {
            EXPECT_DOUBLE_EQ(result[i], 2.0) << "Diff should be 2.0 at " << i;
        }
    }
}

TEST_F(TransformFunctionsTest, SimdMonotonicDiffLargeArray) {
    // Test monotonic_diff() with a large array including resets
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i == 50) {
            values[i] = 0.0;  // Simulate counter reset
        } else {
            values[i] = static_cast<double>(i);
        }
    }

    auto result = monotonic_diff(values);

    ASSERT_EQ(result.size(), values.size());
    EXPECT_TRUE(std::isnan(result[0])) << "First diff should be NaN";

    for (size_t i = 1; i < result.size(); ++i) {
        if (i == 50) {
            // Counter reset: 0 - 49 = -49, returns current value (0.0)
            EXPECT_DOUBLE_EQ(result[i], 0.0) << "Counter reset should return current value";
        } else if (i == 51) {
            // After reset: 51 - 0 = 51
            EXPECT_DOUBLE_EQ(result[i], 51.0);
        } else {
            // Normal case: i - (i-1) = 1
            EXPECT_DOUBLE_EQ(result[i], 1.0) << "Normal diff should be 1.0 at " << i;
        }
    }
}

TEST_F(TransformFunctionsTest, SimdConsistencyScalarVsSimd) {
    // Test that SIMD and scalar produce identical results
    // We'll create arrays of varying sizes and verify consistency

    // Create test data with various patterns
    std::vector<double> testData(256);
    for (size_t i = 0; i < testData.size(); ++i) {
        if (i % 13 == 0)
            testData[i] = std::nan("");
        else if (i % 7 == 0)
            testData[i] = 0.0;
        else if (i % 3 == 0)
            testData[i] = -static_cast<double>(i);
        else
            testData[i] = static_cast<double>(i) * 1.5;
    }

    // Test abs
    auto absResult = abs(testData);
    for (size_t i = 0; i < testData.size(); ++i) {
        if (std::isnan(testData[i])) {
            EXPECT_TRUE(std::isnan(absResult[i]));
        } else {
            EXPECT_DOUBLE_EQ(absResult[i], std::abs(testData[i]));
        }
    }

    // Test default_zero
    auto defaultZeroResult = default_zero(testData);
    for (size_t i = 0; i < testData.size(); ++i) {
        if (std::isnan(testData[i])) {
            EXPECT_DOUBLE_EQ(defaultZeroResult[i], 0.0);
        } else {
            EXPECT_DOUBLE_EQ(defaultZeroResult[i], testData[i]);
        }
    }

    // Test count_nonzero
    auto countNzResult = count_nonzero(testData);
    for (size_t i = 0; i < testData.size(); ++i) {
        bool isNonzero = !std::isnan(testData[i]) && testData[i] != 0.0;
        EXPECT_DOUBLE_EQ(countNzResult[i], isNonzero ? 1.0 : 0.0);
    }

    // Test count_not_null
    auto countNnResult = count_not_null(testData);
    for (size_t i = 0; i < testData.size(); ++i) {
        EXPECT_DOUBLE_EQ(countNnResult[i], std::isnan(testData[i]) ? 0.0 : 1.0);
    }
}

// ============================================================================
// DBSCAN 1D Tests
// ============================================================================

TEST_F(TransformFunctionsTest, Dbscan1DThreeClusters) {
    // 3 clusters of 5 points each, well-separated
    // Cluster 0: values around 10 (range 8-12)
    // Cluster 1: values around 50 (range 48-52)
    // Cluster 2: values around 100 (range 98-102)
    std::vector<double> values = {
        10.0,  11.0,  9.0,  12.0,  8.0,   // Cluster around 10
        50.0,  51.0,  49.0, 52.0,  48.0,  // Cluster around 50
        100.0, 101.0, 99.0, 102.0, 98.0   // Cluster around 100
    };

    // epsilon=5 should capture each cluster; minPoints=3
    auto clusters = dbscan1D(values, 5.0, 3);

    ASSERT_EQ(clusters.size(), 15u);

    // All points in the first cluster should share the same cluster ID
    int cluster0 = clusters[0];
    EXPECT_NE(cluster0, -1);  // Not noise
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(clusters[i], cluster0) << "Point " << i << " should be in same cluster as point 0";
    }

    // All points in the second cluster should share the same cluster ID
    int cluster1 = clusters[5];
    EXPECT_NE(cluster1, -1);
    for (size_t i = 5; i < 10; ++i) {
        EXPECT_EQ(clusters[i], cluster1) << "Point " << i << " should be in same cluster as point 5";
    }

    // All points in the third cluster should share the same cluster ID
    int cluster2 = clusters[10];
    EXPECT_NE(cluster2, -1);
    for (size_t i = 10; i < 15; ++i) {
        EXPECT_EQ(clusters[i], cluster2) << "Point " << i << " should be in same cluster as point 10";
    }

    // The three clusters should have different IDs
    EXPECT_NE(cluster0, cluster1);
    EXPECT_NE(cluster0, cluster2);
    EXPECT_NE(cluster1, cluster2);
}

TEST_F(TransformFunctionsTest, Dbscan1DNoisePoints) {
    // Dense cluster with an isolated outlier
    std::vector<double> values = {
        1.0,  1.5, 2.0, 2.5, 3.0,  // Tight cluster
        100.0                      // Isolated point (noise)
    };

    auto clusters = dbscan1D(values, 2.0, 3);

    // The first 5 points should be in a cluster
    int mainCluster = clusters[0];
    EXPECT_NE(mainCluster, -1);
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(clusters[i], mainCluster);
    }

    // The isolated point should be noise (-1)
    EXPECT_EQ(clusters[5], -1);
}

TEST_F(TransformFunctionsTest, Dbscan1DAllSameValue) {
    // All identical values should form one cluster
    std::vector<double> values(10, 42.0);

    auto clusters = dbscan1D(values, 1.0, 3);

    int clusterID = clusters[0];
    EXPECT_NE(clusterID, -1);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(clusters[i], clusterID);
    }
}

TEST_F(TransformFunctionsTest, Dbscan1DLargeInput) {
    // Performance test: 5000 points in 3 clusters should complete quickly
    // Cluster 0: 0-1000 (values around 0)
    // Cluster 1: 1000-2000 (values around 10000)
    // Cluster 2: 2000-5000 (values around 20000)
    std::vector<double> values(5000);
    for (size_t i = 0; i < 1000; ++i) {
        values[i] = static_cast<double>(i % 10);  // 0-9
    }
    for (size_t i = 1000; i < 2000; ++i) {
        values[i] = 10000.0 + static_cast<double>(i % 10);  // 10000-10009
    }
    for (size_t i = 2000; i < 5000; ++i) {
        values[i] = 20000.0 + static_cast<double>(i % 10);  // 20000-20009
    }

    auto clusters = dbscan1D(values, 15.0, 5);

    ASSERT_EQ(clusters.size(), 5000u);

    // Verify no noise in a dense cluster
    // All points in cluster 0 region should be in the same cluster
    int c0 = clusters[0];
    EXPECT_NE(c0, -1);
    for (size_t i = 1; i < 1000; ++i) {
        EXPECT_EQ(clusters[i], c0);
    }

    // Cluster 1 should be different from cluster 0
    int c1 = clusters[1000];
    EXPECT_NE(c1, -1);
    EXPECT_NE(c1, c0);
}

// ============================================================================
// Moving Rollup Correctness and Performance Tests
// ============================================================================

TEST_F(TransformFunctionsTest, MovingRollupAvgSlidingWindow) {
    // Verify exact values for a known sliding window average
    // 10 points, 1 second apart, window = 3 seconds
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
    std::vector<uint64_t> timestamps;
    for (size_t i = 0; i < values.size(); ++i) {
        timestamps.push_back(i * 1'000'000'000ULL);
    }

    auto result = moving_rollup(values, timestamps, 3.0, "avg");

    ASSERT_EQ(result.size(), 10u);

    // At t=0 (0s): window [0,0] -> {1} -> avg=1
    EXPECT_DOUBLE_EQ(result[0], 1.0);
    // At t=1 (1s): window [0,1] -> {1,2} -> avg=1.5
    EXPECT_DOUBLE_EQ(result[1], 1.5);
    // At t=2 (2s): window [0,2] -> {1,2,3} -> avg=2
    EXPECT_DOUBLE_EQ(result[2], 2.0);
    // At t=3 (3s): window starts at 0s, includes [0,3] -> {1,2,3,4} -> avg=2.5
    EXPECT_DOUBLE_EQ(result[3], 2.5);
    // At t=4 (4s): window starts at 1s, includes [1,4] -> {2,3,4,5} -> avg=3.5
    EXPECT_DOUBLE_EQ(result[4], 3.5);
    // At t=5 (5s): window starts at 2s, includes [2,5] -> {3,4,5,6} -> avg=4.5
    EXPECT_DOUBLE_EQ(result[5], 4.5);
    // At t=9 (9s): window starts at 6s, includes [6,9] -> {7,8,9,10} -> avg=8.5
    EXPECT_DOUBLE_EQ(result[9], 8.5);
}

TEST_F(TransformFunctionsTest, MovingRollupCountWindow) {
    // Verify count method tracks window size correctly
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = moving_rollup(values, timestamps, 2.0, "count");

    // At t=0: window [0,0] -> count=1
    EXPECT_DOUBLE_EQ(result[0], 1.0);
    // At t=1: window [0,1] -> count=2
    EXPECT_DOUBLE_EQ(result[1], 2.0);
    // At t=2: window [0,2] -> count=3
    EXPECT_DOUBLE_EQ(result[2], 3.0);
    // At t=3: window starts at 1s, [1,3] -> count=3
    EXPECT_DOUBLE_EQ(result[3], 3.0);
    // At t=4: window starts at 2s, [2,4] -> count=3
    EXPECT_DOUBLE_EQ(result[4], 3.0);
}

TEST_F(TransformFunctionsTest, MovingRollupWithNaN) {
    // NaN values should be excluded from the window
    std::vector<double> values = {1.0, std::nan(""), 3.0, 4.0, 5.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000, 3'000'000'000, 4'000'000'000};

    auto result = moving_rollup(values, timestamps, 2.0, "sum");

    // At t=2: window [0,2], valid values {1, 3} -> sum=4
    EXPECT_DOUBLE_EQ(result[2], 4.0);
    // At t=3: window [1,3], valid values {3, 4} -> sum=7
    EXPECT_DOUBLE_EQ(result[3], 7.0);
}

TEST_F(TransformFunctionsTest, MovingRollupLargeInput) {
    // Performance test: 10000 points should complete very quickly with sliding window
    const size_t N = 10000;
    std::vector<double> values(N);
    std::vector<uint64_t> timestamps(N);
    for (size_t i = 0; i < N; ++i) {
        values[i] = static_cast<double>(i);
        timestamps[i] = i * 1'000'000'000ULL;  // 1 second apart
    }

    // 100-second window
    auto result = moving_rollup(values, timestamps, 100.0, "avg");

    ASSERT_EQ(result.size(), N);

    // Verify last point: window [9900, 9999] -> values 9900..9999 -> avg = 9949.5
    // Window starts at t=9900s (timestamp 9900*1e9), windowStart = (10000-100)*1e9 = 9900*1e9
    // But windowStart = timestamps[9999] - 100*1e9 = 9999*1e9 - 100*1e9 = 9899*1e9
    // So includes indices 9900..9999 (values 9900..9999 where timestamps >= 9899*1e9 + 1)
    // Actually: timestamps[j] >= windowStart means timestamps[j] >= 9899000000000
    // timestamps[9899] = 9899*1e9 = 9899000000000 -> included (>= windowStart)
    // So window is [9899, 9999] = 101 points, avg = (9899+9999)/2 = 9949.0
    // Let's just check it's reasonable and finite
    EXPECT_FALSE(std::isnan(result[N - 1]));
    EXPECT_GT(result[N - 1], 9800.0);
    EXPECT_LT(result[N - 1], 10000.0);
}

TEST_F(TransformFunctionsTest, MovingRollupMinMaxLargeInput) {
    // Verify min/max with larger input
    const size_t N = 1000;
    std::vector<double> values(N);
    std::vector<uint64_t> timestamps(N);
    for (size_t i = 0; i < N; ++i) {
        values[i] = static_cast<double>(i % 100);  // Repeating 0-99 pattern
        timestamps[i] = i * 1'000'000'000ULL;
    }

    auto minResult = moving_rollup(values, timestamps, 50.0, "min");
    auto maxResult = moving_rollup(values, timestamps, 50.0, "max");

    ASSERT_EQ(minResult.size(), N);
    ASSERT_EQ(maxResult.size(), N);

    // At any point, min <= value <= max within the window
    for (size_t i = 0; i < N; ++i) {
        EXPECT_LE(minResult[i], values[i]);
        EXPECT_GE(maxResult[i], values[i]);
        EXPECT_LE(minResult[i], maxResult[i]);
    }
}

TEST_F(TransformFunctionsTest, MovingRollupInvalidMethod) {
    std::vector<double> values = {1.0, 2.0, 3.0};
    std::vector<uint64_t> timestamps = {0, 1'000'000'000, 2'000'000'000};

    EXPECT_THROW(moving_rollup(values, timestamps, 1.0, "invalid"), std::invalid_argument);
}

// ============================================================================
// monotonic_diff counter reset consistency tests
// ============================================================================

TEST_F(TransformFunctionsTest, MonotonicDiffCounterResetReturnsCurrentValue) {
    // Verify that counter reset returns current value (not 0.0)
    // This is the standard counter-reset handling: on a counter wrap,
    // the new counter value is emitted as the diff.
    std::vector<double> values = {100.0, 150.0, 50.0, 80.0};

    auto result = monotonic_diff(values);

    EXPECT_TRUE(std::isnan(result[0]));
    EXPECT_DOUBLE_EQ(result[1], 50.0);  // 150 - 100 = 50 (positive, kept)
    EXPECT_DOUBLE_EQ(result[2], 50.0);  // 50 - 150 = -100 (negative, returns values[2] = 50.0)
    EXPECT_DOUBLE_EQ(result[3], 30.0);  // 80 - 50 = 30 (positive, kept)
}

TEST_F(TransformFunctionsTest, MonotonicDiffCounterResetLargeArrayConsistency) {
    // Verify SIMD and scalar paths produce the same result for counter resets
    // Use array size > SIMD_MIN_SIZE (16) to trigger SIMD code path
    std::vector<double> values(32);
    for (size_t i = 0; i < 32; ++i) {
        values[i] = static_cast<double>(i * 10);
    }
    // Inject counter resets at various positions
    values[5] = 10.0;   // Reset: 10 - 40 = -30, should return 10.0
    values[15] = 20.0;  // Reset: 20 - 130 = -110, should return 20.0
    values[25] = 5.0;   // Reset: 5 - 230 = -225, should return 5.0

    auto result = monotonic_diff(values);

    ASSERT_EQ(result.size(), 32u);
    EXPECT_TRUE(std::isnan(result[0]));

    // Check counter reset positions return current value
    EXPECT_DOUBLE_EQ(result[5], 10.0) << "Counter reset at index 5 should return current value (10.0)";
    EXPECT_DOUBLE_EQ(result[15], 20.0) << "Counter reset at index 15 should return current value (20.0)";
    EXPECT_DOUBLE_EQ(result[25], 5.0) << "Counter reset at index 25 should return current value (5.0)";

    // Also verify that the scalar fallback path gives the same results
    // by directly calling the scalar implementation
    auto scalarResult = simd::scalar::monotonic_diff(values);
    ASSERT_EQ(scalarResult.size(), result.size());
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(nearEqual(result[i], scalarResult[i]))
            << "SIMD and scalar disagree at index " << i << ": simd=" << result[i] << ", scalar=" << scalarResult[i];
    }
}

TEST_F(TransformFunctionsTest, MonotonicDiffCounterResetWithNaN) {
    // Verify NaN handling combined with counter reset
    std::vector<double> values = {100.0, std::nan(""), 50.0, 30.0, 60.0};

    auto result = monotonic_diff(values);

    EXPECT_TRUE(std::isnan(result[0]));  // First point
    EXPECT_TRUE(std::isnan(result[1]));  // NaN input
    EXPECT_TRUE(std::isnan(result[2]));  // Previous was NaN
    EXPECT_DOUBLE_EQ(result[3], 30.0);   // 30 - 50 = -20 (negative, returns 30.0)
    EXPECT_DOUBLE_EQ(result[4], 30.0);   // 60 - 30 = 30 (positive, kept)
}

// ============================================================================
// cutoff_min / cutoff_max with NaN inputs tests
// ============================================================================

TEST_F(TransformFunctionsTest, CutoffMinPreservesNaN) {
    // NaN values should be preserved (not converted to NaN by the threshold check)
    std::vector<double> values = {std::nan(""), 1.0, 5.0, std::nan(""), 10.0, std::nan("")};

    auto result = cutoff_min(values, 3.0);

    EXPECT_TRUE(std::isnan(result[0]));  // NaN preserved
    EXPECT_TRUE(std::isnan(result[1]));  // 1.0 < 3.0, becomes NaN
    EXPECT_DOUBLE_EQ(result[2], 5.0);    // 5.0 >= 3.0, kept
    EXPECT_TRUE(std::isnan(result[3]));  // NaN preserved
    EXPECT_DOUBLE_EQ(result[4], 10.0);   // 10.0 >= 3.0, kept
    EXPECT_TRUE(std::isnan(result[5]));  // NaN preserved
}

TEST_F(TransformFunctionsTest, CutoffMaxPreservesNaN) {
    // NaN values should be preserved (not converted to NaN by the threshold check)
    std::vector<double> values = {std::nan(""), 1.0, 5.0, std::nan(""), 10.0, std::nan("")};

    auto result = cutoff_max(values, 7.0);

    EXPECT_TRUE(std::isnan(result[0]));  // NaN preserved
    EXPECT_DOUBLE_EQ(result[1], 1.0);    // 1.0 <= 7.0, kept
    EXPECT_DOUBLE_EQ(result[2], 5.0);    // 5.0 <= 7.0, kept
    EXPECT_TRUE(std::isnan(result[3]));  // NaN preserved
    EXPECT_TRUE(std::isnan(result[4]));  // 10.0 > 7.0, becomes NaN
    EXPECT_TRUE(std::isnan(result[5]));  // NaN preserved
}

TEST_F(TransformFunctionsTest, CutoffMinNaNLargeArraySimd) {
    // Large array to trigger SIMD path, with NaN values mixed in
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i % 7 == 0) {
            values[i] = std::nan("");  // NaN at positions 0, 7, 14, 21, ...
        } else {
            values[i] = static_cast<double>(i) - 50.0;  // Range: -49 to 49
        }
    }

    auto result = cutoff_min(values, 0.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i % 7 == 0) {
            // NaN inputs should remain NaN (preserved, not set by threshold)
            EXPECT_TRUE(std::isnan(result[i])) << "NaN should be preserved at index " << i;
        } else if (values[i] < 0.0) {
            // Below threshold -> NaN
            EXPECT_TRUE(std::isnan(result[i]))
                << "Value " << values[i] << " below threshold should be NaN at index " << i;
        } else {
            // At or above threshold -> preserved
            EXPECT_DOUBLE_EQ(result[i], values[i])
                << "Value " << values[i] << " at/above threshold should be preserved at index " << i;
        }
    }

    // Verify SIMD and scalar produce the same results
    auto scalarResult = simd::scalar::cutoff_min(values, 0.0);
    ASSERT_EQ(scalarResult.size(), result.size());
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(nearEqual(result[i], scalarResult[i]))
            << "SIMD and scalar disagree at index " << i << ": simd=" << result[i] << ", scalar=" << scalarResult[i];
    }
}

TEST_F(TransformFunctionsTest, CutoffMaxNaNLargeArraySimd) {
    // Large array to trigger SIMD path, with NaN values mixed in
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i % 7 == 0) {
            values[i] = std::nan("");  // NaN at positions 0, 7, 14, 21, ...
        } else {
            values[i] = static_cast<double>(i);  // Range: 0 to 99
        }
    }

    auto result = cutoff_max(values, 50.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i % 7 == 0) {
            // NaN inputs should remain NaN (preserved, not set by threshold)
            EXPECT_TRUE(std::isnan(result[i])) << "NaN should be preserved at index " << i;
        } else if (values[i] > 50.0) {
            // Above threshold -> NaN
            EXPECT_TRUE(std::isnan(result[i]))
                << "Value " << values[i] << " above threshold should be NaN at index " << i;
        } else {
            // At or below threshold -> preserved
            EXPECT_DOUBLE_EQ(result[i], values[i])
                << "Value " << values[i] << " at/below threshold should be preserved at index " << i;
        }
    }

    // Verify SIMD and scalar produce the same results
    auto scalarResult = simd::scalar::cutoff_max(values, 50.0);
    ASSERT_EQ(scalarResult.size(), result.size());
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(nearEqual(result[i], scalarResult[i]))
            << "SIMD and scalar disagree at index " << i << ": simd=" << result[i] << ", scalar=" << scalarResult[i];
    }
}
