#include "functions/series_alignment.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <vector>

using namespace timestar::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::SizeIs;

class SeriesAlignmentTest : public ::testing::Test {
protected:
    void SetUp() override { setupTestData(); }

    void setupTestData() {
        // Basic test data
        basicTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        basicValues = {1.0, 2.0, 3.0, 4.0, 5.0};

        // Gap test data
        gappyTimestamps = {1000ULL, 2000ULL, 10000ULL, 11000ULL, 20000ULL};
        gappyValues = {1.0, 2.0, 10.0, 11.0, 20.0};
    }

    void expectVectorNear(const std::vector<double>& actual, const std::vector<double>& expected,
                          double tolerance = 1e-10) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); ++i) {
            if (std::isnan(expected[i])) {
                EXPECT_TRUE(std::isnan(actual[i])) << "at index " << i;
            } else {
                EXPECT_NEAR(actual[i], expected[i], tolerance) << "at index " << i;
            }
        }
    }

    // Test data
    std::vector<uint64_t> basicTimestamps, gappyTimestamps;
    std::vector<double> basicValues, gappyValues;
};

// Test basic series alignment
TEST_F(SeriesAlignmentTest, BasicSeriesAlignment) {
    SeriesAlignment aligner;

    auto result = aligner.alignSeries(basicValues, basicTimestamps, 1000ULL);

    EXPECT_GT(result.size(), 0);
    // Basic alignment should return some aligned values
}

TEST_F(SeriesAlignmentTest, AlignmentWithGaps) {
    SeriesAlignment aligner;

    auto result = aligner.alignSeries(gappyValues, gappyTimestamps, 2000ULL);

    EXPECT_GT(result.size(), 0);
    // Should handle series with gaps
}

// Test alignment utilities
TEST_F(SeriesAlignmentTest, AlignmentUtilities) {
    // Test double aligner
    EXPECT_NEAR(alignment_utils::DoubleAligner::safeInterpolate(1.0, 3.0, 0.5), 2.0, 1e-10);
    EXPECT_TRUE(std::isnan(
        alignment_utils::DoubleAligner::safeInterpolate(std::numeric_limits<double>::quiet_NaN(), 3.0, 0.5)));

    EXPECT_TRUE(alignment_utils::DoubleAligner::isValidValue(1.0));
    EXPECT_FALSE(alignment_utils::DoubleAligner::isValidValue(std::numeric_limits<double>::quiet_NaN()));
    EXPECT_FALSE(alignment_utils::DoubleAligner::isValidValue(std::numeric_limits<double>::infinity()));

    // Test bool aligner
    EXPECT_TRUE(alignment_utils::BoolAligner::interpolateBoolean(true, false, 0.3));
    EXPECT_FALSE(alignment_utils::BoolAligner::interpolateBoolean(true, false, 0.7));

    // Test string aligner
    EXPECT_EQ(alignment_utils::StringAligner::interpolateString("hello", "world", 0.3), "hello");
    EXPECT_EQ(alignment_utils::StringAligner::interpolateString("hello", "world", 0.7), "world");
}

// ==================== Tests that catch stub behavior ====================

// The stub returns values unchanged, so result.size() == input.size().
// The real implementation must resample to a regular grid, changing output size.
TEST_F(SeriesAlignmentTest, OutputSizeMatchesRegularGrid) {
    SeriesAlignment aligner;

    // Input: 5 points at irregular timestamps spanning [1000, 5000]
    // Target interval 2000 -> grid: {1000, 3000, 5000} -> 3 output points
    // The stub returns all 5 input values, so size == 5, not 3.
    std::vector<uint64_t> ts = {1000ULL, 1500ULL, 2000ULL, 3500ULL, 5000ULL};
    std::vector<double> vs = {10.0, 15.0, 20.0, 35.0, 50.0};

    auto result = aligner.alignSeries(vs, ts, 2000ULL);

    // Grid: 1000, 3000, 5000 -> 3 points
    EXPECT_EQ(result.size(), 3u) << "Expected 3 resampled points (1000,3000,5000) but got " << result.size();
}

TEST_F(SeriesAlignmentTest, LinearlySpacedInputPassthrough) {
    // When timestamps are already on the regular grid, output values should
    // match input values exactly and the output size should match input size.
    SeriesAlignment aligner;

    std::vector<uint64_t> ts = {0ULL, 1000ULL, 2000ULL, 3000ULL, 4000ULL};
    std::vector<double> vs = {0.0, 10.0, 20.0, 30.0, 40.0};

    auto result = aligner.alignSeries(vs, ts, 1000ULL);

    ASSERT_EQ(result.size(), 5u);
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_NEAR(result[i], vs[i], 1e-10) << "at index " << i;
    }
}

TEST_F(SeriesAlignmentTest, InterpolationAtMidpoint) {
    // Input: t=0 -> 0.0, t=2000 -> 20.0
    // targetInterval=1000 -> grid: {0, 1000, 2000}
    // At t=1000 (midpoint), linear interpolation should give 10.0.
    SeriesAlignment aligner;

    std::vector<uint64_t> ts = {0ULL, 2000ULL};
    std::vector<double> vs = {0.0, 20.0};

    auto result = aligner.alignSeries(vs, ts, 1000ULL);

    ASSERT_EQ(result.size(), 3u);
    EXPECT_NEAR(result[0], 0.0, 1e-10);   // exact match at t=0
    EXPECT_NEAR(result[1], 10.0, 1e-10);  // interpolated at t=1000
    EXPECT_NEAR(result[2], 20.0, 1e-10);  // exact match at t=2000
}

TEST_F(SeriesAlignmentTest, InterpolationQuarterPoints) {
    // Input: t=0 -> 0.0, t=4000 -> 40.0
    // targetInterval=1000 -> grid: {0, 1000, 2000, 3000, 4000}
    // Values should be linearly spaced: 0, 10, 20, 30, 40
    SeriesAlignment aligner;

    std::vector<uint64_t> ts = {0ULL, 4000ULL};
    std::vector<double> vs = {0.0, 40.0};

    auto result = aligner.alignSeries(vs, ts, 1000ULL);

    ASSERT_EQ(result.size(), 5u);
    EXPECT_NEAR(result[0], 0.0, 1e-10);
    EXPECT_NEAR(result[1], 10.0, 1e-10);
    EXPECT_NEAR(result[2], 20.0, 1e-10);
    EXPECT_NEAR(result[3], 30.0, 1e-10);
    EXPECT_NEAR(result[4], 40.0, 1e-10);
}

TEST_F(SeriesAlignmentTest, EmptyInputReturnsEmpty) {
    SeriesAlignment aligner;

    std::vector<uint64_t> ts;
    std::vector<double> vs;

    auto result = aligner.alignSeries(vs, ts, 1000ULL);

    EXPECT_TRUE(result.empty());
}

TEST_F(SeriesAlignmentTest, SinglePointReturnsOneValue) {
    SeriesAlignment aligner;

    std::vector<uint64_t> ts = {5000ULL};
    std::vector<double> vs = {42.0};

    auto result = aligner.alignSeries(vs, ts, 1000ULL);

    ASSERT_EQ(result.size(), 1u);
    EXPECT_NEAR(result[0], 42.0, 1e-10);
}

TEST_F(SeriesAlignmentTest, ZeroIntervalReturnsOriginalValues) {
    // Zero interval is a degenerate case: return the input values unchanged.
    SeriesAlignment aligner;

    auto result = aligner.alignSeries(basicValues, basicTimestamps, 0ULL);

    ASSERT_EQ(result.size(), basicValues.size());
    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_NEAR(result[i], basicValues[i], 1e-10);
    }
}

TEST_F(SeriesAlignmentTest, IrregularTimestampsResampledCorrectly) {
    // Input has irregular spacing; resample to interval=500.
    // ts: 0, 500, 2000  ->  grid with interval=500: {0, 500, 1000, 1500, 2000}
    // vs: 0.0, 5.0, 20.0
    // At t=1000: between (500,5.0) and (2000,20.0) -> 5 + (20-5)*(1000-500)/(2000-500) = 5 + 5 = 10
    // At t=1500: 5 + (20-5)*(1500-500)/(2000-500) = 5 + 10 = 15
    SeriesAlignment aligner;

    std::vector<uint64_t> ts = {0ULL, 500ULL, 2000ULL};
    std::vector<double> vs = {0.0, 5.0, 20.0};

    auto result = aligner.alignSeries(vs, ts, 500ULL);

    ASSERT_EQ(result.size(), 5u);
    EXPECT_NEAR(result[0], 0.0, 1e-10);   // t=0: exact
    EXPECT_NEAR(result[1], 5.0, 1e-10);   // t=500: exact
    EXPECT_NEAR(result[2], 10.0, 1e-9);   // t=1000: interpolated
    EXPECT_NEAR(result[3], 15.0, 1e-9);   // t=1500: interpolated
    EXPECT_NEAR(result[4], 20.0, 1e-10);  // t=2000: exact
}

// main() function removed to avoid multiple definitions when linking with other test files