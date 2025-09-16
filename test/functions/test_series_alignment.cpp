#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/series_alignment.hpp"
#include <vector>
#include <cmath>
#include <algorithm>

using namespace tsdb::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::SizeIs;
using ::testing::ElementsAre;

class SeriesAlignmentTest : public ::testing::Test {
protected:
    void SetUp() override {
        setupTestData();
    }
    
    void setupTestData() {
        // Basic test data
        basicTimestamps = {1000ULL, 2000ULL, 3000ULL, 4000ULL, 5000ULL};
        basicValues = {1.0, 2.0, 3.0, 4.0, 5.0};
        
        // Gap test data
        gappyTimestamps = {1000ULL, 2000ULL, 10000ULL, 11000ULL, 20000ULL};
        gappyValues = {1.0, 2.0, 10.0, 11.0, 20.0};
    }
    
    void expectVectorNear(const std::vector<double>& actual, 
                         const std::vector<double>& expected, 
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
    EXPECT_TRUE(std::isnan(alignment_utils::DoubleAligner::safeInterpolate(
        std::numeric_limits<double>::quiet_NaN(), 3.0, 0.5)));
    
    EXPECT_TRUE(alignment_utils::DoubleAligner::isValidValue(1.0));
    EXPECT_FALSE(alignment_utils::DoubleAligner::isValidValue(
        std::numeric_limits<double>::quiet_NaN()));
    EXPECT_FALSE(alignment_utils::DoubleAligner::isValidValue(
        std::numeric_limits<double>::infinity()));
    
    // Test bool aligner
    EXPECT_TRUE(alignment_utils::BoolAligner::interpolateBoolean(true, false, 0.3));
    EXPECT_FALSE(alignment_utils::BoolAligner::interpolateBoolean(true, false, 0.7));
    
    // Test string aligner
    EXPECT_EQ(alignment_utils::StringAligner::interpolateString("hello", "world", 0.3), "hello");
    EXPECT_EQ(alignment_utils::StringAligner::interpolateString("hello", "world", 0.7), "world");
}

// main() function removed to avoid multiple definitions when linking with other test files