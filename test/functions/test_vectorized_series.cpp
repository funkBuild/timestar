#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "functions/vectorized_series.hpp"
#include <vector>
#include <cmath>

using namespace tsdb::functions;
using ::testing::_;
using ::testing::DoubleNear;
using ::testing::SizeIs;
using ::testing::Each;
using ::testing::Gt;

class VectorizedSeriesTest : public ::testing::Test {
protected:
    void SetUp() override {
        setupTestData();
    }
    
    void setupTestData() {
        // Linear sequence
        linearData = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
        
        // Small dataset for manual verification
        smallData = {2.5, 4.0, 1.5, 8.0, 3.5};
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
    
    std::vector<double> linearData, smallData;
};

// Test VectorizedSeriesTemplate basic functionality
TEST_F(VectorizedSeriesTest, TemplateBasicConstruction) {
    VectorizedSeriesTemplate<double> series(linearData);
    
    EXPECT_EQ(series.size(), 10);
    EXPECT_FALSE(series.empty());
    
    // Test element access
    EXPECT_EQ(series[0], 1.0);
    EXPECT_EQ(series[9], 10.0);
}

TEST_F(VectorizedSeriesTest, TemplateEmptyConstruction) {
    VectorizedSeriesTemplate<double> series;
    
    EXPECT_EQ(series.size(), 0);
    EXPECT_TRUE(series.empty());
}

TEST_F(VectorizedSeriesTest, TemplateAddOperation) {
    VectorizedSeriesTemplate<double> series;
    
    std::vector<double> a = {1.0, 2.0, 3.0, 4.0, 5.0};
    std::vector<double> b = {2.0, 2.0, 2.0, 2.0, 2.0};
    
    auto result = series.add(a, b);
    expectVectorNear(result, {3.0, 4.0, 5.0, 6.0, 7.0});
}

TEST_F(VectorizedSeriesTest, TemplateAddSizeMismatch) {
    VectorizedSeriesTemplate<double> series;
    
    std::vector<double> a = {1.0, 2.0, 3.0};
    std::vector<double> b = {1.0, 2.0};  // Different size
    
    EXPECT_THROW(series.add(a, b), std::invalid_argument);
}

TEST_F(VectorizedSeriesTest, TemplateMultiplyOperation) {
    VectorizedSeriesTemplate<double> series;
    
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    double factor = 2.5;
    
    auto result = series.multiply(values, factor);
    expectVectorNear(result, {2.5, 5.0, 7.5, 10.0, 12.5});
}

TEST_F(VectorizedSeriesTest, TemplateScaleOperation) {
    VectorizedSeriesTemplate<double> series;
    
    std::vector<double> values = {2.0, 4.0, 6.0, 8.0, 10.0};
    double factor = 0.5;
    
    auto result = series.scale(values, factor);
    expectVectorNear(result, {1.0, 2.0, 3.0, 4.0, 5.0});
}

// Test non-template VectorizedSeries
TEST_F(VectorizedSeriesTest, NonTemplateAddOperation) {
    VectorizedSeries series;
    
    std::vector<double> a = {1.0, 3.0, 5.0};
    std::vector<double> b = {2.0, 4.0, 6.0};
    
    auto result = series.add(a, b);
    expectVectorNear(result, {3.0, 7.0, 11.0});
}

TEST_F(VectorizedSeriesTest, NonTemplateMultiplyOperation) {
    VectorizedSeries series;
    
    std::vector<double> values = {1.0, 2.0, 3.0};
    double factor = 3.0;
    
    auto result = series.multiply(values, factor);
    expectVectorNear(result, {3.0, 6.0, 9.0});
}

TEST_F(VectorizedSeriesTest, NonTemplateScaleOperation) {
    VectorizedSeries series;
    
    std::vector<double> values = {10.0, 20.0, 30.0};
    double factor = 0.1;
    
    auto result = series.scale(values, factor);
    expectVectorNear(result, {1.0, 2.0, 3.0});
}

// Test edge cases
TEST_F(VectorizedSeriesTest, EdgeCasesEmptyVectors) {
    VectorizedSeries series;
    
    std::vector<double> empty1, empty2;
    
    auto result = series.add(empty1, empty2);
    EXPECT_TRUE(result.empty());
    
    auto multiplyResult = series.multiply(empty1, 2.0);
    EXPECT_TRUE(multiplyResult.empty());
}

TEST_F(VectorizedSeriesTest, EdgeCasesSingleElement) {
    VectorizedSeries series;
    
    std::vector<double> single1 = {42.0};
    std::vector<double> single2 = {8.0};
    
    auto result = series.add(single1, single2);
    EXPECT_EQ(result.size(), 1);
    EXPECT_NEAR(result[0], 50.0, 1e-10);
    
    auto multiplyResult = series.multiply(single1, 2.0);
    EXPECT_EQ(multiplyResult.size(), 1);
    EXPECT_NEAR(multiplyResult[0], 84.0, 1e-10);
}

// Tests for size-mismatch error handling in non-template VectorizedSeries::add
TEST_F(VectorizedSeriesTest, NonTemplateAddSizeMismatchThrows) {
    VectorizedSeries series;

    std::vector<double> a = {1.0, 2.0, 3.0};
    std::vector<double> b = {1.0, 2.0};  // Shorter than a

    EXPECT_THROW(series.add(a, b), std::invalid_argument);
}

TEST_F(VectorizedSeriesTest, NonTemplateAddSizeMismatchThrowsReversed) {
    VectorizedSeries series;

    std::vector<double> a = {1.0, 2.0};        // Shorter than b
    std::vector<double> b = {1.0, 2.0, 3.0};

    EXPECT_THROW(series.add(a, b), std::invalid_argument);
}

// Tests for size-mismatch error handling in AlignedVectorizedSeries constructor
TEST_F(VectorizedSeriesTest, AlignedVectorizedSeriesSizeMismatchThrows) {
    std::vector<uint64_t> timestamps = {100, 200, 300};
    std::vector<double>   values     = {1.0, 2.0};  // One element short

    EXPECT_THROW((AlignedVectorizedSeries{timestamps, values}), std::invalid_argument);
}

TEST_F(VectorizedSeriesTest, AlignedVectorizedSeriesSizeMismatchThrowsExtraValues) {
    std::vector<uint64_t> timestamps = {100, 200};
    std::vector<double>   values     = {1.0, 2.0, 3.0};  // One element extra

    EXPECT_THROW((AlignedVectorizedSeries{timestamps, values}), std::invalid_argument);
}

// main() function removed to avoid multiple definitions when linking with other test files