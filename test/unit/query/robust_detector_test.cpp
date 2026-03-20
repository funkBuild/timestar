#include "../../lib/query/anomaly/robust_detector.hpp"
#include "../../lib/query/anomaly/anomaly_result.hpp"
#include <gtest/gtest.h>
#include <cmath>
#include <vector>

using namespace timestar::anomaly;

TEST(RobustDetectorTest, DetectsObviousOutlier) {
    RobustDetector detector;
    AnomalyConfig config;
    config.bounds = 3.0;
    config.windowSize = 50;
    config.minDataPoints = 10;

    AnomalyInput input;
    // 100 normal points around 10.0, then one spike at 1000.0
    for (size_t i = 0; i < 100; ++i) {
        input.timestamps.push_back(i * 1000000000ULL);
        input.values.push_back(10.0 + (i % 3) * 0.1);  // slight variation
    }
    input.timestamps.push_back(100 * 1000000000ULL);
    input.values.push_back(1000.0);  // massive outlier

    auto output = detector.detect(input, config);

    EXPECT_EQ(output.scores.size(), 101u);
    // The outlier at index 100 should have a high anomaly score
    EXPECT_GT(output.scores[100], 0.0) << "Outlier should have positive anomaly score";
    // Normal points after warmup should have zero or low scores
    EXPECT_NEAR(output.scores[50], 0.0, 0.01) << "Normal point should have near-zero score";
}

TEST(RobustDetectorTest, HandlesConstantData) {
    RobustDetector detector;
    AnomalyConfig config;
    config.bounds = 3.0;
    config.windowSize = 20;
    config.minDataPoints = 5;

    AnomalyInput input;
    for (size_t i = 0; i < 50; ++i) {
        input.timestamps.push_back(i * 1000000000ULL);
        input.values.push_back(42.0);
    }

    auto output = detector.detect(input, config);
    EXPECT_EQ(output.scores.size(), 50u);
    // All constant — no anomalies expected after warmup
    for (size_t i = config.minDataPoints; i < 50; ++i) {
        EXPECT_NEAR(output.scores[i], 0.0, 0.01)
            << "Constant data should produce zero anomaly scores at index " << i;
    }
}

TEST(RobustDetectorTest, HandlesNaNValues) {
    RobustDetector detector;
    AnomalyConfig config;
    config.bounds = 3.0;
    config.windowSize = 20;
    config.minDataPoints = 5;

    AnomalyInput input;
    for (size_t i = 0; i < 30; ++i) {
        input.timestamps.push_back(i * 1000000000ULL);
        input.values.push_back(i % 5 == 0 ? std::nan("") : 10.0);
    }

    auto output = detector.detect(input, config);
    EXPECT_EQ(output.scores.size(), 30u);
    // NaN points should get score 0
    for (size_t i = config.minDataPoints; i < 30; ++i) {
        if (std::isnan(input.values[i])) {
            EXPECT_NEAR(output.scores[i], 0.0, 0.01);
        }
    }
}

TEST(RobustDetectorTest, OutputSizesMatchInput) {
    RobustDetector detector;
    AnomalyConfig config;
    config.bounds = 3.0;
    config.windowSize = 20;
    config.minDataPoints = 5;

    AnomalyInput input;
    for (size_t i = 0; i < 40; ++i) {
        input.timestamps.push_back(i * 1000000000ULL);
        input.values.push_back(i * 1.0);
    }

    auto output = detector.detect(input, config);
    EXPECT_EQ(output.scores.size(), 40u);
    EXPECT_EQ(output.upper.size(), 40u);
    EXPECT_EQ(output.lower.size(), 40u);
    EXPECT_EQ(output.predictions.size(), 40u);
}
