#ifndef ANOMALY_DETECTOR_H_INCLUDED
#define ANOMALY_DETECTOR_H_INCLUDED

#include "anomaly_result.hpp"
#include <vector>
#include <memory>
#include <cstdint>
#include <cmath>
#include <algorithm>

namespace tsdb {
namespace anomaly {

// Input data for anomaly detection
struct AnomalyInput {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::vector<std::string> groupTags;  // For multi-series support

    bool empty() const { return values.empty(); }
    size_t size() const { return values.size(); }
};

// Output from a single anomaly detection run
struct AnomalyOutput {
    std::vector<double> upper;       // Upper bound values
    std::vector<double> lower;       // Lower bound values
    std::vector<double> scores;      // Anomaly scores (0 = normal, 1 = anomaly)
    std::vector<double> predictions; // Predicted values (for Agile/Robust)
    size_t anomalyCount = 0;         // Count of detected anomalies

    bool empty() const { return upper.empty(); }
    size_t size() const { return upper.size(); }
};

// Abstract base class for anomaly detectors
class AnomalyDetector {
public:
    virtual ~AnomalyDetector() = default;

    // Detect anomalies in the input data
    // Returns bounds and scores for each point
    virtual AnomalyOutput detect(
        const AnomalyInput& input,
        const AnomalyConfig& config
    ) = 0;

    // Get the algorithm name
    virtual std::string algorithmName() const = 0;

    // Check if this detector supports seasonality
    virtual bool supportsSeasonality() const = 0;

protected:
    // Compute anomaly score based on value and bounds
    // Returns 0 if within bounds, or a score > 0 indicating deviation
    static double computeScore(double value, double lower, double upper) {
        if (value < lower) {
            // Below lower bound
            double range = upper - lower;
            if (range > 0) {
                return (lower - value) / range;
            }
            return 1.0;
        } else if (value > upper) {
            // Above upper bound
            double range = upper - lower;
            if (range > 0) {
                return (value - upper) / range;
            }
            return 1.0;
        }
        return 0.0;  // Within bounds
    }

    // Compute rolling mean for a window
    static double rollingMean(
        const std::vector<double>& values,
        size_t endIdx,
        size_t windowSize
    ) {
        if (endIdx == 0 || windowSize == 0) return 0.0;

        size_t start = (endIdx > windowSize) ? endIdx - windowSize : 0;
        double sum = 0.0;
        size_t count = 0;

        for (size_t i = start; i < endIdx; ++i) {
            if (!std::isnan(values[i])) {
                sum += values[i];
                ++count;
            }
        }

        return (count > 0) ? sum / count : 0.0;
    }

    // Compute rolling standard deviation for a window
    static double rollingStdDev(
        const std::vector<double>& values,
        size_t endIdx,
        size_t windowSize,
        double mean
    ) {
        if (endIdx == 0 || windowSize == 0) return 0.0;

        size_t start = (endIdx > windowSize) ? endIdx - windowSize : 0;
        double sumSq = 0.0;
        size_t count = 0;

        for (size_t i = start; i < endIdx; ++i) {
            if (!std::isnan(values[i])) {
                double diff = values[i] - mean;
                sumSq += diff * diff;
                ++count;
            }
        }

        return (count > 1) ? std::sqrt(sumSq / (count - 1)) : 0.0;
    }

    // Estimate data interval from timestamps (in nanoseconds)
    static uint64_t estimateInterval(const std::vector<uint64_t>& timestamps) {
        if (timestamps.size() < 2) {
            return 60000000000ULL;  // Default 1 minute
        }

        // Use median of first few intervals for robustness
        std::vector<uint64_t> intervals;
        size_t sampleSize = std::min(timestamps.size() - 1, size_t(10));

        for (size_t i = 0; i < sampleSize; ++i) {
            if (timestamps[i + 1] > timestamps[i]) {
                intervals.push_back(timestamps[i + 1] - timestamps[i]);
            }
        }

        if (intervals.empty()) {
            return 60000000000ULL;
        }

        std::sort(intervals.begin(), intervals.end());
        return intervals[intervals.size() / 2];
    }
};

// Factory function to create detector by algorithm type
std::unique_ptr<AnomalyDetector> createDetector(Algorithm algorithm);

} // namespace anomaly
} // namespace tsdb

#endif // ANOMALY_DETECTOR_H_INCLUDED
