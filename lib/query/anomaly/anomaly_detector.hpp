#pragma once

#include "anomaly_result.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace timestar {
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
    std::vector<double> upper;        // Upper bound values
    std::vector<double> lower;        // Lower bound values
    std::vector<double> scores;       // Anomaly scores (0 = normal, 1 = anomaly)
    std::vector<double> predictions;  // Predicted values (for Agile/Robust)
    size_t anomalyCount = 0;          // Count of detected anomalies

    bool empty() const { return upper.empty(); }
    size_t size() const { return upper.size(); }
};

// Abstract base class for anomaly detectors
class AnomalyDetector {
public:
    virtual ~AnomalyDetector() = default;

    // Detect anomalies in the input data
    // Returns bounds and scores for each point
    virtual AnomalyOutput detect(const AnomalyInput& input, const AnomalyConfig& config) = 0;

    // Get the algorithm name
    virtual std::string algorithmName() const = 0;

    // Check if this detector supports seasonality
    virtual bool supportsSeasonality() const = 0;

protected:
    // Estimate data interval from timestamps (in nanoseconds)
    static uint64_t estimateInterval(const std::vector<uint64_t>& timestamps) {
        if (timestamps.size() < 2) {
            return 60000000000ULL;  // Default 1 minute
        }

        // Sample gaps from beginning, middle, and end for robustness
        // against irregular starts/ends
        std::vector<uint64_t> intervals;
        size_t n = timestamps.size() - 1;
        size_t samplesPerRegion = std::min(n, size_t(10));

        auto sampleRegion = [&](size_t start, size_t count) {
            for (size_t i = start; i < start + count && i < n; ++i) {
                if (timestamps[i + 1] > timestamps[i]) {
                    intervals.push_back(timestamps[i + 1] - timestamps[i]);
                }
            }
        };

        sampleRegion(0, samplesPerRegion);
        if (n > samplesPerRegion * 2) {
            size_t mid = n / 2 - samplesPerRegion / 2;
            sampleRegion(mid, samplesPerRegion);
        }
        if (n > samplesPerRegion) {
            sampleRegion(n - samplesPerRegion, samplesPerRegion);
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

}  // namespace anomaly
}  // namespace timestar
