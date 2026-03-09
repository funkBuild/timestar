#ifndef BASIC_DETECTOR_H_INCLUDED
#define BASIC_DETECTOR_H_INCLUDED

#include "anomaly_detector.hpp"
#include "simd_anomaly.hpp"

namespace timestar {
namespace anomaly {

// Basic anomaly detection using rolling window statistics
// Computes bounds as: mean ± (bounds × stddev)
// Fast and simple, but doesn't account for seasonality
//
// Performance: O(N) with incremental statistics (was O(N*W))
class BasicDetector : public AnomalyDetector {
public:
    BasicDetector() = default;

    AnomalyOutput detect(
        const AnomalyInput& input,
        const AnomalyConfig& config
    ) override;

    std::string algorithmName() const override { return "basic"; }

    bool supportsSeasonality() const override { return false; }

private:
    // Optimized detection using incremental rolling stats
    // Complexity: O(N) instead of O(N*W)
    AnomalyOutput detectOptimized(
        const AnomalyInput& input,
        const AnomalyConfig& config
    );

    // Legacy rolling stats computation (kept for fallback/validation)
    struct RollingStats {
        double mean;
        double stddev;
        double median;
        double q1;  // 25th percentile
        double q3;  // 75th percentile
    };

    RollingStats computeRollingStats(
        const std::vector<double>& values,
        size_t endIdx,
        size_t windowSize
    );
};

} // namespace anomaly
} // namespace timestar

#endif // BASIC_DETECTOR_H_INCLUDED
