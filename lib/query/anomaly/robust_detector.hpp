#pragma once

#include "anomaly_detector.hpp"
#include "stl_decomposition.hpp"

namespace timestar {
namespace anomaly {

// Robust anomaly detection using STL decomposition
// Decomposes time series into Trend + Seasonal + Residual
// Detects anomalies in the residual component
// Best for stable metrics with consistent seasonal patterns
class RobustDetector : public AnomalyDetector {
public:
    RobustDetector() = default;

    using AnomalyDetector::detect;
    AnomalyOutput detect(const AnomalyInputView& input, const AnomalyConfig& config) override;

    std::string algorithmName() const override { return "robust"; }

    bool supportsSeasonality() const override { return true; }

private:
    // Compute bounds based on residual distribution.
    // 'expected' is the precomputed trend+seasonal series (also the predictions).
    void computeBounds(const STLComponents& stl, std::span<const double> values, const std::vector<double>& expected,
                       double bounds, std::vector<double>& upper, std::vector<double>& lower);
};

}  // namespace anomaly
}  // namespace timestar
