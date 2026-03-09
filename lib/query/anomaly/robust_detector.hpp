#ifndef ROBUST_DETECTOR_H_INCLUDED
#define ROBUST_DETECTOR_H_INCLUDED

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

    AnomalyOutput detect(
        const AnomalyInput& input,
        const AnomalyConfig& config
    ) override;

    std::string algorithmName() const override { return "robust"; }

    bool supportsSeasonality() const override { return true; }

private:
    // Compute bounds based on residual distribution
    void computeBounds(
        const STLComponents& stl,
        const std::vector<double>& values,
        double bounds,
        std::vector<double>& upper,
        std::vector<double>& lower
    );
};

} // namespace anomaly
} // namespace timestar

#endif // ROBUST_DETECTOR_H_INCLUDED
