#pragma once

#include "anomaly_detector.hpp"
#include "stl_decomposition.hpp"

namespace timestar {
namespace anomaly {

// Causal robust anomaly detection using trailing median/MAD estimates.
// Seasonal predictions use the same phase in previous cycles.
class RobustDetector : public AnomalyDetector {
public:
    RobustDetector() = default;

    using AnomalyDetector::detect;
    AnomalyOutput detect(const AnomalyInputView& input, const AnomalyConfig& config) override;

    std::string algorithmName() const override { return "robust"; }

    bool supportsSeasonality() const override { return true; }
};

}  // namespace anomaly
}  // namespace timestar
