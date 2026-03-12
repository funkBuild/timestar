#ifndef AGILE_DETECTOR_H_INCLUDED
#define AGILE_DETECTOR_H_INCLUDED

#include "anomaly_detector.hpp"
#include "simd_anomaly.hpp"

namespace timestar {
namespace anomaly {

// Agile anomaly detection using seasonal prediction
// Adapts quickly to level shifts while respecting seasonal patterns
// Combines recent values with same-time-period historical values
class AgileDetector : public AnomalyDetector {
public:
    AgileDetector() = default;

    AnomalyOutput detect(const AnomalyInput& input, const AnomalyConfig& config) override;

    std::string algorithmName() const override { return "agile"; }

    bool supportsSeasonality() const override { return true; }

private:
    // Prediction parameters
    static constexpr double ALPHA = 0.3;           // Smoothing for level
    static constexpr double BETA = 0.1;            // Smoothing for trend
    static constexpr double GAMMA = 0.3;           // Smoothing for seasonality
    static constexpr double RECENCY_WEIGHT = 0.6;  // Weight for recent vs historical

    // Holt-Winters triple exponential smoothing with seasonality
    struct HoltWintersState {
        double level;
        double trend;
        std::vector<double> seasonal;
    };

    // Initialize Holt-Winters state
    HoltWintersState initializeState(const std::vector<double>& values, size_t seasonalPeriod);

    // Update state and predict next value
    double predictAndUpdate(HoltWintersState& state, double actualValue, size_t seasonalIndex, size_t seasonalPeriod);

    // Compute prediction error statistics
    double computeErrorStdDev(const std::vector<double>& errors, size_t windowSize);
};

}  // namespace anomaly
}  // namespace timestar

#endif  // AGILE_DETECTOR_H_INCLUDED
