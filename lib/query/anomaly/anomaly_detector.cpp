#include "anomaly_detector.hpp"

#include "agile_detector.hpp"
#include "basic_detector.hpp"
#include "robust_detector.hpp"

namespace timestar {
namespace anomaly {

std::unique_ptr<AnomalyDetector> createDetector(Algorithm algorithm) {
    switch (algorithm) {
        case Algorithm::BASIC:
            return std::make_unique<BasicDetector>();
        case Algorithm::AGILE:
            return std::make_unique<AgileDetector>();
        case Algorithm::ROBUST:
            return std::make_unique<RobustDetector>();
        default:
            return std::make_unique<BasicDetector>();
    }
}

}  // namespace anomaly
}  // namespace timestar
