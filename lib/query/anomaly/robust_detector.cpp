#include "robust_detector.hpp"

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>

namespace timestar::anomaly {
namespace {
double median(std::vector<double> values) {
    const size_t middle = values.size() / 2;
    std::nth_element(values.begin(), values.begin() + middle, values.end());
    if (values.size() % 2)
        return values[middle];
    return (values[middle] + *std::max_element(values.begin(), values.begin() + middle)) / 2.0;
}
}  // namespace

AnomalyOutput RobustDetector::detect(const AnomalyInputView& input, const AnomalyConfig& config) {
    AnomalyOutput output;
    const size_t n = input.size();
    if (n == 0)
        return output;
    const double missing = std::numeric_limits<double>::quiet_NaN();
    output.predictions.resize(n, missing);
    output.upper.resize(n, std::numeric_limits<double>::infinity());
    output.lower.resize(n, -std::numeric_limits<double>::infinity());
    output.scores.resize(n, 0.0);

    // Fixed-size, trailing windows make a classification independent of values
    // appended later. Centred STL both anticipated spikes and repainted alerts;
    // its whole-range MAD even let future spikes widen every earlier bound.
    const size_t window = std::max(config.windowSize, config.minDataPoints);
    const size_t period = seasonalityToPeriod(config.seasonality, estimateInterval(input.timestamps));
    const size_t cycles = std::max<size_t>(3, config.stlSeasonalWindow);
    std::deque<double> history;
    std::deque<double> residuals;
    size_t finiteCount = 0;
    for (size_t i = 0; i < n; ++i) {
        const double value = input.values[i];
        if (!std::isfinite(value)) {
            output.upper[i] = output.lower[i] = missing;
            continue;
        }
        std::vector<double> baseline;
        if (period > 0) {
            // Compare the same phase in previous cycles, never this/future cycle.
            for (size_t cycle = 1; cycle <= cycles && cycle <= i / period; ++cycle) {
                const double previous = input.values[i - cycle * period];
                if (std::isfinite(previous))
                    baseline.push_back(previous);
            }
        } else {
            baseline.assign(history.begin(), history.end());
        }
        const double prediction = baseline.empty() ? value : median(baseline);
        output.predictions[i] = prediction;
        const bool ready =
            finiteCount >= config.minDataPoints && baseline.size() >= (period > 0 ? 2 : config.minDataPoints);
        if (ready) {
            // A non-seasonal MAD comes from the trailing level distribution;
            // seasonal MAD comes from earlier one-step prediction residuals.
            std::vector<double> errors =
                period > 0 ? std::vector<double>(residuals.begin(), residuals.end()) : baseline;
            double sigma = 0.0;
            if (!errors.empty()) {
                const double center = median(errors);
                for (double& error : errors)
                    error = std::abs(error - center);
                sigma = 1.4826 * median(std::move(errors));
            }
            sigma = std::max(sigma, std::abs(prediction) * 0.01);
            if (sigma < 1e-10)
                sigma = 1.0;
            const double margin = config.bounds * sigma;
            output.upper[i] = prediction + margin;
            output.lower[i] = prediction - margin;
            output.scores[i] = std::max({value - output.upper[i], output.lower[i] - value, 0.0});
            if (output.scores[i] > 0)
                ++output.anomalyCount;
        }
        // Score before admitting the sample. Median/MAD resist isolated spikes
        // while a sustained change can eventually establish a new baseline.
        if (!baseline.empty()) {
            residuals.push_back(value - prediction);
            if (residuals.size() > window)
                residuals.pop_front();
        }
        history.push_back(value);
        if (history.size() > window)
            history.pop_front();
        ++finiteCount;
    }
    return output;
}
}  // namespace timestar::anomaly
