#include "agile_detector.hpp"

#include "simd_anomaly.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>

namespace timestar {
namespace anomaly {

double AgileDetector::predictAndUpdate(HoltWintersState& state, double actualValue, size_t seasonalIndex,
                                       size_t seasonalPeriod) {
    // Predict based on current state
    double seasonal = (seasonalPeriod > 0) ? state.seasonal[seasonalIndex] : 0.0;
    double prediction = state.level + state.trend + seasonal;

    if (!std::isnan(actualValue)) {
        // Update state (Holt-Winters equations)
        double prevLevel = state.level;

        // Update level
        state.level = ALPHA * (actualValue - seasonal) + (1 - ALPHA) * (prevLevel + state.trend);

        // Update trend
        state.trend = BETA * (state.level - prevLevel) + (1 - BETA) * state.trend;

        // Update seasonal
        if (seasonalPeriod > 0) {
            state.seasonal[seasonalIndex] = GAMMA * (actualValue - state.level) + (1 - GAMMA) * seasonal;
        }
    }

    return prediction;
}

AnomalyOutput AgileDetector::detect(const AnomalyInputView& input, const AnomalyConfig& config) {
    AnomalyOutput output;
    const size_t n = input.size();
    if (!n)
        return output;
    const size_t period = seasonalityToPeriod(config.seasonality, estimateInterval(input.timestamps));
    output.upper.resize(n, std::numeric_limits<double>::infinity());
    output.lower.resize(n, -std::numeric_limits<double>::infinity());
    output.scores.resize(n, 0.0);
    output.predictions.resize(n, std::numeric_limits<double>::quiet_NaN());
    HoltWintersState state{};
    state.seasonal.resize(period > 0 ? std::min(period, n) : 1, 0.0);
    bool initialized = false;
    size_t finiteCount = 0;
    simd::IncrementalRollingStats errors(std::max<size_t>(config.windowSize, 1));
    for (size_t i = 0; i < n; ++i) {
        const double value = input.values[i];
        if (!std::isfinite(value)) {
            output.upper[i] = output.lower[i] = std::numeric_limits<double>::quiet_NaN();
            continue;
        }
        if (!initialized) {
            state.level = value;
            state.trend = 0;
            initialized = true;
        }
        const size_t phase = period > 0 ? i % period : 0;
        const double prediction = state.level + state.trend + (period > 0 ? state.seasonal[phase] : 0.0);
        output.predictions[i] = prediction;
        double learnedValue = value;
        if (finiteCount >= config.minDataPoints) {
            double sigma = std::max(errors.stddev(), std::abs(prediction) * 0.01);
            if (sigma < 1e-10)
                sigma = 1.0;
            const double width = config.bounds * sigma;
            output.upper[i] = prediction + width;
            output.lower[i] = prediction - width;
            output.scores[i] = std::max({value - output.upper[i], output.lower[i] - value, 0.0});
            if (output.scores[i] > 0)
                ++output.anomalyCount;
            // An isolated spike must not set its own threshold or drag the
            // baseline far enough to flag the following normal observations.
            learnedValue = std::clamp(value, output.lower[i], output.upper[i]);
        }
        predictAndUpdate(state, learnedValue, phase, period);
        errors.update(learnedValue - prediction);
        ++finiteCount;
    }
    return output;
}

}  // namespace anomaly
}  // namespace timestar
