#include "linear_forecaster.hpp"

#include "../anomaly/simd_anomaly.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>

namespace timestar {
namespace forecast {

LinearForecaster::LinearFit LinearForecaster::fitLinearRegression(const std::vector<double>& x,
                                                                  const std::vector<double>& y,
                                                                  const std::vector<double>& weights) {
    LinearFit fit{};
    size_t n = x.size();

    if (y.size() != n || weights.size() != n) {
        throw std::invalid_argument("fitLinearRegression: x, y, and weights must have the same size (got " +
                                    std::to_string(n) + ", " + std::to_string(y.size()) + ", " +
                                    std::to_string(weights.size()) + ")");
    }

    if (n < 2) {
        fit.slope = 0.0;
        fit.intercept = n > 0 ? y[0] : 0.0;
        fit.rSquared = 0.0;
        fit.residualStdDev = 0.0;
        fit.sumSquaredX = 0.0;
        fit.meanX = n > 0 ? x[0] : 0.0;
        fit.usedPoints = (n > 0 && std::isfinite(x[0]) && std::isfinite(y[0])) ? 1 : 0;
        return fit;
    }

    const double* px = x.data();
    const double* py = y.data();
    const double* pw = weights.data();

    // ======================================================================
    // Loop 1: Compute weighted means
    //   sumW  += w[i]
    //   sumWX += w[i] * x[i]
    //   sumWY += w[i] * y[i]
    // ======================================================================
    double sumWeights = 0.0;
    double weightedSumX = 0.0;
    double weightedSumY = 0.0;
    size_t usedPoints = 0;  // finite (x, y) pairs -- NOT n, see below

    // Skip non-finite points entirely.  Zeroing the weight and multiplying
    // anyway does NOT skip them: 0.0 * NaN is NaN, so a single missing value
    // poisoned every accumulator and the whole fit came back NaN (slope,
    // intercept and residual std dev all null in the response) despite the
    // "skip NaN values by zeroing their weight" the loops claimed to do.
    // NaN means missing here (docs/nan_policy.md); a gap must cost the fit
    // nothing, which is what a real skip does.
    for (size_t i = 0; i < n; ++i) {
        if (!std::isfinite(px[i]) || !std::isfinite(py[i])) {
            continue;
        }
        const double w = pw[i];
        ++usedPoints;
        sumWeights += w;
        weightedSumX += w * px[i];
        weightedSumY += w * py[i];
    }

    fit.usedPoints = usedPoints;

    if (sumWeights <= 0.0) {
        // All data points are NaN or zero-weighted — no valid regression
        fit.slope = 0.0;
        fit.intercept = 0.0;
        fit.rSquared = 0.0;
        fit.residualStdDev = 0.0;
        fit.sumSquaredX = 0.0;
        fit.meanX = 0.0;
        return fit;
    }

    fit.meanX = weightedSumX / sumWeights;
    double meanY = weightedSumY / sumWeights;

    // ======================================================================
    // Loop 2: Compute weighted covariances
    //   sumXX += w[i] * (x[i] - meanX)^2
    //   sumXY += w[i] * (x[i] - meanX) * (y[i] - meanY)
    //   sumYY += w[i] * (y[i] - meanY)^2
    // ======================================================================
    double sumXY = 0.0;
    double sumXX = 0.0;
    double sumYY = 0.0;

    // Skipped, not zero-weighted -- see loop 1.
    for (size_t i = 0; i < n; ++i) {
        if (!std::isfinite(px[i]) || !std::isfinite(py[i])) {
            continue;
        }
        const double w = pw[i];
        double dx = px[i] - fit.meanX;
        double dy = py[i] - meanY;
        sumXY += w * dx * dy;
        sumXX += w * dx * dx;
        sumYY += w * dy * dy;
    }

    fit.sumSquaredX = sumXX;

    // Compute slope and intercept
    if (std::abs(sumXX) < 1e-10) {
        // No variation in x - return mean
        fit.slope = 0.0;
        fit.intercept = meanY;
    } else {
        fit.slope = sumXY / sumXX;
        fit.intercept = meanY - fit.slope * fit.meanX;
    }

    // Compute R-squared.  No n/k confusion here to fix: sumXX and sumYY are
    // both accumulated over the finite points only, so the ratio is already
    // taken entirely within the k points that entered the fit.
    if (std::abs(sumYY) > 1e-10) {
        double ssReg = fit.slope * fit.slope * sumXX;
        fit.rSquared = std::clamp(ssReg / sumYY, 0.0, 1.0);
    } else {
        fit.rSquared = 1.0;  // Perfect fit (all y values are the same)
    }

    // ======================================================================
    // Loop 3: Compute weighted SSE (residuals)
    //   residual = y[i] - (slope * x[i] + intercept)
    //   sse += w[i] * residual^2
    // ======================================================================
    double sse = 0.0;

    // Skipped, not zero-weighted -- see loop 1.
    for (size_t i = 0; i < n; ++i) {
        if (!std::isfinite(px[i]) || !std::isfinite(py[i])) {
            continue;
        }
        double predicted = fit.slope * px[i] + fit.intercept;
        double residual = py[i] - predicted;
        sse += pw[i] * residual * residual;
    }

    // Degrees of freedom for a 2-parameter model (slope + intercept) are
    // k - 2, over the k points that ACTUALLY entered the fit -- not n, the
    // length of the input vector.
    //
    // The two are the same thing only when every point is finite.  On a
    // fan-out group they are not: the groups share one time axis and a group
    // carries NaN wherever it has no sample (docs/nan_policy.md), so n is the
    // union axis while k is that group's own point count.  Mixing them --
    // sqrt(sse * n / (sumWeights * (n - 2))), with sse and sumWeights summed
    // over k points and n taken from the axis -- inflates the denominator by
    // roughly n/k and makes every confidence band on a sparse group too
    // NARROW: measured -0.25% at k=133/n=200, -8% at k=12/n=200, -42% at
    // k=3/n=200.  Bands that are too narrow are the dangerous direction; they
    // read as confidence the fit does not have.
    //
    // The shape is otherwise unchanged and still correct for the REACTIVE
    // model's non-unit weights: (sse / sumWeights) is the weighted mean
    // squared residual and k/(k-2) is the bias correction, which collapses to
    // the textbook sqrt(SSE / (k - 2)) when the weights are all 1 (DEFAULT and
    // SIMPLE), because then sumWeights == k.
    const double k = static_cast<double>(usedPoints);
    if (usedPoints > 2) {
        fit.residualStdDev = std::sqrt(sse * k / (sumWeights * (k - 2.0)));
    } else {
        fit.residualStdDev = 0.0;
    }

    return fit;
}

// `n` here is the OBSERVATION count -- fit.usedPoints, the points that entered
// the regression -- not the length of the input vector.  The 1/n term in the
// prediction interval is "one over the number of observations"; feeding it the
// union-axis length on a sparse fan-out group shrinks it towards zero and
// narrows the band for exactly the groups that deserve the widest one.
double LinearForecaster::predictionIntervalWidth(const LinearFit& fit, double x, size_t n, double deviations) {
    if (n < 3 || fit.sumSquaredX < 1e-10) {
        return deviations * fit.residualStdDev;
    }

    // Prediction interval formula:
    // PI = t * s * sqrt(1 + 1/n + (x - x_mean)^2 / sum((x_i - x_mean)^2))
    // We use deviations instead of t-statistic for simplicity

    double dx = x - fit.meanX;
    double term = 1.0 + 1.0 / static_cast<double>(n) + (dx * dx) / fit.sumSquaredX;

    return deviations * fit.residualStdDev * std::sqrt(term);
}

ForecastOutput LinearForecaster::forecast(const ForecastInput& input, const ForecastConfig& config,
                                          const std::vector<uint64_t>& forecastTimestamps) {
    ForecastOutput output;

    size_t n = input.size();
    size_t nForecast = forecastTimestamps.size();

    // ForecastInput::size() is the TIMESTAMP count, and nothing in the type
    // ties the two vectors together, so treat a short value column as the
    // shorter input rather than reading past it.  This used to be caught
    // downstream, by fitLinearRegression's "x, y and weights must have the
    // same size" throw -- but that throw is now reached only AFTER the
    // finite-point scan below has already indexed input.values[i] for every
    // i < timestamps.size(), which is an out-of-bounds read on a mismatched
    // input.  Clamping keeps the sizes consistent for every path that follows
    // (SIMPLE halves n, the interval calculation indexes timestamps[n-1]), so
    // no later step can reintroduce the mismatch.
    n = std::min(n, input.values.size());

    if (n < config.minDataPoints) {
        // Not enough data - return empty with error indication
        return output;
    }

    // ...and the same test against the points that are actually THERE.
    //
    // n is the length of the input vector, which on a fan-out group is the
    // SHARED time axis: every group is projected onto the union of all the
    // groups' timestamps and carries NaN wherever it has no sample
    // (docs/nan_policy.md).  A device that stopped reporting is then a group of
    // 0 finite points on a 16-slot axis -- and gating on 16 let it through, at
    // which point fitLinearRegression's sumWeights <= 0 branch escaped as a
    // real answer: a constant 0.0 forecast with a ZERO-WIDTH confidence band,
    // labelled with the device's own group_tags, inside a "status":"success"
    // response.  A fabricated number that names a real device is worse than no
    // number.  The same gate covers the sparse case (6 daily points on a
    // 30-slot axis fitted a flat line with r^2 = 1.0 and zero uncertainty).
    //
    // Returning an EMPTY output is how a forecaster declines: ForecastExecutor
    // ::executeMulti() skips a group whose output is empty, so the group is
    // simply absent from the response rather than present and wrong.  This is
    // exactly the guard SeasonalForecaster::forecast() already had, which is
    // why the identical request answered with algorithm='seasonal' omitted the
    // group while 'linear' fabricated it.
    size_t finitePoints = 0;
    for (size_t i = 0; i < n; ++i) {
        if (std::isfinite(input.values[i])) {
            ++finitePoints;
        }
    }
    if (finitePoints < config.minDataPoints) {
        return output;
    }

    output.historicalCount = n;
    output.forecastCount = nForecast;

    // Convert timestamps to normalized x values for numerical stability
    // Use index-based x values: 0, 1, 2, ... n-1
    std::vector<double> x(n);
    std::vector<double> y;
    std::vector<double> weights(n, 1.0);  // Default: uniform weights

    // Apply model-specific weighting and data selection.
    //
    // Every branch copies exactly n values out of input.values -- never
    // "to the end" -- so that a value column longer than the timestamp column
    // cannot leave y longer than x and trip fitLinearRegression's size check
    // on an input the clamp above already reconciled.  For a well-formed input
    // (the only kind any caller in this tree produces) n IS input.values.size()
    // and this is the same copy as before.
    const size_t originalN = n;  // Save original input size before SIMPLE model halves n
    size_t startIdx = 0;
    switch (config.linearModel) {
        case LinearModelType::DEFAULT:
            // Standard least-squares: use all data with uniform weights
            y.assign(input.values.begin(), input.values.begin() + static_cast<ptrdiff_t>(n));
            for (size_t i = 0; i < n; ++i) {
                x[i] = static_cast<double>(i);
            }
            break;

        case LinearModelType::SIMPLE:
            // Less sensitive to recent changes: use only last half of data
            startIdx = n / 2;
            y.assign(input.values.begin() + static_cast<ptrdiff_t>(startIdx),
                     input.values.begin() + static_cast<ptrdiff_t>(n));
            x.resize(n - startIdx);
            weights.resize(n - startIdx, 1.0);
            for (size_t i = 0; i < x.size(); ++i) {
                x[i] = static_cast<double>(startIdx + i);
            }
            n = x.size();
            break;

        case LinearModelType::REACTIVE:
            // More sensitive to recent changes: exponential decay weighting
            // w[i] = exp(-lambda * (n-1-i)) where lambda ≈ 0.05
            y.assign(input.values.begin(), input.values.begin() + static_cast<ptrdiff_t>(n));
            for (size_t i = 0; i < n; ++i) {
                x[i] = static_cast<double>(i);
                // Exponential decay: more weight on recent points
                double lambda = 0.05;
                weights[i] = std::exp(-lambda * static_cast<double>(n - 1 - i));
            }
            break;
    }

    // Fit linear regression with weights
    auto fit = fitLinearRegression(x, y, weights);

    // The gate above counts finite points across the WHOLE input; the SIMPLE
    // model then fits only the last half of it, so a group whose finite points
    // all sit in the first half still reaches the fit with almost nothing
    // usable in it.  Decline anything the fit cannot put an honest error bar
    // around.
    //
    // THREE, not two.  A 2-parameter model has k - 2 degrees of freedom, so at
    // k == 2 there is no residual left to estimate: the line passes exactly
    // through both points, residualStdDev is forced to 0 and
    // predictionIntervalWidth returns deviations * 0 == 0.  The result is a
    // forecast with a ZERO-WIDTH confidence band labelled with a real device --
    // fabricated certainty, and precisely the failure mode the gate above
    // exists to stop rather than to relocate.  (Live: a group with 18 finite
    // points in slots 0-17 and exactly 2 in slots 30-31 under model='simple'
    // came back as a flat 1270 with upper - lower == 0.000000 at every
    // forecast point.)  A caller cannot tell that band from a confident one.
    //
    // Two points remain a mathematically valid fit, and fitLinearRegression
    // still computes one for any direct caller; what is refused is PUBLISHING
    // it as a forecast with an uncertainty estimate it does not have.
    if (fit.usedPoints < 3) {
        return ForecastOutput{};
    }

    output.slope = fit.slope;
    output.intercept = fit.intercept;
    output.rSquared = fit.rSquared;
    output.residualStdDev = fit.residualStdDev;

    // Generate past values (just copy input).  Clamped to the same n as
    // historicalCount so the two agree: ForecastExecutor::addSeriesPieces
    // reads output.past.back() as "the last historical value", which is only
    // true while past is exactly the historical window.
    output.past.assign(input.values.begin(), input.values.begin() + static_cast<ptrdiff_t>(originalN));

    // Generate forecast values and bounds
    output.forecast.resize(nForecast);
    output.upper.resize(nForecast);
    output.lower.resize(nForecast);

    // Calculate time interval from historical data (use full input range)
    uint64_t interval = 0;
    if (originalN >= 2) {
        interval = (input.timestamps[originalN - 1] - input.timestamps[0]) / (originalN - 1);
    }

    for (size_t i = 0; i < nForecast; ++i) {
        // Calculate x position for this forecast point
        double xForecast;
        if (interval > 0) {
            // Use time-based position
            int64_t timeDiff = static_cast<int64_t>(forecastTimestamps[i] - input.timestamps[0]);
            xForecast = static_cast<double>(timeDiff) / static_cast<double>(interval);
        } else {
            // Fallback: extend linearly
            xForecast = static_cast<double>(n + i);
        }

        // Predict value
        double predicted = fit.slope * xForecast + fit.intercept;
        output.forecast[i] = predicted;

        // Compute prediction interval.  fit.usedPoints, not n: the interval's
        // 1/n term counts OBSERVATIONS, and on a sparse group the input vector
        // is mostly NaN padding.
        double width = predictionIntervalWidth(fit, xForecast, fit.usedPoints, config.deviations);
        output.upper[i] = predicted + width;
        output.lower[i] = predicted - width;
    }

    return output;
}

}  // namespace forecast
}  // namespace timestar
