#include "linear_forecaster.hpp"
#include "../anomaly/simd_anomaly.hpp"
#include "../simd_helpers.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>

#if !TIMESTAR_ANOMALY_DISABLE_SIMD
using timestar::simd::hsum_avx;
static inline double hsum_avx_local(__m256d v) { return hsum_avx(v); }
#endif

namespace timestar {
namespace forecast {

LinearForecaster::LinearFit LinearForecaster::fitLinearRegression(
    const std::vector<double>& x,
    const std::vector<double>& y,
    const std::vector<double>& weights
) {
    LinearFit fit{};
    size_t n = x.size();

    if (y.size() != n || weights.size() != n) {
        throw std::invalid_argument(
            "fitLinearRegression: x, y, and weights must have the same size (got " +
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

#if !TIMESTAR_ANOMALY_DISABLE_SIMD
    if (anomaly::simd::isAvx2Available() && n >= 16) {
        // AVX2 path: 4 accumulators per reduction to hide FMA latency
        __m256d accW0  = _mm256_setzero_pd();
        __m256d accW1  = _mm256_setzero_pd();
        __m256d accW2  = _mm256_setzero_pd();
        __m256d accW3  = _mm256_setzero_pd();
        __m256d accWX0 = _mm256_setzero_pd();
        __m256d accWX1 = _mm256_setzero_pd();
        __m256d accWX2 = _mm256_setzero_pd();
        __m256d accWX3 = _mm256_setzero_pd();
        __m256d accWY0 = _mm256_setzero_pd();
        __m256d accWY1 = _mm256_setzero_pd();
        __m256d accWY2 = _mm256_setzero_pd();
        __m256d accWY3 = _mm256_setzero_pd();

        const size_t simd_end = n & ~size_t(15);

        for (size_t i = 0; i < simd_end; i += 16) {
            // Block 0: i..i+3
            __m256d w0 = _mm256_loadu_pd(pw + i);
            accW0 = _mm256_add_pd(accW0, w0);
            accWX0 = _mm256_fmadd_pd(w0, _mm256_loadu_pd(px + i), accWX0);
            accWY0 = _mm256_fmadd_pd(w0, _mm256_loadu_pd(py + i), accWY0);

            // Block 1: i+4..i+7
            __m256d w1 = _mm256_loadu_pd(pw + i + 4);
            accW1 = _mm256_add_pd(accW1, w1);
            accWX1 = _mm256_fmadd_pd(w1, _mm256_loadu_pd(px + i + 4), accWX1);
            accWY1 = _mm256_fmadd_pd(w1, _mm256_loadu_pd(py + i + 4), accWY1);

            // Block 2: i+8..i+11
            __m256d w2 = _mm256_loadu_pd(pw + i + 8);
            accW2 = _mm256_add_pd(accW2, w2);
            accWX2 = _mm256_fmadd_pd(w2, _mm256_loadu_pd(px + i + 8), accWX2);
            accWY2 = _mm256_fmadd_pd(w2, _mm256_loadu_pd(py + i + 8), accWY2);

            // Block 3: i+12..i+15
            __m256d w3 = _mm256_loadu_pd(pw + i + 12);
            accW3 = _mm256_add_pd(accW3, w3);
            accWX3 = _mm256_fmadd_pd(w3, _mm256_loadu_pd(px + i + 12), accWX3);
            accWY3 = _mm256_fmadd_pd(w3, _mm256_loadu_pd(py + i + 12), accWY3);
        }

        // Reduce 4 accumulators -> 1
        accW0  = _mm256_add_pd(_mm256_add_pd(accW0, accW1), _mm256_add_pd(accW2, accW3));
        accWX0 = _mm256_add_pd(_mm256_add_pd(accWX0, accWX1), _mm256_add_pd(accWX2, accWX3));
        accWY0 = _mm256_add_pd(_mm256_add_pd(accWY0, accWY1), _mm256_add_pd(accWY2, accWY3));
        sumWeights   = hsum_avx_local(accW0);
        weightedSumX = hsum_avx_local(accWX0);
        weightedSumY = hsum_avx_local(accWY0);

        // Scalar remainder (skip NaN values)
        for (size_t i = simd_end; i < n; ++i) {
            double w = (std::isnan(px[i]) || std::isnan(py[i])) ? 0.0 : pw[i];
            sumWeights   += w;
            weightedSumX += w * px[i];
            weightedSumY += w * py[i];
        }
    } else
#endif
    {
        // Scalar fallback (skip NaN values by zeroing their weight)
        for (size_t i = 0; i < n; ++i) {
            double w = (std::isnan(px[i]) || std::isnan(py[i])) ? 0.0 : pw[i];
            sumWeights   += w;
            weightedSumX += w * px[i];
            weightedSumY += w * py[i];
        }
    }

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

#if !TIMESTAR_ANOMALY_DISABLE_SIMD
    if (anomaly::simd::isAvx2Available() && n >= 16) {
        __m256d vMeanX = _mm256_set1_pd(fit.meanX);
        __m256d vMeanY = _mm256_set1_pd(meanY);

        __m256d accXX0 = _mm256_setzero_pd();
        __m256d accXX1 = _mm256_setzero_pd();
        __m256d accXX2 = _mm256_setzero_pd();
        __m256d accXX3 = _mm256_setzero_pd();
        __m256d accXY0 = _mm256_setzero_pd();
        __m256d accXY1 = _mm256_setzero_pd();
        __m256d accXY2 = _mm256_setzero_pd();
        __m256d accXY3 = _mm256_setzero_pd();
        __m256d accYY0 = _mm256_setzero_pd();
        __m256d accYY1 = _mm256_setzero_pd();
        __m256d accYY2 = _mm256_setzero_pd();
        __m256d accYY3 = _mm256_setzero_pd();

        const size_t simd_end = n & ~size_t(15);

        for (size_t i = 0; i < simd_end; i += 16) {
            // Block 0
            __m256d w0  = _mm256_loadu_pd(pw + i);
            __m256d dx0 = _mm256_sub_pd(_mm256_loadu_pd(px + i), vMeanX);
            __m256d dy0 = _mm256_sub_pd(_mm256_loadu_pd(py + i), vMeanY);
            __m256d wdx0 = _mm256_mul_pd(w0, dx0);
            accXX0 = _mm256_fmadd_pd(wdx0, dx0, accXX0);
            accXY0 = _mm256_fmadd_pd(wdx0, dy0, accXY0);
            accYY0 = _mm256_fmadd_pd(_mm256_mul_pd(w0, dy0), dy0, accYY0);

            // Block 1
            __m256d w1  = _mm256_loadu_pd(pw + i + 4);
            __m256d dx1 = _mm256_sub_pd(_mm256_loadu_pd(px + i + 4), vMeanX);
            __m256d dy1 = _mm256_sub_pd(_mm256_loadu_pd(py + i + 4), vMeanY);
            __m256d wdx1 = _mm256_mul_pd(w1, dx1);
            accXX1 = _mm256_fmadd_pd(wdx1, dx1, accXX1);
            accXY1 = _mm256_fmadd_pd(wdx1, dy1, accXY1);
            accYY1 = _mm256_fmadd_pd(_mm256_mul_pd(w1, dy1), dy1, accYY1);

            // Block 2
            __m256d w2  = _mm256_loadu_pd(pw + i + 8);
            __m256d dx2 = _mm256_sub_pd(_mm256_loadu_pd(px + i + 8), vMeanX);
            __m256d dy2 = _mm256_sub_pd(_mm256_loadu_pd(py + i + 8), vMeanY);
            __m256d wdx2 = _mm256_mul_pd(w2, dx2);
            accXX2 = _mm256_fmadd_pd(wdx2, dx2, accXX2);
            accXY2 = _mm256_fmadd_pd(wdx2, dy2, accXY2);
            accYY2 = _mm256_fmadd_pd(_mm256_mul_pd(w2, dy2), dy2, accYY2);

            // Block 3
            __m256d w3  = _mm256_loadu_pd(pw + i + 12);
            __m256d dx3 = _mm256_sub_pd(_mm256_loadu_pd(px + i + 12), vMeanX);
            __m256d dy3 = _mm256_sub_pd(_mm256_loadu_pd(py + i + 12), vMeanY);
            __m256d wdx3 = _mm256_mul_pd(w3, dx3);
            accXX3 = _mm256_fmadd_pd(wdx3, dx3, accXX3);
            accXY3 = _mm256_fmadd_pd(wdx3, dy3, accXY3);
            accYY3 = _mm256_fmadd_pd(_mm256_mul_pd(w3, dy3), dy3, accYY3);
        }

        // Reduce 4 accumulators -> 1
        accXX0 = _mm256_add_pd(_mm256_add_pd(accXX0, accXX1), _mm256_add_pd(accXX2, accXX3));
        accXY0 = _mm256_add_pd(_mm256_add_pd(accXY0, accXY1), _mm256_add_pd(accXY2, accXY3));
        accYY0 = _mm256_add_pd(_mm256_add_pd(accYY0, accYY1), _mm256_add_pd(accYY2, accYY3));
        sumXX = hsum_avx_local(accXX0);
        sumXY = hsum_avx_local(accXY0);
        sumYY = hsum_avx_local(accYY0);

        // Scalar remainder (skip NaN values)
        for (size_t i = simd_end; i < n; ++i) {
            double w = (std::isnan(px[i]) || std::isnan(py[i])) ? 0.0 : pw[i];
            double dx = px[i] - fit.meanX;
            double dy = py[i] - meanY;
            sumXX += w * dx * dx;
            sumXY += w * dx * dy;
            sumYY += w * dy * dy;
        }
    } else
#endif
    {
        // Scalar fallback (skip NaN values)
        for (size_t i = 0; i < n; ++i) {
            double w = (std::isnan(px[i]) || std::isnan(py[i])) ? 0.0 : pw[i];
            double dx = px[i] - fit.meanX;
            double dy = py[i] - meanY;
            sumXY += w * dx * dy;
            sumXX += w * dx * dx;
            sumYY += w * dy * dy;
        }
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

    // Compute R-squared
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

#if !TIMESTAR_ANOMALY_DISABLE_SIMD
    if (anomaly::simd::isAvx2Available() && n >= 16) {
        __m256d vSlope     = _mm256_set1_pd(fit.slope);
        __m256d vIntercept = _mm256_set1_pd(fit.intercept);

        __m256d accSSE0 = _mm256_setzero_pd();
        __m256d accSSE1 = _mm256_setzero_pd();
        __m256d accSSE2 = _mm256_setzero_pd();
        __m256d accSSE3 = _mm256_setzero_pd();

        const size_t simd_end = n & ~size_t(15);

        for (size_t i = 0; i < simd_end; i += 16) {
            // Block 0: residual = y[i] - (slope*x[i] + intercept)
            __m256d pred0 = _mm256_fmadd_pd(vSlope, _mm256_loadu_pd(px + i), vIntercept);
            __m256d res0  = _mm256_sub_pd(_mm256_loadu_pd(py + i), pred0);
            __m256d wres0 = _mm256_mul_pd(_mm256_loadu_pd(pw + i), res0);
            accSSE0 = _mm256_fmadd_pd(wres0, res0, accSSE0);

            // Block 1
            __m256d pred1 = _mm256_fmadd_pd(vSlope, _mm256_loadu_pd(px + i + 4), vIntercept);
            __m256d res1  = _mm256_sub_pd(_mm256_loadu_pd(py + i + 4), pred1);
            __m256d wres1 = _mm256_mul_pd(_mm256_loadu_pd(pw + i + 4), res1);
            accSSE1 = _mm256_fmadd_pd(wres1, res1, accSSE1);

            // Block 2
            __m256d pred2 = _mm256_fmadd_pd(vSlope, _mm256_loadu_pd(px + i + 8), vIntercept);
            __m256d res2  = _mm256_sub_pd(_mm256_loadu_pd(py + i + 8), pred2);
            __m256d wres2 = _mm256_mul_pd(_mm256_loadu_pd(pw + i + 8), res2);
            accSSE2 = _mm256_fmadd_pd(wres2, res2, accSSE2);

            // Block 3
            __m256d pred3 = _mm256_fmadd_pd(vSlope, _mm256_loadu_pd(px + i + 12), vIntercept);
            __m256d res3  = _mm256_sub_pd(_mm256_loadu_pd(py + i + 12), pred3);
            __m256d wres3 = _mm256_mul_pd(_mm256_loadu_pd(pw + i + 12), res3);
            accSSE3 = _mm256_fmadd_pd(wres3, res3, accSSE3);
        }

        // Reduce 4 accumulators -> 1
        accSSE0 = _mm256_add_pd(_mm256_add_pd(accSSE0, accSSE1), _mm256_add_pd(accSSE2, accSSE3));
        sse = hsum_avx_local(accSSE0);

        // Scalar remainder
        for (size_t i = simd_end; i < n; ++i) {
            double predicted = fit.slope * px[i] + fit.intercept;
            double residual  = py[i] - predicted;
            sse += pw[i] * residual * residual;
        }
    } else
#endif
    {
        // Scalar fallback
        for (size_t i = 0; i < n; ++i) {
            double predicted = fit.slope * px[i] + fit.intercept;
            double residual  = py[i] - predicted;
            sse += pw[i] * residual * residual;
        }
    }

    if (n > 2) {
        // Use degrees of freedom (n-2) for 2-parameter model (slope + intercept).
        // For weighted regression: s^2 = (n / sumWeights) * (SSE / (n - 2))
        fit.residualStdDev = std::sqrt(sse * n / (sumWeights * (n - 2)));
    } else {
        fit.residualStdDev = 0.0;
    }

    return fit;
}

double LinearForecaster::predictionIntervalWidth(
    const LinearFit& fit,
    double x,
    size_t n,
    double deviations
) {
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

ForecastOutput LinearForecaster::forecast(
    const ForecastInput& input,
    const ForecastConfig& config,
    const std::vector<uint64_t>& forecastTimestamps
) {
    ForecastOutput output;

    size_t n = input.size();
    size_t nForecast = forecastTimestamps.size();

    if (n < config.minDataPoints) {
        // Not enough data - return empty with error indication
        return output;
    }

    output.historicalCount = n;
    output.forecastCount = nForecast;

    // Convert timestamps to normalized x values for numerical stability
    // Use index-based x values: 0, 1, 2, ... n-1
    std::vector<double> x(n);
    std::vector<double> y;
    std::vector<double> weights(n, 1.0);  // Default: uniform weights

    // Apply model-specific weighting and data selection
    const size_t originalN = n;  // Save original input size before SIMPLE model halves n
    size_t startIdx = 0;
    switch (config.linearModel) {
        case LinearModelType::DEFAULT:
            // Standard least-squares: use all data with uniform weights
            y = input.values;
            for (size_t i = 0; i < n; ++i) {
                x[i] = static_cast<double>(i);
            }
            break;

        case LinearModelType::SIMPLE:
            // Less sensitive to recent changes: use only last half of data
            startIdx = n / 2;
            y.assign(input.values.begin() + startIdx, input.values.end());
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
            y = input.values;
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

    output.slope = fit.slope;
    output.intercept = fit.intercept;
    output.rSquared = fit.rSquared;
    output.residualStdDev = fit.residualStdDev;

    // Generate past values (just copy input)
    output.past = input.values;

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

        // Compute prediction interval
        double width = predictionIntervalWidth(fit, xForecast, n, config.deviations);
        output.upper[i] = predicted + width;
        output.lower[i] = predicted - width;
    }

    return output;
}

} // namespace forecast
} // namespace timestar
