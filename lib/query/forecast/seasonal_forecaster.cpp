#include "seasonal_forecaster.hpp"
#include "periodicity_detector.hpp"
#include "stl_decomposition.hpp"
#include "../anomaly/simd_anomaly.hpp"
#include <cmath>
#include <algorithm>
#include <numeric>

namespace tsdb {
namespace forecast {

std::vector<double> SeasonalForecaster::seasonalDifference(
    const std::vector<double>& y,
    size_t seasonalPeriod
) {
    if (y.size() <= seasonalPeriod) {
        return {};
    }

    std::vector<double> result(y.size() - seasonalPeriod);
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = y[i + seasonalPeriod] - y[i];
    }
    return result;
}

std::vector<double> SeasonalForecaster::regularDifference(const std::vector<double>& y) {
    if (y.size() < 2) {
        return {};
    }

    std::vector<double> result(y.size() - 1);
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = y[i + 1] - y[i];
    }
    return result;
}

std::vector<double> SeasonalForecaster::inverseSeasonalDifference(
    const std::vector<double>& diffed,
    const std::vector<double>& original,
    size_t seasonalPeriod,
    size_t forecastCount
) {
    std::vector<double> result(forecastCount);

    // Guard against empty original or zero seasonal period
    if (original.empty() || seasonalPeriod == 0) {
        for (size_t i = 0; i < forecastCount; ++i) {
            result[i] = (i < diffed.size()) ? diffed[i] : 0.0;
        }
        return result;
    }

    // To invert seasonal differencing, we need: y[t] = y'[t] + y[t-s]
    // For forecasting, we need to use the last seasonal cycle from original data
    for (size_t i = 0; i < forecastCount; ++i) {
        // Get the corresponding seasonal lag value
        // Use signed arithmetic to avoid unsigned underflow when
        // original.size() < seasonalPeriod
        ptrdiff_t baseIdx = static_cast<ptrdiff_t>(original.size()) -
                            static_cast<ptrdiff_t>(seasonalPeriod) +
                            static_cast<ptrdiff_t>(i % seasonalPeriod);
        if (baseIdx >= 0 && static_cast<size_t>(baseIdx) < original.size()) {
            result[i] = diffed[i] + original[static_cast<size_t>(baseIdx)];
        } else if (i >= seasonalPeriod) {
            // Fallback: use the forecast we just made
            result[i] = diffed[i] + result[i - seasonalPeriod];
        } else {
            // Not enough data for seasonal reference: use last available value
            result[i] = diffed[i] + original.back();
        }
    }

    return result;
}

std::vector<double> SeasonalForecaster::inverseRegularDifference(
    const std::vector<double>& diffed,
    double lastValue,
    size_t forecastCount
) {
    std::vector<double> result(forecastCount);
    double cumsum = lastValue;

    for (size_t i = 0; i < forecastCount; ++i) {
        if (i < diffed.size()) {
            cumsum += diffed[i];
        }
        result[i] = cumsum;
    }

    return result;
}

double SeasonalForecaster::autoCorrelation(
    const std::vector<double>& y,
    double mean,
    double variance,
    size_t lag
) {
    if (variance < 1e-10 || lag >= y.size()) {
        return 0.0;
    }

    double sum = 0.0;
    size_t n = y.size();

    for (size_t i = 0; i < n - lag; ++i) {
        sum += (y[i] - mean) * (y[i + lag] - mean);
    }

    return sum / (static_cast<double>(n) * variance);
}

std::vector<double> SeasonalForecaster::levinsonDurbin(
    const std::vector<double>& r,
    size_t order
) {
    // Levinson-Durbin recursion for solving Yule-Walker equations
    // r[0], r[1], ..., r[order] are autocorrelations at lags 0, 1, ..., order

    if (order == 0 || r.size() <= order) {
        return {};
    }

    std::vector<double> phi(order, 0.0);  // AR coefficients
    std::vector<double> phiPrev(order, 0.0);

    // Initialize
    phi[0] = r[1] / r[0];
    double v = r[0] * (1.0 - phi[0] * phi[0]);

    for (size_t k = 1; k < order; ++k) {
        // Compute reflection coefficient
        double num = r[k + 1];
        for (size_t j = 0; j < k; ++j) {
            num -= phi[j] * r[k - j];
        }

        if (std::abs(v) < 1e-10) {
            break;  // Singular - stop recursion
        }

        double lambda = num / v;

        // Update coefficients
        phiPrev = phi;
        phi[k] = lambda;

        for (size_t j = 0; j < k; ++j) {
            phi[j] = phiPrev[j] - lambda * phiPrev[k - 1 - j];
        }

        // Update prediction error variance
        v *= (1.0 - lambda * lambda);
    }

    return phi;
}

std::vector<double> SeasonalForecaster::fitARCoefficients(
    const std::vector<double>& y,
    size_t order
) {
    if (y.size() <= order + 1) {
        return std::vector<double>(order, 0.0);
    }

    double mean = anomaly::simd::vectorMean(y.data(), y.size());
    double variance = anomaly::simd::vectorVariance(y.data(), y.size(), mean);

    if (variance < 1e-10) {
        return std::vector<double>(order, 0.0);
    }

    // Compute autocorrelations r[0] to r[order]
    std::vector<double> r(order + 1);
    r[0] = variance;
    for (size_t k = 1; k <= order; ++k) {
        r[k] = autoCorrelation(y, mean, variance, k) * variance;
    }

    return levinsonDurbin(r, order);
}

std::vector<double> SeasonalForecaster::fitSeasonalARCoefficients(
    const std::vector<double>& y,
    size_t order,
    size_t seasonalPeriod
) {
    if (y.size() <= order * seasonalPeriod + 1) {
        return std::vector<double>(order, 0.0);
    }

    double mean = anomaly::simd::vectorMean(y.data(), y.size());
    double variance = anomaly::simd::vectorVariance(y.data(), y.size(), mean);

    if (variance < 1e-10) {
        return std::vector<double>(order, 0.0);
    }

    // Compute autocorrelations at seasonal lags
    std::vector<double> r(order + 1);
    r[0] = variance;
    for (size_t k = 1; k <= order; ++k) {
        r[k] = autoCorrelation(y, mean, variance, k * seasonalPeriod) * variance;
    }

    return levinsonDurbin(r, order);
}

double SeasonalForecaster::arForecast(
    const std::vector<double>& y,
    const std::vector<double>& arCoeffs,
    double mean
) {
    if (y.empty() || arCoeffs.empty()) {
        return mean;
    }

    double forecast = mean;
    size_t n = y.size();
    size_t p = arCoeffs.size();

    for (size_t i = 0; i < p && i < n; ++i) {
        forecast += arCoeffs[i] * (y[n - 1 - i] - mean);
    }

    return forecast;
}

double SeasonalForecaster::seasonalArForecast(
    const std::vector<double>& y,
    const std::vector<double>& sarCoeffs,
    size_t seasonalPeriod,
    double mean
) {
    if (y.empty() || sarCoeffs.empty() || y.size() < seasonalPeriod) {
        return mean;
    }

    double forecast = mean;
    size_t n = y.size();
    size_t P = sarCoeffs.size();

    for (size_t i = 0; i < P; ++i) {
        size_t lag = (i + 1) * seasonalPeriod;
        if (lag <= n) {
            forecast += sarCoeffs[i] * (y[n - lag] - mean);
        }
    }

    return forecast;
}

double SeasonalForecaster::estimateForecastVariance(
    const std::vector<double>& y,
    const std::vector<double>& arCoeffs,
    double mean,
    size_t horizonSteps
) {
    // Compute in-sample residual variance
    if (y.size() <= arCoeffs.size()) {
        double variance = anomaly::simd::vectorVariance(y.data(), y.size(), mean);
        return variance * horizonSteps;  // Variance grows with horizon
    }

    double sse = 0.0;
    size_t count = 0;
    size_t p = arCoeffs.size();

    for (size_t t = p; t < y.size(); ++t) {
        double predicted = mean;
        for (size_t i = 0; i < p; ++i) {
            predicted += arCoeffs[i] * (y[t - 1 - i] - mean);
        }
        double residual = y[t] - predicted;
        sse += residual * residual;
        ++count;
    }

    double sigmaSquared = (count > 0) ? sse / static_cast<double>(count) : 0.0;

    // Variance increases with forecast horizon
    // For AR(p), this is approximately: sigma^2 * sum_{i=0}^{h-1} psi_i^2
    // We use a simpler approximation: sigma^2 * (1 + c * log(h))
    double horizonFactor = 1.0 + 0.5 * std::log(1.0 + horizonSteps);

    return sigmaSquared * horizonFactor;
}

size_t SeasonalForecaster::detectSeasonalPeriod(
    const std::vector<double>& y,
    uint64_t dataIntervalNs
) {
    if (y.size() < 24) {
        return 0;  // Not enough data
    }

    // Check common periods based on data interval
    // 1 minute interval: check hourly (60), daily (1440)
    // 1 hour interval: check daily (24), weekly (168)

    std::vector<size_t> candidatePeriods;

    if (dataIntervalNs <= 60000000000ULL) {  // <= 1 minute
        candidatePeriods = {60, 1440};  // Hourly, daily
    } else if (dataIntervalNs <= 3600000000000ULL) {  // <= 1 hour
        candidatePeriods = {24, 168};  // Daily, weekly
    } else {
        candidatePeriods = {7, 30};  // Weekly, monthly
    }

    double mean = anomaly::simd::vectorMean(y.data(), y.size());
    double variance = anomaly::simd::vectorVariance(y.data(), y.size(), mean);

    if (variance < 1e-10) {
        return 0;
    }

    // Find the period with highest autocorrelation
    size_t bestPeriod = 0;
    double bestAcf = 0.0;

    for (size_t period : candidatePeriods) {
        if (period >= y.size() / 2) continue;

        double acf = std::abs(autoCorrelation(y, mean, variance, period));
        if (acf > bestAcf && acf > 0.3) {  // Threshold for significance
            bestAcf = acf;
            bestPeriod = period;
        }
    }

    return bestPeriod;
}

ForecastOutput SeasonalForecaster::forecast(
    const ForecastInput& input,
    const ForecastConfig& config,
    const std::vector<uint64_t>& forecastTimestamps
) {
    ForecastOutput output;

    size_t n = input.size();
    size_t nForecast = forecastTimestamps.size();

    if (n < config.minDataPoints) {
        return output;
    }

    output.historicalCount = n;
    output.forecastCount = nForecast;
    output.past = input.values;

    // Calculate data interval
    uint64_t dataInterval = (n >= 2) ?
        (input.timestamps[n-1] - input.timestamps[0]) / (n - 1) :
        60000000000ULL;  // Default 1 minute

    // Determine seasonal period
    size_t seasonalPeriod = 0;

    if (config.forecastSeasonality == ForecastSeasonality::MULTI) {
        // Use MSTL for multiple seasonalities
        return forecastMSTL(input, config, forecastTimestamps);
    }

    if (config.forecastSeasonality == ForecastSeasonality::AUTO) {
        // Auto-detect best period using FFT
        PeriodicityDetector detector;
        seasonalPeriod = detector.detectBestPeriod(input.values, dataInterval);
    } else if (config.forecastSeasonality != ForecastSeasonality::NONE) {
        // Use specified seasonality
        seasonalPeriod = forecastSeasonalityToPeriod(config.forecastSeasonality, dataInterval);
    } else if (config.seasonality != Seasonality::NONE) {
        // Backwards compatibility with old enum
        seasonalPeriod = seasonalityToPeriod(config.seasonality, dataInterval);
    } else {
        // Fallback to existing detection
        seasonalPeriod = detectSeasonalPeriod(input.values, dataInterval);
    }

    // Working copy of data
    std::vector<double> y = input.values;
    double mean = anomaly::simd::vectorMean(y.data(), y.size());

    // Store last values for inverse differencing
    std::vector<double> originalY = y;
    double lastY = y.back();

    // Apply seasonal differencing if period detected
    bool usedSeasonalDiff = false;
    if (seasonalPeriod > 0 && seasonalPeriod < y.size() / 2) {
        y = seasonalDifference(y, seasonalPeriod);
        usedSeasonalDiff = true;
        if (!y.empty()) {
            mean = anomaly::simd::vectorMean(y.data(), y.size());
        }
    }

    // Apply regular differencing (d=1)
    std::vector<double> lastDiffed;
    if (y.size() > 1) {
        lastDiffed = y;
        y = regularDifference(y);
        if (!y.empty()) {
            mean = anomaly::simd::vectorMean(y.data(), y.size());
        }
    }

    if (y.size() < config.minDataPoints) {
        // Not enough data after differencing - use simpler approach
        output.forecast.resize(nForecast);
        output.upper.resize(nForecast);
        output.lower.resize(nForecast);

        double lastVal = input.values.back();
        double variance = anomaly::simd::vectorVariance(
            input.values.data(), input.values.size(),
            anomaly::simd::vectorMean(input.values.data(), input.values.size())
        );
        double stddev = std::sqrt(variance);

        for (size_t i = 0; i < nForecast; ++i) {
            output.forecast[i] = lastVal;
            double width = config.deviations * stddev * std::sqrt(1.0 + i);
            output.upper[i] = lastVal + width;
            output.lower[i] = lastVal - width;
        }

        return output;
    }

    // Fit AR model on differenced data
    size_t arOrder = static_cast<size_t>(config.arOrder);
    arOrder = std::min(arOrder, y.size() / 4);
    auto arCoeffs = fitARCoefficients(y, arOrder);

    // Fit seasonal AR if applicable
    std::vector<double> sarCoeffs;
    if (usedSeasonalDiff && seasonalPeriod > 0) {
        size_t sarOrder = static_cast<size_t>(config.seasonalArOrder);
        sarCoeffs = fitSeasonalARCoefficients(y, sarOrder, seasonalPeriod);
    }

    // Generate forecasts on differenced scale
    std::vector<double> diffForecasts(nForecast);
    std::vector<double> yExtended = y;

    for (size_t i = 0; i < nForecast; ++i) {
        double fc = arForecast(yExtended, arCoeffs, 0.0);  // mean=0 for differenced data

        // Add seasonal AR contribution
        if (!sarCoeffs.empty() && seasonalPeriod > 0) {
            fc += seasonalArForecast(yExtended, sarCoeffs, seasonalPeriod, 0.0);
        }

        diffForecasts[i] = fc;
        yExtended.push_back(fc);
    }

    // Inverse regular differencing
    double lastBeforeDiff = lastDiffed.empty() ? lastY : lastDiffed.back();
    std::vector<double> undiffed = inverseRegularDifference(diffForecasts, lastBeforeDiff, nForecast);

    // Inverse seasonal differencing
    std::vector<double> finalForecasts;
    if (usedSeasonalDiff && seasonalPeriod > 0) {
        finalForecasts = inverseSeasonalDifference(undiffed, originalY, seasonalPeriod, nForecast);
    } else {
        finalForecasts = undiffed;
    }

    // Compute prediction intervals
    output.forecast.resize(nForecast);
    output.upper.resize(nForecast);
    output.lower.resize(nForecast);

    double residualVariance = estimateForecastVariance(y, arCoeffs, 0.0, 1);
    output.residualStdDev = std::sqrt(residualVariance);

    for (size_t i = 0; i < nForecast; ++i) {
        output.forecast[i] = finalForecasts[i];

        // Prediction interval width grows with horizon
        double horizonVariance = estimateForecastVariance(y, arCoeffs, 0.0, i + 1);
        double width = config.deviations * std::sqrt(horizonVariance);

        output.upper[i] = finalForecasts[i] + width;
        output.lower[i] = finalForecasts[i] - width;
    }

    // Compute R-squared (on original scale)
    double totalVar = anomaly::simd::vectorVariance(
        input.values.data(), input.values.size(),
        anomaly::simd::vectorMean(input.values.data(), input.values.size())
    );
    if (totalVar > 1e-10) {
        output.rSquared = 1.0 - residualVariance / totalVar;
        output.rSquared = std::max(0.0, output.rSquared);
    }

    return output;
}

ForecastOutput SeasonalForecaster::forecastMSTL(
    const ForecastInput& input,
    const ForecastConfig& config,
    const std::vector<uint64_t>& forecastTimestamps
) {
    ForecastOutput output;
    size_t n = input.size();
    size_t nForecast = forecastTimestamps.size();

    if (n < config.minDataPoints) {
        return output;
    }

    output.historicalCount = n;
    output.forecastCount = nForecast;
    output.past = input.values;

    // Calculate data interval
    uint64_t dataInterval = (n >= 2) ?
        (input.timestamps[n-1] - input.timestamps[0]) / (n - 1) :
        60000000000ULL;  // Default 1 minute

    // Detect multiple periods
    PeriodicityDetector detector;
    auto detected = detector.detectPeriods(
        input.values,
        config.minPeriod,
        config.maxPeriod > 0 ? config.maxPeriod : n / 2,
        config.maxSeasonalComponents,
        config.seasonalThreshold
    );

    if (detected.empty()) {
        // No seasonality detected - fall back to simple extrapolation
        output.forecast.resize(nForecast);
        output.upper.resize(nForecast);
        output.lower.resize(nForecast);

        double lastVal = input.values.back();
        double variance = anomaly::simd::vectorVariance(
            input.values.data(), input.values.size(),
            anomaly::simd::vectorMean(input.values.data(), input.values.size())
        );
        double stddev = std::sqrt(variance);

        for (size_t i = 0; i < nForecast; ++i) {
            output.forecast[i] = lastVal;
            double width = config.deviations * stddev * std::sqrt(1.0 + static_cast<double>(i) / n);
            output.upper[i] = lastVal + width;
            output.lower[i] = lastVal - width;
        }

        return output;
    }

    // Extract periods
    std::vector<size_t> periods;
    for (const auto& d : detected) {
        periods.push_back(d.period);
    }

    // Perform MSTL decomposition
    STLDecomposer stl;
    auto mstl = stl.decomposeMultiple(input.values, periods);

    if (!mstl.success) {
        return output;  // Return empty on failure
    }

    // Extrapolate each component
    auto trendForecast = extrapolateTrend(mstl.trend, nForecast);

    std::vector<double> combinedSeasonal(nForecast, 0.0);
    for (size_t i = 0; i < mstl.seasonals.size(); ++i) {
        auto sf = extrapolateSeasonal(mstl.seasonals[i], mstl.periods[i], nForecast);
        for (size_t j = 0; j < nForecast; ++j) {
            combinedSeasonal[j] += sf[j];
        }
    }

    // Combine forecasts
    output.forecast.resize(nForecast);
    for (size_t i = 0; i < nForecast; ++i) {
        output.forecast[i] = trendForecast[i] + combinedSeasonal[i];
    }

    // Compute confidence bounds from residual variance
    double residualStdDev = 0.0;
    if (!mstl.residual.empty()) {
        double mean = 0.0;
        for (double r : mstl.residual) mean += r;
        mean /= mstl.residual.size();
        for (double r : mstl.residual) {
            residualStdDev += (r - mean) * (r - mean);
        }
        residualStdDev = std::sqrt(residualStdDev / mstl.residual.size());
    }

    output.residualStdDev = residualStdDev;
    output.upper.resize(nForecast);
    output.lower.resize(nForecast);

    for (size_t i = 0; i < nForecast; ++i) {
        double width = config.deviations * residualStdDev * std::sqrt(1.0 + static_cast<double>(i) / n);
        output.upper[i] = output.forecast[i] + width;
        output.lower[i] = output.forecast[i] - width;
    }

    return output;
}

std::vector<double> SeasonalForecaster::extrapolateTrend(
    const std::vector<double>& trend,
    size_t horizonSteps
) {
    // Linear extrapolation of trend
    std::vector<double> result(horizonSteps);
    size_t n = trend.size();

    if (n < 2) {
        double val = n > 0 ? trend.back() : 0.0;
        std::fill(result.begin(), result.end(), val);
        return result;
    }

    // Fit linear trend to last portion of trend component
    size_t fitPoints = std::min(n, std::max<size_t>(10, n / 4));
    double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
    for (size_t i = 0; i < fitPoints; ++i) {
        double x = static_cast<double>(i);
        double y = trend[n - fitPoints + i];
        sumX += x;
        sumY += y;
        sumXY += x * y;
        sumXX += x * x;
    }

    double denominator = fitPoints * sumXX - sumX * sumX;
    double slope = 0.0;
    double intercept = sumY / fitPoints;  // Default to mean if no variation in x
    if (std::abs(denominator) > 1e-10) {
        slope = (fitPoints * sumXY - sumX * sumY) / denominator;
        intercept = (sumY - slope * sumX) / fitPoints;
    }

    for (size_t i = 0; i < horizonSteps; ++i) {
        double x = static_cast<double>(fitPoints + i);
        result[i] = slope * x + intercept;
    }

    return result;
}

std::vector<double> SeasonalForecaster::extrapolateSeasonal(
    const std::vector<double>& seasonal,
    size_t period,
    size_t horizonSteps
) {
    std::vector<double> result(horizonSteps);
    size_t n = seasonal.size();

    if (period == 0 || n == 0) {
        return result;
    }

    // Repeat last complete cycle with slight damping
    for (size_t i = 0; i < horizonSteps; ++i) {
        size_t idx = (n - period + (i % period)) % n;
        if (idx < n) {
            // Apply slight damping for longer horizons
            double damping = 1.0 - 0.01 * (i / period);
            damping = std::max(0.5, damping);
            result[i] = seasonal[idx] * damping;
        }
    }

    return result;
}

} // namespace forecast
} // namespace tsdb
