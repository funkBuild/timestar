#include "periodicity_detector.hpp"
#include <cmath>
#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <limits>

namespace tsdb {
namespace forecast {

namespace {
    constexpr double PI = 3.14159265358979323846;
    constexpr double MIN_ACF_VALIDATION = 0.2;  // Minimum ACF for period validation
    constexpr size_t MIN_DATA_POINTS = 8;       // Minimum required data points
    constexpr size_t MAX_DFT_SIZE = 4096;       // Cap input size to avoid O(n^2) reactor blocking
}

// Remove linear trend using least squares fit
std::vector<double> PeriodicityDetector::detrend(const std::vector<double>& y) {
    const size_t n = y.size();
    if (n < 2) {
        return y;
    }

    // Compute mean of x (0, 1, 2, ..., n-1) and y
    double meanX = (n - 1) / 2.0;
    double meanY = std::accumulate(y.begin(), y.end(), 0.0) / n;

    // Compute slope: sum((x - meanX) * (y - meanY)) / sum((x - meanX)^2)
    double numerator = 0.0;
    double denominator = 0.0;
    for (size_t i = 0; i < n; ++i) {
        double dx = i - meanX;
        numerator += dx * (y[i] - meanY);
        denominator += dx * dx;
    }

    double slope = (denominator > 1e-10) ? (numerator / denominator) : 0.0;
    double intercept = meanY - slope * meanX;

    // Subtract linear trend
    std::vector<double> detrended(n);
    for (size_t i = 0; i < n; ++i) {
        detrended[i] = y[i] - (slope * i + intercept);
    }

    return detrended;
}

// Apply Hann window to reduce spectral leakage
std::vector<double> PeriodicityDetector::applyHannWindow(const std::vector<double>& y) {
    const size_t n = y.size();
    if (n < 2) {
        return y;
    }

    std::vector<double> windowed(n);
    for (size_t i = 0; i < n; ++i) {
        double window = 0.5 * (1.0 - std::cos(2.0 * PI * i / (n - 1)));
        windowed[i] = y[i] * window;
    }

    return windowed;
}

// Direct Fourier Transform (DFT)
// Returns only the first N/2+1 frequency bins (Nyquist limit) since the DFT
// is symmetric for real-valued input. Input is downsampled if it exceeds
// MAX_DFT_SIZE to keep the O(n^2) computation tractable.
std::vector<std::complex<double>> PeriodicityDetector::dft(const std::vector<double>& values) {
    // Downsample if input exceeds threshold to avoid reactor blocking
    std::vector<double> dftInput;
    if (values.size() > MAX_DFT_SIZE) {
        size_t factor = (values.size() + MAX_DFT_SIZE - 1) / MAX_DFT_SIZE;
        dftInput.reserve(MAX_DFT_SIZE);
        for (size_t i = 0; i < values.size(); i += factor) {
            double sum = 0;
            size_t count = 0;
            for (size_t j = i; j < std::min(i + factor, values.size()); ++j) {
                sum += values[j];
                ++count;
            }
            dftInput.push_back(sum / count);
        }
    } else {
        dftInput = values;
    }

    const size_t n = dftInput.size();
    // Only compute first N/2+1 frequencies (Nyquist limit for real input)
    const size_t halfN = n / 2 + 1;
    std::vector<std::complex<double>> X(halfN);

    for (size_t k = 0; k < halfN; ++k) {
        std::complex<double> sum(0.0, 0.0);
        for (size_t t = 0; t < n; ++t) {
            double angle = -2.0 * PI * k * t / n;
            sum += dftInput[t] * std::complex<double>(std::cos(angle), std::sin(angle));
        }
        X[k] = sum;
    }

    return X;
}

// Compute periodogram (power spectrum)
std::vector<double> PeriodicityDetector::computePeriodogram(const std::vector<double>& y) {
    // Compute DFT (already returns only N/2+1 bins, may downsample internally)
    auto X = dft(y);

    // The effective DFT size is (X.size() - 1) * 2
    const size_t effectiveN = (X.size() - 1) * 2;

    // Compute power spectrum: |X[k]|^2
    std::vector<double> periodogram(X.size());

    for (size_t k = 0; k < X.size(); ++k) {
        double magnitude = std::abs(X[k]);
        periodogram[k] = magnitude * magnitude / effectiveN;
    }

    return periodogram;
}

// Find peaks in periodogram
std::vector<size_t> PeriodicityDetector::findPeaks(
    const std::vector<double>& periodogram,
    double threshold
) {
    if (periodogram.size() < 3) {
        return {};
    }

    std::vector<size_t> peaks;

    // Skip DC component (index 0)
    for (size_t i = 1; i < periodogram.size() - 1; ++i) {
        // Check if it's a local maximum
        if (periodogram[i] > periodogram[i - 1] &&
            periodogram[i] > periodogram[i + 1] &&
            periodogram[i] >= threshold) {
            peaks.push_back(i);
        }
    }

    return peaks;
}

// Compute autocorrelation at given lag
double PeriodicityDetector::autoCorrelation(
    const std::vector<double>& y,
    size_t lag
) {
    const size_t n = y.size();

    if (lag >= n || n == 0) {
        return 0.0;
    }

    // Filter out NaN values - compute mean only from valid values.
    // We need to check if any NaN exists first to avoid unnecessary work.
    bool hasNaN = false;
    for (size_t i = 0; i < n; ++i) {
        if (std::isnan(y[i])) {
            hasNaN = true;
            break;
        }
    }

    if (hasNaN) {
        // Build a clean copy without NaN values for mean/variance,
        // but we still need positional information for autocovariance.
        // Compute mean from non-NaN values.
        double sum = 0.0;
        size_t validCount = 0;
        for (size_t i = 0; i < n; ++i) {
            if (!std::isnan(y[i])) {
                sum += y[i];
                ++validCount;
            }
        }
        if (validCount == 0) {
            return 0.0;
        }
        double mean = sum / static_cast<double>(validCount);

        // Compute variance from non-NaN values
        double variance = 0.0;
        for (size_t i = 0; i < n; ++i) {
            if (!std::isnan(y[i])) {
                double diff = y[i] - mean;
                variance += diff * diff;
            }
        }

        if (variance < 1e-10) {
            return 0.0;
        }

        // Compute autocovariance at lag, skipping pairs where either value is NaN
        double autocovariance = 0.0;
        for (size_t i = 0; i < n - lag; ++i) {
            if (!std::isnan(y[i]) && !std::isnan(y[i + lag])) {
                autocovariance += (y[i] - mean) * (y[i + lag] - mean);
            }
        }

        return autocovariance / variance;
    }

    // Fast path: no NaN values present
    // Compute mean
    double mean = std::accumulate(y.begin(), y.end(), 0.0) / n;

    // Compute variance (lag 0 autocorrelation)
    double variance = 0.0;
    for (size_t i = 0; i < n; ++i) {
        double diff = y[i] - mean;
        variance += diff * diff;
    }

    if (variance < 1e-10) {
        // Constant data, no correlation structure
        return 0.0;
    }

    // Compute autocovariance at lag
    double autocovariance = 0.0;
    for (size_t i = 0; i < n - lag; ++i) {
        autocovariance += (y[i] - mean) * (y[i + lag] - mean);
    }

    // Normalize by variance
    return autocovariance / variance;
}

// Main period detection function
std::vector<DetectedPeriod> PeriodicityDetector::detectPeriods(
    const std::vector<double>& y,
    size_t minPeriod,
    size_t maxPeriod,
    size_t maxPeriods,
    double powerThreshold
) {
    const size_t n = y.size();

    // Validate input
    if (n < MIN_DATA_POINTS) {
        return {};  // Not enough data
    }

    if (minPeriod < 2) {
        minPeriod = 2;
    }

    if (maxPeriod == 0 || maxPeriod > n / 2) {
        maxPeriod = n / 2;
    }

    if (minPeriod > maxPeriod) {
        return {};  // Invalid period range
    }

    // Check if data is constant
    double minVal = *std::min_element(y.begin(), y.end());
    double maxVal = *std::max_element(y.begin(), y.end());
    if (maxVal - minVal < 1e-10) {
        return {};  // No variation in data
    }

    // Step 1: Detrend the data
    auto detrended = detrend(y);

    // Step 2: Apply Hann window
    auto windowed = applyHannWindow(detrended);

    // Step 3: Compute periodogram
    auto periodogram = computePeriodogram(windowed);

    // Step 4: Compute threshold based on median absolute deviation
    // This provides a robust noise floor estimate
    std::vector<double> sortedPeriodogram = periodogram;
    std::sort(sortedPeriodogram.begin(), sortedPeriodogram.end());
    double median = sortedPeriodogram[sortedPeriodogram.size() / 2];

    // Compute MAD
    std::vector<double> deviations(sortedPeriodogram.size());
    for (size_t i = 0; i < sortedPeriodogram.size(); ++i) {
        deviations[i] = std::abs(sortedPeriodogram[i] - median);
    }
    std::sort(deviations.begin(), deviations.end());
    double mad = deviations[deviations.size() / 2];

    // Threshold is median + k * MAD, where k is derived from powerThreshold
    double k = 3.0 + powerThreshold * 10.0;  // k ranges from 3 to 13
    double noiseThreshold = median + k * mad;

    // Also enforce a minimum relative threshold
    double maxPower = *std::max_element(periodogram.begin(), periodogram.end());
    double relativeThreshold = maxPower * powerThreshold;
    double finalThreshold = std::max(noiseThreshold, relativeThreshold);

    // Step 5: Find peaks in periodogram
    auto peakIndices = findPeaks(periodogram, finalThreshold);

    // Step 6: Convert peak indices to periods and validate with ACF
    // Note: When the DFT downsamples by factor f, a frequency index k in the
    // downsampled DFT of size M corresponds to period M/k in the downsampled
    // domain, which maps to period M/k * f = n/k in the original domain.
    // So the formula T = n / k remains correct regardless of downsampling.
    std::vector<DetectedPeriod> candidates;

    for (size_t freqIdx : peakIndices) {
        if (freqIdx == 0) continue;  // Skip DC component

        // Convert frequency index to period: T = n / k
        size_t period = n / freqIdx;

        // Filter by period range
        if (period < minPeriod || period > maxPeriod) {
            continue;
        }

        // Validate with ACF
        double acf = autoCorrelation(detrended, period);

        // Period is valid if ACF is positive and significant
        if (acf < MIN_ACF_VALIDATION) {
            continue;
        }

        // Normalize power to [0, 1]
        double normalizedPower = periodogram[freqIdx] / maxPower;

        // Confidence is geometric mean of normalized power and ACF
        double confidence = std::sqrt(normalizedPower * acf);

        candidates.push_back({
            period,
            periodogram[freqIdx],
            acf,
            confidence
        });
    }

    // Step 7: Sort by confidence and return top-N
    std::sort(candidates.begin(), candidates.end(), std::greater<DetectedPeriod>());

    if (candidates.size() > maxPeriods) {
        candidates.resize(maxPeriods);
    }

    return candidates;
}

// Detect single best period (for 'auto' mode)
size_t PeriodicityDetector::detectBestPeriod(
    const std::vector<double>& y,
    uint64_t dataIntervalNs
) {
    // Use heuristics to determine sensible period range
    const size_t n = y.size();

    if (n < MIN_DATA_POINTS) {
        return 0;
    }

    // Default period range
    size_t minPeriod = 4;
    size_t maxPeriod = std::min(n / 2, size_t(100));  // Cap at 100 to avoid huge periods

    // If we know the data interval, we can make smarter guesses
    if (dataIntervalNs > 0) {
        // Common time periods in nanoseconds
        constexpr uint64_t NS_PER_MINUTE = 60ULL * 1'000'000'000ULL;
        constexpr uint64_t NS_PER_HOUR = 60ULL * NS_PER_MINUTE;
        constexpr uint64_t NS_PER_DAY = 24ULL * NS_PER_HOUR;
        constexpr uint64_t NS_PER_WEEK = 7ULL * NS_PER_DAY;

        // Prefer looking for common seasonal patterns
        // E.g., if data is sampled every hour, look for daily (24) or weekly (168) patterns
        if (dataIntervalNs <= NS_PER_MINUTE) {
            // High-frequency data: look for hourly/daily patterns
            minPeriod = 4;
            maxPeriod = std::min(n / 2, size_t(24 * 60));  // Up to 24 hours of minutes
        } else if (dataIntervalNs <= NS_PER_HOUR) {
            // Hourly data: look for daily/weekly patterns
            minPeriod = 6;   // At least 6 hours
            maxPeriod = std::min(n / 2, size_t(7 * 24));  // Up to weekly
        } else if (dataIntervalNs <= NS_PER_DAY) {
            // Daily data: look for weekly/monthly patterns
            minPeriod = 4;   // At least 4 days
            maxPeriod = std::min(n / 2, size_t(365));  // Up to yearly
        }
    }

    // Detect periods with moderate threshold
    auto periods = detectPeriods(y, minPeriod, maxPeriod, 1, 0.15);

    if (periods.empty()) {
        return 0;  // No significant period found
    }

    // Return the best period
    return periods[0].period;
}

} // namespace forecast
} // namespace tsdb
