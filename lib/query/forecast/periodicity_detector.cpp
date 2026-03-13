#include "periodicity_detector.hpp"

#include "../anomaly/simd_anomaly.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <stdexcept>

namespace timestar {
namespace forecast {

namespace {
constexpr double PI = 3.14159265358979323846;
constexpr double MIN_ACF_VALIDATION = 0.2;  // Minimum ACF for period validation
constexpr size_t MIN_DATA_POINTS = 8;       // Minimum required data points
constexpr size_t MAX_DFT_SIZE = 4096;       // Cap input size to avoid O(n^2) reactor blocking
}  // namespace

// Remove linear trend using least squares fit
std::vector<double> PeriodicityDetector::detrend(const std::vector<double>& y) {
    const size_t n = y.size();
    if (n < 2) {
        return y;
    }

    // Compute mean of x (0, 1, 2, ..., n-1) and y
    double meanX = (n - 1) / 2.0;
    double meanY = anomaly::simd::vectorMean(y.data(), n);

    // Compute slope: sum((x - meanX) * (y - meanY)) / sum((x - meanX)^2)
    double numerator = 0.0;
    double denominator = 0.0;

    // Scalar fallback
    for (size_t i = 0; i < n; ++i) {
        double dx = i - meanX;
        numerator += dx * (y[i] - meanY);
        denominator += dx * dx;
    }

    double slope = (denominator > 1e-10) ? (numerator / denominator) : 0.0;
    double intercept = meanY - slope * meanX;

    // Subtract linear trend
    std::vector<double> detrended(n);

    // Scalar fallback
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

// Radix-2 Cooley-Tukey FFT: O(n log n) instead of O(n²) naive DFT.
// Returns only the first N/2+1 frequency bins (Nyquist limit for real input).
std::vector<std::complex<double>> PeriodicityDetector::fft(const std::vector<double>& values) {
    // Determine FFT size: next power of 2 >= values.size(), capped at MAX_DFT_SIZE
    size_t n = values.size();

    // Downsample if input exceeds threshold
    std::vector<double> fftInput;
    size_t downsampleFactor = 1;
    if (n > MAX_DFT_SIZE) {
        downsampleFactor = (n + MAX_DFT_SIZE - 1) / MAX_DFT_SIZE;
        fftInput.reserve(MAX_DFT_SIZE);
        for (size_t i = 0; i < n; i += downsampleFactor) {
            double sum = 0;
            size_t count = 0;
            for (size_t j = i; j < std::min(i + downsampleFactor, n); ++j) {
                sum += values[j];
                ++count;
            }
            fftInput.push_back(sum / count);
        }
        n = fftInput.size();
    } else {
        fftInput = values;
    }

    // Pad to next power of 2
    size_t fftSize = 1;
    while (fftSize < n)
        fftSize <<= 1;
    fftInput.resize(fftSize, 0.0);  // Zero-pad

    // Store effective N for period mapping: period = effectiveN_ / freqIdx
    // Accounts for both padding and downsampling.
    effectiveN_ = fftSize * downsampleFactor;

    // In-place iterative Cooley-Tukey FFT
    std::vector<std::complex<double>> X(fftSize);
    for (size_t i = 0; i < fftSize; ++i) {
        X[i] = std::complex<double>(fftInput[i], 0.0);
    }

    // Bit-reversal permutation
    for (size_t i = 1, j = 0; i < fftSize; ++i) {
        size_t bit = fftSize >> 1;
        while (j & bit) {
            j ^= bit;
            bit >>= 1;
        }
        j ^= bit;
        if (i < j)
            std::swap(X[i], X[j]);
    }

    // Butterfly stages
    for (size_t len = 2; len <= fftSize; len <<= 1) {
        double angle = -2.0 * PI / static_cast<double>(len);
        std::complex<double> wlen(std::cos(angle), std::sin(angle));

        for (size_t i = 0; i < fftSize; i += len) {
            std::complex<double> w(1.0, 0.0);
            for (size_t j = 0; j < len / 2; ++j) {
                std::complex<double> u = X[i + j];
                std::complex<double> v = X[i + j + len / 2] * w;
                X[i + j] = u + v;
                X[i + j + len / 2] = u - v;
                w *= wlen;
            }
        }
    }

    // Return only first n/2+1 bins (Nyquist limit for original input size)
    size_t halfN = n / 2 + 1;
    X.resize(halfN);
    return X;
}

// Compute periodogram (power spectrum)
std::vector<double> PeriodicityDetector::computePeriodogram(const std::vector<double>& y) {
    // Compute FFT (returns only N/2+1 bins, may downsample internally)
    auto X = fft(y);

    // The effective DFT size is (X.size() - 1) * 2
    const size_t effectiveN = (X.size() - 1) * 2;
    const double invN = 1.0 / static_cast<double>(effectiveN);

    // Compute power spectrum: |X[k]|^2 = re*re + im*im
    // Use std::norm() instead of std::abs()^2 to avoid unnecessary sqrt.
    const size_t numBins = X.size();
    std::vector<double> periodogram(numBins);

    // Scalar fallback: std::norm(z) = re*re + im*im (no sqrt)
    for (size_t k = 0; k < numBins; ++k) {
        periodogram[k] = std::norm(X[k]) * invN;
    }

    return periodogram;
}

// Find peaks in periodogram
std::vector<size_t> PeriodicityDetector::findPeaks(const std::vector<double>& periodogram, double threshold) {
    if (periodogram.size() < 3) {
        return {};
    }

    std::vector<size_t> peaks;

    // Skip DC component (index 0)
    for (size_t i = 1; i < periodogram.size() - 1; ++i) {
        // Check if it's a local maximum
        if (periodogram[i] > periodogram[i - 1] && periodogram[i] > periodogram[i + 1] && periodogram[i] >= threshold) {
            peaks.push_back(i);
        }
    }

    // Check the last bin (can be a peak if it exceeds its left neighbor)
    size_t last = periodogram.size() - 1;
    if (periodogram[last] > periodogram[last - 1] && periodogram[last] >= threshold) {
        peaks.push_back(last);
    }

    return peaks;
}

// Compute autocorrelation at given lag
double PeriodicityDetector::autoCorrelation(const std::vector<double>& y, size_t lag) {
    const size_t n = y.size();

    if (lag >= n || n == 0) {
        return 0.0;
    }

    // Filter out non-finite values (NaN and Inf) - compute mean only from finite values.
    // std::isfinite() returns false for both NaN and Inf, so it catches both cases.
    // We check if any non-finite value exists first to avoid unnecessary work.
    bool hasNonFinite = false;
    for (size_t i = 0; i < n; ++i) {
        if (!std::isfinite(y[i])) {
            hasNonFinite = true;
            break;
        }
    }

    if (hasNonFinite) {
        // Build a clean view without non-finite values for mean/variance,
        // but we still need positional information for autocovariance.
        // Compute mean from finite values only.
        double sum = 0.0;
        size_t validCount = 0;
        for (size_t i = 0; i < n; ++i) {
            if (std::isfinite(y[i])) {
                sum += y[i];
                ++validCount;
            }
        }
        if (validCount == 0) {
            return 0.0;
        }
        double mean = sum / static_cast<double>(validCount);

        // Compute variance from finite values only
        double variance = 0.0;
        for (size_t i = 0; i < n; ++i) {
            if (std::isfinite(y[i])) {
                double diff = y[i] - mean;
                variance += diff * diff;
            }
        }

        if (variance < 1e-10) {
            return 0.0;
        }

        // Compute autocovariance at lag, skipping pairs where either value is non-finite
        double autocovariance = 0.0;
        for (size_t i = 0; i < n - lag; ++i) {
            if (std::isfinite(y[i]) && std::isfinite(y[i + lag])) {
                autocovariance += (y[i] - mean) * (y[i + lag] - mean);
            }
        }

        return autocovariance / variance;
    }

    // Fast path: no NaN values present
    // Use SIMD helpers for mean and variance
    const double* py = y.data();
    double mean = anomaly::simd::vectorMean(py, n);
    double variance = anomaly::simd::vectorSumSquaredDiff(py, n, mean);

    if (variance < 1e-10) {
        // Constant data, no correlation structure
        return 0.0;
    }

    // Compute autocovariance at lag
    const size_t count = n - lag;
    double autocovariance = 0.0;

    // Scalar fallback
    for (size_t i = 0; i < count; ++i) {
        autocovariance += (y[i] - mean) * (y[i + lag] - mean);
    }

    // Normalize by variance (sum of squared diffs, not sample variance)
    return autocovariance / variance;
}

// Main period detection function
std::vector<DetectedPeriod> PeriodicityDetector::detectPeriods(const std::vector<double>& y, size_t minPeriod,
                                                               size_t maxPeriod, size_t maxPeriods,
                                                               double powerThreshold) {
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

    // Sanitize input: replace non-finite values (NaN, Inf, -Inf) with the
    // finite mean so the FFT/detrend pipeline is not poisoned.  autoCorrelation
    // has its own guard and will re-skip non-finite positions, so we only need
    // the sanitized copy for the spectral analysis stages.
    std::vector<double> ySanitized;
    {
        double sum = 0.0;
        size_t validCount = 0;
        for (size_t i = 0; i < n; ++i) {
            if (std::isfinite(y[i])) {
                sum += y[i];
                ++validCount;
            }
        }
        if (validCount == 0) {
            return {};  // No finite data at all
        }
        const double finiteMean = sum / static_cast<double>(validCount);
        if (validCount < n) {
            // There are non-finite values; build a sanitized copy
            ySanitized = y;
            for (size_t i = 0; i < n; ++i) {
                if (!std::isfinite(ySanitized[i])) {
                    ySanitized[i] = finiteMean;
                }
            }
        }
    }
    // Use sanitized copy if we had to fix anything, otherwise use original
    const std::vector<double>& yClean = ySanitized.empty() ? y : ySanitized;

    // Check if data is constant (use the cleaned series so Inf doesn't fool minmax)
    auto [minIt, maxIt] = std::minmax_element(yClean.begin(), yClean.end());
    if (*maxIt - *minIt < 1e-10) {
        return {};  // No variation in data
    }

    // Step 1: Detrend the data
    auto detrended = detrend(yClean);

    // Step 2: Apply Hann window
    auto windowed = applyHannWindow(detrended);

    // Step 3: Compute periodogram
    auto periodogram = computePeriodogram(windowed);

    // Step 4: Compute threshold based on median absolute deviation
    // This provides a robust noise floor estimate
    // Use nth_element for O(n) median instead of O(n log n) sort
    std::vector<double> pgCopy = periodogram;
    size_t medianIdx = pgCopy.size() / 2;
    std::nth_element(pgCopy.begin(), pgCopy.begin() + medianIdx, pgCopy.end());
    double median = pgCopy[medianIdx];

    // Compute MAD using nth_element (reuse pgCopy buffer for deviations)
    for (size_t i = 0; i < pgCopy.size(); ++i) {
        pgCopy[i] = std::abs(periodogram[i] - median);
    }
    std::nth_element(pgCopy.begin(), pgCopy.begin() + medianIdx, pgCopy.end());
    double mad = pgCopy[medianIdx];

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
    // The FFT may zero-pad to the next power of 2, so frequency bin k
    // corresponds to period = effectiveN_ / k (set by fft(), accounts for
    // both downsampling and padding). We use rounded division for accuracy.
    std::vector<DetectedPeriod> candidates;

    for (size_t freqIdx : peakIndices) {
        if (freqIdx == 0)
            continue;  // Skip DC component

        // Convert frequency index to period using effective FFT size (rounded)
        size_t period = (effectiveN_ + freqIdx / 2) / freqIdx;

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

        candidates.push_back({period, periodogram[freqIdx], acf, confidence});
    }

    // Step 7: Sort by confidence and return top-N
    std::sort(candidates.begin(), candidates.end(), std::greater<DetectedPeriod>());

    if (candidates.size() > maxPeriods) {
        candidates.resize(maxPeriods);
    }

    return candidates;
}

// Detect single best period (for 'auto' mode)
size_t PeriodicityDetector::detectBestPeriod(const std::vector<double>& y, uint64_t dataIntervalNs) {
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
            minPeriod = 6;                                // At least 6 hours
            maxPeriod = std::min(n / 2, size_t(7 * 24));  // Up to weekly
        } else if (dataIntervalNs <= NS_PER_DAY) {
            // Daily data: look for weekly/monthly patterns
            minPeriod = 4;                             // At least 4 days
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

}  // namespace forecast
}  // namespace timestar
