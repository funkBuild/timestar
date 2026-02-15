#ifndef PERIODICITY_DETECTOR_H_INCLUDED
#define PERIODICITY_DETECTOR_H_INCLUDED

#include <vector>
#include <cstdint>
#include <cmath>
#include <algorithm>
#include <complex>

namespace tsdb {
namespace forecast {

/**
 * Detected period with strength and confidence metrics
 */
struct DetectedPeriod {
    size_t period;          // Period in data points
    double power;           // Spectral power (unnormalized)
    double acfStrength;     // ACF at this lag (validation)
    double confidence;      // Combined confidence score [0-1]

    bool operator>(const DetectedPeriod& other) const {
        return confidence > other.confidence;
    }
};

/**
 * Periodicity Detector using FFT + ACF hybrid approach
 *
 * Algorithm:
 * 1. Detrend data (remove linear trend)
 * 2. Apply Hann window to reduce spectral leakage
 * 3. Compute DFT and periodogram (power spectrum)
 * 4. Find peaks in periodogram above noise threshold
 * 5. Validate peaks using ACF
 * 6. Return top-N periods sorted by confidence
 */
class PeriodicityDetector {
public:
    /**
     * Detect dominant periods in time series data
     *
     * @param y Input time series
     * @param minPeriod Minimum period to consider (default: 4)
     * @param maxPeriod Maximum period (default: n/2)
     * @param maxPeriods Maximum number of periods to return
     * @param powerThreshold Minimum relative power for a peak (0-1)
     * @return Vector of detected periods sorted by confidence
     */
    std::vector<DetectedPeriod> detectPeriods(
        const std::vector<double>& y,
        size_t minPeriod = 4,
        size_t maxPeriod = 0,
        size_t maxPeriods = 3,
        double powerThreshold = 0.1
    );

    /**
     * Detect the single best period (for 'auto' mode)
     *
     * @param y Input time series
     * @param dataIntervalNs Data interval in nanoseconds (for heuristics)
     * @return Best period, or 0 if no significant period found
     */
    size_t detectBestPeriod(
        const std::vector<double>& y,
        uint64_t dataIntervalNs = 0
    );

    // Public for testing

    /**
     * Remove linear trend from data
     */
    std::vector<double> detrend(const std::vector<double>& y);

    /**
     * Apply Hann window to reduce spectral leakage
     */
    std::vector<double> applyHannWindow(const std::vector<double>& y);

    /**
     * Compute periodogram (power spectrum) using DFT
     * Returns power for frequencies 0 to n/2
     */
    std::vector<double> computePeriodogram(const std::vector<double>& y);

    /**
     * Find peaks in periodogram
     * Returns indices of peaks
     */
    std::vector<size_t> findPeaks(
        const std::vector<double>& periodogram,
        double threshold
    );

    /**
     * Compute autocorrelation at given lag
     */
    double autoCorrelation(
        const std::vector<double>& y,
        size_t lag
    );

private:
    /**
     * Simple DFT implementation (sufficient for typical TSDB data sizes)
     * For very large datasets, could be replaced with FFT library
     */
    std::vector<std::complex<double>> dft(const std::vector<double>& y);
};

} // namespace forecast
} // namespace tsdb

#endif
