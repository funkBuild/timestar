#pragma once

#include "anomaly_result.hpp"
#include "simd_anomaly.hpp"

#include <cstddef>
#include <vector>

namespace timestar {
namespace anomaly {

// Configuration for STL decomposition
struct STLConfig {
    size_t seasonalPeriod = 0;     // Number of observations per seasonal cycle (0 = auto)
    size_t seasonalWindow = 7;     // Window size for seasonal smoothing (odd number)
    size_t trendWindow = 0;        // Window size for trend smoothing (0 = auto)
    size_t lowPassWindow = 0;      // Window size for low-pass filter (0 = auto)
    bool robust = true;            // Use robust fitting (resistant to outliers)
    size_t robustIterations = 2;   // Number of robust iterations
    double robustThreshold = 6.0;  // Threshold for outlier detection (in IQR units)
};

// STL Decomposition using Seasonal-Trend decomposition with LOESS
// Based on Cleveland et al. (1990) "STL: A Seasonal-Trend Decomposition Procedure Based on LOESS"
//
// NOTE: A second STL implementation exists at lib/query/forecast/stl_decomposition.hpp
// (timestar::forecast::STLDecomposer). That version supports multiple seasonalities
// (MSTL), has AVX2-optimized LOESS, and uses MAD-based robustness weights. This
// anomaly version is simpler (IQR-based, single seasonality) and optimized for
// anomaly detection. Both implement the same core algorithm with different trade-offs.
class STLDecomposition {
public:
    // Decompose time series into seasonal, trend, and residual components
    static STLComponents decompose(const std::vector<double>& values, const STLConfig& config);

private:
    // LOESS (Locally Estimated Scatterplot Smoothing) smoother
    static std::vector<double> loess(const std::vector<double>& x, const std::vector<double>& y, size_t windowSize,
                                     const std::vector<double>& weights = {});

    // Simple moving average for trend extraction
    static std::vector<double> movingAverage(const std::vector<double>& values, size_t windowSize);

    // Compute subseries means for seasonal component
    static std::vector<double> computeSeasonalMeans(const std::vector<double>& values, size_t period);

    // Robustness weights based on residuals
    static std::vector<double> computeRobustnessWeights(const std::vector<double>& residuals, double threshold);

    // Tricube weight function for LOESS
    static double tricubeWeight(double u);

    // Median of a vector
    static double median(std::vector<double> v);

    // Interquartile range
    static double iqr(const std::vector<double>& values);
};

}  // namespace anomaly
}  // namespace timestar
