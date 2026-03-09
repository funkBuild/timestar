#ifndef STL_DECOMPOSITION_H_INCLUDED
#define STL_DECOMPOSITION_H_INCLUDED

#include <vector>
#include <cstddef>

namespace timestar {
namespace forecast {

/**
 * Result of STL decomposition
 */
struct STLResult {
    std::vector<double> trend;      // Trend component
    std::vector<double> seasonal;   // Seasonal component
    std::vector<double> residual;   // Residual (remainder)
    size_t period;                  // Seasonal period used
    bool success;                   // Whether decomposition succeeded
};

/**
 * Result of MSTL decomposition (multiple seasonalities)
 */
struct MSTLResult {
    std::vector<double> trend;
    std::vector<std::vector<double>> seasonals;  // One per period
    std::vector<size_t> periods;                  // Periods used
    std::vector<double> residual;
    bool success;
};

/**
 * STL Decomposition
 *
 * Decomposes time series: Y = Trend + Seasonal + Residual
 *
 * Based on Cleveland et al. (1990) "STL: A Seasonal-Trend Decomposition
 * Procedure Based on Loess"
 *
 * NOTE: A simpler STL implementation exists at lib/query/anomaly/stl_decomposition.hpp
 * (timestar::anomaly::STLDecomposition) for anomaly detection. This version is more
 * sophisticated with AVX2-optimized LOESS, MAD-based robustness weights, and MSTL
 * (multiple seasonalities) support for forecasting.
 */
class STLDecomposer {
public:
    /**
     * Perform STL decomposition
     *
     * @param y Input time series
     * @param period Seasonal period
     * @param seasonalWindow Smoothing window for seasonal (odd, >= 7)
     * @param trendWindow Smoothing window for trend (odd, 0 = auto)
     * @param robust Use robust fitting (downweight outliers)
     * @param outerIterations Number of robustness iterations (1 if not robust)
     * @param innerIterations Number of inner loop iterations
     * @return STLResult with trend, seasonal, residual
     */
    STLResult decompose(
        const std::vector<double>& y,
        size_t period,
        size_t seasonalWindow = 7,
        size_t trendWindow = 0,
        bool robust = true,
        size_t outerIterations = 2,
        size_t innerIterations = 2
    );

    /**
     * Perform MSTL decomposition (multiple seasonalities)
     *
     * @param y Input time series
     * @param periods Seasonal periods (should be sorted ascending)
     * @return MSTLResult with trend, multiple seasonals, residual
     */
    MSTLResult decomposeMultiple(
        const std::vector<double>& y,
        std::vector<size_t> periods
    );

    // Public for testing

    /**
     * LOESS (Locally Estimated Scatterplot Smoothing)
     *
     * @param x X values
     * @param y Y values
     * @param xout Points at which to evaluate smooth
     * @param span Smoothing span (fraction of data)
     * @param weights Optional weights
     * @return Smoothed values at xout
     */
    std::vector<double> loess(
        const std::vector<double>& x,
        const std::vector<double>& y,
        const std::vector<double>& xout,
        double span,
        const std::vector<double>& weights = {}
    );

    /**
     * Cycle-subseries smoothing
     * Smooth each position within the seasonal cycle
     */
    std::vector<double> smoothCycleSubseries(
        const std::vector<double>& y,
        size_t period,
        size_t window
    );

    /**
     * Low-pass filter for seasonal component
     */
    std::vector<double> lowPassFilter(
        const std::vector<double>& y,
        size_t period
    );

    /**
     * Compute robustness weights from residuals
     * Using bisquare function
     */
    std::vector<double> computeRobustnessWeights(
        const std::vector<double>& residuals
    );

    /**
     * Fast LOESS for evenly-spaced integer x-values (0, 1, 2, ..., n-1)
     * evaluated at the same points. Uses a sliding window instead of
     * sorting distances, reducing complexity from O(n²) to O(n × window).
     *
     * @param y Y values (length n)
     * @param span Smoothing span (fraction of data)
     * @param weights Optional robustness weights (length n or empty)
     * @return Smoothed values (length n)
     */
    std::vector<double> loessEvenlySpaced(
        const std::vector<double>& y,
        double span,
        const std::vector<double>& weights = {}
    );

private:
    /**
     * Tricube weight function for LOESS
     */
    double tricube(double x);

    /**
     * Bisquare weight function for robustness
     */
    double bisquare(double x);

    /**
     * Weighted linear regression at a point
     */
    double weightedLocalRegression(
        const std::vector<double>& x,
        const std::vector<double>& y,
        double xeval,
        double span,
        const std::vector<double>& weights
    );
};

} // namespace forecast
} // namespace timestar

#endif
