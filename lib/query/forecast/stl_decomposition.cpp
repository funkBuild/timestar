#include "stl_decomposition.hpp"
#include "../anomaly/simd_anomaly.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>
#include <stdexcept>
#include <limits>

#if !TSDB_ANOMALY_DISABLE_SIMD
#include <immintrin.h>
#endif

namespace tsdb {
namespace forecast {

// ============================================================================
// Local SIMD helpers (mirrors hsum_avx from simd_anomaly.cpp which is static)
// ============================================================================

#if !TSDB_ANOMALY_DISABLE_SIMD
static inline double hsum_avx_local(__m256d v) {
    __m128d vlow  = _mm256_castpd256_pd128(v);
    __m128d vhigh = _mm256_extractf128_pd(v, 1);
    vlow = _mm_add_pd(vlow, vhigh);
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    return _mm_cvtsd_f64(_mm_add_sd(vlow, high64));
}
#endif

// ============================================================================
// Weight Functions
// ============================================================================

double STLDecomposer::tricube(double x) {
    if (x < 0.0) x = -x;
    if (x >= 1.0) return 0.0;
    double tmp = 1.0 - x * x * x;
    return tmp * tmp * tmp;
}

double STLDecomposer::bisquare(double x) {
    if (x < 0.0) x = -x;
    if (x >= 1.0) return 0.0;
    double tmp = 1.0 - x * x;
    return tmp * tmp;
}

// ============================================================================
// LOESS Implementation
// ============================================================================

double STLDecomposer::weightedLocalRegression(
    const std::vector<double>& x,
    const std::vector<double>& y,
    double xeval,
    double span,
    const std::vector<double>& weights
) {
    size_t n = x.size();
    if (n == 0) return 0.0;

    // Determine window size
    size_t window = static_cast<size_t>(std::ceil(span * n));
    if (window < 2) window = 2;
    if (window > n) window = n;

    // Find distances and sort to get nearest neighbors
    std::vector<std::pair<double, size_t>> distances;
    distances.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        double dist = std::abs(x[i] - xeval);
        distances.emplace_back(dist, i);
    }
    std::partial_sort(distances.begin(), distances.begin() + window, distances.end());

    // Maximum distance for tricube weights.
    // When maxDist is at or below floating-point noise (< 1e-10), all
    // neighborhood points are effectively at the same distance from xeval —
    // either because x-values are exactly identical (duplicate timestamps) or
    // because they differ only at sub-epsilon precision.  In that situation
    // dividing by maxDist would magnify noise into the [0,1] tricube domain,
    // biasing the weighted regression and collapsing the WLS denominator to
    // ~1e-28.  The correct behaviour is uniform weights (every neighbor is
    // equally close), which is equivalent to a simple weighted mean.
    static constexpr double kDistEpsilon = 1e-10;
    double maxDist = distances[window - 1].first;
    const bool uniformWeights = (maxDist <= kDistEpsilon);
    if (uniformWeights) maxDist = 1.0;  // avoid division; tricube ratio unused below

    // Compute weights and weighted sums for linear regression
    double sumW = 0.0;
    double sumWX = 0.0;
    double sumWY = 0.0;
    double sumWXX = 0.0;
    double sumWXY = 0.0;

    for (size_t i = 0; i < window; ++i) {
        size_t idx = distances[i].second;
        double dist = distances[i].first;

        // Tricube weight based on distance; skip the computation when all
        // neighborhood distances are within epsilon of zero (uniform case).
        double w = uniformWeights ? 1.0 : tricube(dist / maxDist);

        // Apply additional user weights if provided
        if (!weights.empty() && weights[idx] > 0.0) {
            w *= weights[idx];
        }

        if (w > 0.0) {
            double xi = x[idx] - xeval;  // Center around evaluation point
            double yi = y[idx];

            sumW += w;
            sumWX += w * xi;
            sumWY += w * yi;
            sumWXX += w * xi * xi;
            sumWXY += w * xi * yi;
        }
    }

    if (sumW <= 0.0) {
        // Fallback: return nearest point
        return y[distances[0].second];
    }

    // Solve weighted least squares: y = a + b*x
    double denom = sumW * sumWXX - sumWX * sumWX;
    if (std::abs(denom) < 1e-10) {
        // Degenerate case: return weighted mean
        return sumWY / sumW;
    }

    double a = (sumWXX * sumWY - sumWX * sumWXY) / denom;
    // double b = (sumW * sumWXY - sumWX * sumWY) / denom;

    // Since x is centered at xeval, the prediction at xeval is just the intercept
    return a;
}

std::vector<double> STLDecomposer::loess(
    const std::vector<double>& x,
    const std::vector<double>& y,
    const std::vector<double>& xout,
    double span,
    const std::vector<double>& weights
) {
    if (x.size() != y.size()) {
        throw std::invalid_argument("x and y must have the same length");
    }
    if (x.empty()) {
        throw std::invalid_argument("x and y cannot be empty");
    }
    if (span <= 0.0 || span > 1.0) {
        throw std::invalid_argument("span must be in (0, 1]");
    }
    if (!weights.empty() && weights.size() != x.size()) {
        throw std::invalid_argument("weights must match x and y length");
    }

    std::vector<double> result;
    result.reserve(xout.size());

    for (double xeval : xout) {
        result.push_back(weightedLocalRegression(x, y, xeval, span, weights));
    }

    return result;
}

// ============================================================================
// Fast LOESS for Evenly-Spaced Data
// ============================================================================

std::vector<double> STLDecomposer::loessEvenlySpaced(
    const std::vector<double>& y,
    double span,
    const std::vector<double>& weights
) {
    const size_t n = y.size();
    if (n == 0) return {};
    if (n == 1) return y;

    if (span <= 0.0) span = 0.01;
    if (span > 1.0) span = 1.0;

    size_t window = static_cast<size_t>(std::ceil(span * n));
    if (window < 2) window = 2;
    if (window > n) window = n;

    const size_t half = window / 2;
    const bool hasWeights = !weights.empty();
    std::vector<double> result(n);

#if !TSDB_ANOMALY_DISABLE_SIMD
    if (anomaly::simd::isAvx2Available() && window >= 8) {
        // ================================================================
        // AVX2 SIMD path
        // ================================================================
        // Pre-allocate scratch buffers once, reuse across all evaluation points.
        // These hold the per-window tricube weights (optionally multiplied by
        // user weights) and the centered x-coordinates for each window position.
        std::vector<double> wBuf(window);   // combined weights
        std::vector<double> xBuf(window);   // centered x-coordinates

        for (size_t i = 0; i < n; ++i) {
            // ---- sliding window bounds (identical to scalar) ----
            size_t start, end;
            if (i < half) {
                start = 0;
                end = std::min(window, n);
            } else if (i + half >= n) {
                end = n;
                start = (n >= window) ? n - window : 0;
            } else {
                start = i - half;
                end = start + window;
                if (end > n) end = n;
            }

            const size_t wLen = end - start;

            double maxDist = static_cast<double>(
                std::max(i - start, end - 1 - i));
            if (maxDist <= 0.0) maxDist = 1.0;
            const double invMaxDist = 1.0 / maxDist;

            // ---- Step 1: compute tricube weights using AVX2 ----
            // We compute abs(j - i) / maxDist as the normalized distance,
            // then apply the tricube kernel: (1 - u^3)^3 where u < 1, else 0.
            // This also builds the centered x-coordinates xBuf[k] = j - i.
            {
                const __m256d vOne        = _mm256_set1_pd(1.0);
                const __m256d vZero       = _mm256_setzero_pd();
                const __m256d vInvMaxDist = _mm256_set1_pd(invMaxDist);
                const __m256d vSignMask   = _mm256_set1_pd(-0.0); // for abs()

                // Base x value for position start relative to center i
                double baseX = static_cast<double>(start) - static_cast<double>(i);
                __m256d vBaseX = _mm256_set_pd(baseX + 3.0, baseX + 2.0,
                                               baseX + 1.0, baseX);
                const __m256d vFour = _mm256_set1_pd(4.0);

                size_t simdEnd = wLen - (wLen % 4);
                for (size_t k = 0; k < simdEnd; k += 4) {
                    // Store centered x-coordinates
                    _mm256_storeu_pd(&xBuf[k], vBaseX);

                    // Normalized distance: |x| * invMaxDist
                    __m256d vAbsX = _mm256_andnot_pd(vSignMask, vBaseX);
                    __m256d u = _mm256_mul_pd(vAbsX, vInvMaxDist);

                    // Tricube: (1 - u^3)^3, zeroed where u >= 1
                    __m256d u2 = _mm256_mul_pd(u, u);
                    __m256d u3 = _mm256_mul_pd(u2, u);
                    __m256d t  = _mm256_sub_pd(vOne, u3);
                    __m256d t2 = _mm256_mul_pd(t, t);
                    __m256d t3 = _mm256_mul_pd(t2, t);
                    __m256d mask = _mm256_cmp_pd(u, vOne, _CMP_LT_OQ);
                    __m256d w  = _mm256_blendv_pd(vZero, t3, mask);

                    _mm256_storeu_pd(&wBuf[k], w);
                    vBaseX = _mm256_add_pd(vBaseX, vFour);
                }
                // Scalar remainder for tricube + x
                for (size_t k = simdEnd; k < wLen; ++k) {
                    double xj = static_cast<double>(start + k) - static_cast<double>(i);
                    xBuf[k] = xj;
                    double u = std::abs(xj) * invMaxDist;
                    if (u >= 1.0) {
                        wBuf[k] = 0.0;
                    } else {
                        double tmp = 1.0 - u * u * u;
                        wBuf[k] = tmp * tmp * tmp;
                    }
                }
            }

            // ---- Step 2: multiply by user weights (if present) ----
            if (hasWeights) {
                const double* uw = weights.data() + start;
                size_t simdEnd = wLen - (wLen % 4);
                for (size_t k = 0; k < simdEnd; k += 4) {
                    __m256d vw  = _mm256_loadu_pd(&wBuf[k]);
                    __m256d vuw = _mm256_loadu_pd(&uw[k]);
                    _mm256_storeu_pd(&wBuf[k], _mm256_mul_pd(vw, vuw));
                }
                for (size_t k = simdEnd; k < wLen; ++k) {
                    wBuf[k] *= uw[k];
                }
            }

            // ---- Step 3: accumulate 5 regression sums using AVX2 FMA ----
            const double* yp = y.data() + start;
            __m256d vSumW   = _mm256_setzero_pd();
            __m256d vSumWX  = _mm256_setzero_pd();
            __m256d vSumWY  = _mm256_setzero_pd();
            __m256d vSumWXX = _mm256_setzero_pd();
            __m256d vSumWXY = _mm256_setzero_pd();

            size_t simdEnd = wLen - (wLen % 4);
            for (size_t k = 0; k < simdEnd; k += 4) {
                __m256d vw = _mm256_loadu_pd(&wBuf[k]);
                __m256d vx = _mm256_loadu_pd(&xBuf[k]);
                __m256d vy = _mm256_loadu_pd(&yp[k]);

                // sumW += w
                vSumW = _mm256_add_pd(vSumW, vw);
                // wx = w * x
                __m256d vwx = _mm256_mul_pd(vw, vx);
                // sumWX += w * x
                vSumWX = _mm256_add_pd(vSumWX, vwx);
                // sumWY += w * y  (FMA: sumWY = sumWY + w * y)
                vSumWY = _mm256_fmadd_pd(vw, vy, vSumWY);
                // sumWXX += wx * x  (FMA: sumWXX = sumWXX + wx * x)
                vSumWXX = _mm256_fmadd_pd(vwx, vx, vSumWXX);
                // sumWXY += wx * y  (FMA: sumWXY = sumWXY + wx * y)
                vSumWXY = _mm256_fmadd_pd(vwx, vy, vSumWXY);
            }

            double sumW   = hsum_avx_local(vSumW);
            double sumWX  = hsum_avx_local(vSumWX);
            double sumWY  = hsum_avx_local(vSumWY);
            double sumWXX = hsum_avx_local(vSumWXX);
            double sumWXY = hsum_avx_local(vSumWXY);

            // Scalar remainder
            for (size_t k = simdEnd; k < wLen; ++k) {
                double w  = wBuf[k];
                double xj = xBuf[k];
                double yj = yp[k];
                sumW   += w;
                sumWX  += w * xj;
                sumWY  += w * yj;
                sumWXX += w * xj * xj;
                sumWXY += w * xj * yj;
            }

            // ---- Step 4: solve weighted least squares ----
            if (sumW <= 0.0) {
                result[i] = y[i];
                continue;
            }

            double denom = sumW * sumWXX - sumWX * sumWX;
            if (std::abs(denom) < 1e-10) {
                result[i] = sumWY / sumW;
            } else {
                result[i] = (sumWXX * sumWY - sumWX * sumWXY) / denom;
            }
        }

        return result;
    }
#endif // !TSDB_ANOMALY_DISABLE_SIMD

    // ================================================================
    // Scalar fallback (original implementation)
    // ================================================================
    for (size_t i = 0; i < n; ++i) {
        // Sliding window centered on i
        size_t start, end;
        if (i < half) {
            start = 0;
            end = std::min(window, n);
        } else if (i + half >= n) {
            end = n;
            start = (n >= window) ? n - window : 0;
        } else {
            start = i - half;
            end = start + window;
            if (end > n) end = n;
        }

        // maxDist is the largest distance from i to any point in [start, end)
        double maxDist = static_cast<double>(
            std::max(i - start, end - 1 - i));
        if (maxDist <= 0.0) maxDist = 1.0;

        // Weighted linear regression centered at i
        double sumW = 0.0, sumWX = 0.0, sumWY = 0.0;
        double sumWXX = 0.0, sumWXY = 0.0;

        for (size_t j = start; j < end; ++j) {
            double dist = (j >= i) ? static_cast<double>(j - i)
                                   : static_cast<double>(i - j);
            double w = tricube(dist / maxDist);

            if (hasWeights && weights[j] > 0.0) {
                w *= weights[j];
            }

            if (w > 0.0) {
                double xj = static_cast<double>(j) - static_cast<double>(i);
                sumW += w;
                sumWX += w * xj;
                sumWY += w * y[j];
                sumWXX += w * xj * xj;
                sumWXY += w * xj * y[j];
            }
        }

        if (sumW <= 0.0) {
            result[i] = y[i];
            continue;
        }

        double denom = sumW * sumWXX - sumWX * sumWX;
        if (std::abs(denom) < 1e-10) {
            result[i] = sumWY / sumW;
        } else {
            // Intercept of locally weighted regression centered at i
            result[i] = (sumWXX * sumWY - sumWX * sumWXY) / denom;
        }
    }

    return result;
}

// ============================================================================
// Cycle-Subseries Smoothing
// ============================================================================

std::vector<double> STLDecomposer::smoothCycleSubseries(
    const std::vector<double>& y,
    size_t period,
    size_t window
) {
    if (y.empty() || period == 0) {
        return std::vector<double>(y.size(), 0.0);
    }

    size_t n = y.size();
    std::vector<double> result(n);

    // Pre-allocate subseries buffer (max subseries length = ceil(n/period))
    size_t maxSubLen = (n + period - 1) / period;
    std::vector<double> subseries;
    subseries.reserve(maxSubLen);

    // Process each position within the cycle
    for (size_t phase = 0; phase < period; ++phase) {
        // Extract subseries for this phase (no indices needed — arithmetic)
        subseries.clear();
        for (size_t i = phase; i < n; i += period) {
            subseries.push_back(y[i]);
        }

        if (subseries.empty()) continue;

        // Smooth this subseries using fast evenly-spaced LOESS
        double span = static_cast<double>(window) / static_cast<double>(subseries.size());
        if (span > 1.0) span = 1.0;
        if (span < 0.1) span = 0.1;

        auto smoothed = loessEvenlySpaced(subseries, span);

        // Place smoothed values back using arithmetic indices
        for (size_t i = 0, idx = phase; idx < n; ++i, idx += period) {
            result[idx] = smoothed[i];
        }
    }

    return result;
}

// ============================================================================
// Low-Pass Filter
// ============================================================================

std::vector<double> STLDecomposer::lowPassFilter(
    const std::vector<double>& y,
    size_t period
) {
    if (y.empty()) return {};

    size_t n = y.size();

    // O(n) sliding-window moving average (replaces O(n*window) naive version)
    auto movingAverage = [](const std::vector<double>& data, size_t window) -> std::vector<double> {
        if (data.empty() || window == 0) return data;

        size_t n = data.size();
        std::vector<double> result(n);
        size_t half = window / 2;

        // Build initial window for position 0
        size_t end0 = std::min(half + 1, n);
        double sum = 0.0;
        for (size_t j = 0; j < end0; ++j) sum += data[j];
        size_t curStart = 0, curEnd = end0;
        result[0] = sum / static_cast<double>(curEnd - curStart);

        for (size_t i = 1; i < n; ++i) {
            // Expand right edge if possible
            size_t newEnd = std::min(i + half + 1, n);
            while (curEnd < newEnd) {
                sum += data[curEnd];
                ++curEnd;
            }
            // Shrink left edge if necessary
            size_t newStart = (i >= half) ? (i - half) : 0;
            while (curStart < newStart) {
                sum -= data[curStart];
                ++curStart;
            }
            result[i] = sum / static_cast<double>(curEnd - curStart);
        }

        return result;
    };

    // Apply three moving averages as per STL algorithm
    std::vector<double> filtered = movingAverage(y, period);
    filtered = movingAverage(filtered, period);
    filtered = movingAverage(filtered, 3);

    return filtered;
}

// ============================================================================
// Robustness Weights
// ============================================================================

std::vector<double> STLDecomposer::computeRobustnessWeights(
    const std::vector<double>& residuals
) {
    if (residuals.empty()) return {};

    size_t n = residuals.size();

    // Compute median absolute deviation (MAD) using O(n) nth_element
    // Use a copy for median-finding (nth_element rearranges), keep original for weights
    std::vector<double> absResiduals(n);

#if !TSDB_ANOMALY_DISABLE_SIMD
    if (anomaly::simd::isAvx2Available() && n >= 8) {
        // ================================================================
        // AVX2 SIMD path
        // ================================================================

        // ---- Step 1: compute absolute residuals using branchless SIMD abs ----
        {
            const __m256d vSignMask = _mm256_set1_pd(-0.0); // sign bit mask
            const double* src = residuals.data();
            double* dst = absResiduals.data();
            size_t simdEnd = n - (n % 4);

            for (size_t i = 0; i < simdEnd; i += 4) {
                __m256d v = _mm256_loadu_pd(&src[i]);
                __m256d vAbs = _mm256_andnot_pd(vSignMask, v); // clear sign bits
                _mm256_storeu_pd(&dst[i], vAbs);
            }
            // Scalar remainder
            for (size_t i = simdEnd; i < n; ++i) {
                dst[i] = std::abs(src[i]);
            }
        }

        // ---- Step 2: find median via nth_element on a copy ----
        std::vector<double> absForMedian = absResiduals;
        size_t medIdx = n / 2;
        std::nth_element(absForMedian.begin(), absForMedian.begin() + medIdx, absForMedian.end());
        double mad;
        if (n % 2 == 0 && medIdx > 0) {
            double upper = absForMedian[medIdx];
            double lower = *std::max_element(absForMedian.begin(), absForMedian.begin() + medIdx);
            mad = (lower + upper) / 2.0;
        } else {
            mad = absForMedian[medIdx];
        }

        double scale = 6.0 * mad;

        // When scale is near zero, all residuals are negligibly small — there
        // are no outliers and every point should be treated equally.  Return
        // uniform weights of 1.0 rather than running bisquare with a
        // near-zero denominator (which would produce numerically unstable
        // or arbitrary results).
        if (scale < 1e-10) {
            return std::vector<double>(n, 1.0);
        }

        // ---- Step 3: compute bisquare weights using branchless SIMD ----
        // bisquare(u) = (1 - u^2)^2 where u < 1, else 0
        std::vector<double> weights(n);
        {
            const __m256d vOne  = _mm256_set1_pd(1.0);
            const __m256d vZero = _mm256_setzero_pd();
            const __m256d vInvScale = _mm256_set1_pd(1.0 / scale);
            const double* absPtr = absResiduals.data();
            double* wPtr = weights.data();
            size_t simdEnd = n - (n % 4);

            for (size_t i = 0; i < simdEnd; i += 4) {
                // u = absResiduals[i] / scale
                __m256d vAbs = _mm256_loadu_pd(&absPtr[i]);
                __m256d u = _mm256_mul_pd(vAbs, vInvScale);

                // t = 1 - u^2
                __m256d u2 = _mm256_mul_pd(u, u);
                __m256d t  = _mm256_sub_pd(vOne, u2);

                // bisquare = t^2
                __m256d bisq = _mm256_mul_pd(t, t);

                // Zero out where u >= 1.0
                __m256d mask = _mm256_cmp_pd(u, vOne, _CMP_LT_OQ);
                __m256d result = _mm256_blendv_pd(vZero, bisq, mask);

                _mm256_storeu_pd(&wPtr[i], result);
            }
            // Scalar remainder
            for (size_t i = simdEnd; i < n; ++i) {
                double u = absPtr[i] / scale;
                if (u >= 1.0) {
                    wPtr[i] = 0.0;
                } else {
                    double t = 1.0 - u * u;
                    wPtr[i] = t * t;
                }
            }
        }

        return weights;
    }
#endif // !TSDB_ANOMALY_DISABLE_SIMD

    // ================================================================
    // Scalar fallback
    // ================================================================
    for (size_t i = 0; i < n; ++i) {
        absResiduals[i] = std::abs(residuals[i]);
    }

    std::vector<double> absForMedian = absResiduals;
    size_t medIdx = n / 2;
    std::nth_element(absForMedian.begin(), absForMedian.begin() + medIdx, absForMedian.end());
    double mad;
    if (n % 2 == 0 && medIdx > 0) {
        // Even n: average of two middle elements for correct median
        double upper = absForMedian[medIdx];
        double lower = *std::max_element(absForMedian.begin(), absForMedian.begin() + medIdx);
        mad = (lower + upper) / 2.0;
    } else {
        mad = absForMedian[medIdx];
    }

    // Scale factor for MAD to estimate standard deviation.
    // When scale is near zero, all residuals are negligibly small — there
    // are no outliers and every point should be treated equally.  Return
    // uniform weights of 1.0 rather than running bisquare with a
    // near-zero denominator (which would produce numerically unstable
    // or arbitrary results).
    double scale = 6.0 * mad;
    if (scale < 1e-10) {
        return std::vector<double>(n, 1.0);
    }

    // Compute bisquare weights (using original absResiduals with correct indices)
    std::vector<double> weights(n);
    for (size_t i = 0; i < n; ++i) {
        weights[i] = bisquare(absResiduals[i] / scale);
    }

    return weights;
}

// ============================================================================
// Main STL Decomposition
// ============================================================================

STLResult STLDecomposer::decompose(
    const std::vector<double>& y,
    size_t period,
    size_t seasonalWindow,
    size_t trendWindow,
    bool robust,
    size_t outerIterations,
    size_t innerIterations
) {
    STLResult result;
    result.period = period;
    result.success = false;

    // Validation
    if (y.empty()) {
        return result;
    }

    size_t n = y.size();
    if (n < 2 * period) {
        // Need at least 2 full cycles
        return result;
    }

    if (seasonalWindow < 7 || seasonalWindow % 2 == 0) {
        seasonalWindow = 7;  // Must be odd and >= 7
    }

    // Auto-determine trend window if not specified
    if (trendWindow == 0) {
        double numerator = 1.5 * period;
        double denominator = 1.0 - 1.5 / static_cast<double>(seasonalWindow);
        trendWindow = static_cast<size_t>(std::ceil(numerator / denominator));
        if (trendWindow % 2 == 0) trendWindow++;  // Ensure odd
        if (trendWindow < 3) trendWindow = 3;
    }

    // Ensure trend window is odd
    if (trendWindow % 2 == 0) trendWindow++;

    // Initialize components
    std::vector<double> trend(n, 0.0);
    std::vector<double> seasonal(n, 0.0);
    std::vector<double> residual(n, 0.0);
    std::vector<double> robustnessWeights(n, 1.0);

    if (!robust) {
        outerIterations = 1;
    }

    // Main STL loop
    for (size_t outer = 0; outer < outerIterations; ++outer) {
        for (size_t inner = 0; inner < innerIterations; ++inner) {
            // Step 1: Detrend (SIMD-accelerated)
            std::vector<double> detrended(n);
            anomaly::simd::vectorSubtract(y.data(), trend.data(), detrended.data(), n);

            // Step 2: Cycle-subseries smoothing
            std::vector<double> rawSeasonal = smoothCycleSubseries(detrended, period, seasonalWindow);

            // Step 3: Low-pass filter to get smooth seasonal
            std::vector<double> smoothSeasonal = lowPassFilter(rawSeasonal, period);

            // Step 4: Remove low-frequency from seasonal (SIMD-accelerated)
            anomaly::simd::vectorSubtract(rawSeasonal.data(), smoothSeasonal.data(), seasonal.data(), n);

            // Step 5: Deseasonalize (SIMD-accelerated)
            std::vector<double> deseasonalized(n);
            anomaly::simd::vectorSubtract(y.data(), seasonal.data(), deseasonalized.data(), n);

            // Step 6: Trend smoothing via fast evenly-spaced LOESS
            double trendSpan = static_cast<double>(trendWindow) / static_cast<double>(n);
            if (trendSpan > 1.0) trendSpan = 1.0;

            trend = loessEvenlySpaced(deseasonalized, trendSpan, robustnessWeights);
        }

        // Compute residuals and update robustness weights (SIMD-accelerated)
        anomaly::simd::vectorSubtract(y.data(), trend.data(), residual.data(), n);
        anomaly::simd::vectorSubtract(residual.data(), seasonal.data(), residual.data(), n);

        if (robust && outer < outerIterations - 1) {
            robustnessWeights = computeRobustnessWeights(residual);
        }
    }

    // Final residual calculation (SIMD-accelerated)
    anomaly::simd::vectorSubtract(y.data(), trend.data(), residual.data(), n);
    anomaly::simd::vectorSubtract(residual.data(), seasonal.data(), residual.data(), n);

    result.trend = std::move(trend);
    result.seasonal = std::move(seasonal);
    result.residual = std::move(residual);
    result.success = true;

    return result;
}

// ============================================================================
// MSTL Decomposition (Multiple Seasonalities)
// ============================================================================

MSTLResult STLDecomposer::decomposeMultiple(
    const std::vector<double>& y,
    std::vector<size_t> periods
) {
    MSTLResult result;
    result.success = false;

    if (y.empty() || periods.empty()) {
        return result;
    }

    // Sort periods in ascending order
    std::sort(periods.begin(), periods.end());

    // Remove duplicates
    periods.erase(std::unique(periods.begin(), periods.end()), periods.end());

    size_t n = y.size();

    // Check that we have enough data for the largest period
    if (n < 2 * periods.back()) {
        return result;
    }

    std::vector<double> remainder = y;
    std::vector<std::vector<double>> seasonals;

    // Extract each seasonal component starting with smallest period
    for (size_t period : periods) {
        // Decompose remainder to extract this seasonal component
        STLResult stlResult = decompose(remainder, period, 7, 0, true, 2, 2);

        if (!stlResult.success) {
            return result;
        }

        // Store this seasonal component
        seasonals.push_back(stlResult.seasonal);

        // Remove this seasonal from remainder (SIMD-accelerated)
        anomaly::simd::vectorSubtract(remainder.data(), stlResult.seasonal.data(), remainder.data(), n);
    }

    // Final decomposition of remainder to get trend
    // Use the largest period for trend extraction
    STLResult finalResult = decompose(remainder, periods.back(), 7, 0, true, 2, 1);

    if (!finalResult.success) {
        return result;
    }

    result.trend = finalResult.trend;
    result.seasonals = std::move(seasonals);
    result.periods = std::move(periods);
    result.residual = finalResult.residual;
    result.success = true;

    return result;
}

} // namespace forecast
} // namespace tsdb
