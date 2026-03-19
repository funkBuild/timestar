#pragma once

// Single-includer header: only transform_functions_test.cpp includes this file.
// Splitting into .hpp/.cpp would add complexity for no compile-time benefit.

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <numeric>
#include <stdexcept>
#include <vector>

// Include SIMD-optimized implementations
#include "transform_functions_simd.hpp"

namespace timestar {
namespace transform {

/**
 * Transform Functions
 *
 * Functions that transform time series data point-by-point or using
 * sliding windows. These are applied after query aggregation.
 */

// ============================================================================
// Rate Functions - Calculate derivatives and rates of change
// ============================================================================

/**
 * diff() - Difference between consecutive points
 * Returns: diff[i] = value[i] - value[i-1], diff[0] = NaN
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> diff(const std::vector<double>& values) {
    return simd::diff(values);
}

/**
 * dt() - Time delta between consecutive points in seconds
 * Returns: dt[i] = (time[i] - time[i-1]) / 1e9, dt[0] = NaN
 * Note: Returns NaN for non-monotonic timestamps (where time[i] <= time[i-1])
 */
inline std::vector<double> dt(const std::vector<uint64_t>& timestamps) {
    if (timestamps.size() < 2) {
        return std::vector<double>(timestamps.size(), std::nan(""));
    }

    std::vector<double> result(timestamps.size());
    result[0] = std::nan("");

    for (size_t i = 1; i < timestamps.size(); ++i) {
        // Check for non-monotonic timestamps to prevent unsigned underflow
        if (timestamps[i] <= timestamps[i - 1]) {
            result[i] = std::nan("");
        } else {
            // Convert nanoseconds to seconds
            result[i] = static_cast<double>(timestamps[i] - timestamps[i - 1]) / 1e9;
        }
    }

    return result;
}

/**
 * derivative() - First derivative (rate of change per second)
 * Returns: derivative[i] = diff[i] / dt[i]
 * Note: Returns NaN for non-monotonic timestamps
 */
inline std::vector<double> derivative(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    if (values.size() < 2 || values.size() != timestamps.size()) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    std::vector<double> result(values.size());
    result[0] = std::nan("");

    for (size_t i = 1; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else if (timestamps[i] <= timestamps[i - 1]) {
            // Non-monotonic timestamps - prevent unsigned underflow
            result[i] = std::nan("");
        } else {
            double dv = values[i] - values[i - 1];
            double dtSec = static_cast<double>(timestamps[i] - timestamps[i - 1]) / 1e9;
            result[i] = dv / dtSec;
        }
    }

    return result;
}

/**
 * rate() - Like derivative but skips non-monotonic increases (for counters)
 * Returns: rate[i] = max(0, diff[i]) / dt[i], handles counter resets
 * Note: Returns NaN for non-monotonic timestamps
 * Note: Counter resets (negative diffs) are treated as rate = 0
 */
inline std::vector<double> rate(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    if (values.size() < 2 || values.size() != timestamps.size()) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    std::vector<double> result(values.size());
    result[0] = std::nan("");

    for (size_t i = 1; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else if (timestamps[i] <= timestamps[i - 1]) {
            // Non-monotonic timestamps - prevent unsigned underflow
            result[i] = std::nan("");
        } else {
            double dv = values[i] - values[i - 1];
            double dtSec = static_cast<double>(timestamps[i] - timestamps[i - 1]) / 1e9;

            if (dv < 0) {
                // Counter reset - treat as 0 rate
                result[i] = 0.0;
            } else {
                result[i] = dv / dtSec;
            }
        }
    }

    return result;
}

/**
 * per_second() - Alias for rate()
 */
inline std::vector<double> per_second(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    return rate(values, timestamps);
}

/**
 * per_minute() - Rate multiplied by 60
 * Uses SIMD optimization for the multiplication when available
 */
inline std::vector<double> per_minute(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    auto r = rate(values, timestamps);
    simd::multiply_inplace(r, 60.0);
    return r;
}

/**
 * per_hour() - Rate multiplied by 3600
 * Uses SIMD optimization for the multiplication when available
 */
inline std::vector<double> per_hour(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    auto r = rate(values, timestamps);
    simd::multiply_inplace(r, 3600.0);
    return r;
}

/**
 * monotonic_diff() - Difference that handles counter resets (resets to 0)
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> monotonic_diff(const std::vector<double>& values) {
    return simd::monotonic_diff(values);
}

// ============================================================================
// Arithmetic Functions
// ============================================================================

/**
 * abs() - Absolute value of each point
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> abs(const std::vector<double>& values) {
    return simd::abs(values);
}

/**
 * log2() - Base-2 logarithm
 */
inline std::vector<double> log2(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] > 0) ? std::log2(values[i]) : std::nan("");
    }
    return result;
}

/**
 * log10() - Base-10 logarithm
 */
inline std::vector<double> log10(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] > 0) ? std::log10(values[i]) : std::nan("");
    }
    return result;
}

/**
 * cumsum() - Cumulative sum
 */
inline std::vector<double> cumsum(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    double sum = 0.0;

    for (size_t i = 0; i < values.size(); ++i) {
        if (std::isnan(values[i])) {
            result[i] = sum;  // Skip NaN in accumulation
        } else {
            sum += values[i];
            result[i] = sum;
        }
    }

    return result;
}

/**
 * integral() - Integral using trapezoidal rule (area under curve)
 * Time is in seconds
 * Note: Non-monotonic timestamps are skipped (area not accumulated for those intervals)
 */
inline std::vector<double> integral(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    if (values.size() != timestamps.size()) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    if (values.empty()) return {};

    std::vector<double> result(values.size());
    double area = 0.0;
    result[0] = 0.0;

    for (size_t i = 1; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = area;  // Keep previous area
        } else if (timestamps[i] <= timestamps[i - 1]) {
            // Non-monotonic timestamps - skip this interval
            result[i] = area;
        } else {
            double dtSec = static_cast<double>(timestamps[i] - timestamps[i - 1]) / 1e9;
            // Trapezoidal rule: area += (v1 + v2) / 2 * dt
            area += (values[i - 1] + values[i]) / 2.0 * dtSec;
            result[i] = area;
        }
    }

    return result;
}

// ============================================================================
// Smoothing Functions
// ============================================================================

/**
 * ewma() - Exponentially Weighted Moving Average
 *
 * @param values Input values
 * @param span Number of periods for the decay (alpha = 2 / (span + 1))
 *
 * EWMA formula: ewma[i] = alpha * value[i] + (1-alpha) * ewma[i-1]
 */
inline std::vector<double> ewma(const std::vector<double>& values, int span) {
    if (values.empty() || span < 1) {
        return values;
    }

    double alpha = 2.0 / (span + 1.0);
    std::vector<double> result(values.size());

    // Find first non-NaN value to initialize
    size_t startIdx = 0;
    while (startIdx < values.size() && std::isnan(values[startIdx])) {
        result[startIdx] = std::nan("");
        startIdx++;
    }

    if (startIdx >= values.size()) {
        return result;
    }

    result[startIdx] = values[startIdx];  // Initialize with first value

    for (size_t i = startIdx + 1; i < values.size(); ++i) {
        if (std::isnan(values[i])) {
            result[i] = result[i - 1];  // Carry forward
        } else {
            result[i] = alpha * values[i] + (1 - alpha) * result[i - 1];
        }
    }

    return result;
}

/**
 * median() - Median filter with sliding window
 *
 * @param values Input values
 * @param window Window size (should be odd for symmetry)
 */
inline std::vector<double> median(const std::vector<double>& values, int window) {
    if (values.empty() || window < 1) {
        return values;
    }

    // Ensure odd window for symmetry
    if (window % 2 == 0) {
        window++;
    }

    std::vector<double> result(values.size());
    int halfWindow = window / 2;

    for (size_t i = 0; i < values.size(); ++i) {
        // Collect values in window
        std::vector<double> windowValues;
        windowValues.reserve(window);

        int start = static_cast<int>(i) - halfWindow;
        int end = static_cast<int>(i) + halfWindow;

        for (int j = start; j <= end; ++j) {
            if (j >= 0 && j < static_cast<int>(values.size())) {
                if (!std::isnan(values[j])) {
                    windowValues.push_back(values[j]);
                }
            }
        }

        if (windowValues.empty()) {
            result[i] = std::nan("");
        } else {
            // Find median
            size_t n = windowValues.size();
            size_t mid = n / 2;

            if (n % 2 == 1) {
                // Odd: single middle element
                std::nth_element(windowValues.begin(), windowValues.begin() + mid, windowValues.end());
                result[i] = windowValues[mid];
            } else {
                // Even: average of two middle elements
                // nth_element partitions so elements before mid are all <= windowValues[mid]
                std::nth_element(windowValues.begin(), windowValues.begin() + mid, windowValues.end());
                double upperMid = windowValues[mid];

                // Find the maximum of elements before mid (which is the (mid-1)th element)
                double lowerMid = *std::max_element(windowValues.begin(), windowValues.begin() + mid);

                result[i] = (lowerMid + upperMid) / 2.0;
            }
        }
    }

    return result;
}

/**
 * autosmooth() - Automatically selects smoothing span based on data characteristics
 * Uses a heuristic: span = max(3, min(sqrt(n), 20))
 */
inline std::vector<double> autosmooth(const std::vector<double>& values) {
    if (values.empty()) {
        return values;
    }

    // Heuristic: span based on data length
    int span = static_cast<int>(std::sqrt(values.size()));
    span = std::max(3, std::min(span, 20));

    return ewma(values, span);
}

// ============================================================================
// Exclusion Functions
// ============================================================================

/**
 * clamp_min() - Clamp values to minimum (values below become min)
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> clamp_min(const std::vector<double>& values, double minVal) {
    return simd::clamp_min(values, minVal);
}

/**
 * clamp_max() - Clamp values to maximum (values above become max)
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> clamp_max(const std::vector<double>& values, double maxVal) {
    return simd::clamp_max(values, maxVal);
}

/**
 * cutoff_min() - Remove values below threshold (set to NaN)
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> cutoff_min(const std::vector<double>& values, double threshold) {
    return simd::cutoff_min(values, threshold);
}

/**
 * cutoff_max() - Remove values above threshold (set to NaN)
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> cutoff_max(const std::vector<double>& values, double threshold) {
    return simd::cutoff_max(values, threshold);
}

// ============================================================================
// Interpolation Functions
// ============================================================================

/**
 * default_zero() - Replace NaN values with zero
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> default_zero(const std::vector<double>& values) {
    return simd::default_zero(values);
}

/**
 * fill() - Fill missing values using specified method
 * Methods: "zero", "null", "linear", "last"
 */
inline std::vector<double> fill(const std::vector<double>& values, const std::string& method) {
    if (method == "zero") {
        return default_zero(values);
    }

    if (method == "null") {
        return values;  // No change
    }

    if (method == "last") {
        std::vector<double> result(values.size());
        double lastValid = std::nan("");

        for (size_t i = 0; i < values.size(); ++i) {
            if (std::isnan(values[i])) {
                result[i] = lastValid;
            } else {
                result[i] = values[i];
                lastValid = values[i];
            }
        }
        return result;
    }

    if (method == "linear") {
        std::vector<double> result = values;

        // Find gaps and interpolate
        for (size_t i = 0; i < result.size(); ++i) {
            if (std::isnan(result[i])) {
                // Find previous and next valid values
                size_t prevIdx = i;
                size_t nextIdx = i;

                // Find previous valid
                if (i > 0) {
                    for (size_t j = i - 1;; --j) {
                        if (!std::isnan(result[j])) {
                            prevIdx = j;
                            break;
                        }
                        if (j == 0)
                            break;
                    }
                }

                // Find next valid
                for (size_t j = i + 1; j < result.size(); ++j) {
                    if (!std::isnan(result[j])) {
                        nextIdx = j;
                        break;
                    }
                }

                // Interpolate if we have both bounds
                if (prevIdx < i && nextIdx > i) {
                    double prev = result[prevIdx];
                    double next = result[nextIdx];
                    double t = static_cast<double>(i - prevIdx) / (nextIdx - prevIdx);
                    result[i] = prev + t * (next - prev);
                } else if (prevIdx < i) {
                    result[i] = result[prevIdx];  // Forward fill
                } else if (nextIdx > i) {
                    result[i] = result[nextIdx];  // Back fill
                }
            }
        }
        return result;
    }

    throw std::invalid_argument("Unknown fill method: " + method);
}

// ============================================================================
// Count Functions
// ============================================================================

/**
 * count_nonzero() - Count of non-zero values at each point
 * Returns 1 if value is non-zero, 0 otherwise
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> count_nonzero(const std::vector<double>& values) {
    return simd::count_nonzero(values);
}

/**
 * count_not_null() - Count of non-null values at each point
 * Returns 1 if value is not NaN, 0 otherwise
 * Uses SIMD optimization when available for large arrays
 */
inline std::vector<double> count_not_null(const std::vector<double>& values) {
    return simd::count_not_null(values);
}

// ============================================================================
// Rollup Functions
// ============================================================================

/**
 * moving_rollup() - Rolling window aggregation
 *
 * @param values Input values
 * @param timestamps Input timestamps
 * @param windowSeconds Window size in seconds
 * @param method Aggregation method: "avg", "sum", "min", "max", "count"
 *
 * Uses a sliding window with a monotonically advancing start index for O(n)
 * amortized complexity instead of O(n^2). Since timestamps are sorted, the
 * window start can only move forward as i advances.
 */
inline std::vector<double> moving_rollup(const std::vector<double>& values, const std::vector<uint64_t>& timestamps,
                                         double windowSeconds, const std::string& method) {
    if (values.empty() || values.size() != timestamps.size()) {
        return values;
    }

    uint64_t windowNs = static_cast<uint64_t>(windowSeconds * 1e9);
    std::vector<double> result(values.size());

    // Validate method upfront to avoid checking per-iteration
    if (method != "avg" && method != "sum" && method != "min" && method != "max" && method != "count") {
        throw std::invalid_argument("Unknown rollup method: " + method);
    }

    // Method-specific O(n) amortized implementations.
    // The start pointer advances monotonically since timestamps are sorted,
    // so total work across all iterations is O(n) for each method.
    const size_t n = values.size();
    size_t start = 0;

    if (method == "avg" || method == "sum") {
        // Incremental sum: add entering values, subtract leaving values.
        // NaN values are skipped (not added to runningSum, not counted).
        double runningSum = 0.0;
        size_t validCount = 0;

        for (size_t i = 0; i < n; ++i) {
            uint64_t windowStart = (timestamps[i] > windowNs) ? timestamps[i] - windowNs : 0;

            // Add the new element
            if (!std::isnan(values[i])) {
                runningSum += values[i];
                ++validCount;
            }

            // Remove elements that fell out of the window
            while (start < i && timestamps[start] < windowStart) {
                if (!std::isnan(values[start])) {
                    runningSum -= values[start];
                    --validCount;
                }
                ++start;
            }

            if (validCount == 0) {
                result[i] = std::nan("");
            } else if (method == "avg") {
                result[i] = runningSum / static_cast<double>(validCount);
            } else {
                result[i] = runningSum;
            }
        }
    } else if (method == "count") {
        // Incremental count of non-NaN values in the window.
        size_t validCount = 0;

        for (size_t i = 0; i < n; ++i) {
            uint64_t windowStart = (timestamps[i] > windowNs) ? timestamps[i] - windowNs : 0;

            if (!std::isnan(values[i])) {
                ++validCount;
            }

            while (start < i && timestamps[start] < windowStart) {
                if (!std::isnan(values[start])) {
                    --validCount;
                }
                ++start;
            }

            if (validCount == 0) {
                result[i] = std::nan("");
            } else {
                result[i] = static_cast<double>(validCount);
            }
        }
    } else if (method == "min" || method == "max") {
        // Monotone deque for O(n) amortized min/max.
        // The deque stores indices of candidate extremes. For min, the front
        // holds the index of the current minimum; values are kept in
        // non-decreasing order. For max, non-increasing order.
        std::deque<size_t> dq;

        // Lambda to check whether a candidate should be evicted from the back.
        // For min: evict if back value >= new value (keep strictly increasing).
        // For max: evict if back value <= new value (keep strictly decreasing).
        const bool isMin = (method == "min");

        for (size_t i = 0; i < n; ++i) {
            uint64_t windowStart = (timestamps[i] > windowNs) ? timestamps[i] - windowNs : 0;

            // Remove elements that fell out of the window from the front
            while (start < i && timestamps[start] < windowStart) {
                if (!dq.empty() && dq.front() == start) {
                    dq.pop_front();
                }
                ++start;
            }

            // Only insert non-NaN values into the deque
            if (!std::isnan(values[i])) {
                if (isMin) {
                    while (!dq.empty() && values[dq.back()] >= values[i]) {
                        dq.pop_back();
                    }
                } else {
                    while (!dq.empty() && values[dq.back()] <= values[i]) {
                        dq.pop_back();
                    }
                }
                dq.push_back(i);
            }

            if (dq.empty()) {
                result[i] = std::nan("");
            } else {
                result[i] = values[dq.front()];
            }
        }
    }

    return result;
}

// ============================================================================
// Timeshift Functions
// ============================================================================

/**
 * timeshift() - Shift timestamps by offset
 * Note: This adjusts timestamps, values remain the same
 * For comparison purposes, you'd query with shifted time range instead
 *
 * @param timestamps Input timestamps
 * @param offsetSeconds Offset in seconds (negative = look back)
 * @return Shifted timestamps
 */
inline std::vector<uint64_t> timeshift(const std::vector<uint64_t>& timestamps, double offsetSeconds) {
    int64_t offsetNs = static_cast<int64_t>(offsetSeconds * 1e9);
    std::vector<uint64_t> result(timestamps.size());

    for (size_t i = 0; i < timestamps.size(); ++i) {
        int64_t shifted = static_cast<int64_t>(timestamps[i]) + offsetNs;
        result[i] = (shifted > 0) ? static_cast<uint64_t>(shifted) : 0;
    }

    return result;
}

// ============================================================================
// Rank Functions - Select top/bottom N series
// ============================================================================

/**
 * Series data structure for ranking functions
 */
struct RankedSeries {
    size_t index;  // Original series index
    std::vector<double> values;
    std::vector<std::string> tags;  // Series identifying tags
    double rankValue;               // Value used for ranking
};

/**
 * Calculate ranking value for a series using specified method
 */
inline double calculateRankValue(const std::vector<double>& values, const std::string& method) {
    // Filter out NaN values
    std::vector<double> validValues;
    validValues.reserve(values.size());
    for (double v : values) {
        if (!std::isnan(v)) {
            validValues.push_back(v);
        }
    }

    if (validValues.empty()) {
        return std::nan("");
    }

    if (method == "mean" || method == "avg") {
        double sum = std::accumulate(validValues.begin(), validValues.end(), 0.0);
        return sum / validValues.size();
    }

    if (method == "min") {
        return *std::min_element(validValues.begin(), validValues.end());
    }

    if (method == "max") {
        return *std::max_element(validValues.begin(), validValues.end());
    }

    if (method == "last") {
        return validValues.back();
    }

    if (method == "sum") {
        return std::accumulate(validValues.begin(), validValues.end(), 0.0);
    }

    if (method == "area") {
        // Sum of absolute values (magnitude accumulator)
        // Note: This is NOT true area under curve (would require timestamps for integration).
        // For Datadog compatibility, this metric represents total magnitude over the series.
        double sumAbs = 0.0;
        for (double v : validValues) {
            sumAbs += std::abs(v);
        }
        return sumAbs;
    }

    if (method == "l2norm") {
        // L2 norm (Euclidean distance from origin)
        double sumSq = 0.0;
        for (double v : validValues) {
            sumSq += v * v;
        }
        return std::sqrt(sumSq);
    }

    throw std::invalid_argument("Unknown ranking method: " + method);
}

/**
 * top() - Select top N series by ranking method
 *
 * @param seriesValues Vector of series, each containing its values
 * @param n Number of series to return
 * @param method Ranking method: "mean", "min", "max", "last", "sum", "area", "l2norm"
 * @param order "desc" (default) for highest first, "asc" for lowest first
 * @return Indices of selected series (in ranking order)
 */
inline std::vector<size_t> top(const std::vector<std::vector<double>>& seriesValues, size_t n,
                               const std::string& method = "mean", const std::string& order = "desc") {
    if (seriesValues.empty() || n == 0) {
        return {};
    }

    // Calculate rank values for all series
    std::vector<std::pair<size_t, double>> ranked;
    ranked.reserve(seriesValues.size());

    for (size_t i = 0; i < seriesValues.size(); ++i) {
        double rankVal = calculateRankValue(seriesValues[i], method);
        if (!std::isnan(rankVal)) {
            ranked.emplace_back(i, rankVal);
        }
    }

    // Sort by rank value
    if (order == "asc") {
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) { return a.second < b.second; });
    } else {
        // Default: descending (top = highest)
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
    }

    // Return top N indices
    std::vector<size_t> result;
    size_t count = std::min(n, ranked.size());
    result.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        result.push_back(ranked[i].first);
    }

    return result;
}

/**
 * top_offset() - Select top N series after skipping offset
 *
 * @param seriesValues Vector of series values
 * @param n Number of series to return
 * @param method Ranking method
 * @param order Sort order
 * @param offset Number of series to skip
 * @return Indices of selected series
 */
inline std::vector<size_t> top_offset(const std::vector<std::vector<double>>& seriesValues, size_t n,
                                      const std::string& method = "mean", const std::string& order = "desc",
                                      size_t offset = 0) {
    if (seriesValues.empty() || n == 0) {
        return {};
    }

    // Calculate rank values for all series
    std::vector<std::pair<size_t, double>> ranked;
    ranked.reserve(seriesValues.size());

    for (size_t i = 0; i < seriesValues.size(); ++i) {
        double rankVal = calculateRankValue(seriesValues[i], method);
        if (!std::isnan(rankVal)) {
            ranked.emplace_back(i, rankVal);
        }
    }

    // Sort by rank value
    if (order == "asc") {
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) { return a.second < b.second; });
    } else {
        std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) { return a.second > b.second; });
    }

    // Return N indices after offset
    std::vector<size_t> result;

    if (offset >= ranked.size()) {
        return result;
    }

    size_t count = std::min(n, ranked.size() - offset);
    result.reserve(count);

    for (size_t i = offset; i < offset + count; ++i) {
        result.push_back(ranked[i].first);
    }

    return result;
}

/**
 * bottom() - Select bottom N series (same as top with asc order)
 */
inline std::vector<size_t> bottom(const std::vector<std::vector<double>>& seriesValues, size_t n,
                                  const std::string& method = "mean",
                                  const std::string& order = "asc"  // Default: ascending for bottom
) {
    return top(seriesValues, n, method, order);
}

// ============================================================================
// Regression Functions
// ============================================================================

/**
 * Regression result structure
 */
struct RegressionResult {
    double slope;
    double intercept;
    double rSquared;  // Coefficient of determination
    double residualStdDev;
    std::vector<double> fittedValues;
};

/**
 * trend_line() - Ordinary Least Squares (OLS) regression
 *
 * Fits y = slope * x + intercept where x is time index
 * Returns fitted values (the trend line)
 */
inline std::vector<double> trend_line(const std::vector<double>& values, const std::vector<uint64_t>& timestamps) {
    if (values.size() < 2 || values.size() != timestamps.size()) {
        return values;
    }

    // Collect valid points
    std::vector<double> x, y;
    x.reserve(values.size());
    y.reserve(values.size());

    uint64_t t0 = timestamps[0];
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            // Convert timestamp to seconds from start
            x.push_back(static_cast<double>(timestamps[i] - t0) / 1e9);
            y.push_back(values[i]);
        }
    }

    if (x.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    // Calculate means
    double xMean = std::accumulate(x.begin(), x.end(), 0.0) / x.size();
    double yMean = std::accumulate(y.begin(), y.end(), 0.0) / y.size();

    // Calculate slope and intercept using least squares
    double numerator = 0.0, denominator = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        numerator += (x[i] - xMean) * (y[i] - yMean);
        denominator += (x[i] - xMean) * (x[i] - xMean);
    }

    double slope = (denominator != 0) ? numerator / denominator : 0.0;
    double intercept = yMean - slope * xMean;

    // Generate fitted values for all timestamps
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        double xi = static_cast<double>(timestamps[i] - t0) / 1e9;
        result[i] = slope * xi + intercept;
    }

    return result;
}

/**
 * trend_line_extended() - Returns regression coefficients and stats
 */
inline RegressionResult trend_line_extended(const std::vector<double>& values,
                                            const std::vector<uint64_t>& timestamps) {
    RegressionResult result;
    result.slope = 0.0;
    result.intercept = 0.0;
    result.rSquared = 0.0;
    result.residualStdDev = 0.0;

    if (values.size() < 2 || values.size() != timestamps.size()) {
        result.fittedValues = values;
        return result;
    }

    // Collect valid points
    std::vector<double> x, y;
    std::vector<size_t> validIndices;

    uint64_t t0 = timestamps[0];
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            x.push_back(static_cast<double>(timestamps[i] - t0) / 1e9);
            y.push_back(values[i]);
            validIndices.push_back(i);
        }
    }

    if (x.size() < 2) {
        result.fittedValues.resize(values.size(), std::nan(""));
        return result;
    }

    // Calculate means
    double xMean = std::accumulate(x.begin(), x.end(), 0.0) / x.size();
    double yMean = std::accumulate(y.begin(), y.end(), 0.0) / y.size();

    // Calculate slope and intercept
    double numerator = 0.0, denominator = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        numerator += (x[i] - xMean) * (y[i] - yMean);
        denominator += (x[i] - xMean) * (x[i] - xMean);
    }

    result.slope = (denominator != 0) ? numerator / denominator : 0.0;
    result.intercept = yMean - result.slope * xMean;

    // Calculate R-squared and residual std dev
    double ssTotal = 0.0, ssResidual = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        double fitted = result.slope * x[i] + result.intercept;
        ssTotal += (y[i] - yMean) * (y[i] - yMean);
        ssResidual += (y[i] - fitted) * (y[i] - fitted);
    }

    result.rSquared = (ssTotal != 0) ? 1.0 - ssResidual / ssTotal : 1.0;
    result.residualStdDev = (x.size() > 2) ? std::sqrt(ssResidual / (x.size() - 2)) : 0.0;

    // Generate fitted values
    result.fittedValues.resize(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        double xi = static_cast<double>(timestamps[i] - t0) / 1e9;
        result.fittedValues[i] = result.slope * xi + result.intercept;
    }

    return result;
}

/**
 * robust_trend() - Robust regression using iteratively reweighted least squares
 *
 * Uses Huber loss function to reduce influence of outliers.
 * Performs weighted least squares with weights based on residuals.
 */
inline std::vector<double> robust_trend(
    const std::vector<double>& values, const std::vector<uint64_t>& timestamps, int maxIterations = 10,
    double huberDelta = 1.345  // Huber's delta (1.345 gives 95% efficiency for normal data)
) {
    if (values.size() < 2 || values.size() != timestamps.size()) {
        return values;
    }

    // Collect valid points
    std::vector<double> x, y;
    uint64_t t0 = timestamps[0];

    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            x.push_back(static_cast<double>(timestamps[i] - t0) / 1e9);
            y.push_back(values[i]);
        }
    }

    if (x.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    // Initial OLS estimate
    double xMean = std::accumulate(x.begin(), x.end(), 0.0) / x.size();
    double yMean = std::accumulate(y.begin(), y.end(), 0.0) / y.size();

    double numerator = 0.0, denominator = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        numerator += (x[i] - xMean) * (y[i] - yMean);
        denominator += (x[i] - xMean) * (x[i] - xMean);
    }

    double slope = (denominator != 0) ? numerator / denominator : 0.0;
    double intercept = yMean - slope * xMean;

    // Iteratively reweight
    std::vector<double> weights(x.size(), 1.0);

    for (int iter = 0; iter < maxIterations; ++iter) {
        // Calculate residuals and MAD (median absolute deviation)
        std::vector<double> residuals(x.size());
        for (size_t i = 0; i < x.size(); ++i) {
            residuals[i] = y[i] - (slope * x[i] + intercept);
        }

        // Calculate MAD
        std::vector<double> absResiduals(residuals.size());
        for (size_t i = 0; i < residuals.size(); ++i) {
            absResiduals[i] = std::abs(residuals[i]);
        }
        std::nth_element(absResiduals.begin(), absResiduals.begin() + absResiduals.size() / 2, absResiduals.end());
        double mad = absResiduals[absResiduals.size() / 2] * 1.4826;  // Scale factor for normal

        if (mad < 1e-10)
            mad = 1e-10;  // Prevent division by zero

        // Update weights using Huber function
        for (size_t i = 0; i < x.size(); ++i) {
            double r = std::abs(residuals[i]) / mad;
            weights[i] = (r <= huberDelta) ? 1.0 : huberDelta / r;
        }

        // Weighted least squares
        double sumW = 0, sumWx = 0, sumWy = 0, sumWxx = 0, sumWxy = 0;
        for (size_t i = 0; i < x.size(); ++i) {
            double w = weights[i];
            sumW += w;
            sumWx += w * x[i];
            sumWy += w * y[i];
            sumWxx += w * x[i] * x[i];
            sumWxy += w * x[i] * y[i];
        }

        double denom = sumW * sumWxx - sumWx * sumWx;
        if (std::abs(denom) < 1e-10)
            break;

        slope = (sumW * sumWxy - sumWx * sumWy) / denom;
        intercept = (sumWy - slope * sumWx) / sumW;
    }

    // Generate fitted values
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        double xi = static_cast<double>(timestamps[i] - t0) / 1e9;
        result[i] = slope * xi + intercept;
    }

    return result;
}

/**
 * piecewise_constant() - Fit step function with automatic breakpoint detection
 *
 * Uses a simple change-point detection algorithm based on cumulative sum (CUSUM).
 * Returns fitted values as step function.
 */
inline std::vector<double> piecewise_constant(const std::vector<double>& values, size_t minSegmentSize = 5,
                                              double threshold = 2.0  // Threshold in standard deviations
) {
    if (values.empty()) {
        return values;
    }

    // Collect valid values
    std::vector<double> validValues;
    std::vector<size_t> validIndices;

    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            validValues.push_back(values[i]);
            validIndices.push_back(i);
        }
    }

    if (validValues.empty()) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    if (validValues.size() < minSegmentSize * 2) {
        // Not enough data for segmentation - return mean
        double mean = std::accumulate(validValues.begin(), validValues.end(), 0.0) / validValues.size();
        return std::vector<double>(values.size(), mean);
    }

    // Calculate overall statistics
    double globalMean = std::accumulate(validValues.begin(), validValues.end(), 0.0) / validValues.size();
    double sumSq = 0;
    for (double v : validValues) {
        sumSq += (v - globalMean) * (v - globalMean);
    }
    double globalStd = std::sqrt(sumSq / validValues.size());
    if (globalStd < 1e-10)
        globalStd = 1e-10;

    // Find change points using CUSUM
    std::vector<size_t> changePoints;
    changePoints.push_back(0);

    std::vector<double> cusum(validValues.size());
    cusum[0] = 0;

    for (size_t i = 1; i < validValues.size(); ++i) {
        cusum[i] = cusum[i - 1] + (validValues[i] - globalMean);
    }

    // Find significant changes
    for (size_t i = minSegmentSize; i < validValues.size() - minSegmentSize; ++i) {
        // Check if this point is a local maximum of |CUSUM|
        size_t segmentStart = changePoints.back();
        size_t leftCount = i - segmentStart;
        size_t rightCount = validValues.size() - i;

        // Safety check to avoid division by zero
        if (leftCount == 0 || rightCount == 0)
            continue;

        double leftMean = 0, rightMean = 0;
        for (size_t j = segmentStart; j < i; ++j) {
            leftMean += validValues[j];
        }
        leftMean /= leftCount;

        for (size_t j = i; j < validValues.size(); ++j) {
            rightMean += validValues[j];
        }
        rightMean /= rightCount;

        // Check if difference is significant
        if (std::abs(rightMean - leftMean) > threshold * globalStd) {
            if (segmentStart + minSegmentSize <= i) {
                changePoints.push_back(i);
            }
        }
    }
    changePoints.push_back(validValues.size());

    // Calculate mean for each segment
    std::vector<double> segmentMeans;
    for (size_t s = 0; s < changePoints.size() - 1; ++s) {
        double sum = 0;
        for (size_t i = changePoints[s]; i < changePoints[s + 1]; ++i) {
            sum += validValues[i];
        }
        segmentMeans.push_back(sum / (changePoints[s + 1] - changePoints[s]));
    }

    // Generate result
    std::vector<double> result(values.size());
    size_t currentSegment = 0;
    size_t validIdx = 0;

    for (size_t i = 0; i < values.size(); ++i) {
        if (validIdx < validIndices.size() && validIndices[validIdx] == i) {
            while (currentSegment < changePoints.size() - 1 && validIdx >= changePoints[currentSegment + 1]) {
                currentSegment++;
            }
            result[i] = segmentMeans[std::min(currentSegment, segmentMeans.size() - 1)];
            validIdx++;
        } else {
            // For NaN positions, use closest segment mean
            result[i] = segmentMeans[std::min(currentSegment, segmentMeans.size() - 1)];
        }
    }

    return result;
}

// ============================================================================
// Algorithm Functions - Spatial Anomaly Detection
// ============================================================================

/**
 * Outlier detection algorithms
 */
enum class OutlierAlgorithm {
    DBSCAN,  // Density-based clustering
    MAD      // Median Absolute Deviation
};

/**
 * Result of outlier detection for a single series
 */
struct OutlierResult {
    size_t seriesIndex;
    double outlierScore;       // How much of an outlier (0-1)
    double outlierPercentage;  // Percentage of points that are outliers
    bool isOutlier;
};

/**
 * Helper: Calculate median of a sorted vector (handles both odd and even sizes)
 */
inline double calculateMedian(const std::vector<double>& sorted) {
    if (sorted.empty())
        return 0.0;
    size_t n = sorted.size();
    if (n % 2 == 1) {
        return sorted[n / 2];
    } else {
        return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
    }
}

/**
 * Helper: Calculate Median Absolute Deviation for outlier detection
 *
 * Note: The 1.4826 scale factor assumes approximately normal distribution.
 * For non-normal data, the MAD may be less accurate.
 */
inline double calculateMAD(const std::vector<double>& values) {
    if (values.empty())
        return 0.0;

    // Filter out NaN values
    std::vector<double> validValues;
    validValues.reserve(values.size());
    for (double v : values) {
        if (!std::isnan(v)) {
            validValues.push_back(v);
        }
    }

    if (validValues.empty())
        return 0.0;

    std::vector<double> sorted = validValues;
    std::sort(sorted.begin(), sorted.end());

    double median = calculateMedian(sorted);

    std::vector<double> deviations;
    deviations.reserve(validValues.size());
    for (double v : validValues) {
        deviations.push_back(std::abs(v - median));
    }

    std::sort(deviations.begin(), deviations.end());
    return calculateMedian(deviations) * 1.4826;  // Scale factor for normal distribution
}

/**
 * Helper: Simple DBSCAN clustering for 1D data
 * Returns cluster assignments (-1 = noise/outlier)
 *
 * Uses sorted order with binary search for O(n log n) neighbor finding
 * instead of brute-force O(n^2) per point.
 */
inline std::vector<int> dbscan1D(const std::vector<double>& values, double epsilon, size_t minPoints) {
    std::vector<int> clusters(values.size(), -1);
    int currentCluster = 0;

    // Create sorted index for efficient neighbor finding via binary search
    std::vector<size_t> sortedIdx(values.size());
    std::iota(sortedIdx.begin(), sortedIdx.end(), 0);
    std::sort(sortedIdx.begin(), sortedIdx.end(), [&values](size_t a, size_t b) { return values[a] < values[b]; });

    // Build sorted values array and reverse map (original index -> sorted position)
    std::vector<double> sortedValues(values.size());
    std::vector<size_t> originalToSorted(values.size());
    for (size_t s = 0; s < values.size(); ++s) {
        sortedValues[s] = values[sortedIdx[s]];
        originalToSorted[sortedIdx[s]] = s;
    }

    // Lambda: find all neighbors of a point using binary search on sorted values
    // Returns original indices of all points within epsilon of values[origIdx]
    auto findNeighbors = [&](size_t origIdx) -> std::vector<size_t> {
        double val = values[origIdx];
        double lo = val - epsilon;
        double hi = val + epsilon;

        // Binary search for range [lo, hi] in sortedValues
        auto itLow = std::lower_bound(sortedValues.begin(), sortedValues.end(), lo);
        auto itHigh = std::upper_bound(sortedValues.begin(), sortedValues.end(), hi);

        std::vector<size_t> neighbors;
        size_t startPos = static_cast<size_t>(itLow - sortedValues.begin());
        size_t endPos = static_cast<size_t>(itHigh - sortedValues.begin());
        neighbors.reserve(endPos - startPos);

        for (size_t s = startPos; s < endPos; ++s) {
            neighbors.push_back(sortedIdx[s]);
        }
        return neighbors;
    };

    std::vector<bool> visited(values.size(), false);

    for (size_t i = 0; i < values.size(); ++i) {
        if (visited[i])
            continue;
        visited[i] = true;

        // Find neighbors within epsilon using binary search
        std::vector<size_t> neighbors = findNeighbors(i);

        if (neighbors.size() < minPoints) {
            clusters[i] = -1;  // Noise
        } else {
            // Expand cluster
            clusters[i] = currentCluster;

            for (size_t k = 0; k < neighbors.size(); ++k) {
                size_t j = neighbors[k];
                if (!visited[j]) {
                    visited[j] = true;

                    std::vector<size_t> jNeighbors = findNeighbors(j);

                    if (jNeighbors.size() >= minPoints) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
                        neighbors.insert(neighbors.end(), jNeighbors.begin(), jNeighbors.end());
#pragma GCC diagnostic pop
                    }
                }

                if (clusters[j] == -1) {
                    clusters[j] = currentCluster;
                }
            }

            currentCluster++;
        }
    }

    return clusters;
}

/**
 * outliers() - Identify series that behave differently from their peers
 *
 * This is spatial anomaly detection - at each point in time, find which series
 * are outliers compared to the other series.
 *
 * @param seriesValues Vector of series, each containing time-aligned values
 * @param algorithm "dbscan" or "mad"
 * @param tolerance Sensitivity threshold (higher = more tolerant, fewer outliers)
 * @param minPercentage Minimum percentage of points that must be outliers (0-100)
 * @return Vector of outlier results for each series
 */
inline std::vector<OutlierResult> outliers(const std::vector<std::vector<double>>& seriesValues,
                                           const std::string& algorithm = "mad", double tolerance = 3.0,
                                           double minPercentage = 50.0) {
    std::vector<OutlierResult> results;

    if (seriesValues.empty()) {
        return results;
    }

    // Ensure all series have the same length
    size_t numPoints = seriesValues[0].size();
    for (const auto& series : seriesValues) {
        if (series.size() != numPoints) {
            // Return empty results for mismatched series lengths
            return results;
        }
    }

    size_t numSeries = seriesValues.size();

    // Count outlier points per series
    std::vector<size_t> outlierCounts(numSeries, 0);
    std::vector<double> outlierScores(numSeries, 0.0);

    // For each time point, determine which series are outliers
    for (size_t t = 0; t < numPoints; ++t) {
        // Collect values at this time point
        std::vector<double> valuesAtT;
        std::vector<size_t> validIndices;

        for (size_t s = 0; s < numSeries; ++s) {
            if (!std::isnan(seriesValues[s][t])) {
                valuesAtT.push_back(seriesValues[s][t]);
                validIndices.push_back(s);
            }
        }

        if (valuesAtT.size() < 3) {
            continue;  // Need at least 3 series to detect outliers
        }

        if (algorithm == "dbscan") {
            // DBSCAN-based detection
            // Calculate epsilon based on data spread and tolerance
            double dataRange = *std::max_element(valuesAtT.begin(), valuesAtT.end()) -
                               *std::min_element(valuesAtT.begin(), valuesAtT.end());
            double epsilon = dataRange / tolerance;
            size_t minPoints = std::max(2UL, valuesAtT.size() / 4);

            auto clusters = dbscan1D(valuesAtT, epsilon, minPoints);

            // Points in cluster -1 are outliers
            for (size_t i = 0; i < clusters.size(); ++i) {
                if (clusters[i] == -1) {
                    outlierCounts[validIndices[i]]++;
                }
            }
        } else {
            // MAD-based detection (default)
            // Calculate median
            std::vector<double> sorted = valuesAtT;
            std::sort(sorted.begin(), sorted.end());
            double median = calculateMedian(sorted);

            double mad = calculateMAD(valuesAtT);

            // If MAD is 0 (most values equal median), check data range
            // to handle case where outliers exist but MAD is 0
            double dataMin = sorted.front();
            double dataMax = sorted.back();
            double dataRange = dataMax - dataMin;

            if (mad < 1e-10) {
                // MAD is 0, but if there's any spread, mark distant values as outliers
                if (dataRange > 1e-10) {
                    // Use a fraction of range as threshold
                    double threshold = dataRange / (tolerance * 2);
                    for (size_t i = 0; i < valuesAtT.size(); ++i) {
                        double deviation = std::abs(valuesAtT[i] - median);
                        if (deviation > threshold) {
                            outlierCounts[validIndices[i]]++;
                            outlierScores[validIndices[i]] += deviation / dataRange;
                        }
                    }
                }
                // If no range either, all values are the same - no outliers
                continue;
            }

            // Check each value against MAD threshold
            for (size_t i = 0; i < valuesAtT.size(); ++i) {
                double zScore = std::abs(valuesAtT[i] - median) / mad;
                if (zScore > tolerance) {
                    outlierCounts[validIndices[i]]++;
                    outlierScores[validIndices[i]] += zScore;
                }
            }
        }
    }

    // Calculate results for each series
    for (size_t s = 0; s < numSeries; ++s) {
        OutlierResult result;
        result.seriesIndex = s;
        result.outlierPercentage = (numPoints > 0) ? (static_cast<double>(outlierCounts[s]) / numPoints * 100.0) : 0.0;
        result.outlierScore = (outlierCounts[s] > 0) ? outlierScores[s] / outlierCounts[s] : 0.0;
        result.isOutlier = result.outlierPercentage >= minPercentage;
        results.push_back(result);
    }

    return results;
}

/**
 * outliers_indices() - Returns indices of outlier series
 *
 * Convenience function that returns just the indices of series marked as outliers.
 */
inline std::vector<size_t> outliers_indices(const std::vector<std::vector<double>>& seriesValues,
                                            const std::string& algorithm = "mad", double tolerance = 3.0,
                                            double minPercentage = 50.0) {
    auto results = outliers(seriesValues, algorithm, tolerance, minPercentage);

    std::vector<size_t> outlierIndices;
    for (const auto& result : results) {
        if (result.isOutlier) {
            outlierIndices.push_back(result.seriesIndex);
        }
    }

    return outlierIndices;
}

/**
 * outliers_mask() - Returns boolean mask of outlier points per series
 *
 * For each series, returns a vector where true indicates the point is an outlier
 * compared to other series at that timestamp.
 */
inline std::vector<std::vector<bool>> outliers_mask(const std::vector<std::vector<double>>& seriesValues,
                                                    const std::string& algorithm = "mad", double tolerance = 3.0) {
    if (seriesValues.empty()) {
        return {};
    }

    size_t numPoints = seriesValues[0].size();
    size_t numSeries = seriesValues.size();

    // Validate all series have the same length
    for (const auto& series : seriesValues) {
        if (series.size() != numPoints) {
            // Return empty masks for mismatched series lengths
            return {};
        }
    }

    // Initialize masks
    std::vector<std::vector<bool>> masks(numSeries, std::vector<bool>(numPoints, false));

    // For each time point, determine outliers
    for (size_t t = 0; t < numPoints; ++t) {
        std::vector<double> valuesAtT;
        std::vector<size_t> validIndices;

        for (size_t s = 0; s < numSeries; ++s) {
            if (!std::isnan(seriesValues[s][t])) {
                valuesAtT.push_back(seriesValues[s][t]);
                validIndices.push_back(s);
            }
        }

        if (valuesAtT.size() < 3)
            continue;

        if (algorithm == "dbscan") {
            double dataRange = *std::max_element(valuesAtT.begin(), valuesAtT.end()) -
                               *std::min_element(valuesAtT.begin(), valuesAtT.end());
            double epsilon = dataRange / tolerance;
            size_t minPoints = std::max(2UL, valuesAtT.size() / 4);

            auto clusters = dbscan1D(valuesAtT, epsilon, minPoints);

            for (size_t i = 0; i < clusters.size(); ++i) {
                if (clusters[i] == -1) {
                    masks[validIndices[i]][t] = true;
                }
            }
        } else {
            // MAD-based
            std::vector<double> sorted = valuesAtT;
            std::sort(sorted.begin(), sorted.end());
            double median = calculateMedian(sorted);

            double mad = calculateMAD(valuesAtT);

            // If MAD is 0 (most values equal median), check data range
            double dataMin = sorted.front();
            double dataMax = sorted.back();
            double dataRange = dataMax - dataMin;

            if (mad < 1e-10) {
                // MAD is 0, but if there's any spread, mark distant values as outliers
                if (dataRange > 1e-10) {
                    double threshold = dataRange / (tolerance * 2);
                    for (size_t i = 0; i < valuesAtT.size(); ++i) {
                        double deviation = std::abs(valuesAtT[i] - median);
                        if (deviation > threshold) {
                            masks[validIndices[i]][t] = true;
                        }
                    }
                }
                continue;
            }

            for (size_t i = 0; i < valuesAtT.size(); ++i) {
                double zScore = std::abs(valuesAtT[i] - median) / mad;
                if (zScore > tolerance) {
                    masks[validIndices[i]][t] = true;
                }
            }
        }
    }

    return masks;
}

}  // namespace transform
}  // namespace timestar
