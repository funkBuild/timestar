#pragma once

/**
 * SIMD-Optimized Transform Functions
 *
 * Highway-dispatched implementations for Tier 1 transform functions.
 * Highway automatically selects the best available ISA at runtime
 * (AVX-512, AVX2, SSE4, NEON, etc.) via its foreach_target mechanism.
 *
 * Falls back to scalar for small arrays where SIMD overhead exceeds benefit.
 */

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

namespace timestar {
namespace transform {
namespace simd {

// ============================================================================
// Scalar Fallback Implementations
// ============================================================================
namespace scalar {

inline std::vector<double> abs(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::abs(values[i]);
    }
    return result;
}

inline std::vector<double> default_zero(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? 0.0 : values[i];
    }
    return result;
}

inline std::vector<double> count_nonzero(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] != 0.0) ? 1.0 : 0.0;
    }
    return result;
}

inline std::vector<double> count_not_null(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? 0.0 : 1.0;
    }
    return result;
}

inline std::vector<double> clamp_min(const std::vector<double>& values, double minVal) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? values[i] : std::max(values[i], minVal);
    }
    return result;
}

inline std::vector<double> clamp_max(const std::vector<double>& values, double maxVal) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? values[i] : std::min(values[i], maxVal);
    }
    return result;
}

inline std::vector<double> cutoff_min(const std::vector<double>& values, double threshold) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] < threshold) ? std::nan("") : values[i];
    }
    return result;
}

inline std::vector<double> cutoff_max(const std::vector<double>& values, double threshold) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] > threshold) ? std::nan("") : values[i];
    }
    return result;
}

inline std::vector<double> diff(const std::vector<double>& values) {
    if (values.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }
    std::vector<double> result(values.size());
    result[0] = std::nan("");
    for (size_t i = 1; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else {
            result[i] = values[i] - values[i - 1];
        }
    }
    return result;
}

inline std::vector<double> monotonic_diff(const std::vector<double>& values) {
    if (values.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }
    std::vector<double> result(values.size());
    result[0] = std::nan("");
    for (size_t i = 1; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else {
            double dv = values[i] - values[i - 1];
            result[i] = (dv >= 0) ? dv : values[i];  // counter reset: use current value
        }
    }
    return result;
}

// Multiply all non-NaN values by a constant
inline void multiply_inplace(std::vector<double>& values, double factor) {
    for (auto& v : values) {
        if (!std::isnan(v)) {
            v *= factor;
        }
    }
}

inline std::vector<double> exp(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::exp(values[i]);  // exp(NaN) = NaN
    }
    return result;
}

inline std::vector<double> round(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::round(values[i]);  // half away from zero; round(NaN) = NaN
    }
    return result;
}

inline std::vector<double> sign(const std::vector<double>& values) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        double v = values[i];
        result[i] = std::isnan(v) ? v : (v > 0.0 ? 1.0 : (v < 0.0 ? -1.0 : 0.0));
    }
    return result;
}

// Per-second first derivative: out[i] = (v[i]-v[i-1]) / ((ts[i]-ts[i-1]) in seconds).
// out[0] = NaN; NaN operands or non-increasing timestamps yield NaN.
inline std::vector<double> deriv(const std::vector<double>& values, const std::vector<uint64_t>& ts) {
    std::vector<double> result(values.size(), std::nan(""));
    for (size_t i = 1; i < values.size(); ++i) {
        double dtNs = static_cast<double>(ts[i] - ts[i - 1]);
        if (ts[i] > ts[i - 1] && !std::isnan(values[i]) && !std::isnan(values[i - 1])) {
            result[i] = (values[i] - values[i - 1]) * 1e9 / dtNs;
        }
    }
    return result;
}

// NaN-skipping mean and population stddev in two passes.
// Returns the count of non-NaN values (0 => mean/stddev are meaningless).
inline size_t mean_stddev_skipnan(const std::vector<double>& values, double& mean, double& stddev) {
    double sum = 0.0;
    size_t count = 0;
    for (double v : values) {
        if (!std::isnan(v)) {
            sum += v;
            ++count;
        }
    }
    if (count == 0) {
        mean = stddev = 0.0;
        return 0;
    }
    mean = sum / static_cast<double>(count);
    double m2 = 0.0;
    for (double v : values) {
        if (!std::isnan(v)) {
            double d = v - mean;
            m2 += d * d;
        }
    }
    stddev = std::sqrt(m2 / static_cast<double>(count));
    return count;
}

// out[i] = (v[i] - sub) * mul; NaN passes through naturally.
inline std::vector<double> scale_shift(const std::vector<double>& values, double sub, double mul) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] - sub) * mul;
    }
    return result;
}

}  // namespace scalar

// ============================================================================
// Dispatch Functions — Highway selects best ISA at runtime
// ============================================================================

// Minimum array size to benefit from SIMD (below this, overhead exceeds benefit).
// Set to 8 (two AVX2 iterations of 4 doubles) consistent with simd_aggregator
// and simd_anomaly which use the same simple single-accumulator 4-wide loops.
// Modules with 4-accumulator unrolled loops (forecast/) use >= 16 because they
// process 16 elements per iteration.
constexpr size_t SIMD_MIN_SIZE = 8;

// Highway-dispatched implementations (defined in transform_functions_simd.cpp)
std::vector<double> abs(const std::vector<double>& values);
std::vector<double> default_zero(const std::vector<double>& values);
std::vector<double> count_nonzero(const std::vector<double>& values);
std::vector<double> count_not_null(const std::vector<double>& values);
std::vector<double> clamp_min(const std::vector<double>& values, double minVal);
std::vector<double> clamp_max(const std::vector<double>& values, double maxVal);
std::vector<double> cutoff_min(const std::vector<double>& values, double threshold);
std::vector<double> cutoff_max(const std::vector<double>& values, double threshold);
std::vector<double> diff(const std::vector<double>& values);
std::vector<double> monotonic_diff(const std::vector<double>& values);
void multiply_inplace(std::vector<double>& values, double factor);
std::vector<double> exp(const std::vector<double>& values);
std::vector<double> round(const std::vector<double>& values);
std::vector<double> sign(const std::vector<double>& values);
std::vector<double> deriv(const std::vector<double>& values, const std::vector<uint64_t>& timestamps);
size_t mean_stddev_skipnan(const std::vector<double>& values, double& mean, double& stddev);
std::vector<double> scale_shift(const std::vector<double>& values, double sub, double mul);

}  // namespace simd
}  // namespace transform
}  // namespace timestar
