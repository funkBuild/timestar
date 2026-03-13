#ifndef TRANSFORM_FUNCTIONS_SIMD_H_INCLUDED
#define TRANSFORM_FUNCTIONS_SIMD_H_INCLUDED

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

// Highway handles ISA dispatch automatically — always available.
inline bool isAvx2Available() { return true; }
inline bool isAvx512Available() { return true; }

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

}  // namespace simd
}  // namespace transform
}  // namespace timestar

#endif  // TRANSFORM_FUNCTIONS_SIMD_H_INCLUDED
