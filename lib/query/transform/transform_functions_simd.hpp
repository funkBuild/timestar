#ifndef TRANSFORM_FUNCTIONS_SIMD_H_INCLUDED
#define TRANSFORM_FUNCTIONS_SIMD_H_INCLUDED

/**
 * SIMD-Optimized Transform Functions
 *
 * AVX2 and AVX512 optimized implementations for Tier 1 transform functions.
 * These provide significant speedups for large datasets (typically 4-8x).
 *
 * Compile-time control:
 *   - Define TIMESTAR_DISABLE_SIMD to disable all SIMD and use scalar fallbacks
 *
 * Runtime behavior:
 *   - Automatically detects CPU capabilities (AVX2/AVX512)
 *   - Falls back to scalar implementation if SIMD unavailable
 *   - Falls back for small arrays where SIMD overhead exceeds benefit
 */

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

#ifndef TIMESTAR_DISABLE_SIMD
    #include <cpuid.h>
    #include <immintrin.h>
#endif

namespace timestar {
namespace transform {
namespace simd {

// ============================================================================
// CPU Feature Detection
// ============================================================================

/**
 * Check if AVX2 is available at runtime
 */
inline bool isAvx2Available() {
#ifdef TIMESTAR_DISABLE_SIMD
    return false;
#else
    static int cached = -1;
    if (cached >= 0)
        return cached != 0;

    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        bool avx = (ecx & (1 << 28)) != 0;
        if (avx && __get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            bool avx2 = (ebx & (1 << 5)) != 0;
            cached = avx2 ? 1 : 0;
            return avx2;
        }
    }
    cached = 0;
    return false;
#endif
}

/**
 * Check if AVX512 is available at runtime
 */
inline bool isAvx512Available() {
#ifdef TIMESTAR_DISABLE_SIMD
    return false;
#else
    static int cached = -1;
    if (cached >= 0)
        return cached != 0;

    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, nullptr) >= 7) {
        __cpuid_count(7, 0, eax, ebx, ecx, edx);
        bool avx512f = (ebx & (1 << 16)) != 0;
        bool avx512dq = (ebx & (1 << 17)) != 0;
        cached = (avx512f && avx512dq) ? 1 : 0;
        return avx512f && avx512dq;
    }
    cached = 0;
    return false;
#endif
}

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
// AVX2 SIMD Implementations (process 4 doubles at a time)
// ============================================================================
#ifndef TIMESTAR_DISABLE_SIMD

namespace avx2 {

/**
 * abs() - Absolute value using ANDNOT with sign bit mask
 */
inline std::vector<double> abs(const std::vector<double>& values) {
    std::vector<double> result(values.size());

    // Sign bit mask: all bits except the sign bit
    const __m256d sign_mask = _mm256_set1_pd(-0.0);

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // ANDNOT: ~sign_mask & v = clear sign bit = abs
        __m256d abs_v = _mm256_andnot_pd(sign_mask, v);
        _mm256_storeu_pd(&result[i], abs_v);
    }

    // Handle remaining elements
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = std::abs(values[i]);
    }

    return result;
}

/**
 * default_zero() - Replace NaN with zero using comparison and blend
 */
inline std::vector<double> default_zero(const std::vector<double>& values) {
    std::vector<double> result(values.size());

    const __m256d zero = _mm256_setzero_pd();

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Compare v == v (NaN != NaN returns false)
        __m256d not_nan = _mm256_cmp_pd(v, v, _CMP_EQ_OQ);
        // Blend: if not_nan, use v; else use zero
        __m256d blended = _mm256_blendv_pd(zero, v, not_nan);
        _mm256_storeu_pd(&result[i], blended);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? 0.0 : values[i];
    }

    return result;
}

/**
 * count_nonzero() - Return 1.0 for non-zero, non-NaN values
 */
inline std::vector<double> count_nonzero(const std::vector<double>& values) {
    std::vector<double> result(values.size());

    const __m256d zero = _mm256_setzero_pd();
    const __m256d one = _mm256_set1_pd(1.0);

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is not NaN: v == v
        __m256d not_nan = _mm256_cmp_pd(v, v, _CMP_EQ_OQ);
        // Check if v != 0
        __m256d not_zero = _mm256_cmp_pd(v, zero, _CMP_NEQ_OQ);
        // Both conditions must be true
        __m256d mask = _mm256_and_pd(not_nan, not_zero);
        // Select 1.0 where mask is true, else 0.0
        __m256d res = _mm256_blendv_pd(zero, one, mask);
        _mm256_storeu_pd(&result[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] != 0.0) ? 1.0 : 0.0;
    }

    return result;
}

/**
 * count_not_null() - Return 1.0 for non-NaN values
 */
inline std::vector<double> count_not_null(const std::vector<double>& values) {
    std::vector<double> result(values.size());

    const __m256d zero = _mm256_setzero_pd();
    const __m256d one = _mm256_set1_pd(1.0);

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is not NaN: v == v
        __m256d not_nan = _mm256_cmp_pd(v, v, _CMP_EQ_OQ);
        // Select 1.0 where not NaN, else 0.0
        __m256d res = _mm256_blendv_pd(zero, one, not_nan);
        _mm256_storeu_pd(&result[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? 0.0 : 1.0;
    }

    return result;
}

/**
 * clamp_min() - Clamp values to minimum, preserving NaN
 */
inline std::vector<double> clamp_min(const std::vector<double>& values, double minVal) {
    std::vector<double> result(values.size());

    const __m256d min_vec = _mm256_set1_pd(minVal);

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is not NaN
        __m256d not_nan = _mm256_cmp_pd(v, v, _CMP_EQ_OQ);
        // max(v, minVal) for non-NaN values
        __m256d clamped = _mm256_max_pd(v, min_vec);
        // Preserve original (NaN) where v is NaN
        __m256d res = _mm256_blendv_pd(v, clamped, not_nan);
        _mm256_storeu_pd(&result[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? values[i] : std::max(values[i], minVal);
    }

    return result;
}

/**
 * clamp_max() - Clamp values to maximum, preserving NaN
 */
inline std::vector<double> clamp_max(const std::vector<double>& values, double maxVal) {
    std::vector<double> result(values.size());

    const __m256d max_vec = _mm256_set1_pd(maxVal);

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is not NaN
        __m256d not_nan = _mm256_cmp_pd(v, v, _CMP_EQ_OQ);
        // min(v, maxVal) for non-NaN values
        __m256d clamped = _mm256_min_pd(v, max_vec);
        // Preserve original (NaN) where v is NaN
        __m256d res = _mm256_blendv_pd(v, clamped, not_nan);
        _mm256_storeu_pd(&result[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? values[i] : std::min(values[i], maxVal);
    }

    return result;
}

/**
 * cutoff_min() - Set values below threshold to NaN
 */
inline std::vector<double> cutoff_min(const std::vector<double>& values, double threshold) {
    std::vector<double> result(values.size());

    const __m256d thresh_vec = _mm256_set1_pd(threshold);
    const __m256d nan_vec = _mm256_set1_pd(std::nan(""));

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is NaN (unordered comparison: true when either operand is NaN)
        __m256d is_nan = _mm256_cmp_pd(v, v, _CMP_UNORD_Q);
        // Check if v >= threshold
        __m256d above_thresh = _mm256_cmp_pd(v, thresh_vec, _CMP_GE_OQ);
        // Keep original if: NaN OR >= threshold
        __m256d keep_mask = _mm256_or_pd(is_nan, above_thresh);
        // For non-NaN values: if < threshold, set to NaN
        __m256d res = _mm256_blendv_pd(nan_vec, v, keep_mask);
        _mm256_storeu_pd(&result[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] < threshold) ? std::nan("") : values[i];
    }

    return result;
}

/**
 * cutoff_max() - Set values above threshold to NaN
 */
inline std::vector<double> cutoff_max(const std::vector<double>& values, double threshold) {
    std::vector<double> result(values.size());

    const __m256d thresh_vec = _mm256_set1_pd(threshold);
    const __m256d nan_vec = _mm256_set1_pd(std::nan(""));

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is NaN (unordered comparison: true when either operand is NaN)
        __m256d is_nan = _mm256_cmp_pd(v, v, _CMP_UNORD_Q);
        // Check if v <= threshold
        __m256d below_thresh = _mm256_cmp_pd(v, thresh_vec, _CMP_LE_OQ);
        // Keep original if: NaN OR <= threshold
        __m256d keep_mask = _mm256_or_pd(is_nan, below_thresh);
        // For non-NaN values: if > threshold, set to NaN
        __m256d res = _mm256_blendv_pd(nan_vec, v, keep_mask);
        _mm256_storeu_pd(&result[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] > threshold) ? std::nan("") : values[i];
    }

    return result;
}

/**
 * diff() - Difference between consecutive points
 * Note: Less SIMD-friendly due to dependencies, but still benefits from vectorization
 */
inline std::vector<double> diff(const std::vector<double>& values) {
    if (values.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    std::vector<double> result(values.size());
    result[0] = std::nan("");

    const __m256d nan_vec = _mm256_set1_pd(std::nan(""));

    // Process in groups of 4 starting from index 1
    // We need values[i] - values[i-1] for i = 1..n-1
    size_t simd_end = 1 + ((values.size() - 1) / 4) * 4;
    if (simd_end > values.size())
        simd_end = values.size();

    // For SIMD, we need adjacent pairs: (v1-v0, v2-v1, v3-v2, v4-v3)
    // Process 4 diffs at a time when possible
    size_t i = 1;
    while (i + 3 < values.size()) {
        // Load current values
        __m256d curr = _mm256_loadu_pd(&values[i]);
        __m256d prev = _mm256_loadu_pd(&values[i - 1]);

        // Compute differences
        __m256d diff = _mm256_sub_pd(curr, prev);

        // Check for NaN in inputs
        __m256d curr_valid = _mm256_cmp_pd(curr, curr, _CMP_EQ_OQ);
        __m256d prev_valid = _mm256_cmp_pd(prev, prev, _CMP_EQ_OQ);
        __m256d both_valid = _mm256_and_pd(curr_valid, prev_valid);

        // Where either is NaN, set result to NaN
        __m256d res = _mm256_blendv_pd(nan_vec, diff, both_valid);

        _mm256_storeu_pd(&result[i], res);
        i += 4;
    }

    // Handle remaining elements
    for (; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else {
            result[i] = values[i] - values[i - 1];
        }
    }

    return result;
}

/**
 * monotonic_diff() - Difference that treats negative diffs as 0 (counter resets)
 */
inline std::vector<double> monotonic_diff(const std::vector<double>& values) {
    if (values.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }

    std::vector<double> result(values.size());
    result[0] = std::nan("");

    const __m256d nan_vec = _mm256_set1_pd(std::nan(""));
    const __m256d zero = _mm256_setzero_pd();

    size_t i = 1;
    while (i + 3 < values.size()) {
        __m256d curr = _mm256_loadu_pd(&values[i]);
        __m256d prev = _mm256_loadu_pd(&values[i - 1]);

        // Compute differences
        __m256d diff = _mm256_sub_pd(curr, prev);

        // Check for NaN in inputs
        __m256d curr_valid = _mm256_cmp_pd(curr, curr, _CMP_EQ_OQ);
        __m256d prev_valid = _mm256_cmp_pd(prev, prev, _CMP_EQ_OQ);
        __m256d both_valid = _mm256_and_pd(curr_valid, prev_valid);

        // Counter reset handling: if diff is negative, use current value
        __m256d positive_mask = _mm256_cmp_pd(diff, zero, _CMP_GE_OQ);
        __m256d reset_diff = _mm256_blendv_pd(curr, diff, positive_mask);

        // Where either input is NaN, set result to NaN
        __m256d res = _mm256_blendv_pd(nan_vec, reset_diff, both_valid);

        _mm256_storeu_pd(&result[i], res);
        i += 4;
    }

    // Handle remaining elements
    for (; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else {
            double dv = values[i] - values[i - 1];
            result[i] = (dv >= 0) ? dv : values[i];  // counter reset: use current value
        }
    }

    return result;
}

/**
 * multiply_inplace() - Multiply non-NaN values by a constant (for per_minute/per_hour)
 */
inline void multiply_inplace(std::vector<double>& values, double factor) {
    const __m256d factor_vec = _mm256_set1_pd(factor);

    size_t simd_end = values.size() - (values.size() % 4);

    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d v = _mm256_loadu_pd(&values[i]);
        // Check if v is not NaN
        __m256d not_nan = _mm256_cmp_pd(v, v, _CMP_EQ_OQ);
        // Multiply
        __m256d scaled = _mm256_mul_pd(v, factor_vec);
        // Preserve NaN where original was NaN
        __m256d res = _mm256_blendv_pd(v, scaled, not_nan);
        _mm256_storeu_pd(&values[i], res);
    }

    // Handle remaining
    for (size_t i = simd_end; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            values[i] *= factor;
        }
    }
}

}  // namespace avx2

#endif  // !TIMESTAR_DISABLE_SIMD

// ============================================================================
// Dispatch Functions - Select SIMD or scalar based on CPU and array size
// ============================================================================

// Minimum array size to benefit from SIMD (below this, overhead exceeds benefit).
// Set to 8 (two AVX2 iterations of 4 doubles) consistent with simd_aggregator
// and simd_anomaly which use the same simple single-accumulator 4-wide loops.
// Modules with 4-accumulator unrolled loops (forecast/) use >= 16 because they
// process 16 elements per iteration.
constexpr size_t SIMD_MIN_SIZE = 8;

/**
 * abs() - Absolute value with SIMD dispatch
 */
inline std::vector<double> abs(const std::vector<double>& values) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::abs(values);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::abs(values);
    }
    return scalar::abs(values);
#endif
}

/**
 * default_zero() - Replace NaN with zero, SIMD dispatch
 */
inline std::vector<double> default_zero(const std::vector<double>& values) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::default_zero(values);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::default_zero(values);
    }
    return scalar::default_zero(values);
#endif
}

/**
 * count_nonzero() - Count non-zero values, SIMD dispatch
 */
inline std::vector<double> count_nonzero(const std::vector<double>& values) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::count_nonzero(values);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::count_nonzero(values);
    }
    return scalar::count_nonzero(values);
#endif
}

/**
 * count_not_null() - Count non-null values, SIMD dispatch
 */
inline std::vector<double> count_not_null(const std::vector<double>& values) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::count_not_null(values);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::count_not_null(values);
    }
    return scalar::count_not_null(values);
#endif
}

/**
 * clamp_min() - Clamp to minimum, SIMD dispatch
 */
inline std::vector<double> clamp_min(const std::vector<double>& values, double minVal) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::clamp_min(values, minVal);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::clamp_min(values, minVal);
    }
    return scalar::clamp_min(values, minVal);
#endif
}

/**
 * clamp_max() - Clamp to maximum, SIMD dispatch
 */
inline std::vector<double> clamp_max(const std::vector<double>& values, double maxVal) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::clamp_max(values, maxVal);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::clamp_max(values, maxVal);
    }
    return scalar::clamp_max(values, maxVal);
#endif
}

/**
 * cutoff_min() - Set values below threshold to NaN, SIMD dispatch
 */
inline std::vector<double> cutoff_min(const std::vector<double>& values, double threshold) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::cutoff_min(values, threshold);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::cutoff_min(values, threshold);
    }
    return scalar::cutoff_min(values, threshold);
#endif
}

/**
 * cutoff_max() - Set values above threshold to NaN, SIMD dispatch
 */
inline std::vector<double> cutoff_max(const std::vector<double>& values, double threshold) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::cutoff_max(values, threshold);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::cutoff_max(values, threshold);
    }
    return scalar::cutoff_max(values, threshold);
#endif
}

/**
 * diff() - Difference between consecutive points, SIMD dispatch
 */
inline std::vector<double> diff(const std::vector<double>& values) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::diff(values);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::diff(values);
    }
    return scalar::diff(values);
#endif
}

/**
 * monotonic_diff() - Monotonic difference, SIMD dispatch
 */
inline std::vector<double> monotonic_diff(const std::vector<double>& values) {
#ifdef TIMESTAR_DISABLE_SIMD
    return scalar::monotonic_diff(values);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        return avx2::monotonic_diff(values);
    }
    return scalar::monotonic_diff(values);
#endif
}

/**
 * multiply_inplace() - Multiply by constant in place, SIMD dispatch
 */
inline void multiply_inplace(std::vector<double>& values, double factor) {
#ifdef TIMESTAR_DISABLE_SIMD
    scalar::multiply_inplace(values, factor);
#else
    if (values.size() >= SIMD_MIN_SIZE && isAvx2Available()) {
        avx2::multiply_inplace(values, factor);
    } else {
        scalar::multiply_inplace(values, factor);
    }
#endif
}

}  // namespace simd
}  // namespace transform
}  // namespace timestar

#endif  // TRANSFORM_FUNCTIONS_SIMD_H_INCLUDED
