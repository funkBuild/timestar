#include "simd_aggregator.hpp"
#include <algorithm>
#include <cstring>
#include <cpuid.h>

namespace tsdb {
namespace simd {

// Forward declarations of helper functions
static double hsum_double_avx(__m256d v);
static double hmin_double_avx(__m256d v);
static double hmax_double_avx(__m256d v);

// Check CPU support for AVX2
bool SimdAggregator::isAvx2Available() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        // Check for AVX support
        bool avx = (ecx & (1 << 28)) != 0;

        if (avx && __get_cpuid_max(0, nullptr) >= 7) {
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            // Check for AVX2 support
            bool avx2 = (ebx & (1 << 5)) != 0;
            return avx2;
        }
    }
    return false;
}

// Check CPU support for AVX512
bool SimdAggregator::isAvx512Available() {
    unsigned int eax, ebx, ecx, edx;
    if (__get_cpuid_max(0, nullptr) >= 7) {
        __cpuid_count(7, 0, eax, ebx, ecx, edx);
        // Check for AVX512F (foundation) and AVX512DQ (additional instructions)
        bool avx512f = (ebx & (1 << 16)) != 0;
        bool avx512dq = (ebx & (1 << 17)) != 0;
        return avx512f && avx512dq;
    }
    return false;
}

// Horizontal sum of a 256-bit vector containing 4 doubles
static double hsum_double_avx(__m256d v) {
    __m128d vlow  = _mm256_castpd256_pd128(v);
    __m128d vhigh = _mm256_extractf128_pd(v, 1); // high 128
    vlow  = _mm_add_pd(vlow, vhigh);            // reduce down to 128
    
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    return _mm_cvtsd_f64(_mm_add_sd(vlow, high64));  // reduce to scalar
}

// Horizontal minimum of a 256-bit vector
static double hmin_double_avx(__m256d v) {
    __m128d vlow  = _mm256_castpd256_pd128(v);
    __m128d vhigh = _mm256_extractf128_pd(v, 1);
    vlow  = _mm_min_pd(vlow, vhigh);
    
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    return _mm_cvtsd_f64(_mm_min_sd(vlow, high64));
}

// Horizontal maximum of a 256-bit vector
static double hmax_double_avx(__m256d v) {
    __m128d vlow  = _mm256_castpd256_pd128(v);
    __m128d vhigh = _mm256_extractf128_pd(v, 1);
    vlow  = _mm_max_pd(vlow, vhigh);
    
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    return _mm_cvtsd_f64(_mm_max_sd(vlow, high64));
}

// AVX512-optimized sum calculation (8 doubles at a time)
static double calculateSum_AVX512(const double* values, size_t count) {
    __m512d sum_vec = _mm512_setzero_pd();
    size_t simd_end = count - (count % 8);

    // Main SIMD loop - process 8 doubles at a time
    for (size_t i = 0; i < simd_end; i += 8) {
        __m512d vals = _mm512_loadu_pd(&values[i]);
        sum_vec = _mm512_add_pd(sum_vec, vals);
    }

    // Horizontal sum using AVX512 reduce
    double sum = _mm512_reduce_add_pd(sum_vec);

    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        sum += values[i];
    }

    return sum;
}

// AVX2-optimized sum calculation (4 doubles at a time)
static double calculateSum_AVX2(const double* values, size_t count) {
    __m256d sum_vec = _mm256_setzero_pd();
    size_t simd_end = count - (count % 4);

    // Main SIMD loop - process 4 doubles at a time
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d vals = _mm256_loadu_pd(&values[i]);
        sum_vec = _mm256_add_pd(sum_vec, vals);
    }

    double sum = hsum_double_avx(sum_vec);

    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        sum += values[i];
    }

    return sum;
}

// SIMD-optimized sum calculation with cascading fallback
double SimdAggregator::calculateSum(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();

    // Use AVX512 for best performance (process 8 doubles at once)
    if (isAvx512Available() && count >= 16) {
        return calculateSum_AVX512(values, count);
    }

    // Fall back to AVX2 (process 4 doubles at once)
    if (isAvx2Available() && count >= 8) {
        return calculateSum_AVX2(values, count);
    }

    // Fall back to scalar for small arrays or no SIMD support
    return scalar::calculateSum(values, count);
}

// SIMD-optimized average calculation
double SimdAggregator::calculateAvg(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();
    return calculateSum(values, count) / static_cast<double>(count);
}

// AVX512-optimized minimum calculation (8 doubles at a time)
static double calculateMin_AVX512(const double* values, size_t count) {
    __m512d min_vec = _mm512_loadu_pd(&values[0]);
    size_t simd_end = count - (count % 8);

    for (size_t i = 8; i < simd_end; i += 8) {
        __m512d vals = _mm512_loadu_pd(&values[i]);
        min_vec = _mm512_min_pd(min_vec, vals);
    }

    // Horizontal min using AVX512 reduce
    double min_val = _mm512_reduce_min_pd(min_vec);

    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        min_val = std::min(min_val, values[i]);
    }

    return min_val;
}

// AVX2-optimized minimum calculation (4 doubles at a time)
static double calculateMin_AVX2(const double* values, size_t count) {
    __m256d min_vec = _mm256_loadu_pd(&values[0]);
    size_t simd_end = count - (count % 4);

    for (size_t i = 4; i < simd_end; i += 4) {
        __m256d vals = _mm256_loadu_pd(&values[i]);
        min_vec = _mm256_min_pd(min_vec, vals);
    }

    double min_val = hmin_double_avx(min_vec);

    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        min_val = std::min(min_val, values[i]);
    }

    return min_val;
}

// SIMD-optimized minimum calculation with cascading fallback
double SimdAggregator::calculateMin(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();

    // Use AVX512 for best performance (process 8 doubles at once)
    if (isAvx512Available() && count >= 16) {
        return calculateMin_AVX512(values, count);
    }

    // Fall back to AVX2 (process 4 doubles at once)
    if (isAvx2Available() && count >= 8) {
        return calculateMin_AVX2(values, count);
    }

    // Fall back to scalar for small arrays or no SIMD support
    return scalar::calculateMin(values, count);
}

// AVX512-optimized maximum calculation (8 doubles at a time)
static double calculateMax_AVX512(const double* values, size_t count) {
    __m512d max_vec = _mm512_loadu_pd(&values[0]);
    size_t simd_end = count - (count % 8);

    for (size_t i = 8; i < simd_end; i += 8) {
        __m512d vals = _mm512_loadu_pd(&values[i]);
        max_vec = _mm512_max_pd(max_vec, vals);
    }

    // Horizontal max using AVX512 reduce
    double max_val = _mm512_reduce_max_pd(max_vec);

    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        max_val = std::max(max_val, values[i]);
    }

    return max_val;
}

// AVX2-optimized maximum calculation (4 doubles at a time)
static double calculateMax_AVX2(const double* values, size_t count) {
    __m256d max_vec = _mm256_loadu_pd(&values[0]);
    size_t simd_end = count - (count % 4);

    for (size_t i = 4; i < simd_end; i += 4) {
        __m256d vals = _mm256_loadu_pd(&values[i]);
        max_vec = _mm256_max_pd(max_vec, vals);
    }

    double max_val = hmax_double_avx(max_vec);

    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        max_val = std::max(max_val, values[i]);
    }

    return max_val;
}

// SIMD-optimized maximum calculation with cascading fallback
double SimdAggregator::calculateMax(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();

    // Use AVX512 for best performance (process 8 doubles at once)
    if (isAvx512Available() && count >= 16) {
        return calculateMax_AVX512(values, count);
    }

    // Fall back to AVX2 (process 4 doubles at once)
    if (isAvx2Available() && count >= 8) {
        return calculateMax_AVX2(values, count);
    }

    // Fall back to scalar for small arrays or no SIMD support
    return scalar::calculateMax(values, count);
}

// SIMD-optimized variance calculation
double SimdAggregator::calculateVariance(const double* values, size_t count, double mean) {
    if (count <= 1) return 0.0;
    
    if (!isAvx2Available() || count < 8) {
        return scalar::calculateVariance(values, count, mean);
    }
    
    __m256d mean_vec = _mm256_set1_pd(mean);
    __m256d sum_sq_diff = _mm256_setzero_pd();
    size_t simd_end = count - (count % 4);
    
    // Main SIMD loop - calculate sum of squared differences
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d vals = _mm256_loadu_pd(&values[i]);
        __m256d diff = _mm256_sub_pd(vals, mean_vec);
        __m256d sq_diff = _mm256_mul_pd(diff, diff);
        sum_sq_diff = _mm256_add_pd(sum_sq_diff, sq_diff);
    }
    
    double variance = hsum_double_avx(sum_sq_diff);
    
    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        double diff = values[i] - mean;
        variance += diff * diff;
    }
    
    return variance / (count - 1);
}

// SIMD-optimized dot product
double SimdAggregator::dotProduct(const double* a, const double* b, size_t count) {
    if (count == 0) return 0.0;
    
    if (!isAvx2Available() || count < 8) {
        double sum = 0.0;
        for (size_t i = 0; i < count; ++i) {
            sum += a[i] * b[i];
        }
        return sum;
    }
    
    __m256d sum_vec = _mm256_setzero_pd();
    size_t simd_end = count - (count % 4);
    
    // Main SIMD loop
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d a_vals = _mm256_loadu_pd(&a[i]);
        __m256d b_vals = _mm256_loadu_pd(&b[i]);
        __m256d prod = _mm256_mul_pd(a_vals, b_vals);
        sum_vec = _mm256_add_pd(sum_vec, prod);
    }
    
    double dot = hsum_double_avx(sum_vec);
    
    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        dot += a[i] * b[i];
    }
    
    return dot;
}

// Batch bucket sum calculation using SIMD
void SimdAggregator::calculateBucketSums(
    const double* values,
    const size_t* bucket_indices,
    size_t num_buckets,
    size_t values_per_bucket,
    double* bucket_sums) {
    
    if (!isAvx2Available()) {
        // Fallback to scalar
        for (size_t b = 0; b < num_buckets; ++b) {
            double sum = 0.0;
            size_t start = bucket_indices[b];
            size_t end = (b + 1 < num_buckets) ? bucket_indices[b + 1] : start + values_per_bucket;
            for (size_t i = start; i < end; ++i) {
                sum += values[i];
            }
            bucket_sums[b] = sum;
        }
        return;
    }
    
    // Process multiple buckets in parallel when possible
    for (size_t b = 0; b < num_buckets; ++b) {
        size_t start = bucket_indices[b];
        size_t end = (b + 1 < num_buckets) ? bucket_indices[b + 1] : start + values_per_bucket;
        size_t count = end - start;
        
        if (count > 0) {
            bucket_sums[b] = calculateSum(&values[start], count);
        } else {
            bucket_sums[b] = 0.0;
        }
    }
}

// Fast histogram computation for percentile calculations
void SimdAggregator::computeHistogram(
    const double* values,
    size_t count,
    double min_val,
    double max_val,
    size_t num_bins,
    uint32_t* histogram) {
    
    if (count == 0 || num_bins == 0) return;
    
    // Clear histogram
    std::memset(histogram, 0, num_bins * sizeof(uint32_t));
    
    double range = max_val - min_val;
    if (range <= 0) {
        histogram[0] = count;
        return;
    }
    
    double scale = (num_bins - 1) / range;
    
    if (!isAvx2Available() || count < 8) {
        // Scalar fallback
        for (size_t i = 0; i < count; ++i) {
            int bin = static_cast<int>((values[i] - min_val) * scale);
            bin = std::max(0, std::min(static_cast<int>(num_bins - 1), bin));
            histogram[bin]++;
        }
        return;
    }
    
    // SIMD version - process 4 values at a time
    __m256d min_vec = _mm256_set1_pd(min_val);
    __m256d scale_vec = _mm256_set1_pd(scale);
    __m256d zero_vec = _mm256_setzero_pd();
    __m256d max_bin_vec = _mm256_set1_pd(num_bins - 1);
    
    size_t simd_end = count - (count % 4);
    
    for (size_t i = 0; i < simd_end; i += 4) {
        __m256d vals = _mm256_loadu_pd(&values[i]);
        __m256d normalized = _mm256_sub_pd(vals, min_vec);
        __m256d scaled = _mm256_mul_pd(normalized, scale_vec);
        
        // Clamp to valid bin range
        scaled = _mm256_max_pd(scaled, zero_vec);
        scaled = _mm256_min_pd(scaled, max_bin_vec);
        
        // Convert to integers and update histogram
        alignas(32) double bins[4];
        _mm256_store_pd(bins, scaled);
        
        for (int j = 0; j < 4; ++j) {
            int bin = static_cast<int>(bins[j]);
            histogram[bin]++;
        }
    }
    
    // Handle remaining elements
    for (size_t i = simd_end; i < count; ++i) {
        int bin = static_cast<int>((values[i] - min_val) * scale);
        bin = std::max(0, std::min(static_cast<int>(num_bins - 1), bin));
        histogram[bin]++;
    }
}

// Scalar fallback implementations
namespace scalar {

double calculateSum(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();
    double sum = 0.0;
    for (size_t i = 0; i < count; ++i) {
        sum += values[i];
    }
    return sum;
}

double calculateAvg(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();
    return calculateSum(values, count) / static_cast<double>(count);
}

double calculateMin(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();
    
    double min_val = values[0];
    for (size_t i = 1; i < count; ++i) {
        min_val = std::min(min_val, values[i]);
    }
    return min_val;
}

double calculateMax(const double* values, size_t count) {
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();
    
    double max_val = values[0];
    for (size_t i = 1; i < count; ++i) {
        max_val = std::max(max_val, values[i]);
    }
    return max_val;
}

double calculateVariance(const double* values, size_t count, double mean) {
    if (count <= 1) return 0.0;
    
    double sum_sq_diff = 0.0;
    for (size_t i = 0; i < count; ++i) {
        double diff = values[i] - mean;
        sum_sq_diff += diff * diff;
    }
    return sum_sq_diff / (count - 1);
}

} // namespace scalar

} // namespace simd
} // namespace tsdb