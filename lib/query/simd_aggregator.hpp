#pragma once

#include <cstdint>
#include <limits>
#include <vector>

namespace timestar {
namespace simd {

// SIMD-optimized aggregation functions using Google Highway.
// Highway automatically selects the best available ISA at runtime
// (AVX-512, AVX2, SSE4, NEON, etc.) via its foreach_target mechanism.

class SimdAggregator {
public:
    // Check if AVX2 is available at runtime.
    // With Highway, dispatch is handled automatically — always returns true.
    static inline bool isAvx2Available() { return true; }

    // Check if AVX512 is available at runtime.
    // With Highway, dispatch is handled automatically — always returns true.
    static inline bool isAvx512Available() { return true; }

    // SIMD-optimized sum calculation
    // ~4x faster than scalar for large arrays
    static double calculateSum(const double* values, size_t count);

    // SIMD-optimized average calculation
    static double calculateAvg(const double* values, size_t count);

    // SIMD-optimized minimum calculation
    // Uses parallel comparison to find min across vector lanes
    static double calculateMin(const double* values, size_t count);

    // SIMD-optimized maximum calculation
    static double calculateMax(const double* values, size_t count);

    // SIMD-optimized variance calculation (for stddev)
    static double calculateVariance(const double* values, size_t count, double mean);

    // Batch operations for time-bucketed aggregation
    // Process multiple buckets in parallel
    static void calculateBucketSums(const double* values, size_t total_values, const size_t* bucket_indices,
                                    size_t num_buckets, size_t values_per_bucket, double* bucket_sums);

    // SIMD-optimized dot product (useful for weighted aggregations)
    static double dotProduct(const double* a, const double* b, size_t count);

    // Fast histogram computation for percentile calculations
    static void computeHistogram(const double* values, size_t count, double min_val, double max_val, size_t num_bins,
                                 uint32_t* histogram);

};

// Fallback scalar implementations for when NaN is detected in min/max
namespace scalar {
double calculateSum(const double* values, size_t count);
double calculateAvg(const double* values, size_t count);
double calculateMin(const double* values, size_t count);
double calculateMax(const double* values, size_t count);
double calculateVariance(const double* values, size_t count, double mean);
}  // namespace scalar

}  // namespace simd
}  // namespace timestar
