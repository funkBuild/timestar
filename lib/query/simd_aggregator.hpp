#ifndef __SIMD_AGGREGATOR_H_INCLUDED__
#define __SIMD_AGGREGATOR_H_INCLUDED__

#include <immintrin.h>  // For AVX2 intrinsics
#include <vector>
#include <cstdint>
#include <limits>

namespace tsdb {
namespace simd {

// SIMD-optimized aggregation functions using AVX2 (256-bit vectors)
// These process 4 doubles at a time for significant speedup on large datasets

class SimdAggregator {
public:
    // Check if AVX2 is available at runtime
    static bool isAvx2Available();

    // Check if AVX512 is available at runtime
    static bool isAvx512Available();
    
    // SIMD-optimized sum calculation
    // ~4x faster than scalar for large arrays
    static double calculateSum(const double* values, size_t count);
    
    // SIMD-optimized average calculation
    static double calculateAvg(const double* values, size_t count);
    
    // SIMD-optimized minimum calculation
    // Uses parallel comparison to find min across 4 lanes
    static double calculateMin(const double* values, size_t count);
    
    // SIMD-optimized maximum calculation
    static double calculateMax(const double* values, size_t count);
    
    // SIMD-optimized variance calculation (for stddev)
    static double calculateVariance(const double* values, size_t count, double mean);
    
    // Batch operations for time-bucketed aggregation
    // Process multiple buckets in parallel
    static void calculateBucketSums(
        const double* values,
        const size_t* bucket_indices,  
        size_t num_buckets,
        size_t values_per_bucket,
        double* bucket_sums);
    
    // SIMD-optimized dot product (useful for weighted aggregations)
    static double dotProduct(const double* a, const double* b, size_t count);
    
    // Fast histogram computation for percentile calculations
    static void computeHistogram(
        const double* values,
        size_t count,
        double min_val,
        double max_val,
        size_t num_bins,
        uint32_t* histogram);
    
private:
    
    // Aligned memory allocation for optimal SIMD performance
    static double* alignedAlloc(size_t count);
    static void alignedFree(double* ptr);
};

// Fallback scalar implementations for when SIMD isn't available
namespace scalar {
    double calculateSum(const double* values, size_t count);
    double calculateAvg(const double* values, size_t count);
    double calculateMin(const double* values, size_t count);
    double calculateMax(const double* values, size_t count);
    double calculateVariance(const double* values, size_t count, double mean);
}

} // namespace simd
} // namespace tsdb

#endif // __SIMD_AGGREGATOR_H_INCLUDED__