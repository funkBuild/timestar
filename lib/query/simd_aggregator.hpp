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
    // SIMD-optimized sum calculation
    // ~4x faster than scalar for large arrays
    static double calculateSum(const double* values, size_t count);
    // Fused single-pass sum + NaN-skipping min/max (one memory pass instead
    // of three kernel calls). Sum propagates NaN like calculateSum; min/max
    // skip NaN like calculateMin/calculateMax.
    static void calculateSumMinMax(const double* values, size_t count, double& outSum, double& outMin, double& outMax);

    // SIMD-optimized minimum calculation
    // Uses parallel comparison to find min across vector lanes
    static double calculateMin(const double* values, size_t count);

    // SIMD-optimized maximum calculation
    static double calculateMax(const double* values, size_t count);

    // SIMD-optimized variance calculation (for stddev)
    static double calculateVariance(const double* values, size_t count, double mean);

    // SIMD-optimized dot product (useful for weighted aggregations)
    static double dotProduct(const double* a, const double* b, size_t count);
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
