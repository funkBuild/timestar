// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "query/simd_aggregator.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "simd_aggregator.hpp"

#include "hwy/highway.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// SIMD-optimized sum: accumulate doubles in vector lanes, then horizontal reduce.
// Unrolled ×4 with independent accumulators to hide FP add latency (a single
// loop-carried accumulator limits throughput to 1/latency of the add unit).
double CalculateSum(const double* values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto sum0 = hn::Zero(d);
    auto sum1 = hn::Zero(d);
    auto sum2 = hn::Zero(d);
    auto sum3 = hn::Zero(d);
    size_t i = 0;

    for (; i + 4 * N <= count; i += 4 * N) {
        sum0 = hn::Add(sum0, hn::LoadU(d, &values[i]));
        sum1 = hn::Add(sum1, hn::LoadU(d, &values[i + N]));
        sum2 = hn::Add(sum2, hn::LoadU(d, &values[i + 2 * N]));
        sum3 = hn::Add(sum3, hn::LoadU(d, &values[i + 3 * N]));
    }
    sum0 = hn::Add(hn::Add(sum0, sum1), hn::Add(sum2, sum3));

    for (; i + N <= count; i += N) {
        sum0 = hn::Add(sum0, hn::LoadU(d, &values[i]));
    }

    double sum = hn::ReduceSum(d, sum0);

    // Scalar tail
    for (; i < count; ++i) {
        sum += values[i];
    }

    return sum;
}

// SIMD-optimized minimum, NaN-skipping in a single fused pass: NaN lanes are
// replaced with +inf (the identity for min) so no separate NaN pre-scan is
// needed. If the result is +inf the input was all-NaN (or genuinely all +inf);
// the dispatch level resolves that ambiguity via the scalar path.
double CalculateMinSkipNaN(const double* values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto identity = hn::Set(d, std::numeric_limits<double>::infinity());

    auto min0 = identity;
    auto min1 = identity;
    auto min2 = identity;
    auto min3 = identity;
    size_t i = 0;

    for (; i + 4 * N <= count; i += 4 * N) {
        auto v0 = hn::LoadU(d, &values[i]);
        auto v1 = hn::LoadU(d, &values[i + N]);
        auto v2 = hn::LoadU(d, &values[i + 2 * N]);
        auto v3 = hn::LoadU(d, &values[i + 3 * N]);
        min0 = hn::Min(min0, hn::IfThenElse(hn::IsNaN(v0), identity, v0));
        min1 = hn::Min(min1, hn::IfThenElse(hn::IsNaN(v1), identity, v1));
        min2 = hn::Min(min2, hn::IfThenElse(hn::IsNaN(v2), identity, v2));
        min3 = hn::Min(min3, hn::IfThenElse(hn::IsNaN(v3), identity, v3));
    }
    min0 = hn::Min(hn::Min(min0, min1), hn::Min(min2, min3));

    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &values[i]);
        min0 = hn::Min(min0, hn::IfThenElse(hn::IsNaN(v), identity, v));
    }

    double min_val = hn::ReduceMin(d, min0);

    // Scalar tail (NaN-skipping)
    for (; i < count; ++i) {
        if (!std::isnan(values[i]) && values[i] < min_val) {
            min_val = values[i];
        }
    }

    return min_val;
}

// SIMD-optimized maximum, NaN-skipping in a single fused pass (see CalculateMinSkipNaN).
// NaN lanes are replaced with -inf (the identity for max).
double CalculateMaxSkipNaN(const double* values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    const auto identity = hn::Set(d, -std::numeric_limits<double>::infinity());

    auto max0 = identity;
    auto max1 = identity;
    auto max2 = identity;
    auto max3 = identity;
    size_t i = 0;

    for (; i + 4 * N <= count; i += 4 * N) {
        auto v0 = hn::LoadU(d, &values[i]);
        auto v1 = hn::LoadU(d, &values[i + N]);
        auto v2 = hn::LoadU(d, &values[i + 2 * N]);
        auto v3 = hn::LoadU(d, &values[i + 3 * N]);
        max0 = hn::Max(max0, hn::IfThenElse(hn::IsNaN(v0), identity, v0));
        max1 = hn::Max(max1, hn::IfThenElse(hn::IsNaN(v1), identity, v1));
        max2 = hn::Max(max2, hn::IfThenElse(hn::IsNaN(v2), identity, v2));
        max3 = hn::Max(max3, hn::IfThenElse(hn::IsNaN(v3), identity, v3));
    }
    max0 = hn::Max(hn::Max(max0, max1), hn::Max(max2, max3));

    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &values[i]);
        max0 = hn::Max(max0, hn::IfThenElse(hn::IsNaN(v), identity, v));
    }

    double max_val = hn::ReduceMax(d, max0);

    // Scalar tail (NaN-skipping)
    for (; i < count; ++i) {
        if (!std::isnan(values[i]) && values[i] > max_val) {
            max_val = values[i];
        }
    }

    return max_val;
}

// SIMD-optimized variance: sum of squared differences from mean.
double CalculateVariance(const double* values, size_t count, double mean) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto mean_vec = hn::Set(d, mean);
    auto sum_sq_diff = hn::Zero(d);
    size_t i = 0;

    // Main SIMD loop: compute (value - mean)^2 using MulAdd
    for (; i + N <= count; i += N) {
        auto vals = hn::LoadU(d, &values[i]);
        auto diff = hn::Sub(vals, mean_vec);
        sum_sq_diff = hn::MulAdd(diff, diff, sum_sq_diff);
    }

    double variance = hn::ReduceSum(d, sum_sq_diff);

    // Scalar tail
    for (; i < count; ++i) {
        double diff = values[i] - mean;
        variance += diff * diff;
    }

    return variance / static_cast<double>(count);
}

// SIMD-optimized dot product: a[i] * b[i] accumulated via MulAdd.
double DotProduct(const double* a, const double* b, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto sum_vec = hn::Zero(d);
    size_t i = 0;

    // Main SIMD loop
    for (; i + N <= count; i += N) {
        auto a_vals = hn::LoadU(d, &a[i]);
        auto b_vals = hn::LoadU(d, &b[i]);
        sum_vec = hn::MulAdd(a_vals, b_vals, sum_vec);
    }

    double dot = hn::ReduceSum(d, sum_vec);

    // Scalar tail
    for (; i < count; ++i) {
        dot += a[i] * b[i];
    }

    return dot;
}

// SIMD-accelerated histogram bin assignment.
// Computes bin indices in SIMD, then scatters to histogram bins in scalar
// (scatter is inherently serial due to potential bin conflicts).
void ComputeHistogram(const double* values, size_t count, double min_val, double scale, size_t num_bins,
                      uint32_t* histogram) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto min_vec = hn::Set(d, min_val);
    auto scale_vec = hn::Set(d, scale);
    auto zero_vec = hn::Zero(d);
    auto max_bin_vec = hn::Set(d, static_cast<double>(num_bins - 1));

    // Scratch buffer for storing computed bin values.
    // Use HWY_MAX_LANES_D to support variable-width SIMD (including ARM SVE).
    static constexpr size_t kMaxLanes = HWY_MAX_LANES_D(hn::ScalableTag<double>);
    alignas(64) double bins[kMaxLanes];

    size_t i = 0;

    // Main SIMD loop
    for (; i + N <= count; i += N) {
        auto vals = hn::LoadU(d, &values[i]);
        auto normalized = hn::Sub(vals, min_vec);
        auto scaled = hn::Mul(normalized, scale_vec);

        // Clamp to valid bin range [0, num_bins-1]
        scaled = hn::Max(scaled, zero_vec);
        scaled = hn::Min(scaled, max_bin_vec);

        hn::StoreU(scaled, d, bins);

        // Scalar scatter into histogram bins
        for (size_t j = 0; j < N; ++j) {
            int bin = static_cast<int>(bins[j]);
            histogram[bin]++;
        }
    }

    // Scalar tail — clamp the double BEFORE casting to int to avoid UB.
    // (A double outside [INT_MIN, INT_MAX] has undefined behavior on int cast.)
    const double max_bin_d = static_cast<double>(num_bins - 1);
    for (; i < count; ++i) {
        double bin_d = (values[i] - min_val) * scale;
        bin_d = std::max(0.0, std::min(max_bin_d, bin_d));
        int bin = static_cast<int>(bin_d);
        histogram[bin]++;
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE
namespace timestar {
namespace simd {

HWY_EXPORT(CalculateSum);
HWY_EXPORT(CalculateMinSkipNaN);
HWY_EXPORT(CalculateMaxSkipNaN);
HWY_EXPORT(CalculateVariance);
HWY_EXPORT(DotProduct);
HWY_EXPORT(ComputeHistogram);

// --- SimdAggregator static method implementations (dispatch via Highway) ---

double SimdAggregator::calculateSum(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();
    return HWY_DYNAMIC_DISPATCH(CalculateSum)(values, count);
}

double SimdAggregator::calculateAvg(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();
    return calculateSum(values, count) / static_cast<double>(count);
}

double SimdAggregator::calculateMin(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();

    // Fused single-pass kernel: NaN lanes are masked to +inf (min identity),
    // so no separate NaN pre-scan is needed. A +inf result is ambiguous
    // (all-NaN input vs genuine +inf values) — resolve via the scalar path,
    // which returns NaN for all-NaN input.
    double result = HWY_DYNAMIC_DISPATCH(CalculateMinSkipNaN)(values, count);
    if (HWY_UNLIKELY(result == std::numeric_limits<double>::infinity())) {
        return scalar::calculateMin(values, count);
    }
    return result;
}

double SimdAggregator::calculateMax(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();

    // Fused single-pass kernel: NaN lanes are masked to -inf (max identity).
    // A -inf result is ambiguous (all-NaN vs genuine -inf) — resolve via scalar.
    double result = HWY_DYNAMIC_DISPATCH(CalculateMaxSkipNaN)(values, count);
    if (HWY_UNLIKELY(result == -std::numeric_limits<double>::infinity())) {
        return scalar::calculateMax(values, count);
    }
    return result;
}

double SimdAggregator::calculateVariance(const double* values, size_t count, double mean) {
    if (count <= 1)
        return 0.0;
    return HWY_DYNAMIC_DISPATCH(CalculateVariance)(values, count, mean);
}

void SimdAggregator::calculateBucketSums(const double* values, size_t total_values, const size_t* bucket_indices,
                                         size_t num_buckets, size_t values_per_bucket, double* bucket_sums) {
    for (size_t b = 0; b < num_buckets; ++b) {
        size_t start = bucket_indices[b];
        size_t end = (b + 1 < num_buckets) ? bucket_indices[b + 1] : start + values_per_bucket;
        end = std::min(end, total_values);
        size_t count = (end > start) ? end - start : 0;

        if (count > 0) {
            bucket_sums[b] = calculateSum(&values[start], count);
        } else {
            bucket_sums[b] = 0.0;
        }
    }
}

double SimdAggregator::dotProduct(const double* a, const double* b, size_t count) {
    if (count == 0)
        return 0.0;
    return HWY_DYNAMIC_DISPATCH(DotProduct)(a, b, count);
}

void SimdAggregator::computeHistogram(const double* values, size_t count, double min_val, double max_val,
                                      size_t num_bins, uint32_t* histogram) {
    if (count == 0 || num_bins == 0)
        return;

    // Clear histogram
    std::memset(histogram, 0, num_bins * sizeof(uint32_t));

    double range = max_val - min_val;
    if (range <= 0) {
        histogram[0] = static_cast<uint32_t>(std::min(count, static_cast<size_t>(UINT32_MAX)));
        return;
    }

    double scale = (num_bins - 1) / range;

    HWY_DYNAMIC_DISPATCH(ComputeHistogram)(values, count, min_val, scale, num_bins, histogram);
}

// --- Scalar fallback implementations ---

namespace scalar {

double calculateSum(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();
    double sum = 0.0;
    for (size_t i = 0; i < count; ++i) {
        sum += values[i];
    }
    return sum;
}

double calculateAvg(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();
    return calculateSum(values, count) / static_cast<double>(count);
}

double calculateMin(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();

    // Skip leading NaN values to find first real value
    size_t start = 0;
    while (start < count && std::isnan(values[start]))
        ++start;
    if (start == count)
        return std::numeric_limits<double>::quiet_NaN();

    double min_val = values[start];
    for (size_t i = start + 1; i < count; ++i) {
        if (!std::isnan(values[i]) && values[i] < min_val) {
            min_val = values[i];
        }
    }
    return min_val;
}

double calculateMax(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();

    // Skip leading NaN values to find first real value
    size_t start = 0;
    while (start < count && std::isnan(values[start]))
        ++start;
    if (start == count)
        return std::numeric_limits<double>::quiet_NaN();

    double max_val = values[start];
    for (size_t i = start + 1; i < count; ++i) {
        if (!std::isnan(values[i]) && values[i] > max_val) {
            max_val = values[i];
        }
    }
    return max_val;
}

double calculateVariance(const double* values, size_t count, double mean) {
    if (count <= 1)
        return 0.0;

    double sum_sq_diff = 0.0;
    for (size_t i = 0; i < count; ++i) {
        double diff = values[i] - mean;
        sum_sq_diff += diff * diff;
    }
    return sum_sq_diff / count;  // population variance (consistent with AggregationState)
}

}  // namespace scalar

}  // namespace simd
}  // namespace timestar
#endif  // HWY_ONCE
