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

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// SIMD-optimized sum: accumulate doubles in vector lanes, then horizontal reduce.
double CalculateSum(const double* values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto sum_vec = hn::Zero(d);
    size_t i = 0;

    // Main SIMD loop
    for (; i + N <= count; i += N) {
        auto vals = hn::LoadU(d, &values[i]);
        sum_vec = hn::Add(sum_vec, vals);
    }

    double sum = hn::ReduceSum(d, sum_vec);

    // Scalar tail
    for (; i < count; ++i) {
        sum += values[i];
    }

    return sum;
}

// SIMD-optimized minimum: parallel min reduction across vector lanes.
// Caller must ensure no NaN values are present (NaN handling is done at dispatch level).
double CalculateMin(const double* values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto min_vec = hn::Set(d, values[0]);
    size_t i = 0;

    // Main SIMD loop
    for (; i + N <= count; i += N) {
        auto vals = hn::LoadU(d, &values[i]);
        min_vec = hn::Min(min_vec, vals);
    }

    double min_val = hn::ReduceMin(d, min_vec);

    // Scalar tail
    for (; i < count; ++i) {
        min_val = std::min(min_val, values[i]);
    }

    return min_val;
}

// SIMD-optimized maximum: parallel max reduction across vector lanes.
// Caller must ensure no NaN values are present (NaN handling is done at dispatch level).
double CalculateMax(const double* values, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);

    auto max_vec = hn::Set(d, values[0]);
    size_t i = 0;

    // Main SIMD loop
    for (; i + N <= count; i += N) {
        auto vals = hn::LoadU(d, &values[i]);
        max_vec = hn::Max(max_vec, vals);
    }

    double max_val = hn::ReduceMax(d, max_vec);

    // Scalar tail
    for (; i < count; ++i) {
        max_val = std::max(max_val, values[i]);
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

    // Scratch buffer for storing computed bin values (max 8 lanes for AVX-512)
    alignas(64) double bins[8];

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

    // Scalar tail
    for (; i < count; ++i) {
        int bin = static_cast<int>((values[i] - min_val) * scale);
        bin = std::max(0, std::min(static_cast<int>(num_bins - 1), bin));
        histogram[bin]++;
    }
}

// Element-wise addition: dst[i] += src[i]
void AddArrays(double* HWY_RESTRICT dst, const double* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto d_vec = hn::LoadU(d, &dst[i]);
        auto s_vec = hn::LoadU(d, &src[i]);
        hn::StoreU(hn::Add(d_vec, s_vec), d, &dst[i]);
    }
    for (; i < count; ++i) dst[i] += src[i];
}

// Element-wise minimum: dst[i] = min(dst[i], src[i])
void MinArrays(double* HWY_RESTRICT dst, const double* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto d_vec = hn::LoadU(d, &dst[i]);
        auto s_vec = hn::LoadU(d, &src[i]);
        hn::StoreU(hn::Min(d_vec, s_vec), d, &dst[i]);
    }
    for (; i < count; ++i) dst[i] = std::min(dst[i], src[i]);
}

// Element-wise maximum: dst[i] = max(dst[i], src[i])
void MaxArrays(double* HWY_RESTRICT dst, const double* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto d_vec = hn::LoadU(d, &dst[i]);
        auto s_vec = hn::LoadU(d, &src[i]);
        hn::StoreU(hn::Max(d_vec, s_vec), d, &dst[i]);
    }
    for (; i < count; ++i) dst[i] = std::max(dst[i], src[i]);
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
HWY_EXPORT(CalculateMin);
HWY_EXPORT(CalculateMax);
HWY_EXPORT(CalculateVariance);
HWY_EXPORT(DotProduct);
HWY_EXPORT(ComputeHistogram);
HWY_EXPORT(AddArrays);
HWY_EXPORT(MinArrays);
HWY_EXPORT(MaxArrays);

// Quick scan for any NaN in the input array.
// Used to guard SIMD min/max paths whose intrinsics have undefined NaN behavior.
static bool containsNaN(const double* values, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        if (HWY_UNLIKELY(std::isnan(values[i])))
            return true;
    }
    return false;
}

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

    // SIMD min has undefined behavior with NaN inputs.
    // If any NaN is present, fall through to the scalar path which correctly skips NaN.
    if (HWY_UNLIKELY(containsNaN(values, count))) {
        return scalar::calculateMin(values, count);
    }

    return HWY_DYNAMIC_DISPATCH(CalculateMin)(values, count);
}

double SimdAggregator::calculateMax(const double* values, size_t count) {
    if (count == 0)
        return std::numeric_limits<double>::quiet_NaN();

    // SIMD max has undefined behavior with NaN inputs.
    // If any NaN is present, fall through to the scalar path which correctly skips NaN.
    if (HWY_UNLIKELY(containsNaN(values, count))) {
        return scalar::calculateMax(values, count);
    }

    return HWY_DYNAMIC_DISPATCH(CalculateMax)(values, count);
}

double SimdAggregator::calculateVariance(const double* values, size_t count, double mean) {
    if (count <= 1)
        return 0.0;
    return HWY_DYNAMIC_DISPATCH(CalculateVariance)(values, count, mean);
}

void SimdAggregator::calculateBucketSums(const double* values, const size_t* bucket_indices, size_t num_buckets,
                                         size_t values_per_bucket, double* bucket_sums) {
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
        histogram[0] = count;
        return;
    }

    double scale = (num_bins - 1) / range;

    HWY_DYNAMIC_DISPATCH(ComputeHistogram)(values, count, min_val, scale, num_bins, histogram);
}

void SimdAggregator::addArrays(double* dst, const double* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(AddArrays)(dst, src, count);
}

void SimdAggregator::minArrays(double* dst, const double* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(MinArrays)(dst, src, count);
}

void SimdAggregator::maxArrays(double* dst, const double* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(MaxArrays)(dst, src, count);
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
