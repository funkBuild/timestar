// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "query/transform/transform_functions_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "transform_functions_simd.hpp"

#include "hwy/highway.h"

#include <cmath>

HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace transform {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// ---------------------------------------------------------------------------
// 1. Abs -- clear sign bit
// ---------------------------------------------------------------------------
void Abs(const double* in, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        hn::StoreU(hn::Abs(v), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = std::abs(in[i]);
}

// ---------------------------------------------------------------------------
// 2. DefaultZero -- replace NaN with 0.0
// ---------------------------------------------------------------------------
void DefaultZero(const double* in, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto zero = hn::Zero(d);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto is_nan = hn::IsNaN(v);
        // Where NaN, use zero; otherwise keep v
        hn::StoreU(hn::IfThenElse(is_nan, zero, v), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = std::isnan(in[i]) ? 0.0 : in[i];
}

// ---------------------------------------------------------------------------
// 3. CountNonzero -- 1.0 if !NaN && !=0, else 0.0
// ---------------------------------------------------------------------------
void CountNonzero(const double* in, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto zero = hn::Zero(d);
    auto one = hn::Set(d, 1.0);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto not_nan = hn::Not(hn::IsNaN(v));
        auto not_zero = hn::Ne(v, zero);
        auto mask = hn::And(not_nan, not_zero);
        hn::StoreU(hn::IfThenElseZero(mask, one), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = (!std::isnan(in[i]) && in[i] != 0.0) ? 1.0 : 0.0;
}

// ---------------------------------------------------------------------------
// 4. CountNotNull -- 1.0 if !NaN, else 0.0
// ---------------------------------------------------------------------------
void CountNotNull(const double* in, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto one = hn::Set(d, 1.0);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto not_nan = hn::Not(hn::IsNaN(v));
        hn::StoreU(hn::IfThenElseZero(not_nan, one), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = std::isnan(in[i]) ? 0.0 : 1.0;
}

// ---------------------------------------------------------------------------
// 5. ClampMin -- max(v, min_val), preserving NaN
// ---------------------------------------------------------------------------
void ClampMin(const double* in, double min_val, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto min_vec = hn::Set(d, min_val);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto is_nan = hn::IsNaN(v);
        auto clamped = hn::Max(v, min_vec);
        // Preserve NaN where original was NaN
        hn::StoreU(hn::IfThenElse(is_nan, v, clamped), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = std::isnan(in[i]) ? in[i] : std::max(in[i], min_val);
}

// ---------------------------------------------------------------------------
// 6. ClampMax -- min(v, max_val), preserving NaN
// ---------------------------------------------------------------------------
void ClampMax(const double* in, double max_val, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto max_vec = hn::Set(d, max_val);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto is_nan = hn::IsNaN(v);
        auto clamped = hn::Min(v, max_vec);
        // Preserve NaN where original was NaN
        hn::StoreU(hn::IfThenElse(is_nan, v, clamped), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = std::isnan(in[i]) ? in[i] : std::min(in[i], max_val);
}

// ---------------------------------------------------------------------------
// 7. CutoffMin -- v < threshold => NaN, else keep
// ---------------------------------------------------------------------------
void CutoffMin(const double* in, double threshold, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto thresh_vec = hn::Set(d, threshold);
    auto nan_vec = hn::Set(d, std::nan(""));
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto is_nan = hn::IsNaN(v);
        auto above_thresh = hn::Ge(v, thresh_vec);
        // Keep original if: NaN OR >= threshold
        auto keep_mask = hn::Or(is_nan, above_thresh);
        hn::StoreU(hn::IfThenElse(keep_mask, v, nan_vec), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = (!std::isnan(in[i]) && in[i] < threshold) ? std::nan("") : in[i];
}

// ---------------------------------------------------------------------------
// 8. CutoffMax -- v > threshold => NaN, else keep
// ---------------------------------------------------------------------------
void CutoffMax(const double* in, double threshold, double* out, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto thresh_vec = hn::Set(d, threshold);
    auto nan_vec = hn::Set(d, std::nan(""));
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &in[i]);
        auto is_nan = hn::IsNaN(v);
        auto below_thresh = hn::Le(v, thresh_vec);
        // Keep original if: NaN OR <= threshold
        auto keep_mask = hn::Or(is_nan, below_thresh);
        hn::StoreU(hn::IfThenElse(keep_mask, v, nan_vec), d, &out[i]);
    }
    for (; i < count; ++i)
        out[i] = (!std::isnan(in[i]) && in[i] > threshold) ? std::nan("") : in[i];
}

// ---------------------------------------------------------------------------
// 9. Diff -- out[0]=NaN, out[i]=in[i]-in[i-1], NaN propagates
// ---------------------------------------------------------------------------
void Diff(const double* in, double* out, size_t count) {
    if (count == 0)
        return;
    out[0] = std::nan("");
    if (count < 2)
        return;

    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto nan_vec = hn::Set(d, std::nan(""));

    size_t i = 1;
    for (; i + N <= count; i += N) {
        auto curr = hn::LoadU(d, &in[i]);
        auto prev = hn::LoadU(d, &in[i - 1]);
        auto diff = hn::Sub(curr, prev);
        // If either input is NaN, result is NaN
        auto curr_nan = hn::IsNaN(curr);
        auto prev_nan = hn::IsNaN(prev);
        auto any_nan = hn::Or(curr_nan, prev_nan);
        hn::StoreU(hn::IfThenElse(any_nan, nan_vec, diff), d, &out[i]);
    }
    for (; i < count; ++i) {
        if (std::isnan(in[i]) || std::isnan(in[i - 1]))
            out[i] = std::nan("");
        else
            out[i] = in[i] - in[i - 1];
    }
}

// ---------------------------------------------------------------------------
// 10. MonotonicDiff -- like diff but negative => current value (counter reset)
// ---------------------------------------------------------------------------
void MonotonicDiff(const double* in, double* out, size_t count) {
    if (count == 0)
        return;
    out[0] = std::nan("");
    if (count < 2)
        return;

    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto nan_vec = hn::Set(d, std::nan(""));
    auto zero = hn::Zero(d);

    size_t i = 1;
    for (; i + N <= count; i += N) {
        auto curr = hn::LoadU(d, &in[i]);
        auto prev = hn::LoadU(d, &in[i - 1]);
        auto diff = hn::Sub(curr, prev);

        // If either input is NaN, result is NaN
        auto curr_nan = hn::IsNaN(curr);
        auto prev_nan = hn::IsNaN(prev);
        auto any_nan = hn::Or(curr_nan, prev_nan);

        // Counter reset: if diff < 0, use current value
        auto positive_mask = hn::Ge(diff, zero);
        auto reset_diff = hn::IfThenElse(positive_mask, diff, curr);

        hn::StoreU(hn::IfThenElse(any_nan, nan_vec, reset_diff), d, &out[i]);
    }
    for (; i < count; ++i) {
        if (std::isnan(in[i]) || std::isnan(in[i - 1])) {
            out[i] = std::nan("");
        } else {
            double dv = in[i] - in[i - 1];
            out[i] = (dv >= 0) ? dv : in[i];
        }
    }
}

// ---------------------------------------------------------------------------
// 11. MultiplyInplace -- v *= factor, preserving NaN
// ---------------------------------------------------------------------------
void MultiplyInplace(double* values, double factor, size_t count) {
    const hn::ScalableTag<double> d;
    const size_t N = hn::Lanes(d);
    auto factor_vec = hn::Set(d, factor);
    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &values[i]);
        auto is_nan = hn::IsNaN(v);
        auto scaled = hn::Mul(v, factor_vec);
        // Preserve NaN where original was NaN
        hn::StoreU(hn::IfThenElse(is_nan, v, scaled), d, &values[i]);
    }
    for (; i < count; ++i) {
        if (!std::isnan(values[i]))
            values[i] *= factor;
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace transform
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// ── Dispatch table + non-SIMD code (compiled once) ──────────────────────────
#if HWY_ONCE
namespace timestar {
namespace transform {
namespace simd {

HWY_EXPORT(Abs);
HWY_EXPORT(DefaultZero);
HWY_EXPORT(CountNonzero);
HWY_EXPORT(CountNotNull);
HWY_EXPORT(ClampMin);
HWY_EXPORT(ClampMax);
HWY_EXPORT(CutoffMin);
HWY_EXPORT(CutoffMax);
HWY_EXPORT(Diff);
HWY_EXPORT(MonotonicDiff);
HWY_EXPORT(MultiplyInplace);

std::vector<double> abs(const std::vector<double>& values) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::abs(values);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(Abs)(values.data(), result.data(), values.size());
    return result;
}

std::vector<double> default_zero(const std::vector<double>& values) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::default_zero(values);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(DefaultZero)(values.data(), result.data(), values.size());
    return result;
}

std::vector<double> count_nonzero(const std::vector<double>& values) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::count_nonzero(values);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(CountNonzero)(values.data(), result.data(), values.size());
    return result;
}

std::vector<double> count_not_null(const std::vector<double>& values) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::count_not_null(values);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(CountNotNull)(values.data(), result.data(), values.size());
    return result;
}

std::vector<double> clamp_min(const std::vector<double>& values, double minVal) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::clamp_min(values, minVal);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(ClampMin)(values.data(), minVal, result.data(), values.size());
    return result;
}

std::vector<double> clamp_max(const std::vector<double>& values, double maxVal) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::clamp_max(values, maxVal);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(ClampMax)(values.data(), maxVal, result.data(), values.size());
    return result;
}

std::vector<double> cutoff_min(const std::vector<double>& values, double threshold) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::cutoff_min(values, threshold);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(CutoffMin)(values.data(), threshold, result.data(), values.size());
    return result;
}

std::vector<double> cutoff_max(const std::vector<double>& values, double threshold) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::cutoff_max(values, threshold);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(CutoffMax)(values.data(), threshold, result.data(), values.size());
    return result;
}

std::vector<double> diff(const std::vector<double>& values) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::diff(values);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(Diff)(values.data(), result.data(), values.size());
    return result;
}

std::vector<double> monotonic_diff(const std::vector<double>& values) {
    if (values.size() < SIMD_MIN_SIZE)
        return scalar::monotonic_diff(values);
    std::vector<double> result(values.size());
    HWY_DYNAMIC_DISPATCH(MonotonicDiff)(values.data(), result.data(), values.size());
    return result;
}

void multiply_inplace(std::vector<double>& values, double factor) {
    if (values.size() < SIMD_MIN_SIZE) {
        scalar::multiply_inplace(values, factor);
        return;
    }
    HWY_DYNAMIC_DISPATCH(MultiplyInplace)(values.data(), factor, values.size());
}

}  // namespace simd
}  // namespace transform
}  // namespace timestar
#endif  // HWY_ONCE
