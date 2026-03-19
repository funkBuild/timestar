// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "alp/alp_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "alp/alp_simd.hpp"

#include "hwy/highway.h"

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace alp {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// ---------------------------------------------------------------------------
// ALP reconstruction: int64 -> double via (encoded * frac_val / fact_val)
//
// The int64->double conversion is the key win: on x86 without AVX-512 DQ,
// this requires a multi-instruction sequence that GCC cannot auto-vectorize.
// Highway handles this transparently across all ISA targets.
// ---------------------------------------------------------------------------
void AlpReconstructKernel(const int64_t* HWY_RESTRICT encoded, size_t count,
                          double frac_val, double fact_val,
                          double* HWY_RESTRICT out) {
    const hn::ScalableTag<double> dd;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(dd);

    const auto frac_vec = hn::Set(dd, frac_val);
    const auto fact_vec = hn::Set(dd, fact_val);

    size_t i = 0;

    // Main SIMD loop: convert int64 -> double, then multiply by frac, divide by fact
    for (; i + N <= count; i += N) {
        const auto ival = hn::LoadU(di, &encoded[i]);
        auto dval = hn::ConvertTo(dd, ival);
        dval = hn::Mul(dval, frac_vec);
        dval = hn::Div(dval, fact_vec);
        hn::StoreU(dval, dd, &out[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        out[i] = static_cast<double>(encoded[i]) * frac_val / fact_val;
    }
}

// ---------------------------------------------------------------------------
// FFOR base addition: out[i] = in[i] + base (int64)
// Used after bit-unpacking to add back the frame-of-reference minimum.
// ---------------------------------------------------------------------------
void FforAddBaseKernel(const int64_t* HWY_RESTRICT in, size_t count,
                       int64_t base, int64_t* HWY_RESTRICT out) {
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(di);

    const auto base_vec = hn::Set(di, base);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        const auto vals = hn::LoadU(di, &in[i]);
        hn::StoreU(hn::Add(vals, base_vec), di, &out[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        out[i] = in[i] + base;
    }
}

// ---------------------------------------------------------------------------
// FFOR base addition: out[i] = in[i] + base (uint64)
// ---------------------------------------------------------------------------
void FforAddBaseU64Kernel(const uint64_t* HWY_RESTRICT in, size_t count,
                          uint64_t base, uint64_t* HWY_RESTRICT out) {
    const hn::ScalableTag<uint64_t> du;
    const size_t N = hn::Lanes(du);

    const auto base_vec = hn::Set(du, base);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        const auto vals = hn::LoadU(du, &in[i]);
        hn::StoreU(hn::Add(vals, base_vec), du, &out[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        out[i] = in[i] + base;
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace alp
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE
namespace alp {
namespace simd {

HWY_EXPORT(AlpReconstructKernel);
HWY_EXPORT(FforAddBaseKernel);
HWY_EXPORT(FforAddBaseU64Kernel);

void alpReconstruct(const int64_t* encoded, size_t count,
                    double frac_val, double fact_val, double* out) {
    if (count == 0) return;
    HWY_DYNAMIC_DISPATCH(AlpReconstructKernel)(encoded, count, frac_val, fact_val, out);
}

void fforAddBase(const int64_t* in, size_t count, int64_t base, int64_t* out) {
    if (count == 0) return;
    HWY_DYNAMIC_DISPATCH(FforAddBaseKernel)(in, count, base, out);
}

void fforAddBaseU64(const uint64_t* in, size_t count, uint64_t base, uint64_t* out) {
    if (count == 0) return;
    HWY_DYNAMIC_DISPATCH(FforAddBaseU64Kernel)(in, count, base, out);
}

}  // namespace simd
}  // namespace alp
#endif  // HWY_ONCE
