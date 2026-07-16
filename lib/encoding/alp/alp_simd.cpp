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

#include <bit>
#include <cmath>
#include <limits>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar::alp {
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
void AlpReconstructKernel(const int64_t* HWY_RESTRICT encoded, size_t count, double frac_val, double fact_val,
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
// ALP scale loop (fac == 0 only): single-pass, mask-based.
//
// Per vector of doubles v:
//   scaled  = v * fact          (fact = 10^exp)
//   rounded = Round(scaled)
//   enc     = (int64)rounded
//   dec     = (double)enc * 1.0 / fact   -- EXACT mul-then-div order to match
//                                           the scalar scaleValue round-trip
//   exact   = (dec == v) && |rounded| <= 2^53 && !(v is -0.0)
//
// NaN/inf fail the Eq / range checks naturally; -0.0 must be masked explicitly
// because Eq(+0.0, -0.0) is true in IEEE 754. enc is stored for EVERY lane
// (exception lanes hold junk that the caller overwrites with the FOR base);
// min/max accumulate over exact lanes only via masked identity. Exceptions
// (rare) are harvested from the inverted mask bits scalar-side, in ascending
// position order.
//
// Only dispatched when the selected target has native i64<->f64 conversions
// (AVX3) -- see alpScaleSimdAvailable(). On lesser ISAs the emulated
// conversions made a June 2026 attempt 25% SLOWER than scalar.
// ---------------------------------------------------------------------------
size_t AlpScaleF0Kernel(const double* HWY_RESTRICT values, size_t count, double fact_val, int64_t* HWY_RESTRICT encoded,
                        int64_t* HWY_RESTRICT min_out, int64_t* HWY_RESTRICT max_out,
                        uint16_t* HWY_RESTRICT exc_positions, uint64_t* HWY_RESTRICT exc_values) {
    const hn::ScalableTag<double> dd;
    const hn::ScalableTag<int64_t> di;
    const hn::ScalableTag<uint64_t> du;
    const size_t N = hn::Lanes(dd);

    const auto fact_vec = hn::Set(dd, fact_val);
    const auto frac_vec = hn::Set(dd, 1.0);                 // FRAC_ARR[0]
    const auto max_safe = hn::Set(dd, 9007199254740992.0);  // 2^53 (inclusive, matches scalar)
    const auto neg_zero_bits = hn::Set(du, 0x8000000000000000ULL);

    const auto i64_max_vec = hn::Set(di, std::numeric_limits<int64_t>::max());
    const auto i64_min_vec = hn::Set(di, std::numeric_limits<int64_t>::min());
    auto min_vec = i64_max_vec;
    auto max_vec = i64_min_vec;

    size_t n_exc = 0;
    size_t i = 0;

    // Mask bit buffer: f64 lanes <= 8 on x86 (AVX-512); 32 bytes covers any
    // target this codebase compiles for (up to 256 lanes).
    uint8_t mask_bits[32];

    for (; i + N <= count; i += N) {
        const auto v = hn::LoadU(dd, values + i);
        const auto scaled = hn::Mul(v, fact_vec);
        const auto rounded = hn::Round(scaled);
        const auto enc = hn::ConvertTo(di, rounded);
        const auto dec = hn::Div(hn::Mul(hn::ConvertTo(dd, enc), frac_vec), fact_vec);

        const auto is_neg_zero = hn::RebindMask(dd, hn::Eq(hn::BitCast(du, v), neg_zero_bits));
        const auto exact = hn::AndNot(is_neg_zero, hn::And(hn::Eq(dec, v), hn::Le(hn::Abs(rounded), max_safe)));

        hn::StoreU(enc, di, encoded + i);

        const auto mi = hn::RebindMask(di, exact);
        min_vec = hn::Min(min_vec, hn::IfThenElse(mi, enc, i64_max_vec));
        max_vec = hn::Max(max_vec, hn::IfThenElse(mi, enc, i64_min_vec));

        if (HWY_UNLIKELY(!hn::AllTrue(dd, exact))) {
            hn::StoreMaskBits(dd, exact, mask_bits);
            for (size_t j = 0; j < N; ++j) {
                if (!((mask_bits[j / 8] >> (j % 8)) & 1)) {
                    exc_positions[n_exc] = static_cast<uint16_t>(i + j);
                    exc_values[n_exc] = std::bit_cast<uint64_t>(values[i + j]);
                    ++n_exc;
                }
            }
        }
    }

    int64_t min_val = hn::ReduceMin(di, min_vec);
    int64_t max_val = hn::ReduceMax(di, max_vec);

    // Scalar tail: replicates the scalar scaleValue semantics for fac == 0.
    for (; i < count; ++i) {
        const double v = values[i];
        const uint64_t bits = std::bit_cast<uint64_t>(v);
        if (bits != 0x8000000000000000ULL && !std::isnan(v) && !std::isinf(v)) {
            const double rounded = std::round(v * fact_val);
            if (rounded <= 9007199254740992.0 && rounded >= -9007199254740992.0) {
                const int64_t e = static_cast<int64_t>(rounded);
                const double dec = static_cast<double>(e) * 1.0 / fact_val;
                if (dec == v) {
                    encoded[i] = e;
                    if (e < min_val)
                        min_val = e;
                    if (e > max_val)
                        max_val = e;
                    continue;
                }
            }
        }
        encoded[i] = 0;  // placeholder (caller rewrites exception slots to base)
        exc_positions[n_exc] = static_cast<uint16_t>(i);
        exc_values[n_exc] = bits;
        ++n_exc;
    }

    *min_out = min_val;
    *max_out = max_val;
    return n_exc;
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace timestar::alp
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE
namespace timestar::alp {
namespace simd {

HWY_EXPORT(AlpReconstructKernel);
HWY_EXPORT(AlpScaleF0Kernel);

void alpReconstruct(const int64_t* encoded, size_t count, double frac_val, double fact_val, double* out) {
    if (count == 0)
        return;
    HWY_DYNAMIC_DISPATCH(AlpReconstructKernel)(encoded, count, frac_val, fact_val, out);
}

bool alpScaleSimdAvailable() {
    // The masked single-pass scale kernel only pays off with native i64<->f64
    // conversions (AVX-512 DQ, i.e. Highway's AVX3 family). Emulated
    // conversions on lesser targets regressed 25% in a June 2026 attempt.
    static const bool available = (hwy::SupportedTargets() & HWY_AVX3) != 0;
    return available;
}

size_t alpScaleF0(const double* values, size_t count, double fact_val, int64_t* encoded, int64_t* min_out,
                  int64_t* max_out, uint16_t* exc_positions, uint64_t* exc_values) {
    return HWY_DYNAMIC_DISPATCH(AlpScaleF0Kernel)(values, count, fact_val, encoded, min_out, max_out, exc_positions,
                                                  exc_values);
}

}  // namespace simd
}  // namespace timestar::alp
#endif  // HWY_ONCE
