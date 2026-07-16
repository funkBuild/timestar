#pragma once

#include <cstddef>
#include <cstdint>

namespace timestar::alp {
namespace simd {

// SIMD-accelerated ALP decode reconstruction: converts int64 encoded values
// to doubles via (int64 * frac_val / fact_val).  Uses Highway int64->double
// conversion + fused multiply-divide, which is substantially faster than
// scalar code (GCC cannot auto-vectorize the int64->double cast).
//
// Writes exactly `count` doubles to `out`.
// Caller must ensure `encoded` has at least `count` int64_t values and
// `out` has space for `count` doubles.
void alpReconstruct(const int64_t* encoded, size_t count, double frac_val, double fact_val, double* out);

// True when the dynamic-dispatch target has native i64<->f64 conversions
// (AVX-512 DQ / Highway AVX3). The scale kernel below must only be used when
// this returns true; emulated conversions on lesser ISAs are slower than the
// scalar loop.
bool alpScaleSimdAvailable();

// SIMD ALP scale loop for fac == 0: computes enc = (int64)Round(v * fact_val)
// for every value, verifies the round-trip ((double)enc * 1.0 / fact_val == v)
// per lane, and:
//   - writes enc to encoded[0..count) (exception slots hold placeholders the
//     caller must overwrite, matching the scalar path's behavior)
//   - returns min/max over exact values only (INT64_MAX/INT64_MIN when none)
//   - appends exceptions (ascending position, raw double bits) to
//     exc_positions/exc_values, which must have room for `count` entries
// Returns the number of exceptions. NaN, inf, -0.0, and out-of-range values
// are classified as exceptions exactly like the scalar scaleValue path.
size_t alpScaleF0(const double* values, size_t count, double fact_val, int64_t* encoded, int64_t* min_out,
                  int64_t* max_out, uint16_t* exc_positions, uint64_t* exc_values);

}  // namespace simd
}  // namespace timestar::alp
