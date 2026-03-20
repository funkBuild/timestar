#pragma once

#include <cstddef>
#include <cstdint>

namespace alp {
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

}  // namespace simd
}  // namespace alp
