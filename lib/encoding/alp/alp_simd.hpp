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
void alpReconstruct(const int64_t* encoded, size_t count,
                    double frac_val, double fact_val, double* out);

// SIMD-accelerated FFOR base addition for int64 arrays.
// Computes out[i] = in[i] + base for `count` elements.
// Used for bw=0 (fill) and bw=64 (identity + offset) FFOR paths,
// and can be called after unpacking deltas to add back the FOR base.
void fforAddBase(const int64_t* in, size_t count, int64_t base, int64_t* out);

// SIMD-accelerated FFOR base addition for uint64 arrays (ALP_RD).
void fforAddBaseU64(const uint64_t* in, size_t count, uint64_t base, uint64_t* out);

}  // namespace simd
}  // namespace alp
