#pragma once

#include <cstddef>
#include <cstdint>

namespace timestar::index::simd {

// SIMD-accelerated harmonic sum for HLL estimate.
// Returns sum of 1.0 / (1 << registers[i]) for i in [0, count), plus
// the count of zero-valued registers via *zero_count.
// Uses a precomputed LUT + SIMD gather to avoid per-element branching.
void hllEstimateSum(const uint8_t* registers, size_t count, double* sum_out, int* zero_count);

// SIMD-accelerated register clamp: registers[i] = min(registers[i], max_val)
void hllClampRegisters(uint8_t* registers, size_t count, uint8_t max_val);

}  // namespace timestar::index::simd
