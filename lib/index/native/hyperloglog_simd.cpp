// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "index/native/hyperloglog_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "hyperloglog_simd.hpp"

#include "hwy/highway.h"

#include <cstring>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace index {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Element-wise max of uint8_t arrays: dst[i] = max(dst[i], src[i]).
// Processes full SIMD vectors (16/32/64 bytes) per iteration.
void MergeRegisters(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto a = hn::LoadU(d, &dst[i]);
        auto b = hn::LoadU(d, &src[i]);
        hn::StoreU(hn::Max(a, b), d, &dst[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        if (src[i] > dst[i]) dst[i] = src[i];
    }
}

// Clamp uint8_t registers: registers[i] = min(registers[i], max_val).
void ClampRegisters(uint8_t* HWY_RESTRICT registers, size_t count, uint8_t max_val) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);
    const auto limit = hn::Set(d, max_val);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(d, &registers[i]);
        hn::StoreU(hn::Min(v, limit), d, &registers[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        if (registers[i] > max_val) registers[i] = max_val;
    }
}

// Harmonic sum for HLL estimate: sum += 1.0 / (1ULL << registers[i]).
// Uses a precomputed LUT indexed by register value.
// Also counts zero-valued registers for linear counting correction.
void EstimateSum(const uint8_t* HWY_RESTRICT registers, size_t count,
                 double* HWY_RESTRICT sum_out, int* HWY_RESTRICT zero_count) {
    // Precomputed LUT: lut[r] = 1.0 / (1ULL << r) = 2^(-r) for r in [0, 63].
    // Registers are clamped to [0, 51] before this is called, but we fill 64
    // entries for safety and alignment.
    static constexpr double kLut[64] = {
        1.0,                    // 2^0
        0.5,                    // 2^-1
        0.25,                   // 2^-2
        0.125,                  // 2^-3
        0.0625,                 // 2^-4
        0.03125,                // 2^-5
        0.015625,               // 2^-6
        0.0078125,              // 2^-7
        0.00390625,             // 2^-8
        0.001953125,            // 2^-9
        0.0009765625,           // 2^-10
        0.00048828125,          // 2^-11
        0.000244140625,         // 2^-12
        0.0001220703125,        // 2^-13
        6.103515625e-05,        // 2^-14
        3.0517578125e-05,       // 2^-15
        1.52587890625e-05,      // 2^-16
        7.62939453125e-06,      // 2^-17
        3.814697265625e-06,     // 2^-18
        1.9073486328125e-06,    // 2^-19
        9.5367431640625e-07,    // 2^-20
        4.76837158203125e-07,   // 2^-21
        2.384185791015625e-07,  // 2^-22
        1.1920928955078125e-07, // 2^-23
        5.960464477539063e-08,  // 2^-24
        2.9802322387695312e-08, // 2^-25
        1.4901161193847656e-08, // 2^-26
        7.450580596923828e-09,  // 2^-27
        3.725290298461914e-09,  // 2^-28
        1.862645149230957e-09,  // 2^-29
        9.313225746154785e-10,  // 2^-30
        4.656612873077393e-10,  // 2^-31
        2.3283064365386963e-10, // 2^-32
        1.1641532182693481e-10, // 2^-33
        5.820766091346741e-11,  // 2^-34
        2.9103830456733704e-11, // 2^-35
        1.4551915228366852e-11, // 2^-36
        7.275957614183426e-12,  // 2^-37
        3.637978807091713e-12,  // 2^-38
        1.8189894035458565e-12, // 2^-39
        9.094947017729282e-13,  // 2^-40
        4.547473508864641e-13,  // 2^-41
        2.2737367544323206e-13, // 2^-42
        1.1368683772161603e-13, // 2^-43
        5.684341886080802e-14,  // 2^-44
        2.842170943040401e-14,  // 2^-45
        1.4210854715202004e-14, // 2^-46
        7.105427357601002e-15,  // 2^-47
        3.552713678800501e-15,  // 2^-48
        1.7763568394002505e-15, // 2^-49
        8.881784197001252e-16,  // 2^-50
        4.440892098500626e-16,  // 2^-51
        2.220446049250313e-16,  // 2^-52
        1.1102230246251565e-16, // 2^-53
        5.551115123125783e-17,  // 2^-54
        2.7755575615628914e-17, // 2^-55
        1.3877787807814457e-17, // 2^-56
        6.938893903907228e-18,  // 2^-57
        3.469446951953614e-18,  // 2^-58
        1.734723475976807e-18,  // 2^-59
        8.673617379884035e-19,  // 2^-60
        4.336808689942018e-19,  // 2^-61
        2.168404344971009e-19,  // 2^-62
        1.0842021724855044e-19, // 2^-63
    };

    const hn::ScalableTag<double> dd;
    const size_t ND = hn::Lanes(dd);  // doubles per SIMD vector (2, 4, or 8)

    auto sum_vec = hn::Zero(dd);
    int zeros = 0;

    // We process ND uint8_t values at a time, doing a scalar LUT gather
    // into an aligned buffer, then SIMD-accumulating the doubles.
    static constexpr size_t kMaxLanes = HWY_MAX_LANES_D(hn::ScalableTag<double>);
    alignas(64) double buf[kMaxLanes];

    size_t i = 0;
    for (; i + ND <= count; i += ND) {
        // Scalar gather from LUT (the LUT is L1-hot after first access)
        for (size_t j = 0; j < ND; ++j) {
            buf[j] = kLut[registers[i + j]];
        }
        auto vals = hn::Load(dd, buf);
        sum_vec = hn::Add(sum_vec, vals);

        // Count zeros — branch-free: compare each byte
        for (size_t j = 0; j < ND; ++j) {
            zeros += (registers[i + j] == 0);
        }
    }

    double sum = hn::ReduceSum(dd, sum_vec);

    // Scalar tail
    for (; i < count; ++i) {
        sum += kLut[registers[i]];
        zeros += (registers[i] == 0);
    }

    *sum_out = sum;
    *zero_count = zeros;
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace index
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE
namespace timestar {
namespace index {
namespace simd {

HWY_EXPORT(MergeRegisters);
HWY_EXPORT(ClampRegisters);
HWY_EXPORT(EstimateSum);

void hllMergeRegisters(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(MergeRegisters)(dst, src, count);
}

void hllClampRegisters(uint8_t* registers, size_t count, uint8_t max_val) {
    HWY_DYNAMIC_DISPATCH(ClampRegisters)(registers, count, max_val);
}

void hllEstimateSum(const uint8_t* registers, size_t count, double* sum_out, int* zero_count) {
    HWY_DYNAMIC_DISPATCH(EstimateSum)(registers, count, sum_out, zero_count);
}

}  // namespace simd
}  // namespace index
}  // namespace timestar
#endif  // HWY_ONCE
