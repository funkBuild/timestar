// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "encoding/zigzag_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "hwy/highway.h"

#include <cstddef>
#include <cstdint>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace zigzag_simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// SIMD ZigZag encode: (x << 1) ^ (x >> 63) for each int64_t element.
void ZigZagEncodeBatch(const int64_t* HWY_RESTRICT input, uint64_t* HWY_RESTRICT output, size_t count) {
    const hn::ScalableTag<int64_t> di;
    const hn::ScalableTag<uint64_t> du;
    const size_t N = hn::Lanes(di);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(di, &input[i]);

        // ZigZag encode: (x << 1) ^ (x >> 63)
        // ShiftRight on signed int64_t is arithmetic (sign-extending) in Highway.
        auto shifted = hn::ShiftLeft<1>(v);
        auto sign = hn::ShiftRight<63>(v);
        auto zz = hn::BitCast(du, hn::Xor(shifted, sign));

        hn::StoreU(zz, du, &output[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        auto x = static_cast<uint64_t>(input[i]);
        output[i] = (x << 1) ^ static_cast<uint64_t>(input[i] >> 63);
    }
}

// SIMD ZigZag decode: (y >> 1) ^ -(y & 1) for each uint64_t element.
void ZigZagDecodeBatch(const uint64_t* HWY_RESTRICT input, int64_t* HWY_RESTRICT output, size_t count) {
    const hn::ScalableTag<uint64_t> du;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(du);

    auto one = hn::Set(du, 1ULL);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(du, &input[i]);

        // ZigZag decode: (y >> 1) ^ -(y & 1)
        auto shifted = hn::ShiftRight<1>(v);
        auto mask = hn::And(v, one);           // y & 1
        auto negmask = hn::Sub(hn::Zero(du), mask);  // -(y & 1)  i.e. 0 or 0xFFFF...
        auto decoded = hn::Xor(shifted, negmask);

        hn::StoreU(hn::BitCast(di, decoded), di, &output[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        uint64_t y = input[i];
        output[i] = static_cast<int64_t>((y >> 1) ^ -(y & 0x1));
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace zigzag_simd
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE

#include "zigzag_simd.hpp"

namespace zigzag_simd {

HWY_EXPORT(ZigZagEncodeBatch);
HWY_EXPORT(ZigZagDecodeBatch);

void zigzagEncodeSIMD(const int64_t* input, uint64_t* output, size_t count) {
    HWY_DYNAMIC_DISPATCH(ZigZagEncodeBatch)(input, output, count);
}

void zigzagDecodeSIMD(const uint64_t* input, int64_t* output, size_t count) {
    HWY_DYNAMIC_DISPATCH(ZigZagDecodeBatch)(input, output, count);
}

}  // namespace zigzag_simd

#endif  // HWY_ONCE
