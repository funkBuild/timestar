// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "storage/bloom_filter_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "bloom_filter_simd.hpp"

#include "hwy/highway.h"

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace bloom {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

void BitwiseAndInplaceKernel(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vd = hn::LoadU(d, dst + i);
        auto vs = hn::LoadU(d, src + i);
        hn::StoreU(hn::And(vd, vs), d, dst + i);
    }
    for (; i < count; ++i) {
        dst[i] &= src[i];
    }
}

void BitwiseOrInplaceKernel(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vd = hn::LoadU(d, dst + i);
        auto vs = hn::LoadU(d, src + i);
        hn::StoreU(hn::Or(vd, vs), d, dst + i);
    }
    for (; i < count; ++i) {
        dst[i] |= src[i];
    }
}

void BitwiseXorInplaceKernel(uint8_t* HWY_RESTRICT dst, const uint8_t* HWY_RESTRICT src, size_t count) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto vd = hn::LoadU(d, dst + i);
        auto vs = hn::LoadU(d, src + i);
        hn::StoreU(hn::Xor(vd, vs), d, dst + i);
    }
    for (; i < count; ++i) {
        dst[i] ^= src[i];
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace bloom
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch table + public API (compiled once)
// =============================================================================
#if HWY_ONCE

namespace timestar {
namespace bloom {
namespace simd {

HWY_EXPORT(BitwiseAndInplaceKernel);
HWY_EXPORT(BitwiseOrInplaceKernel);
HWY_EXPORT(BitwiseXorInplaceKernel);

void bitwiseAndInplace(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(BitwiseAndInplaceKernel)(dst, src, count);
}

void bitwiseOrInplace(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(BitwiseOrInplaceKernel)(dst, src, count);
}

void bitwiseXorInplace(uint8_t* dst, const uint8_t* src, size_t count) {
    HWY_DYNAMIC_DISPATCH(BitwiseXorInplaceKernel)(dst, src, count);
}

}  // namespace simd
}  // namespace bloom
}  // namespace timestar

#endif  // HWY_ONCE
