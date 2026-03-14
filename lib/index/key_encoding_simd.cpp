// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "index/key_encoding_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "key_encoding_simd.hpp"

#include "hwy/highway.h"

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace index {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Scan for escape characters: backslash(\), comma(,), equals(=), space( )
// Returns index of first match, or len if none found.
size_t FindFirstEscapeCharKernel(const char* HWY_RESTRICT data, size_t len) {
    const hn::ScalableTag<uint8_t> d;
    const size_t N = hn::Lanes(d);

    const auto v_backslash = hn::Set(d, static_cast<uint8_t>('\\'));
    const auto v_comma = hn::Set(d, static_cast<uint8_t>(','));
    const auto v_equals = hn::Set(d, static_cast<uint8_t>('='));
    const auto v_space = hn::Set(d, static_cast<uint8_t>(' '));

    const auto* p = reinterpret_cast<const uint8_t*>(data);
    size_t i = 0;

    for (; i + N <= len; i += N) {
        auto v = hn::LoadU(d, p + i);

        // Check all four escape characters with OR
        auto match = hn::Or(
            hn::Or(hn::Eq(v, v_backslash), hn::Eq(v, v_comma)),
            hn::Or(hn::Eq(v, v_equals), hn::Eq(v, v_space)));

        if (!hn::AllFalse(d, match)) {
            // Find exact position of first match
            size_t idx = hn::FindFirstTrue(d, match);
            return i + idx;
        }
    }

    // Scalar tail
    for (; i < len; ++i) {
        char c = data[i];
        if (c == '\\' || c == ',' || c == '=' || c == ' ') {
            return i;
        }
    }

    return len;
}

}  // namespace HWY_NAMESPACE
}  // namespace simd
}  // namespace index
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// =============================================================================
// Dispatch (compiled once)
// =============================================================================
#if HWY_ONCE

namespace timestar::index::simd {

HWY_EXPORT(FindFirstEscapeCharKernel);

size_t findFirstEscapeChar(const char* data, size_t len) {
    return HWY_DYNAMIC_DISPATCH(FindFirstEscapeCharKernel)(data, len);
}

}  // namespace timestar::index::simd

#endif  // HWY_ONCE
