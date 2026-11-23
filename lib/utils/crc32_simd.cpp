// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "utils/crc32_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "utils/crc32.hpp"

#include "hwy/highway.h"

// =============================================================================
// Carry-less-multiply CRC32 (compiled once per target ISA by foreach_target)
// =============================================================================
//
// Same checksum as the slicing-by-8 tables in crc32.hpp — polynomial
// 0xEDB88320, the zlib/PNG CRC32 — so nothing on disk changes. Instead of
// eight table lookups per 8 bytes, the message is folded 64 bytes at a time:
// four 128-bit accumulators are each multiplied (carry-less) by a constant that
// advances them 512 bits through the polynomial division, and XORed with the
// next 64 bytes. The accumulators are then folded together, reduced to 64 bits
// and finished with a Barrett reduction. The constants are the standard ones
// for this polynomial (Gopal et al., "Fast CRC Computation for Generic
// Polynomials Using PCLMULQDQ Instruction", Intel 2009).
//
// Only targets with a native carry-less multiply get the kernel; Highway's
// emulation elsewhere is slower than the tables, so those report "nothing
// consumed" and the caller keeps using them.
HWY_BEFORE_NAMESPACE();
namespace timestar::crc32simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

#if HWY_ARCH_X86 && HWY_TARGET <= HWY_SSE4

// Folds `length` bytes (a multiple of 16, at least 64) into the running CRC
// state and returns the new state.
uint32_t FoldKernel(uint32_t crc, const uint8_t* HWY_RESTRICT data, size_t length) {
    const hn::Full128<uint64_t> d;
    const hn::Full128<uint8_t> d8;
    using V = hn::Vec128<uint64_t>;

    alignas(16) static constexpr uint64_t kFold512[2] = {0x0154442bd4ULL, 0x01c6e41596ULL};
    alignas(16) static constexpr uint64_t kFold128[2] = {0x01751997d0ULL, 0x00ccaa009eULL};
    alignas(16) static constexpr uint64_t kFold64[2] = {0x0163cd6124ULL, 0};
    alignas(16) static constexpr uint64_t kBarrett[2] = {0x01db710641ULL, 0x01f7011641ULL};
    alignas(16) const uint64_t initial[2] = {crc, 0};

    const auto load = [&](const uint8_t* p) { return hn::BitCast(d, hn::LoadU(d8, p)); };
    // (acc * k) mod P for both halves, plus the next 128 message bits.
    const auto fold = [](V acc, V k, V next) {
        return hn::Xor(hn::Xor(hn::CLMulLower(acc, k), hn::CLMulUpper(acc, k)), next);
    };

    V x1 = hn::Xor(load(data), hn::Load(d, initial));
    V x2 = load(data + 16);
    V x3 = load(data + 32);
    V x4 = load(data + 48);
    data += 64;
    length -= 64;

    const V k512 = hn::Load(d, kFold512);
    while (length >= 64) {
        x1 = fold(x1, k512, load(data));
        x2 = fold(x2, k512, load(data + 16));
        x3 = fold(x3, k512, load(data + 32));
        x4 = fold(x4, k512, load(data + 48));
        data += 64;
        length -= 64;
    }

    // Four accumulators into one, then any remaining 16-byte blocks.
    const V k128 = hn::Load(d, kFold128);
    x1 = fold(x1, k128, x2);
    x1 = fold(x1, k128, x3);
    x1 = fold(x1, k128, x4);
    while (length >= 16) {
        x1 = fold(x1, k128, load(data));
        data += 16;
        length -= 16;
    }

    // 128 bits -> 64 bits.
    const V low32 = hn::Set(d, 0xFFFFFFFFULL);
    x1 = hn::Xor(hn::ShiftRightBytes<8>(d, x1), hn::CLMulLower(x1, hn::ShiftRightBytes<8>(d, k128)));
    const V upper = hn::ShiftRightBytes<4>(d, x1);
    x1 = hn::Xor(hn::CLMulLower(hn::And(x1, low32), hn::Load(d, kFold64)), upper);

    // Barrett reduction to 32 bits.
    const V barrett = hn::Load(d, kBarrett);
    V t = hn::CLMulLower(hn::And(x1, low32), hn::ShiftRightBytes<8>(d, barrett));
    t = hn::CLMulLower(hn::And(t, low32), barrett);
    x1 = hn::Xor(x1, t);
    return static_cast<uint32_t>(hn::GetLane(x1) >> 32);
}

size_t FoldPrefix(uint32_t* crc, const uint8_t* data, size_t length) {
    const size_t usable = length & ~size_t{15};
    if (usable < 64) {
        return 0;
    }
    *crc = FoldKernel(*crc, data, usable);
    return usable;
}

#else

size_t FoldPrefix(uint32_t* /*crc*/, const uint8_t* /*data*/, size_t /*length*/) {
    return 0;
}

#endif

// NOLINTNEXTLINE(google-readability-namespace-comments)
}  // namespace HWY_NAMESPACE
}  // namespace timestar::crc32simd
HWY_AFTER_NAMESPACE();

#if HWY_ONCE
namespace timestar::crc32simd {

HWY_EXPORT(FoldPrefix);

size_t foldPrefix(uint32_t* crc, const uint8_t* data, size_t length) {
    return HWY_DYNAMIC_DISPATCH(FoldPrefix)(crc, data, length);
}

}  // namespace timestar::crc32simd
#endif  // HWY_ONCE
