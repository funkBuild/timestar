// Highway foreach_target re-inclusion mechanism.
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "encoding/float/float_encoder_simd.cpp"
#include "float_encoder_simd.hpp"

#include "hwy/foreach_target.h"
#include "hwy/highway.h"

#include <bit>
#include <cstring>

HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace encoding {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Highway-accelerated Gorilla XOR float encoding.
//
// The SIMD benefit is in the XOR computation and zero detection phase.
// For each block of N uint64_t values (where N = Lanes, adapting to
// AVX-512=8, AVX2=4, SSE4=2, etc.):
//   1. Load N doubles, reinterpret as uint64_t
//   2. Build previous-value vector (last_value, cur[0], cur[1], ...)
//   3. XOR current vs previous using SIMD
//   4. Detect zero XOR results using SIMD comparison
//   5. Store XOR results, then bit-pack serially (inherently sequential)
CompressedBuffer EncodeFloatXor(std::span<const double> values) {
    if (values.empty()) {
        return CompressedBuffer();
    }

    CompressedBuffer buffer;

    // Pre-allocate capacity based on worst case (64 bits per value + control bits)
    size_t estimated_bits = values.size() * 66;
    buffer.reserve((estimated_bits + 63) / 64 + 16);

    uint64_t last_value = std::bit_cast<uint64_t>(values[0]);
    int data_bits = 0;
    int prev_lzb = -1;
    int prev_tzb = -1;

    buffer.write<64>(last_value);

    size_t i = 1;

    const hn::ScalableTag<uint64_t> d;
    const size_t N = hn::Lanes(d);

    // Process in blocks of N for SIMD optimization
    if (values.size() >= N + 1) {
        // Aligned scratch buffers sized for the widest possible vector.
        // 8 = max lanes for AVX-512 on uint64_t.
        alignas(64) uint64_t xor_results[8];
        alignas(64) uint64_t current_values[8];
        alignas(64) uint64_t prev_values[8];

        const size_t full_blocks_end = ((values.size() - 1) / N) * N + 1;

        for (; i < full_blocks_end; i += N) {
            // Load N double values directly as uint64_t using SIMD.
            // doubles and uint64_t are both 8 bytes; bit pattern is preserved.
            auto current = hn::LoadU(d, reinterpret_cast<const uint64_t*>(&values[i]));
            hn::StoreU(current, d, current_values);

            // Build previous values: [last_value, cur[0], cur[1], ..., cur[N-2]]
            prev_values[0] = last_value;
            for (size_t k = 1; k < N; ++k) {
                prev_values[k] = current_values[k - 1];
            }

            auto prev = hn::LoadU(d, prev_values);

            // XOR current vs previous using SIMD
            auto xor_vec = hn::Xor(current, prev);

            // Store XOR results for serial bit-packing
            hn::StoreU(xor_vec, d, xor_results);

            // Bit-packing is inherently serial
            for (size_t j = 0; j < N; ++j) {
                uint64_t xor_val = xor_results[j];

                if (xor_val == 0) {
                    buffer.writeFixed<0b0, 1>();
                } else {
                    int lzb = __builtin_clzll(xor_val);
                    int tzb = __builtin_ctzll(xor_val);

                    if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                        if (data_bits + 2 <= 64) {
                            uint64_t combined = (0b01ULL) | ((xor_val >> prev_tzb) << 2);
                            buffer.write(combined, data_bits + 2);
                        } else {
                            buffer.writeFixed<0b01, 2>();
                            buffer.write(xor_val >> prev_tzb, data_bits);
                        }
                    } else {
                        if (lzb > 31)
                            lzb = 31;
                        data_bits = 64 - lzb - tzb;

                        if (data_bits + 13 <= 64) {
                            uint64_t control_block =
                                (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                            uint64_t combined = control_block | ((xor_val >> tzb) << 13);
                            buffer.write(combined, data_bits + 13);
                        } else {
                            uint64_t control_block =
                                (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                            buffer.write(control_block, 13);
                            buffer.write(xor_val >> tzb, data_bits);
                        }

                        prev_lzb = lzb;
                        prev_tzb = tzb;
                    }
                }
            }

            last_value = current_values[N - 1];
        }
    }

    // Scalar tail for remaining values
    for (; i < values.size(); i++) {
        const uint64_t current_value = std::bit_cast<uint64_t>(values[i]);
        const uint64_t xor_value = current_value ^ last_value;

        if (xor_value == 0) {
            buffer.writeFixed<0b0, 1>();
        } else {
            int lzb = __builtin_clzll(xor_value);
            int tzb = __builtin_ctzll(xor_value);

            if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                if (data_bits + 2 <= 64) {
                    uint64_t combined = (0b01ULL) | ((xor_value >> prev_tzb) << 2);
                    buffer.write(combined, data_bits + 2);
                } else {
                    buffer.writeFixed<0b01, 2>();
                    buffer.write(xor_value >> prev_tzb, data_bits);
                }
            } else {
                if (lzb > 31)
                    lzb = 31;
                data_bits = 64 - lzb - tzb;

                if (data_bits + 13 <= 64) {
                    uint64_t control_block =
                        (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                    uint64_t combined = control_block | ((xor_value >> tzb) << 13);
                    buffer.write(combined, data_bits + 13);
                } else {
                    uint64_t control_block =
                        (0b11ULL) | (uint64_t(lzb) << 2) | (uint64_t(data_bits == 64 ? 0 : data_bits) << 7);
                    buffer.write(control_block, 13);
                    buffer.write(xor_value >> tzb, data_bits);
                }

                prev_lzb = lzb;
                prev_tzb = tzb;
            }
        }

        last_value = current_value;
    }

    return buffer;
}

}  // namespace HWY_NAMESPACE
}  // namespace encoding
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// ---- Dispatch table + public API (compiled once) ----
#if HWY_ONCE
namespace timestar {
namespace encoding {

HWY_EXPORT(EncodeFloatXor);

CompressedBuffer DispatchEncodeFloatXor(std::span<const double> values) {
    return HWY_DYNAMIC_DISPATCH(EncodeFloatXor)(values);
}

}  // namespace encoding
}  // namespace timestar

// ---- FloatEncoderSIMD public methods ----

bool FloatEncoderSIMD::isAvailable() {
    return true;
}

CompressedBuffer FloatEncoderSIMD::encode(std::span<const double> values) {
    return timestar::encoding::DispatchEncodeFloatXor(values);
}

CompressedBuffer FloatEncoderSIMD::encodeSafe(std::span<const double> values) {
    return encode(values);
}

    // ---- FloatEncoderAVX512 public methods ----
    // Both classes delegate to the same Highway dispatch function.
    // Highway automatically selects AVX-512 when the CPU supports it.
    #include "float_encoder_avx512.hpp"

bool FloatEncoderAVX512::isAvailable() {
    return true;
}

bool FloatEncoderAVX512::hasAVX512F() {
    return true;
}

bool FloatEncoderAVX512::hasAVX512DQ() {
    return true;
}

CompressedBuffer FloatEncoderAVX512::encode(std::span<const double> values) {
    return timestar::encoding::DispatchEncodeFloatXor(values);
}

CompressedBuffer FloatEncoderAVX512::encodeSafe(std::span<const double> values) {
    return encode(values);
}
#endif  // HWY_ONCE
