// Highway foreach_target re-inclusion mechanism.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "encoding/integer/integer_encoder_simd.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "integer_encoder_simd.hpp"

#include "../simple16.hpp"
#include "../zigzag.hpp"

#include "hwy/highway.h"

#include <cstring>

HWY_BEFORE_NAMESPACE();
namespace timestar {
namespace encoding {
namespace integer_simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// ---------------------------------------------------------------------------
// SIMD ZigZag encode: (x << 1) ^ (x >> 63)    [arithmetic shift for signed]
// ---------------------------------------------------------------------------
void ZigZagEncodeBatch(const int64_t* HWY_RESTRICT in, uint64_t* HWY_RESTRICT out, size_t count) {
    const hn::ScalableTag<int64_t> di;
    const hn::ScalableTag<uint64_t> du;
    const size_t N = hn::Lanes(di);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto v = hn::LoadU(di, &in[i]);

        // (x << 1): shift left by 1 on unsigned reinterpretation
        auto shifted = hn::ShiftLeft<1>(hn::BitCast(du, v));

        // (x >> 63): arithmetic right shift by 63 on signed gives 0 or -1
        auto sign = hn::ShiftRight<63>(v);

        // XOR: (x << 1) ^ (x >> 63)
        auto result = hn::Xor(shifted, hn::BitCast(du, sign));

        hn::StoreU(result, du, &out[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        out[i] = ZigZag::zigzagEncode(in[i]);
    }
}

// ---------------------------------------------------------------------------
// SIMD ZigZag decode: (y >> 1) ^ -(y & 1)
// ---------------------------------------------------------------------------
void ZigZagDecodeBatch(const uint64_t* HWY_RESTRICT in, int64_t* HWY_RESTRICT out, size_t count) {
    const hn::ScalableTag<uint64_t> du;
    const hn::ScalableTag<int64_t> di;
    const size_t N = hn::Lanes(du);

    const auto one = hn::Set(du, uint64_t{1});
    const auto zero = hn::Zero(du);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        auto y = hn::LoadU(du, &in[i]);

        // y >> 1
        auto shifted = hn::ShiftRight<1>(y);

        // y & 1
        auto lsb = hn::And(y, one);

        // -(y & 1)  =  0 - (y & 1)
        auto neg_lsb = hn::Sub(zero, lsb);

        // (y >> 1) ^ -(y & 1)
        auto result = hn::Xor(shifted, neg_lsb);

        hn::StoreU(hn::BitCast(di, result), di, &out[i]);
    }

    // Scalar tail
    for (; i < count; ++i) {
        out[i] = ZigZag::zigzagDecode(in[i]);
    }
}

}  // namespace HWY_NAMESPACE
}  // namespace integer_simd
}  // namespace encoding
}  // namespace timestar
HWY_AFTER_NAMESPACE();

// ── Dispatch table + public API (compiled once) ─────────────────────────────
#if HWY_ONCE

#include "integer_encoder_avx512.hpp"

namespace timestar {
namespace encoding {
namespace integer_simd {

HWY_EXPORT(ZigZagEncodeBatch);
HWY_EXPORT(ZigZagDecodeBatch);

// Internal dispatch wrappers (must be in same namespace as HWY_EXPORT)
void dispatchZigZagEncode(const int64_t* in, uint64_t* out, size_t count) {
    HWY_DYNAMIC_DISPATCH(ZigZagEncodeBatch)(in, out, count);
}

void dispatchZigZagDecode(const uint64_t* in, int64_t* out, size_t count) {
    HWY_DYNAMIC_DISPATCH(ZigZagDecodeBatch)(in, out, count);
}

}  // namespace integer_simd
}  // namespace encoding
}  // namespace timestar

// Convenience aliases
static inline void hwZigZagEncode(const int64_t* in, uint64_t* out, size_t count) {
    timestar::encoding::integer_simd::dispatchZigZagEncode(in, out, count);
}

static inline void hwZigZagDecode(const uint64_t* in, int64_t* out, size_t count) {
    timestar::encoding::integer_simd::dispatchZigZagDecode(in, out, count);
}

// ---------------------------------------------------------------------------
// IntegerEncoderSIMD -- public API
// ---------------------------------------------------------------------------

bool IntegerEncoderSIMD::isAvailable() {
    return true;  // Highway handles runtime dispatch
}

AlignedBuffer IntegerEncoderSIMD::encode(std::span<const uint64_t> values) {
    if (values.empty()) {
        return AlignedBuffer();
    }

    std::vector<uint64_t> encoded;
    encoded.reserve(values.size() + 2);

    uint64_t start_value = values[0];
    encoded.push_back(start_value);

    if (values.size() == 1) {
        return Simple16::encode(encoded);
    }

    int64_t delta = static_cast<int64_t>(values[1]) - static_cast<int64_t>(values[0]);
    encoded.push_back(ZigZag::zigzagEncode(delta));

    const size_t size = values.size();
    size_t i = 2;

    // Batch delta-of-delta computation + Highway SIMD ZigZag encode.
    // Process in chunks of 8 for good throughput on both AVX2 and AVX-512.
    if (size >= 10) {
        alignas(64) int64_t deltas[8];
        alignas(64) uint64_t encoded_batch[8];

        for (; i + 7 < size; i += 8) {
            deltas[0] = (static_cast<int64_t>(values[i + 0]) - static_cast<int64_t>(values[i - 1])) -
                        (static_cast<int64_t>(values[i - 1]) - static_cast<int64_t>(values[i - 2]));
            deltas[1] = (static_cast<int64_t>(values[i + 1]) - static_cast<int64_t>(values[i + 0])) -
                        (static_cast<int64_t>(values[i + 0]) - static_cast<int64_t>(values[i - 1]));
            deltas[2] = (static_cast<int64_t>(values[i + 2]) - static_cast<int64_t>(values[i + 1])) -
                        (static_cast<int64_t>(values[i + 1]) - static_cast<int64_t>(values[i + 0]));
            deltas[3] = (static_cast<int64_t>(values[i + 3]) - static_cast<int64_t>(values[i + 2])) -
                        (static_cast<int64_t>(values[i + 2]) - static_cast<int64_t>(values[i + 1]));
            deltas[4] = (static_cast<int64_t>(values[i + 4]) - static_cast<int64_t>(values[i + 3])) -
                        (static_cast<int64_t>(values[i + 3]) - static_cast<int64_t>(values[i + 2]));
            deltas[5] = (static_cast<int64_t>(values[i + 5]) - static_cast<int64_t>(values[i + 4])) -
                        (static_cast<int64_t>(values[i + 4]) - static_cast<int64_t>(values[i + 3]));
            deltas[6] = (static_cast<int64_t>(values[i + 6]) - static_cast<int64_t>(values[i + 5])) -
                        (static_cast<int64_t>(values[i + 5]) - static_cast<int64_t>(values[i + 4]));
            deltas[7] = (static_cast<int64_t>(values[i + 7]) - static_cast<int64_t>(values[i + 6])) -
                        (static_cast<int64_t>(values[i + 6]) - static_cast<int64_t>(values[i + 5]));

            hwZigZagEncode(deltas, encoded_batch, 8);

            for (size_t j = 0; j < 8; j++) {
                encoded.push_back(encoded_batch[j]);
            }
        }
    }

    // Process remaining values with 4x unrolling + SIMD ZigZag
    if (i + 3 < size) {
        alignas(64) int64_t deltas[4];
        alignas(64) uint64_t encoded_batch[4];

        for (; i + 3 < size; i += 4) {
            deltas[0] = (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1])) -
                        (static_cast<int64_t>(values[i - 1]) - static_cast<int64_t>(values[i - 2]));
            deltas[1] = (static_cast<int64_t>(values[i + 1]) - static_cast<int64_t>(values[i])) -
                        (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1]));
            deltas[2] = (static_cast<int64_t>(values[i + 2]) - static_cast<int64_t>(values[i + 1])) -
                        (static_cast<int64_t>(values[i + 1]) - static_cast<int64_t>(values[i]));
            deltas[3] = (static_cast<int64_t>(values[i + 3]) - static_cast<int64_t>(values[i + 2])) -
                        (static_cast<int64_t>(values[i + 2]) - static_cast<int64_t>(values[i + 1]));

            hwZigZagEncode(deltas, encoded_batch, 4);

            encoded.push_back(encoded_batch[0]);
            encoded.push_back(encoded_batch[1]);
            encoded.push_back(encoded_batch[2]);
            encoded.push_back(encoded_batch[3]);
        }
    }

    // Handle remaining values (scalar)
    for (; i < size; i++) {
        int64_t D = (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i - 1])) -
                    (static_cast<int64_t>(values[i - 1]) - static_cast<int64_t>(values[i - 2]));
        encoded.push_back(ZigZag::zigzagEncode(D));
    }

    return Simple16::encode(encoded);
}

std::pair<size_t, size_t> IntegerEncoderSIMD::decode(Slice& encoded, unsigned int timestampSize,
                                                     std::vector<uint64_t>& values, uint64_t minTime,
                                                     uint64_t maxTime) {
    // Optimized memory allocation
    const size_t current_size = values.size();
    const size_t estimated_new = timestampSize;

    if (values.capacity() < current_size + estimated_new) {
        values.reserve(current_size + estimated_new + (estimated_new >> 2));  // 25% extra
    }

    std::vector<uint64_t> deltaValues = Simple16::decode(encoded, timestampSize);

    size_t nSkipped = 0, nAdded = 0;

    if (deltaValues.empty()) {
        return {nSkipped, nAdded};
    }

    uint64_t last_decoded = deltaValues[0];

    // First value
    if (last_decoded < minTime) {
        nSkipped++;
    } else if (last_decoded <= maxTime) {
        values.push_back(last_decoded);
        nAdded++;
    } else {
        return {nSkipped, nAdded};
    }

    if (deltaValues.size() < 2) {
        return {nSkipped, nAdded};
    }

    // Second value
    int64_t delta = ZigZag::zigzagDecode(deltaValues[1]);
    last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

    if (last_decoded < minTime) {
        nSkipped++;
    } else if (last_decoded <= maxTime) {
        values.push_back(last_decoded);
        nAdded++;
    } else {
        return {nSkipped, nAdded};
    }

    const size_t size = deltaValues.size();
    size_t i = 2;

    // Process in batches of 8 using Highway-dispatched SIMD ZigZag decode
    if (size >= 10) {
        alignas(64) int64_t decoded_batch[8];

        for (; i + 7 < size; i += 8) {
            hwZigZagDecode(&deltaValues[i], decoded_batch, 8);

            // Sequential reconstruction (data-dependent prefix sum)
            for (size_t j = 0; j < 8; j++) {
                delta += decoded_batch[j];
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

                if (last_decoded < minTime) {
                    nSkipped++;
                } else if (last_decoded > maxTime) {
                    return {nSkipped, nAdded};
                } else {
                    values.push_back(last_decoded);
                    nAdded++;
                }
            }
        }
    }

    // Process 4 values at a time with SIMD ZigZag
    {
        alignas(64) int64_t decoded_batch[4];

        for (; i + 3 < size; i += 4) {
            hwZigZagDecode(&deltaValues[i], decoded_batch, 4);

            delta += decoded_batch[0];
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += decoded_batch[1];
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += decoded_batch[2];
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += decoded_batch[3];
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }
        }
    }

    // Handle remaining values (scalar)
    for (; i < size; i++) {
        int64_t encD = ZigZag::zigzagDecode(deltaValues[i]);
        delta += encD;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

        if (last_decoded < minTime) {
            nSkipped++;
            continue;
        }

        if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        }

        values.push_back(last_decoded);
        nAdded++;
    }

    return {nSkipped, nAdded};
}

// ---------------------------------------------------------------------------
// IntegerEncoderAVX512 -- delegates to the same Highway dispatch.
// Since Highway automatically selects the best target (including AVX-512
// when available), both classes produce identical results.
// ---------------------------------------------------------------------------

bool IntegerEncoderAVX512::isAvailable() {
    return true;  // Highway handles runtime dispatch
}

AlignedBuffer IntegerEncoderAVX512::encode(std::span<const uint64_t> values) {
    return IntegerEncoderSIMD::encode(values);
}

std::pair<size_t, size_t> IntegerEncoderAVX512::decode(Slice& encoded, unsigned int timestampSize,
                                                       std::vector<uint64_t>& values, uint64_t minTime,
                                                       uint64_t maxTime) {
    return IntegerEncoderSIMD::decode(encoded, timestampSize, values, minTime, maxTime);
}

#endif  // HWY_ONCE
