// CRITICAL: foreach_target.h MUST be the first include after HWY_TARGET_INCLUDE.
// Highway re-includes this entire file once per SIMD target (SSE4, AVX2, etc.).
// If other headers appear before foreach_target.h, Highway only compiles the
// baseline target and silently drops all higher ISAs — causing a ~10% perf regression
// with no build errors. clang-format will try to alphabetize this; the guards prevent it.
// clang-format off
#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "encoding/integer/integer_encoder_ffor.cpp"
#include "hwy/foreach_target.h"
// clang-format on

#include "integer_encoder_ffor.hpp"

#include "../alp/alp_ffor.hpp"
#include "../zigzag.hpp"

#include "hwy/highway.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <vector>

// =============================================================================
// SIMD kernels (compiled once per target ISA by foreach_target)
// =============================================================================
HWY_BEFORE_NAMESPACE();
namespace ffor_enc {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// ZigZag-encode a block of int64_t deltas into uint64_t output, and
// simultaneously track unsigned min/max across all output values.
// `deltas` are pre-computed delta-of-delta values (sequential dependency,
// computed scalar by the caller).  Handles any count (including tail elements
// that don't fill a full vector).
void ZigZagEncodeAndMinMax(const int64_t* HWY_RESTRICT deltas, uint64_t* HWY_RESTRICT out, size_t count,
                           uint64_t* HWY_RESTRICT min_out, uint64_t* HWY_RESTRICT max_out) {
    const hn::ScalableTag<int64_t> di;
    const hn::ScalableTag<uint64_t> du;
    const size_t N = hn::Lanes(di);

    auto vmin = hn::Set(du, *min_out);
    auto vmax = hn::Set(du, *max_out);

    size_t i = 0;
    for (; i + N <= count; i += N) {
        // Load signed deltas
        auto v = hn::LoadU(di, &deltas[i]);

        // ZigZag encode: (x << 1) ^ (x >> 63)
        // ShiftRight on signed int64_t is arithmetic (sign-extending) in Highway.
        auto shifted = hn::ShiftLeft<1>(v);
        auto sign = hn::ShiftRight<63>(v);
        auto zz = hn::BitCast(du, hn::Xor(shifted, sign));

        // Store zigzag-encoded values
        hn::StoreU(zz, du, &out[i]);

        // Track unsigned min/max
        vmin = hn::Min(vmin, zz);
        vmax = hn::Max(vmax, zz);
    }

    // Horizontal reduction of SIMD accumulators
    uint64_t cur_min = hn::ReduceMin(du, vmin);
    uint64_t cur_max = hn::ReduceMax(du, vmax);

    // Scalar tail: process remaining elements that don't fill a full vector
    for (; i < count; ++i) {
        int64_t x = deltas[i];
        uint64_t zz = (static_cast<uint64_t>(x) << 1) ^ static_cast<uint64_t>(x >> 63);
        out[i] = zz;
        if (zz < cur_min)
            cur_min = zz;
        if (zz > cur_max)
            cur_max = zz;
    }

    *min_out = cur_min;
    *max_out = cur_max;
}

}  // namespace HWY_NAMESPACE
}  // namespace ffor_enc
HWY_AFTER_NAMESPACE();

// =============================================================================
// Non-SIMD code + dispatch table (compiled once)
// =============================================================================
#if HWY_ONCE

// Block header field width guards: block_count is 11 bits (max 2047),
// exception_count is 10 bits (max 1023 = BLOCK_SIZE/4 max).
// If BLOCK_SIZE is ever increased, these static_asserts will fire.
static_assert(IntegerEncoderFFOR::BLOCK_SIZE <= 2047, "BLOCK_SIZE exceeds block_count header field capacity (11 bits)");
static_assert(IntegerEncoderFFOR::BLOCK_SIZE / 4 <= 1023,
              "max exception_count exceeds header field capacity (10 bits)");

namespace ffor_enc {
HWY_EXPORT(ZigZagEncodeAndMinMax);

// Wrapper callable from outside ffor_enc namespace (HWY_DYNAMIC_DISPATCH
// must resolve the dispatch table in the same namespace as HWY_EXPORT).
inline void dispatchZigZagEncodeAndMinMax(const int64_t* deltas, uint64_t* out, size_t count, uint64_t* min_out,
                                          uint64_t* max_out) {
    HWY_DYNAMIC_DISPATCH(ZigZagEncodeAndMinMax)(deltas, out, count, min_out, max_out);
}
}  // namespace ffor_enc

namespace {

// Thread-local scratch buffers for FFOR encoding/decoding.
// Since Seastar uses a shard-per-core model (one thread per shard, no sharing),
// thread_local is safe and avoids any synchronization overhead.
// After the first few encode/decode calls, all buffers reach their maximum needed
// capacity and subsequent calls are allocation-free.
struct FFORScratchBuffers {
    // Encode path
    std::vector<uint64_t> zigzag;         // up to BLOCK_SIZE (1024) - filled per-block
    std::vector<uint64_t> clean;          // up to BLOCK_SIZE (1024)
    std::vector<uint16_t> exc_positions;  // up to BLOCK_SIZE/4
    std::vector<uint64_t> exc_values;     // up to BLOCK_SIZE/4

    // Scratch for scalar delta-of-delta values before SIMD zigzag encode
    std::vector<int64_t> deltas;  // up to BLOCK_SIZE (1024)

    // Decode path: no scratch buffers needed - decode uses a stack-allocated
    // block buffer (8KB) that stays hot in L1 cache.

    FFORScratchBuffers() {
        zigzag.reserve(1024);
        clean.reserve(1024);
        exc_positions.reserve(256);
        exc_values.reserve(256);
        deltas.reserve(1024);
    }
};

static FFORScratchBuffers& getScratch() {
    static thread_local FFORScratchBuffers scratch;
    return scratch;
}

// Number of bits needed to represent the range [0, range].
inline uint8_t bitsForRange(uint64_t range) {
    if (range == 0)
        return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(range));
}

// Encode the block header into two uint64_t words.
//   Word 0: [0:10] block_count, [11:17] bit_width, [18:27] exception_count
//   Word 1: base (FOR reference)
inline void writeBlockHeader(AlignedBuffer& buf, uint16_t block_count, uint8_t bw, uint16_t exc_count, uint64_t base) {
    uint64_t w0 = static_cast<uint64_t>(block_count) | (static_cast<uint64_t>(bw) << 11) |
                  (static_cast<uint64_t>(exc_count) << 18);
    buf.write(w0);
    buf.write(base);
}

// Read a block header from a Slice.
struct BlockHeader {
    uint16_t block_count;
    uint8_t bw;
    uint16_t exc_count;
    uint64_t base;
};

inline BlockHeader readBlockHeader(Slice& s) {
    // Direct pointer read: 2 x uint64_t = 16 bytes, one bounds check + advance
    const size_t hdr_bytes = 2 * sizeof(uint64_t);
    if (s.offset + hdr_bytes > s.length_) [[unlikely]] {
        throw std::runtime_error("FFOR decode: insufficient data for block header");
    }
    const uint8_t* ptr = s.data + s.offset;
    s.offset += hdr_bytes;

    uint64_t w0, base;
    std::memcpy(&w0, ptr, sizeof(uint64_t));
    std::memcpy(&base, ptr + sizeof(uint64_t), sizeof(uint64_t));

    return {static_cast<uint16_t>(w0 & 0x7FF), static_cast<uint8_t>((w0 >> 11) & 0x7F),
            static_cast<uint16_t>((w0 >> 18) & 0x3FF), base};
}

// ---- FFOR packing helpers (delegates to alp::ffor for the heavy lifting) ----

// Encode one block of zigzag values into the AlignedBuffer.
// min_val and max_val are pre-computed by the caller during zigzag encoding,
// eliminating a redundant min/max scan over the block.
void encodeBlock(const uint64_t* values, size_t count, AlignedBuffer& buf, uint64_t min_val, uint64_t max_val) {
    // ---- Fast path: all values identical (min == max) ----
    // This is the most common case for constant-interval TimeStar timestamps,
    // where delta-of-delta produces all zeros from index 2 onward.
    // Skip histogram, suffix sum, bit-width search, clean array, and FFOR pack.
    // Just emit a 2-word header: bw=0, exc_count=0, base=min_val.
    if (min_val == max_val) {
        writeBlockHeader(buf, static_cast<uint16_t>(count), /*bw=*/0, /*exc_count=*/0, min_val);
        return;
    }

    uint8_t bw_full = bitsForRange(max_val - min_val);

    // 2. Build per-value bit-width histogram (relative to min)
    //    bw_hist[k] = count of values whose (value - min_val) needs exactly k bits.
    std::array<uint32_t, 65> bw_hist{};
    for (size_t i = 0; i < count; ++i) {
        uint64_t delta = values[i] - min_val;
        // std::countl_zero is defined for 0 (returns 64), avoiding a branch.
        uint8_t vbw = static_cast<uint8_t>(std::bit_width(delta));
        bw_hist[vbw]++;
    }

    // 3. Suffix sum: suffix[k] = number of values needing > k bits.
    //    A value needing k bits fits in a field of width w iff w >= k.
    //    So exceptions for candidate width `cand` = suffix[cand].
    std::array<uint32_t, 65> suffix{};
    suffix[64] = 0;  // no value needs > 64 bits
    for (int k = 63; k >= 0; --k) {
        suffix[k] = suffix[k + 1] + bw_hist[k + 1];
    }

    // 4. Find the bit-width that minimizes total encoded size
    const size_t HEADER_BYTES = 16;                             // 2 x uint64_t
    const uint32_t max_exc = static_cast<uint32_t>(count / 4);  // cap at 25%

    size_t best_size = SIZE_MAX;
    uint8_t best_bw = bw_full;

    for (uint8_t cand = 0; cand <= bw_full; ++cand) {
        uint32_t exc = suffix[cand];  // values that don't fit in `cand` bits
        if (exc > max_exc)
            continue;

        size_t ffor_bytes = alp::ffor_packed_words(count, cand) * 8;
        size_t exc_pos_bytes = (exc > 0) ? (static_cast<size_t>(exc + 3) / 4) * 8 : 0;
        size_t exc_val_bytes = static_cast<size_t>(exc) * 8;
        size_t total = HEADER_BYTES + ffor_bytes + exc_pos_bytes + exc_val_bytes;

        if (total < best_size) {
            best_size = total;
            best_bw = cand;
        }
    }

    // ---- No-exception fast path (common case for well-behaved data) ----
    // When all values fit in best_bw bits, skip the clean array copy entirely
    // and pass the original values pointer directly to ffor_pack_u64.
    // This avoids an 8KB memcpy per 1024-element block in the common case.
    if (suffix[best_bw] == 0) {
        // 5a. Write header with exc_count=0
        writeBlockHeader(buf, static_cast<uint16_t>(count), best_bw, /*exc_count=*/0, min_val);

        // 6a. FFOR pack directly from input values (no clean array needed)
        size_t packed_words = alp::ffor_packed_words(count, best_bw);
        if (packed_words > 0) {
            size_t packed_bytes = packed_words * sizeof(uint64_t);
            uint8_t* dest = buf.grow_uninit(packed_bytes);
            std::memset(dest, 0, packed_bytes);  // ffor_pack_u64 ORs into the buffer
            alp::ffor_pack_u64(values, count, min_val, best_bw, reinterpret_cast<uint64_t*>(dest));
        }
        return;
    }

    // ---- Exception path (rare: some values don't fit in best_bw bits) ----

    // 5b. Identify exceptions and build the clean array for FFOR packing.
    //     Exceptions are values whose (value - min_val) doesn't fit in best_bw bits.
    //     Replace them with min_val (base) so they contribute 0 delta.
    uint64_t threshold = (best_bw >= 64) ? UINT64_MAX : (best_bw == 0) ? 0 : (1ULL << best_bw) - 1;

    auto& s = getScratch();
    auto& exc_positions = s.exc_positions;
    auto& exc_values = s.exc_values;
    auto& clean = s.clean;
    exc_positions.clear();
    exc_values.clear();
    clean.resize(count);

    for (size_t i = 0; i < count; ++i) {
        uint64_t delta = values[i] - min_val;
        if (delta > threshold) {
            exc_positions.push_back(static_cast<uint16_t>(i));
            exc_values.push_back(values[i]);
            clean[i] = min_val;
        } else {
            clean[i] = values[i];
        }
    }

    const uint16_t actual_exc = static_cast<uint16_t>(exc_positions.size());

    // 6b. Write block header (with actual exception count)
    writeBlockHeader(buf, static_cast<uint16_t>(count), best_bw, actual_exc, min_val);

    // 7b. FFOR pack from clean array
    size_t packed_words = alp::ffor_packed_words(count, best_bw);
    if (packed_words > 0) {
        size_t packed_bytes = packed_words * sizeof(uint64_t);
        uint8_t* dest = buf.grow_uninit(packed_bytes);
        std::memset(dest, 0, packed_bytes);  // ffor_pack_u64 ORs into the buffer
        alp::ffor_pack_u64(clean.data(), count, min_val, best_bw, reinterpret_cast<uint64_t*>(dest));
    }

    // 8b. Write exception positions (4 x uint16_t per uint64_t word)
    if (actual_exc > 0) {
        size_t pos_words = (actual_exc + 3) / 4;
        for (size_t w = 0; w < pos_words; ++w) {
            uint64_t word = 0;
            for (size_t j = 0; j < 4; ++j) {
                size_t idx = w * 4 + j;
                if (idx < actual_exc) {
                    word |= static_cast<uint64_t>(exc_positions[idx]) << (j * 16);
                }
            }
            buf.write(word);
        }

        // 9b. Write exception values
        buf.write_array(exc_values.data(), actual_exc);
    }
}

// Decode one block of zigzag values from the Slice into a caller-provided buffer.
// Writes exactly `hdr.block_count` values to `out`. Returns the block count.
// The caller must ensure `out` has room for at least BLOCK_SIZE (1024) elements.
size_t decodeBlockInto(Slice& s, uint64_t* out) {
    auto hdr = readBlockHeader(s);

    // Validate bit-width: legal values are 0..64.  A corrupt block with bw > 64
    // implies max_val < min_val (unsigned underflow in the original range
    // computation), which would cause ffor_packed_words() to return a huge value
    // and result in a buffer over-read.
    if (hdr.bw > 64) [[unlikely]] {
        throw std::runtime_error("Corrupt FFOR block: bit_width > 64 (implies max_val < min_val)");
    }

    // Validate block_count: cannot exceed the buffer size (BLOCK_SIZE = 1024).
    if (hdr.block_count > IntegerEncoderFFOR::BLOCK_SIZE) [[unlikely]] {
        throw std::runtime_error("Corrupt FFOR block: block_count (" + std::to_string(hdr.block_count) +
                                 ") exceeds BLOCK_SIZE (" + std::to_string(IntegerEncoderFFOR::BLOCK_SIZE) + ")");
    }

    // Validate exception count: cannot exceed the number of values in the block.
    if (hdr.exc_count > hdr.block_count) [[unlikely]] {
        throw std::runtime_error("Corrupt FFOR block: exception_count exceeds block_count");
    }

    // FFOR unpack
    if (hdr.bw == 0) {
        // All values are base
        std::fill_n(out, hdr.block_count, hdr.base);
    } else {
        // Direct pointer access to packed data - skip per-word bounds checks and copy.
        const size_t packed_words = alp::ffor_packed_words(hdr.block_count, hdr.bw);
        const size_t packed_bytes = packed_words * sizeof(uint64_t);

        // Bounds check: ensure the packed data fits within the remaining slice.
        if (s.offset + packed_bytes > s.length_) [[unlikely]] {
            throw std::runtime_error("Corrupt FFOR block: packed data extends beyond end of buffer");
        }

        const uint8_t* packed_ptr = s.data + s.offset;
        s.offset += packed_bytes;

        // Copy to an aligned stack buffer -- the source slice may not be 8-byte
        // aligned, and reinterpret_cast<const uint64_t*> on unaligned data
        // is UB (crashes with SIGBUS on ARM / Graviton).
        // Max packed_words = ceil(BLOCK_SIZE * 64 / 64) = BLOCK_SIZE = 1024 (8KB stack).
        alignas(8) uint64_t aligned_packed[IntegerEncoderFFOR::BLOCK_SIZE];
        std::memcpy(aligned_packed, packed_ptr, packed_bytes);

        alp::ffor_unpack_u64(aligned_packed, hdr.block_count, hdr.base, hdr.bw, out);
    }

    // Patch exceptions
    if (hdr.exc_count > 0) {
        // Read exception positions directly from Slice pointer.
        // Positions are packed 4 x uint16_t per uint64_t word.
        const size_t pos_words = (hdr.exc_count + 3) / 4;
        const size_t pos_bytes = pos_words * sizeof(uint64_t);

        // Bounds check: exception position words.
        if (s.offset + pos_bytes > s.length_) [[unlikely]] {
            throw std::runtime_error("Corrupt FFOR block: exception positions extend beyond end of buffer");
        }

        const uint8_t* pos_ptr = s.data + s.offset;
        s.offset += pos_bytes;

        // Read exception values directly from Slice pointer.
        const size_t val_bytes = hdr.exc_count * sizeof(uint64_t);

        // Bounds check: exception values.
        if (s.offset + val_bytes > s.length_) [[unlikely]] {
            throw std::runtime_error("Corrupt FFOR block: exception values extend beyond end of buffer");
        }

        const uint8_t* val_ptr = s.data + s.offset;
        s.offset += val_bytes;

        // Decode positions and patch values in one pass
        for (size_t w = 0; w < pos_words; ++w) {
            uint64_t word;
            std::memcpy(&word, pos_ptr + w * sizeof(uint64_t), sizeof(uint64_t));
            const size_t base_idx = w * 4;
            const size_t remaining = hdr.exc_count - base_idx;
            const size_t count = remaining < 4 ? remaining : 4;
            for (size_t j = 0; j < count; ++j) {
                uint16_t pos = static_cast<uint16_t>((word >> (j * 16)) & 0xFFFF);
                if (pos >= hdr.block_count) [[unlikely]] {
                    throw std::runtime_error("Corrupt FFOR block: exception position (" + std::to_string(pos) +
                                             ") >= block_count (" + std::to_string(hdr.block_count) + ")");
                }
                uint64_t val;
                std::memcpy(&val, val_ptr + (base_idx + j) * sizeof(uint64_t), sizeof(uint64_t));
                out[pos] = val;
            }
        }
    }

    return hdr.block_count;
}

// Shared encode implementation: fuses delta-of-delta + zigzag encoding with
// per-block min/max tracking, eliminating a redundant min/max scan in encodeBlock().
// Zigzag values are computed block-at-a-time into the scratch buffer, and min/max
// are tracked simultaneously during that computation.
//
// The delta-of-delta computation has sequential memory dependencies and stays
// scalar.  The zigzag encode + min/max tracking is dispatched to the best
// available SIMD target via Highway (AVX-512, AVX2, SSE4, NEON, ...).
void encodeImpl(std::span<const uint64_t> values, AlignedBuffer& buf) {
    constexpr size_t BS = IntegerEncoderFFOR::BLOCK_SIZE;
    const size_t sz = values.size();
    const size_t num_blocks = (sz + BS - 1) / BS;

    auto& scratch = getScratch();
    auto& zigzag = scratch.zigzag;
    auto& deltas = scratch.deltas;
    zigzag.resize(BS);  // ensure capacity; only reallocates on first call
    deltas.resize(BS);  // scratch for delta-of-delta values

    // Estimate: header + base + worst-case packed data per block
    buf.reserve(buf.size() + num_blocks * (16 + BS * 8));

    // State carried across blocks for delta-of-delta encoding.
    // `val_idx` tracks our position in the input values array.
    size_t val_idx = 0;

    for (size_t b = 0; b < num_blocks; ++b) {
        const size_t block_start = b * BS;
        const size_t block_count = std::min(BS, sz - block_start);

        // Compute zigzag values for this block and track min/max simultaneously.
        uint64_t block_min = UINT64_MAX;
        uint64_t block_max = 0;
        size_t zz_idx = 0;

        // Handle the first two values of the entire sequence specially.
        // values[0] is stored raw; values[1] is zigzag(delta).
        // From index 2 onward, all values are zigzag(delta-of-delta).
        // These special cases only occur in the first block.
        if (val_idx == 0) {
            // First value: raw
            uint64_t zz = values[0];
            zigzag[0] = zz;
            block_min = zz;
            block_max = zz;
            zz_idx = 1;
            val_idx = 1;

            if (zz_idx < block_count && val_idx < sz) {
                // Second value: zigzag(delta)
                // Precondition: all input values must fit in int64_t (< 2^63).
                // This holds for nanosecond timestamps (until year ~2262) and ZigZag-encoded
                // int64_t values (range 0..2^63-1 for non-negative originals).
                // Two's complement subtraction is correct even for values near INT64_MAX,
                // as long as the delta between consecutive values fits in int64_t.
                int64_t delta = static_cast<int64_t>(values[1]) - static_cast<int64_t>(values[0]);
                zz = ZigZag::zigzagEncode(delta);
                zigzag[1] = zz;
                if (zz < block_min)
                    block_min = zz;
                if (zz > block_max)
                    block_max = zz;
                zz_idx = 2;
                val_idx = 2;
            }
        }

        // Remaining values in this block: zigzag(delta-of-delta).
        // Phase 1: Compute delta-of-delta values into the scratch buffer (scalar,
        //          sequential memory dependency prevents SIMD here).
        // Phase 2: ZigZag-encode and track min/max via Highway SIMD dispatch.

        // Count how many delta-of-delta values we'll compute this block
        size_t dd_count = 0;
        {
            size_t vi = val_idx;
            size_t zi = zz_idx;
            while (zi < block_count && vi < sz && vi >= 2) {
                // uint64_t -> int64_t casts and signed arithmetic: valid for nanosecond
                // timestamps until year ~2262; signed overflow UB is avoided because
                // consecutive timestamp deltas are small relative to int64_t range.
                deltas[dd_count] = (static_cast<int64_t>(values[vi]) - static_cast<int64_t>(values[vi - 1])) -
                                   (static_cast<int64_t>(values[vi - 1]) - static_cast<int64_t>(values[vi - 2]));
                ++dd_count;
                ++vi;
                ++zi;
            }
        }

        if (dd_count > 0) {
            // Highway SIMD dispatch: zigzag-encode deltas[] into zigzag[zz_idx..],
            // updating block_min/block_max along the way.  The kernel handles
            // both the SIMD main loop and scalar tail internally.
            ffor_enc::dispatchZigZagEncodeAndMinMax(deltas.data(), &zigzag[zz_idx], dd_count, &block_min, &block_max);

            val_idx += dd_count;
            zz_idx += dd_count;
        }

        // Scalar tail: any remaining delta-of-delta values not covered by the SIMD dispatch.
        // val_idx is always >= 2 here (first two values handled above).
        for (; zz_idx < block_count && val_idx < sz; ++zz_idx, ++val_idx) {
            int64_t D = (static_cast<int64_t>(values[val_idx]) - static_cast<int64_t>(values[val_idx - 1])) -
                        (static_cast<int64_t>(values[val_idx - 1]) - static_cast<int64_t>(values[val_idx - 2]));
            uint64_t zz = ZigZag::zigzagEncode(D);
            zigzag[zz_idx] = zz;
            if (zz < block_min)
                block_min = zz;
            if (zz > block_max)
                block_max = zz;
        }

        // Guard against empty block (shouldn't happen, but defensive)
        if (block_min > block_max) {
            block_min = block_max = 0;
        }

        encodeBlock(zigzag.data(), block_count, buf, block_min, block_max);
    }
}

}  // anonymous namespace

// =============================================================================
// Public API
// =============================================================================

AlignedBuffer IntegerEncoderFFOR::encode(std::span<const uint64_t> values) {
    AlignedBuffer buf;
    if (values.empty())
        return buf;
    encodeImpl(values, buf);
    return buf;
}

size_t IntegerEncoderFFOR::encodeInto(std::span<const uint64_t> values, AlignedBuffer& target) {
    if (values.empty())
        return 0;
    const size_t startPos = target.size();
    encodeImpl(values, target);
    return target.size() - startPos;
}

std::pair<size_t, size_t> IntegerEncoderFFOR::decode(Slice& encoded, unsigned int timestampSize,
                                                     std::vector<uint64_t>& values, uint64_t minTime,
                                                     uint64_t maxTime) {
    // Fused decode: unpack one FFOR block at a time into a small stack buffer,
    // then immediately perform zigzag decode + delta reconstruction + time filtering.
    // Benefits:
    //   1. 8KB block buffer (1024 x 8B) stays hot in L1 cache
    //   2. No large intermediate deltaValues allocation
    //   3. Early exit when maxTime exceeded (skip remaining blocks)

    constexpr size_t BS = BLOCK_SIZE;   // 1024
    alignas(64) uint64_t blockBuf[BS];  // 8KB stack buffer - fits in L1 cache

    size_t nSkipped = 0, nAdded = 0;

    // Optimized memory allocation
    const size_t current_size = values.size();
    if (values.capacity() < current_size + timestampSize) {
        values.reserve(current_size + timestampSize + (timestampSize >> 2));
    }

    // =========================================================================
    // Fast path: no time filtering (minTime==0 && maxTime==UINT64_MAX).
    // This is the common case for full-block reads in benchmarks and many real
    // queries. Eliminates 2N branches (minTime/maxTime checks) from the hot loop.
    // =========================================================================
    if (minTime == 0 && maxTime == UINT64_MAX) {
        uint64_t last_decoded = 0;
        int64_t delta = 0;
        size_t global_idx = 0;
        size_t total_decoded = 0;

        while (total_decoded < timestampSize && encoded.remaining() >= 16) {
            const size_t block_count = decodeBlockInto(encoded, blockBuf);
            total_decoded += block_count;

            size_t local_i = 0;

            // Handle value[0]: raw timestamp
            if (global_idx == 0 && local_i < block_count) {
                last_decoded = blockBuf[0];
                values.push_back(last_decoded);
                local_i = 1;
                global_idx = 1;
            }

            // Handle value[1]: zigzag(delta)
            if (global_idx == 1 && local_i < block_count) {
                delta = ZigZag::zigzagDecode(blockBuf[local_i]);
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
                values.push_back(last_decoded);
                local_i++;
                global_idx = 2;
            }

            // Main unrolled-by-4 loop: no time checks, just reconstruct + push_back
            for (; local_i + 3 < block_count; local_i += 4) {
                int64_t dd0 = ZigZag::zigzagDecode(blockBuf[local_i]);
                int64_t dd1 = ZigZag::zigzagDecode(blockBuf[local_i + 1]);
                int64_t dd2 = ZigZag::zigzagDecode(blockBuf[local_i + 2]);
                int64_t dd3 = ZigZag::zigzagDecode(blockBuf[local_i + 3]);

                delta += dd0;
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
                values.push_back(last_decoded);

                delta += dd1;
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
                values.push_back(last_decoded);

                delta += dd2;
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
                values.push_back(last_decoded);

                delta += dd3;
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
                values.push_back(last_decoded);
            }

            // Scalar tail
            for (; local_i < block_count; ++local_i) {
                int64_t dd = ZigZag::zigzagDecode(blockBuf[local_i]);
                delta += dd;
                last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
                values.push_back(last_decoded);
            }
        }

        nAdded = values.size() - current_size;
        return {0, nAdded};
    }

    // =========================================================================
    // Filtered path: time-range checks on every value (minTime/maxTime active).
    // =========================================================================

    // Delta reconstruction state carried across blocks.
    // global_idx tracks our position in the logical sequence (0 = first value ever).
    uint64_t last_decoded = 0;
    int64_t delta = 0;
    size_t global_idx = 0;
    size_t total_decoded = 0;

    while (total_decoded < timestampSize && encoded.remaining() >= 16) {
        // Unpack one block into the stack-local buffer (L1-hot)
        const size_t block_count = decodeBlockInto(encoded, blockBuf);
        total_decoded += block_count;

        // Determine where to start consuming values within this block.
        // The first two values of the ENTIRE sequence need special handling:
        //   global_idx 0: raw timestamp
        //   global_idx 1: zigzag(delta)
        //   global_idx 2+: zigzag(delta-of-delta)
        size_t local_i = 0;

        // Handle value[0] of the entire sequence (raw timestamp)
        if (global_idx == 0 && local_i < block_count) {
            last_decoded = blockBuf[0];
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded <= maxTime) {
                values.push_back(last_decoded);
                nAdded++;
            } else {
                return {nSkipped, nAdded};
            }
            local_i = 1;
            global_idx = 1;
        }

        // Handle value[1] of the entire sequence (zigzag(delta))
        if (global_idx == 1 && local_i < block_count) {
            delta = ZigZag::zigzagDecode(blockBuf[local_i]);
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded <= maxTime) {
                values.push_back(last_decoded);
                nAdded++;
            } else {
                return {nSkipped, nAdded};
            }
            local_i++;
            global_idx = 2;
        }

        // Main reconstruction loop (unrolled by 4) for zigzag(delta-of-delta) values.
        // blockBuf is L1-hot since we just wrote it via decodeBlockInto().
        for (; local_i + 3 < block_count; local_i += 4) {
            int64_t dd0 = ZigZag::zigzagDecode(blockBuf[local_i]);
            int64_t dd1 = ZigZag::zigzagDecode(blockBuf[local_i + 1]);
            int64_t dd2 = ZigZag::zigzagDecode(blockBuf[local_i + 2]);
            int64_t dd3 = ZigZag::zigzagDecode(blockBuf[local_i + 3]);

            delta += dd0;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += dd1;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += dd2;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) {
                nSkipped++;
            } else if (last_decoded > maxTime) {
                return {nSkipped, nAdded};
            } else {
                values.push_back(last_decoded);
                nAdded++;
            }

            delta += dd3;
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

        // Scalar tail for remaining values in this block
        for (; local_i < block_count; ++local_i) {
            int64_t dd = ZigZag::zigzagDecode(blockBuf[local_i]);
            delta += dd;
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
    }

    return {nSkipped, nAdded};
}

#endif  // HWY_ONCE
