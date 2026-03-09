#include "integer_encoder_ffor.hpp"
#include "../zigzag.hpp"
#include "../alp/alp_ffor.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace {

// Thread-local scratch buffers for FFOR encoding/decoding.
// Since Seastar uses a shard-per-core model (one thread per shard, no sharing),
// thread_local is safe and avoids any synchronization overhead.
// After the first few encode/decode calls, all buffers reach their maximum needed
// capacity and subsequent calls are allocation-free.
struct FFORScratchBuffers {
    // Encode path
    std::vector<uint64_t> zigzag;          // up to BLOCK_SIZE (1024) - filled per-block
    std::vector<uint64_t> clean;           // up to BLOCK_SIZE (1024)
    std::vector<uint16_t> exc_positions;   // up to BLOCK_SIZE/4
    std::vector<uint64_t> exc_values;      // up to BLOCK_SIZE/4

    // Decode path: no scratch buffers needed - decode uses a stack-allocated
    // block buffer (8KB) that stays hot in L1 cache.

    FFORScratchBuffers() {
        zigzag.reserve(1024);
        clean.reserve(1024);
        exc_positions.reserve(256);
        exc_values.reserve(256);
    }
};

static FFORScratchBuffers& getScratch() {
    static thread_local FFORScratchBuffers scratch;
    return scratch;
}

// Number of bits needed to represent the range [0, range].
inline uint8_t bitsForRange(uint64_t range) {
    if (range == 0) return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(range));
}

// Encode the block header into two uint64_t words.
//   Word 0: [0:10] block_count, [11:17] bit_width, [18:27] exception_count
//   Word 1: base (FOR reference)
inline void writeBlockHeader(AlignedBuffer &buf, uint16_t block_count,
                             uint8_t bw, uint16_t exc_count, uint64_t base) {
    uint64_t w0 = static_cast<uint64_t>(block_count)
                | (static_cast<uint64_t>(bw) << 11)
                | (static_cast<uint64_t>(exc_count) << 18);
    buf.write(w0);
    buf.write(base);
}

// Read a block header from a Slice.
struct BlockHeader {
    uint16_t block_count;
    uint8_t  bw;
    uint16_t exc_count;
    uint64_t base;
};

inline BlockHeader readBlockHeader(Slice &s) {
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

    return {
        static_cast<uint16_t>(w0 & 0x7FF),
        static_cast<uint8_t>((w0 >> 11) & 0x7F),
        static_cast<uint16_t>((w0 >> 18) & 0x3FF),
        base
    };
}

// ---- FFOR packing helpers (delegates to alp::ffor for the heavy lifting) ----

// Encode one block of zigzag values into the AlignedBuffer.
// min_val and max_val are pre-computed by the caller during zigzag encoding,
// eliminating a redundant min/max scan over the block.
void encodeBlock(const uint64_t *values, size_t count, AlignedBuffer &buf,
                 uint64_t min_val, uint64_t max_val) {
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
        uint8_t vbw = (delta == 0) ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(delta));
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
    const size_t HEADER_BYTES = 16;              // 2 x uint64_t
    const uint32_t max_exc = static_cast<uint32_t>(count / 4);  // cap at 25%

    size_t best_size = SIZE_MAX;
    uint8_t best_bw = bw_full;

    for (uint8_t cand = 0; cand <= bw_full; ++cand) {
        uint32_t exc = suffix[cand];            // values that don't fit in `cand` bits
        if (exc > max_exc) continue;

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
            alp::ffor_pack_u64(values, count, min_val, best_bw,
                               reinterpret_cast<uint64_t*>(dest));
        }
        return;
    }

    // ---- Exception path (rare: some values don't fit in best_bw bits) ----

    // 5b. Identify exceptions and build the clean array for FFOR packing.
    //     Exceptions are values whose (value - min_val) doesn't fit in best_bw bits.
    //     Replace them with min_val (base) so they contribute 0 delta.
    uint64_t threshold = (best_bw >= 64) ? UINT64_MAX
                       : (best_bw == 0)  ? 0
                       : (1ULL << best_bw) - 1;

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
        alp::ffor_pack_u64(clean.data(), count, min_val, best_bw,
                           reinterpret_cast<uint64_t*>(dest));
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
size_t decodeBlockInto(Slice &s, uint64_t* out) {
    auto hdr = readBlockHeader(s);

    // Validate bit-width: legal values are 0..64.  A corrupt block with bw > 64
    // implies max_val < min_val (unsigned underflow in the original range
    // computation), which would cause ffor_packed_words() to return a huge value
    // and result in a buffer over-read.
    if (hdr.bw > 64) [[unlikely]] {
        throw std::runtime_error(
            "Corrupt FFOR block: bit_width > 64 (implies max_val < min_val)");
    }

    // Validate block_count: cannot exceed the buffer size (BLOCK_SIZE = 1024).
    if (hdr.block_count > IntegerEncoderFFOR::BLOCK_SIZE) [[unlikely]] {
        throw std::runtime_error(
            "Corrupt FFOR block: block_count (" + std::to_string(hdr.block_count) +
            ") exceeds BLOCK_SIZE (" + std::to_string(IntegerEncoderFFOR::BLOCK_SIZE) + ")");
    }

    // Validate exception count: cannot exceed the number of values in the block.
    if (hdr.exc_count > hdr.block_count) [[unlikely]] {
        throw std::runtime_error(
            "Corrupt FFOR block: exception_count exceeds block_count");
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
            throw std::runtime_error(
                "Corrupt FFOR block: packed data extends beyond end of buffer");
        }

        const uint8_t* packed_ptr = s.data + s.offset;
        s.offset += packed_bytes;

        // Copy to an aligned stack buffer — the source slice may not be 8-byte
        // aligned, and reinterpret_cast<const uint64_t*> on unaligned data
        // is UB (crashes with SIGBUS on ARM / Graviton).
        // Max packed_words = ceil(BLOCK_SIZE * 64 / 64) = BLOCK_SIZE = 1024 (8KB stack).
        alignas(8) uint64_t aligned_packed[IntegerEncoderFFOR::BLOCK_SIZE];
        std::memcpy(aligned_packed, packed_ptr, packed_bytes);

        alp::ffor_unpack_u64(aligned_packed,
                             hdr.block_count, hdr.base,
                             hdr.bw, out);
    }

    // Patch exceptions
    if (hdr.exc_count > 0) {
        // Read exception positions directly from Slice pointer.
        // Positions are packed 4 x uint16_t per uint64_t word.
        const size_t pos_words = (hdr.exc_count + 3) / 4;
        const size_t pos_bytes = pos_words * sizeof(uint64_t);

        // Bounds check: exception position words.
        if (s.offset + pos_bytes > s.length_) [[unlikely]] {
            throw std::runtime_error(
                "Corrupt FFOR block: exception positions extend beyond end of buffer");
        }

        const uint8_t* pos_ptr = s.data + s.offset;
        s.offset += pos_bytes;

        // Read exception values directly from Slice pointer.
        const size_t val_bytes = hdr.exc_count * sizeof(uint64_t);

        // Bounds check: exception values.
        if (s.offset + val_bytes > s.length_) [[unlikely]] {
            throw std::runtime_error(
                "Corrupt FFOR block: exception values extend beyond end of buffer");
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
                    throw std::runtime_error(
                        "Corrupt FFOR block: exception position (" +
                        std::to_string(pos) + ") >= block_count (" +
                        std::to_string(hdr.block_count) + ")");
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
void encodeImpl(std::span<const uint64_t> values, AlignedBuffer &buf) {
    constexpr size_t BS = IntegerEncoderFFOR::BLOCK_SIZE;
    const size_t sz = values.size();
    const size_t num_blocks = (sz + BS - 1) / BS;

    auto& zigzag = getScratch().zigzag;
    zigzag.resize(BS);  // ensure capacity; only reallocates on first call

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
                int64_t delta = static_cast<int64_t>(values[1]) - static_cast<int64_t>(values[0]);
                zz = ZigZag::zigzagEncode(delta);
                zigzag[1] = zz;
                if (zz < block_min) block_min = zz;
                if (zz > block_max) block_max = zz;
                zz_idx = 2;
                val_idx = 2;
            }
        }

        // Remaining values in this block: zigzag(delta-of-delta)
        // Process 4 values at a time for better ILP on min/max tracking.
#if defined(__AVX2__) && defined(__AVX512VL__) && defined(__AVX512DQ__)
        // AVX2 + AVX-512 VL path: use 256-bit YMM registers for 4x int64 operations.
        // The delta-of-delta computation stays scalar (sequential memory dependencies),
        // but zigzag encode and min/max tracking use full 256-bit SIMD.
        {
            __m256i vmin = _mm256_set1_epi64x(static_cast<int64_t>(block_min));
            __m256i vmax = _mm256_set1_epi64x(static_cast<int64_t>(block_max));

            for (; zz_idx + 3 < block_count && val_idx >= 2; val_idx += 4, zz_idx += 4) {
                // Delta-of-delta: sequential memory accesses, kept scalar
                int64_t D0 = (static_cast<int64_t>(values[val_idx])     - static_cast<int64_t>(values[val_idx - 1]))
                           - (static_cast<int64_t>(values[val_idx - 1]) - static_cast<int64_t>(values[val_idx - 2]));
                int64_t D1 = (static_cast<int64_t>(values[val_idx + 1]) - static_cast<int64_t>(values[val_idx]))
                           - (static_cast<int64_t>(values[val_idx])     - static_cast<int64_t>(values[val_idx - 1]));
                int64_t D2 = (static_cast<int64_t>(values[val_idx + 2]) - static_cast<int64_t>(values[val_idx + 1]))
                           - (static_cast<int64_t>(values[val_idx + 1]) - static_cast<int64_t>(values[val_idx]));
                int64_t D3 = (static_cast<int64_t>(values[val_idx + 3]) - static_cast<int64_t>(values[val_idx + 2]))
                           - (static_cast<int64_t>(values[val_idx + 2]) - static_cast<int64_t>(values[val_idx + 1]));

                // Load 4 deltas into a YMM register
                __m256i deltas = _mm256_set_epi64x(D3, D2, D1, D0);

                // ZigZag encode 4 values: (x << 1) ^ (x >> 63)
                // AVX-512 VL provides vpsraq for 64-bit arithmetic right shift on YMM
                __m256i shifted = _mm256_slli_epi64(deltas, 1);
                __m256i sign = _mm256_srai_epi64(deltas, 63);   // AVX-512 VL
                __m256i zz_vec = _mm256_xor_si256(shifted, sign);

                // Store 4 zigzag values directly to scratch buffer
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(&zigzag[zz_idx]), zz_vec);

                // Unsigned min/max tracking using AVX-512 VL vpminuq/vpmaxuq on YMM
                vmin = _mm256_min_epu64(vmin, zz_vec);
                vmax = _mm256_max_epu64(vmax, zz_vec);
            }

            // Horizontal reduction of vmin/vmax: reduce 4 lanes -> 1 scalar each
            // Extract high 128-bit half and reduce with low half
            __m128i min_lo = _mm256_castsi256_si128(vmin);
            __m128i min_hi = _mm256_extracti128_si256(vmin, 1);
            __m128i min_r = _mm_min_epu64(min_lo, min_hi);  // AVX-512 VL: 2 lanes
            // Reduce 2 lanes to 1: shuffle high lane to low and reduce
            __m128i min_shuf = _mm_unpackhi_epi64(min_r, min_r);
            min_r = _mm_min_epu64(min_r, min_shuf);
            block_min = static_cast<uint64_t>(_mm_cvtsi128_si64(min_r));

            __m128i max_lo = _mm256_castsi256_si128(vmax);
            __m128i max_hi = _mm256_extracti128_si256(vmax, 1);
            __m128i max_r = _mm_max_epu64(max_lo, max_hi);
            __m128i max_shuf = _mm_unpackhi_epi64(max_r, max_r);
            max_r = _mm_max_epu64(max_r, max_shuf);
            block_max = static_cast<uint64_t>(_mm_cvtsi128_si64(max_r));
        }
#else
        // Scalar fallback: process 4 values at a time with manual min/max reduction
        for (; zz_idx + 3 < block_count && val_idx >= 2; val_idx += 4, zz_idx += 4) {
            int64_t D0 = (static_cast<int64_t>(values[val_idx])     - static_cast<int64_t>(values[val_idx - 1]))
                       - (static_cast<int64_t>(values[val_idx - 1]) - static_cast<int64_t>(values[val_idx - 2]));
            int64_t D1 = (static_cast<int64_t>(values[val_idx + 1]) - static_cast<int64_t>(values[val_idx]))
                       - (static_cast<int64_t>(values[val_idx])     - static_cast<int64_t>(values[val_idx - 1]));
            int64_t D2 = (static_cast<int64_t>(values[val_idx + 2]) - static_cast<int64_t>(values[val_idx + 1]))
                       - (static_cast<int64_t>(values[val_idx + 1]) - static_cast<int64_t>(values[val_idx]));
            int64_t D3 = (static_cast<int64_t>(values[val_idx + 3]) - static_cast<int64_t>(values[val_idx + 2]))
                       - (static_cast<int64_t>(values[val_idx + 2]) - static_cast<int64_t>(values[val_idx + 1]));

            uint64_t zz0 = ZigZag::zigzagEncode(D0);
            uint64_t zz1 = ZigZag::zigzagEncode(D1);
            uint64_t zz2 = ZigZag::zigzagEncode(D2);
            uint64_t zz3 = ZigZag::zigzagEncode(D3);

            zigzag[zz_idx]     = zz0;
            zigzag[zz_idx + 1] = zz1;
            zigzag[zz_idx + 2] = zz2;
            zigzag[zz_idx + 3] = zz3;

            uint64_t lo01 = zz0 < zz1 ? zz0 : zz1;
            uint64_t hi01 = zz0 > zz1 ? zz0 : zz1;
            uint64_t lo23 = zz2 < zz3 ? zz2 : zz3;
            uint64_t hi23 = zz2 > zz3 ? zz2 : zz3;
            uint64_t lo = lo01 < lo23 ? lo01 : lo23;
            uint64_t hi = hi01 > hi23 ? hi01 : hi23;
            if (lo < block_min) block_min = lo;
            if (hi > block_max) block_max = hi;
        }
#endif

        // Scalar tail for remaining values in this block
        for (; zz_idx < block_count && val_idx < sz; ++zz_idx, ++val_idx) {
            if (val_idx < 2) {
                // This handles the edge case where block_count <= 2 and we're
                // still in the first two values (already handled above, but guard).
                // Decrement zz_idx so the loop increment doesn't skip a buffer slot,
                // leaving stale/uninitialized data in zigzag[].
                --zz_idx;
                continue;
            }
            int64_t D = (static_cast<int64_t>(values[val_idx])     - static_cast<int64_t>(values[val_idx - 1]))
                      - (static_cast<int64_t>(values[val_idx - 1]) - static_cast<int64_t>(values[val_idx - 2]));
            uint64_t zz = ZigZag::zigzagEncode(D);
            zigzag[zz_idx] = zz;
            if (zz < block_min) block_min = zz;
            if (zz > block_max) block_max = zz;
        }

        // Guard against empty block (shouldn't happen, but defensive)
        if (block_min > block_max) { block_min = block_max = 0; }

        encodeBlock(zigzag.data(), block_count, buf, block_min, block_max);
    }
}

} // anonymous namespace

// =============================================================================
// Public API
// =============================================================================

AlignedBuffer IntegerEncoderFFOR::encode(std::span<const uint64_t> values) {
    AlignedBuffer buf;
    if (values.empty()) return buf;
    encodeImpl(values, buf);
    return buf;
}

size_t IntegerEncoderFFOR::encodeInto(std::span<const uint64_t> values, AlignedBuffer &target) {
    if (values.empty()) return 0;
    const size_t startPos = target.size();
    encodeImpl(values, target);
    return target.size() - startPos;
}

std::pair<size_t, size_t> IntegerEncoderFFOR::decode(
    Slice &encoded, unsigned int timestampSize,
    std::vector<uint64_t> &values, uint64_t minTime, uint64_t maxTime) {

    // Fused decode: unpack one FFOR block at a time into a small stack buffer,
    // then immediately perform zigzag decode + delta reconstruction + time filtering.
    // Benefits:
    //   1. 8KB block buffer (1024 × 8B) stays hot in L1 cache
    //   2. No large intermediate deltaValues allocation
    //   3. Early exit when maxTime exceeded (skip remaining blocks)

    constexpr size_t BS = BLOCK_SIZE;  // 1024
    alignas(64) uint64_t blockBuf[BS]; // 8KB stack buffer - fits in L1 cache

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
            if (last_decoded < minTime) { nSkipped++; }
            else if (last_decoded > maxTime) { return {nSkipped, nAdded}; }
            else { values.push_back(last_decoded); nAdded++; }

            delta += dd1;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) { nSkipped++; }
            else if (last_decoded > maxTime) { return {nSkipped, nAdded}; }
            else { values.push_back(last_decoded); nAdded++; }

            delta += dd2;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) { nSkipped++; }
            else if (last_decoded > maxTime) { return {nSkipped, nAdded}; }
            else { values.push_back(last_decoded); nAdded++; }

            delta += dd3;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
            if (last_decoded < minTime) { nSkipped++; }
            else if (last_decoded > maxTime) { return {nSkipped, nAdded}; }
            else { values.push_back(last_decoded); nAdded++; }
        }

        // Scalar tail for remaining values in this block
        for (; local_i < block_count; ++local_i) {
            int64_t dd = ZigZag::zigzagDecode(blockBuf[local_i]);
            delta += dd;
            last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

            if (last_decoded < minTime) { nSkipped++; continue; }
            if (last_decoded > maxTime) { return {nSkipped, nAdded}; }
            values.push_back(last_decoded);
            nAdded++;
        }

    }

    return {nSkipped, nAdded};
}
