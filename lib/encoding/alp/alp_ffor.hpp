#ifndef ALP_FFOR_HPP_INCLUDED
#define ALP_FFOR_HPP_INCLUDED

#include <cstdint>
#include <cstddef>
#include <cstring>

namespace alp {

// Frame-of-Reference + bit-packing for int64 values.
// Packs (value - base) into fixed bit-width words.
// GCC 14 -O3 -march=native auto-vectorizes these loops effectively.
//
// For power-of-2 bit-widths (1, 2, 4, 8, 16, 32), values pack perfectly
// into uint64 words with no word-spanning, enabling branchless tight loops
// that the compiler can auto-vectorize.

// Calculate number of uint64_t words needed to store `count` values at `bw` bits each
inline size_t ffor_packed_words(size_t count, uint8_t bw) {
    if (bw == 0) return 0;
    return (static_cast<uint64_t>(count) * bw + 63) / 64;
}

// ---------------------------------------------------------------------------
// Specialized pack/unpack for power-of-2 bit-widths where values never span
// word boundaries (64 is divisible by 1, 2, 4, 8, 16, 32).
// These are branchless inner loops that the compiler can auto-vectorize.
// ---------------------------------------------------------------------------

namespace detail {

// Pack values into words for a known power-of-2 bit-width.
// Template parameter BW must be one of {1, 2, 4, 8, 16, 32}.
// Values per word = 64 / BW.
template<unsigned BW>
inline void pack_pow2_u64(const uint64_t* values, size_t count, uint64_t base,
                          uint64_t* out) {
    constexpr unsigned VPW = 64 / BW;  // values per word
    constexpr uint64_t mask = (1ULL << BW) - 1;
    const size_t n_words = ffor_packed_words(count, BW);

    size_t i = 0;
    for (size_t w = 0; w < n_words && i < count; ++w) {
        uint64_t word = 0;
        for (unsigned j = 0; j < VPW && i < count; ++j, ++i) {
            word |= ((values[i] - base) & mask) << (j * BW);
        }
        out[w] = word;
    }
}

template<unsigned BW>
inline void pack_pow2_i64(const int64_t* values, size_t count, int64_t base,
                          uint64_t* out) {
    constexpr unsigned VPW = 64 / BW;
    constexpr uint64_t mask = (1ULL << BW) - 1;
    const size_t n_words = ffor_packed_words(count, BW);

    size_t i = 0;
    for (size_t w = 0; w < n_words && i < count; ++w) {
        uint64_t word = 0;
        for (unsigned j = 0; j < VPW && i < count; ++j, ++i) {
            word |= (static_cast<uint64_t>(values[i] - base) & mask) << (j * BW);
        }
        out[w] = word;
    }
}

// Unpack values from words for a known power-of-2 bit-width.
template<unsigned BW>
inline void unpack_pow2_u64(const uint64_t* in, size_t count, uint64_t base,
                            uint64_t* out) {
    constexpr unsigned VPW = 64 / BW;
    constexpr uint64_t mask = (1ULL << BW) - 1;
    const size_t n_words = ffor_packed_words(count, BW);

    size_t i = 0;
    for (size_t w = 0; w < n_words && i < count; ++w) {
        uint64_t word = in[w];
        for (unsigned j = 0; j < VPW && i < count; ++j, ++i) {
            out[i] = base + ((word >> (j * BW)) & mask);
        }
    }
}

template<unsigned BW>
inline void unpack_pow2_i64(const uint64_t* in, size_t count, int64_t base,
                            int64_t* out) {
    constexpr unsigned VPW = 64 / BW;
    constexpr uint64_t mask = (1ULL << BW) - 1;
    const size_t n_words = ffor_packed_words(count, BW);

    size_t i = 0;
    for (size_t w = 0; w < n_words && i < count; ++w) {
        uint64_t word = in[w];
        for (unsigned j = 0; j < VPW && i < count; ++j, ++i) {
            out[i] = base + static_cast<int64_t>((word >> (j * BW)) & mask);
        }
    }
}

} // namespace detail

// Pack `count` int64 values into bit-packed uint64 words.
// Each value is stored as (values[i] - base) using `bw` bits.
// Caller must ensure (values[i] - base) fits in `bw` bits.
// `out` must have space for ffor_packed_words(count, bw) words.
inline void ffor_pack(const int64_t* values, size_t count, int64_t base,
                      uint8_t bw, uint64_t* out) {
    if (bw == 0 || count == 0) return;

    if (bw == 64) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = static_cast<uint64_t>(values[i] - base);
        }
        return;
    }

    // Specialized branchless loops for power-of-2 bit-widths.
    // These write complete words, so no pre-zeroing needed.
    switch (bw) {
    case 1:  detail::pack_pow2_i64<1>(values, count, base, out);  return;
    case 2:  detail::pack_pow2_i64<2>(values, count, base, out);  return;
    case 4:  detail::pack_pow2_i64<4>(values, count, base, out);  return;
    case 8:  detail::pack_pow2_i64<8>(values, count, base, out);  return;
    case 16: detail::pack_pow2_i64<16>(values, count, base, out); return;
    case 32: detail::pack_pow2_i64<32>(values, count, base, out); return;
    default: break;
    }

    // Generic fallback for non-power-of-2 bit-widths.
    // Values may span word boundaries, requiring the spanning check.
    const size_t n_words = ffor_packed_words(count, bw);
    std::memset(out, 0, n_words * sizeof(uint64_t));

    const uint64_t mask = (1ULL << bw) - 1;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        const uint64_t delta = static_cast<uint64_t>(values[i] - base) & mask;
        const size_t word_idx = bit_pos >> 6;       // bit_pos / 64
        const unsigned bit_idx = bit_pos & 63;      // bit_pos % 64

        out[word_idx] |= delta << bit_idx;

        // Handle value spanning two words
        if (bit_idx + bw > 64) {
            out[word_idx + 1] |= delta >> (64 - bit_idx);
        }

        bit_pos += bw;
    }
}

// Unsigned variant for ALP_RD right-parts
inline void ffor_pack_u64(const uint64_t* values, size_t count, uint64_t base,
                          uint8_t bw, uint64_t* out) {
    if (bw == 0 || count == 0) return;

    if (bw == 64) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = values[i] - base;
        }
        return;
    }

    // Specialized branchless loops for power-of-2 bit-widths.
    // These write complete words, so no pre-zeroing needed.
    switch (bw) {
    case 1:  detail::pack_pow2_u64<1>(values, count, base, out);  return;
    case 2:  detail::pack_pow2_u64<2>(values, count, base, out);  return;
    case 4:  detail::pack_pow2_u64<4>(values, count, base, out);  return;
    case 8:  detail::pack_pow2_u64<8>(values, count, base, out);  return;
    case 16: detail::pack_pow2_u64<16>(values, count, base, out); return;
    case 32: detail::pack_pow2_u64<32>(values, count, base, out); return;
    default: break;
    }

    // Generic fallback for non-power-of-2 bit-widths.
    const size_t n_words = ffor_packed_words(count, bw);
    std::memset(out, 0, n_words * sizeof(uint64_t));

    const uint64_t mask = (1ULL << bw) - 1;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        const uint64_t delta = (values[i] - base) & mask;
        const size_t word_idx = bit_pos >> 6;
        const unsigned bit_idx = bit_pos & 63;

        out[word_idx] |= delta << bit_idx;

        if (bit_idx + bw > 64) {
            out[word_idx + 1] |= delta >> (64 - bit_idx);
        }

        bit_pos += bw;
    }
}

// Unpack `count` int64 values from bit-packed uint64 words.
// Reconstructs values[i] = unpacked_delta + base.
inline void ffor_unpack(const uint64_t* in, size_t count, int64_t base,
                        uint8_t bw, int64_t* out) {
    if (count == 0) return;

    if (bw == 0) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = base;
        }
        return;
    }

    if (bw == 64) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = base + static_cast<int64_t>(in[i]);
        }
        return;
    }

    // Specialized branchless loops for power-of-2 bit-widths.
    switch (bw) {
    case 1:  detail::unpack_pow2_i64<1>(in, count, base, out);  return;
    case 2:  detail::unpack_pow2_i64<2>(in, count, base, out);  return;
    case 4:  detail::unpack_pow2_i64<4>(in, count, base, out);  return;
    case 8:  detail::unpack_pow2_i64<8>(in, count, base, out);  return;
    case 16: detail::unpack_pow2_i64<16>(in, count, base, out); return;
    case 32: detail::unpack_pow2_i64<32>(in, count, base, out); return;
    default: break;
    }

    // Generic fallback for non-power-of-2 bit-widths.
    const uint64_t mask = (1ULL << bw) - 1;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        const size_t word_idx = bit_pos >> 6;
        const unsigned bit_idx = bit_pos & 63;

        uint64_t delta = (in[word_idx] >> bit_idx) & mask;

        // Handle value spanning two words
        if (bit_idx + bw > 64) {
            const unsigned overflow_bits = bit_idx + bw - 64;
            delta |= (in[word_idx + 1] & ((1ULL << overflow_bits) - 1)) << (64 - bit_idx);
        }

        out[i] = base + static_cast<int64_t>(delta);
        bit_pos += bw;
    }
}

// Unsigned unpack variant for ALP_RD right-parts
inline void ffor_unpack_u64(const uint64_t* in, size_t count, uint64_t base,
                            uint8_t bw, uint64_t* out) {
    if (count == 0) return;

    if (bw == 0) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = base;
        }
        return;
    }

    if (bw == 64) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = base + in[i];
        }
        return;
    }

    // Specialized branchless loops for power-of-2 bit-widths.
    switch (bw) {
    case 1:  detail::unpack_pow2_u64<1>(in, count, base, out);  return;
    case 2:  detail::unpack_pow2_u64<2>(in, count, base, out);  return;
    case 4:  detail::unpack_pow2_u64<4>(in, count, base, out);  return;
    case 8:  detail::unpack_pow2_u64<8>(in, count, base, out);  return;
    case 16: detail::unpack_pow2_u64<16>(in, count, base, out); return;
    case 32: detail::unpack_pow2_u64<32>(in, count, base, out); return;
    default: break;
    }

    // Generic fallback for non-power-of-2 bit-widths.
    const uint64_t mask = (1ULL << bw) - 1;
    size_t bit_pos = 0;

    for (size_t i = 0; i < count; ++i) {
        const size_t word_idx = bit_pos >> 6;
        const unsigned bit_idx = bit_pos & 63;

        uint64_t delta = (in[word_idx] >> bit_idx) & mask;

        if (bit_idx + bw > 64) {
            const unsigned overflow_bits = bit_idx + bw - 64;
            delta |= (in[word_idx + 1] & ((1ULL << overflow_bits) - 1)) << (64 - bit_idx);
        }

        out[i] = base + delta;
        bit_pos += bw;
    }
}

} // namespace alp

#endif // ALP_FFOR_HPP_INCLUDED
