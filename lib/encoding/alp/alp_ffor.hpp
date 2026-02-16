#ifndef ALP_FFOR_HPP_INCLUDED
#define ALP_FFOR_HPP_INCLUDED

#include <cstdint>
#include <cstddef>
#include <cstring>

namespace alp {

// Frame-of-Reference + bit-packing for int64 values.
// Packs (value - base) into fixed bit-width words.
// GCC 14 -O3 -march=native auto-vectorizes these loops effectively.

// Calculate number of uint64_t words needed to store `count` values at `bw` bits each
inline size_t ffor_packed_words(size_t count, uint8_t bw) {
    if (bw == 0) return 0;
    return (static_cast<uint64_t>(count) * bw + 63) / 64;
}

// Pack `count` int64 values into bit-packed uint64 words.
// Each value is stored as (values[i] - base) using `bw` bits.
// Caller must ensure (values[i] - base) fits in `bw` bits.
// `out` must have space for ffor_packed_words(count, bw) words.
inline void ffor_pack(const int64_t* values, size_t count, int64_t base,
                      uint8_t bw, uint64_t* out) {
    if (bw == 0 || count == 0) return;

    const size_t n_words = ffor_packed_words(count, bw);
    std::memset(out, 0, n_words * sizeof(uint64_t));

    if (bw == 64) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = static_cast<uint64_t>(values[i] - base);
        }
        return;
    }

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

    const size_t n_words = ffor_packed_words(count, bw);
    std::memset(out, 0, n_words * sizeof(uint64_t));

    if (bw == 64) {
        for (size_t i = 0; i < count; ++i) {
            out[i] = values[i] - base;
        }
        return;
    }

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
