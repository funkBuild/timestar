#include "alp_rd.hpp"

#include "alp_constants.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <vector>

namespace timestar::alp {

namespace {

// Thread-local scratch for encodeBlock. Seastar is shard-per-core (one thread
// per shard), so thread_local is safe and avoids per-block heap allocations.
// ALP encode paths are synchronous (no co_await between fill and use).
struct ALPRDScratch {
    std::vector<uint64_t> sorted_left;  // fallback path: up to ALP_VECTOR_SIZE entries
    std::vector<uint8_t> slot_of;       // fast path: freq-table slot per value
    ALPRDScratch() {
        sorted_left.reserve(ALP_VECTOR_SIZE);
        slot_of.reserve(ALP_VECTOR_SIZE);
    }
};

ALPRDScratch& rdScratch() {
    static thread_local ALPRDScratch scratch;
    return scratch;
}

}  // namespace

uint8_t ALPRD::findBestSplit(const double* values, size_t count) {
    // Try different right-bit-count values and find the one that yields
    // the fewest exceptions (left parts fitting in a dictionary of ≤8 entries).
    // We sample to keep this fast.
    //
    // Key insight: unsigned right-shift preserves order, so after sorting the
    // sampled bit patterns ONCE, the distinct left parts under any shift `rb`
    // are adjacent runs in the sorted array. One linear scan per candidate
    // counts distinct left parts and accumulates the top-8 run frequencies —
    // no hash maps, no per-candidate sorting.
    const size_t sample_count = std::min(count, ALP_SAMPLE_SIZE);

    static_assert(ALP_SAMPLE_SIZE <= 256, "sample stack buffer sized for ALP_SAMPLE_SIZE");
    std::array<uint64_t, 256> sample;
    for (size_t i = 0; i < sample_count; ++i) {
        sample[i] = std::bit_cast<uint64_t>(values[i]);
    }
    std::sort(sample.begin(), sample.begin() + sample_count);

    uint8_t best_right_bits = 32;  // default split at middle
    size_t best_exceptions = count + 1;

    // Try right bit counts from 8 to 56 in steps of 4
    for (uint8_t rb = 8; rb <= 56; rb += 4) {
        // Scan runs of equal left parts in the sorted sample.
        size_t distinct = 0;
        // Top-8 run frequencies (min-replace selection keeps the 8 largest).
        std::array<size_t, ALP_RD_MAX_DICT_SIZE> top_freq{};

        size_t i = 0;
        while (i < sample_count) {
            const uint64_t left = sample[i] >> rb;
            size_t j = i + 1;
            while (j < sample_count && (sample[j] >> rb) == left) {
                ++j;
            }
            const size_t freq = j - i;
            ++distinct;

            // Replace the smallest of the current top-8 if this run is larger.
            size_t min_idx = 0;
            for (size_t k = 1; k < ALP_RD_MAX_DICT_SIZE; ++k) {
                if (top_freq[k] < top_freq[min_idx]) {
                    min_idx = k;
                }
            }
            if (freq > top_freq[min_idx]) {
                top_freq[min_idx] = freq;
            }
            i = j;
        }

        if (distinct <= ALP_RD_MAX_DICT_SIZE) {
            // All fit in dictionary - zero exceptions
            if (0 < best_exceptions) {
                best_exceptions = 0;
                best_right_bits = rb;
            }
        } else {
            size_t covered = 0;
            for (size_t f : top_freq) {
                covered += f;
            }
            const size_t exceptions = sample_count - covered;

            // Scale exceptions to full count estimate
            size_t estimated = (exceptions * count + sample_count - 1) / sample_count;
            if (estimated < best_exceptions) {
                best_exceptions = estimated;
                best_right_bits = rb;
            }
        }
    }

    return best_right_bits;
}

void ALPRD::encodeBlock(const double* values, size_t count, uint8_t right_bit_count, ALPRDBlockResult& result) {
    result.right_bit_count = right_bit_count;
    result.exception_positions.clear();
    result.exception_values.clear();
    result.left_indices.resize(count);
    result.right_parts.resize(count);

    const uint64_t right_mask = (right_bit_count == 64) ? ~0ULL : ((1ULL << right_bit_count) - 1);

    auto& scratch = rdScratch();

    // Pass 1: extract right parts (+ min/max) and count left-part frequencies
    // in a small open-addressed hash table (256 slots, <=128 distinct so load
    // factor stays <=0.5 and probes are short). findBestSplit chose the split
    // to keep distinct left parts near the dictionary size, so blocks
    // typically have only a handful of distinct values — one probe per lookup.
    // Each value's table slot is recorded so pass 2 is a pure remap — no
    // per-value dictionary search.
    constexpr size_t kHashSlots = 256;  // power of two
    constexpr size_t kMaxDistinct = 128;
    uint64_t h_key[kHashSlots];
    uint32_t h_cnt[kHashSlots];  // 0 = empty slot
    uint8_t used_slots[kMaxDistinct];
    size_t distinct = 0;
    bool small_table_ok = true;
    std::memset(h_cnt, 0, sizeof(h_cnt));

    scratch.slot_of.resize(count);
    uint8_t* slot_of = scratch.slot_of.data();

    uint64_t right_min = ~0ULL;
    uint64_t right_max = 0;

    for (size_t i = 0; i < count; ++i) {
        const uint64_t bits = std::bit_cast<uint64_t>(values[i]);
        const uint64_t right = bits & right_mask;
        result.right_parts[i] = right;
        right_min = (right < right_min) ? right : right_min;
        right_max = (right > right_max) ? right : right_max;

        const uint64_t left = bits >> right_bit_count;
        size_t slot = (left * 0x9E3779B97F4A7C15ULL) >> 56;
        while (true) {
            if (h_cnt[slot] == 0) {
                if (distinct == kMaxDistinct) {
                    small_table_ok = false;
                    break;
                }
                h_key[slot] = left;
                h_cnt[slot] = 1;
                used_slots[distinct++] = static_cast<uint8_t>(slot);
                break;
            }
            if (h_key[slot] == left) {
                ++h_cnt[slot];
                break;
            }
            slot = (slot + 1) & (kHashSlots - 1);
        }
        if (!small_table_ok) {
            break;
        }
        slot_of[i] = static_cast<uint8_t>(slot);
    }

    // FFOR parameters for right parts (finish the scan if pass 1 bailed early)
    if (!small_table_ok) {
        for (size_t i = 0; i < count; ++i) {
            const uint64_t right = std::bit_cast<uint64_t>(values[i]) & right_mask;
            result.right_parts[i] = right;
            right_min = (right < right_min) ? right : right_min;
            right_max = (right > right_max) ? right : right_max;
        }
    }
    result.right_for_base = right_min;
    const uint64_t right_range = right_max - right_min;
    result.right_bw = (right_range == 0) ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(right_range));

    // Select the top-8 most frequent left parts via insertion into small
    // fixed-size arrays kept sorted by frequency (descending). Ties keep the
    // earlier entry — deterministic, and any valid dictionary ordering decodes
    // correctly since indices reference it.
    uint64_t dict_val[ALP_RD_MAX_DICT_SIZE];
    uint32_t dict_freq[ALP_RD_MAX_DICT_SIZE];
    size_t dict_size = 0;

    auto offer = [&](uint64_t left, uint32_t freq) {
        if (dict_size < ALP_RD_MAX_DICT_SIZE) {
            size_t pos = dict_size;
            while (pos > 0 && dict_freq[pos - 1] < freq) {
                dict_val[pos] = dict_val[pos - 1];
                dict_freq[pos] = dict_freq[pos - 1];
                --pos;
            }
            dict_val[pos] = left;
            dict_freq[pos] = freq;
            ++dict_size;
        } else if (freq > dict_freq[ALP_RD_MAX_DICT_SIZE - 1]) {
            size_t pos = ALP_RD_MAX_DICT_SIZE - 1;
            while (pos > 0 && dict_freq[pos - 1] < freq) {
                dict_val[pos] = dict_val[pos - 1];
                dict_freq[pos] = dict_freq[pos - 1];
                --pos;
            }
            dict_val[pos] = left;
            dict_freq[pos] = freq;
        }
    };

    if (small_table_ok) {
        for (size_t d = 0; d < distinct; ++d) {
            const size_t slot = used_slots[d];
            offer(h_key[slot], h_cnt[slot]);
        }
    } else {
        // High-cardinality fallback: sort a scratch copy and run-count
        // (shift preserves order, so equal left parts are adjacent).
        scratch.sorted_left.resize(count);
        for (size_t i = 0; i < count; ++i) {
            scratch.sorted_left[i] = std::bit_cast<uint64_t>(values[i]) >> right_bit_count;
        }
        std::sort(scratch.sorted_left.begin(), scratch.sorted_left.end());

        const uint64_t* sorted = scratch.sorted_left.data();
        size_t i = 0;
        while (i < count) {
            const uint64_t left = sorted[i];
            size_t j = i + 1;
            while (j < count && sorted[j] == left) {
                ++j;
            }
            offer(left, static_cast<uint32_t>(j - i));
            i = j;
        }
    }

    result.dictionary.resize(dict_size);
    for (size_t i = 0; i < dict_size; ++i) {
        result.dictionary[i] = dict_val[i];
    }

    // Bits needed for dictionary index
    if (dict_size <= 1) {
        result.left_bw = 0;
    } else {
        result.left_bw = static_cast<uint8_t>(64 - __builtin_clzll(dict_size - 1));
    }

    // Pass 2: emit dictionary indices / exceptions.
    if (small_table_ok) {
        // Map each hash-table slot to its dictionary index (0xFF = exception).
        uint8_t remap[kHashSlots];
        std::memset(remap, 0xFF, sizeof(remap));
        for (size_t d = 0; d < dict_size; ++d) {
            for (size_t s = 0; s < distinct; ++s) {
                const size_t slot = used_slots[s];
                if (h_key[slot] == dict_val[d]) {
                    remap[slot] = static_cast<uint8_t>(d);
                    break;
                }
            }
        }

        for (size_t i = 0; i < count; ++i) {
            const uint8_t m = remap[slot_of[i]];
            if (m != 0xFF) {
                result.left_indices[i] = m;
            } else {
                // Exception: store full raw value, use index 0 as placeholder
                result.left_indices[i] = 0;
                result.exception_positions.push_back(static_cast<uint16_t>(i));
                result.exception_values.push_back(std::bit_cast<uint64_t>(values[i]));
            }
        }
    } else {
        // Fallback: linear scan over the <=8-entry dictionary per value.
        for (size_t i = 0; i < count; ++i) {
            const uint64_t bits = std::bit_cast<uint64_t>(values[i]);
            const uint64_t left = bits >> right_bit_count;

            size_t idx = dict_size;
            for (size_t d = 0; d < dict_size; ++d) {
                if (dict_val[d] == left) {
                    idx = d;
                    break;
                }
            }

            if (idx < dict_size) {
                result.left_indices[i] = static_cast<uint8_t>(idx);
            } else {
                result.left_indices[i] = 0;
                result.exception_positions.push_back(static_cast<uint16_t>(i));
                result.exception_values.push_back(bits);
            }
        }
    }
}

}  // namespace timestar::alp
