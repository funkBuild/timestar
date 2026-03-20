#include "alp_rd.hpp"

#include "alp_constants.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <unordered_map>

namespace alp {

uint8_t ALPRD::findBestSplit(const double* values, size_t count) {
    // Try different right-bit-count values and find the one that yields
    // the fewest exceptions (left parts fitting in a dictionary of ≤8 entries).
    // We sample to keep this fast.
    const size_t sample_count = std::min(count, ALP_SAMPLE_SIZE);

    uint8_t best_right_bits = 32;  // default split at middle
    size_t best_exceptions = count + 1;

    // Try right bit counts from 8 to 56 in steps of 4
    for (uint8_t rb = 8; rb <= 56; rb += 4) {
        const uint64_t right_mask = (rb == 64) ? ~0ULL : ((1ULL << rb) - 1);
        std::unordered_map<uint64_t, size_t> left_freq;

        for (size_t i = 0; i < sample_count; ++i) {
            uint64_t bits = std::bit_cast<uint64_t>(values[i]);
            uint64_t left = bits >> rb;
            left_freq[left]++;
        }

        // Count how many distinct left parts
        if (left_freq.size() <= ALP_RD_MAX_DICT_SIZE) {
            // All fit in dictionary - zero exceptions
            if (0 < best_exceptions) {
                best_exceptions = 0;
                best_right_bits = rb;
            }
        } else {
            // Pick top-8 by frequency, count exceptions
            std::vector<std::pair<uint64_t, size_t>> freq_vec(left_freq.begin(), left_freq.end());
            std::partial_sort(freq_vec.begin(), freq_vec.begin() + ALP_RD_MAX_DICT_SIZE, freq_vec.end(),
                              [](const auto& a, const auto& b) { return a.second > b.second; });

            std::unordered_map<uint64_t, bool> top8;
            for (size_t i = 0; i < ALP_RD_MAX_DICT_SIZE && i < freq_vec.size(); ++i) {
                top8[freq_vec[i].first] = true;
            }

            size_t exceptions = 0;
            for (size_t i = 0; i < sample_count; ++i) {
                uint64_t bits = std::bit_cast<uint64_t>(values[i]);
                uint64_t left = bits >> rb;
                if (top8.find(left) == top8.end()) {
                    exceptions++;
                }
            }

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

ALPRDBlockResult ALPRD::encodeBlock(const double* values, size_t count, uint8_t right_bit_count) {
    ALPRDBlockResult result;
    result.right_bit_count = right_bit_count;

    const uint64_t right_mask = (right_bit_count == 64) ? ~0ULL : ((1ULL << right_bit_count) - 1);

    // Build frequency map for left parts
    std::unordered_map<uint64_t, size_t> left_freq;
    for (size_t i = 0; i < count; ++i) {
        uint64_t bits = std::bit_cast<uint64_t>(values[i]);
        uint64_t left = bits >> right_bit_count;
        left_freq[left]++;
    }

    // Build dictionary from top-N most frequent left parts
    std::vector<std::pair<uint64_t, size_t>> freq_vec(left_freq.begin(), left_freq.end());
    std::sort(freq_vec.begin(), freq_vec.end(), [](const auto& a, const auto& b) { return a.second > b.second; });

    const size_t dict_size = std::min(freq_vec.size(), ALP_RD_MAX_DICT_SIZE);
    result.dictionary.resize(dict_size);
    std::unordered_map<uint64_t, uint8_t> dict_map;
    for (size_t i = 0; i < dict_size; ++i) {
        result.dictionary[i] = freq_vec[i].first;
        dict_map[freq_vec[i].first] = static_cast<uint8_t>(i);
    }

    // Bits needed for dictionary index
    if (dict_size <= 1) {
        result.left_bw = 0;
    } else {
        result.left_bw = static_cast<uint8_t>(64 - __builtin_clzll(dict_size - 1));
    }

    // Encode each value
    result.left_indices.resize(count);
    result.right_parts.resize(count);

    uint64_t right_min = ~0ULL;
    uint64_t right_max = 0;

    for (size_t i = 0; i < count; ++i) {
        uint64_t bits = std::bit_cast<uint64_t>(values[i]);
        uint64_t left = bits >> right_bit_count;
        uint64_t right = bits & right_mask;

        result.right_parts[i] = right;
        if (right < right_min)
            right_min = right;
        if (right > right_max)
            right_max = right;

        auto it = dict_map.find(left);
        if (it != dict_map.end()) {
            result.left_indices[i] = it->second;
        } else {
            // Exception: store full raw value, use index 0 as placeholder
            result.left_indices[i] = 0;
            result.exception_positions.push_back(static_cast<uint16_t>(i));
            result.exception_values.push_back(bits);
        }
    }

    // FFOR parameters for right parts
    result.right_for_base = right_min;
    uint64_t right_range = right_max - right_min;
    if (right_range == 0) {
        result.right_bw = 0;
    } else {
        result.right_bw = static_cast<uint8_t>(64 - __builtin_clzll(right_range));
    }

    return result;
}

}  // namespace alp
