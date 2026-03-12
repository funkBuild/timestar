#ifndef ALP_RD_HPP_INCLUDED
#define ALP_RD_HPP_INCLUDED

#include <cstddef>
#include <cstdint>
#include <vector>

namespace alp {

// ALP_RD (Real Doubles) fallback encoder for doubles that resist decimal encoding.
// Splits each double's 64-bit representation into:
//   - Left part:  upper bits, dictionary-encoded (up to 8 entries)
//   - Right part: lower bits, FFOR bit-packed
// Exceptions are stored for left parts not in the dictionary.

struct ALPRDBlockResult {
    // Dictionary for left parts
    std::vector<uint64_t> dictionary;  // up to ALP_RD_MAX_DICT_SIZE entries

    // Per-value data
    std::vector<uint8_t> left_indices;  // index into dictionary per value
    std::vector<uint64_t> right_parts;  // lower bits per value

    // Exceptions: values whose left part isn't in dictionary
    std::vector<uint16_t> exception_positions;
    std::vector<uint64_t> exception_values;  // raw bit_cast<uint64_t>(double)

    // Bit split position (how many bits in right part)
    uint8_t right_bit_count = 0;
    uint8_t left_bw = 0;   // bits needed to encode dictionary index
    uint8_t right_bw = 0;  // bits needed for FFOR of right parts
    uint64_t right_for_base = 0;
};

class ALPRD {
public:
    // Find the best bit-split position for a block of doubles.
    // Returns the number of right bits that minimizes exceptions.
    static uint8_t findBestSplit(const double* values, size_t count);

    // Encode a block of doubles using ALP_RD scheme.
    static ALPRDBlockResult encodeBlock(const double* values, size_t count, uint8_t right_bit_count);

    // Decode a block: reconstruct doubles from dictionary, left indices, right parts, and exceptions.
    static void decodeBlock(const ALPRDBlockResult& block, size_t count, double* out);
};

}  // namespace alp

#endif  // ALP_RD_HPP_INCLUDED
