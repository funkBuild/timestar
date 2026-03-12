#ifndef INTEGER_ENCODER_FFOR_H_INCLUDED
#define INTEGER_ENCODER_FFOR_H_INCLUDED

#include "../../storage/aligned_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <span>
#include <vector>

/**
 * IntegerEncoderFFOR - FFOR (Frame-of-Reference) based integer encoder
 *
 * Uses delta-of-delta encoding followed by ZigZag (same as the Basic encoder),
 * then replaces Simple16 variable-width packing with block-based FFOR
 * bit-packing and an exception mechanism for outliers.
 *
 * Key advantages over Simple16:
 * - Constant-interval timestamps compress to ~0 bits/value (vs 2+ bits/value)
 * - Branchless, auto-vectorizable packing/unpacking loops
 * - Per-block exception mechanism handles outliers without inflating all values
 *
 * Block format (per block of up to BLOCK_SIZE values):
 *   Word 0 (header): [0:10] block_count, [11:17] bit_width, [18:27] exception_count
 *   Word 1:          base (FOR reference = min zigzag value in block)
 *   Words 2..N:      FFOR-packed deltas (ceil(block_count * bw / 64) words)
 *   Words N+1..M:    exception positions (4 x uint16_t per word)
 *   Words M+1..end:  exception values (one uint64_t each)
 */
class IntegerEncoderFFOR {
public:
    static constexpr size_t BLOCK_SIZE = 1024;

    static AlignedBuffer encode(std::span<const uint64_t> values);

    static size_t encodeInto(std::span<const uint64_t> values, AlignedBuffer& target);

    static std::pair<size_t, size_t> decode(Slice& encoded, unsigned int timestampSize, std::vector<uint64_t>& values,
                                            uint64_t startTime = 0, uint64_t maxTime = UINT64_MAX);

    static bool isAvailable() { return true; }
};

#endif  // INTEGER_ENCODER_FFOR_H_INCLUDED
