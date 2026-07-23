#pragma once

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <cstdint>
#include <vector>

class BoolEncoderRLE {
public:
    static AlignedBuffer encode(const std::vector<bool>& values);
    static size_t encodeInto(const std::vector<bool>& values, AlignedBuffer& target);
    // Returns the number of values ACTUALLY decoded (may be < length on a
    // truncated stream); the block-level caller enforces the count contract.
    static size_t decode(Slice& encoded, size_t nToSkip, size_t length, std::vector<bool>& out);

    // Decode straight to doubles (1.0/0.0) for the aggregation path.
    // Appends `length` values to `out` via bulk fill per run — avoids the
    // vector<bool> round-trip + per-bit-proxy conversion loop.
    static void decodeToDouble(Slice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);

private:
    static void writeVarint(AlignedBuffer& buf, uint64_t value);
    static uint64_t readVarint(Slice& slice);
};
