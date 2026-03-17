#pragma once

#include "../../storage/compressed_buffer.hpp"

#include <cstdint>
#include <span>
#include <vector>

class AlignedBuffer;

class ALPEncoder {
public:
    // Encode doubles using ALP compression.
    // Returns a CompressedBuffer compatible with the existing encoder API.
    static CompressedBuffer encode(std::span<const double> values);

    // Encode doubles directly into an AlignedBuffer (zero intermediate allocation).
    // Writes uint64_t words directly into the target buffer, avoiding the
    // CompressedBuffer allocation and subsequent copy.
    // Returns the number of bytes written.
    static size_t encodeInto(std::span<const double> values, AlignedBuffer& target);
};
