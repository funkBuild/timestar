#pragma once

#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <vector>

class ALPDecoder {
public:
    // Decode ALP-compressed data from a CompressedSlice.
    // Matches the FloatDecoderBasic API signature.
    // Returns the number of values ACTUALLY decoded from the stream, which may
    // be fewer than `length` if the stream ran out. See docs: the block-level
    // caller enforces produced-vs-expected; decoders only report.
    static size_t decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);
};
