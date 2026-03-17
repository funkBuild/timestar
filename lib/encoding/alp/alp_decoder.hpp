#pragma once

#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <vector>

class ALPDecoder {
public:
    // Decode ALP-compressed data from a CompressedSlice.
    // Matches the FloatDecoderBasic API signature.
    static void decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);
};
