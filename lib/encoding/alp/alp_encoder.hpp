#ifndef ALP_ENCODER_HPP_INCLUDED
#define ALP_ENCODER_HPP_INCLUDED

#include <vector>
#include <cstdint>

#include "../../storage/compressed_buffer.hpp"

class ALPEncoder {
public:
    // Encode doubles using ALP compression.
    // Returns a CompressedBuffer compatible with the existing encoder API.
    static CompressedBuffer encode(const std::vector<double>& values);
};

#endif // ALP_ENCODER_HPP_INCLUDED
