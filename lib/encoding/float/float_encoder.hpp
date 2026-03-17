#pragma once

#include "../../storage/compressed_buffer.hpp"
#include "../../storage/slice_buffer.hpp"

#include <cstdint>
#include <span>
#include <vector>

class FloatEncoderBasic {
private:
public:
    FloatEncoderBasic() {}

    static CompressedBuffer encode(std::span<const double> values);
    static void decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out);
};
