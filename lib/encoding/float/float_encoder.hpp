#ifndef FLOAT_ENCODER_BASIC_H_INCLUDED
#define FLOAT_ENCODER_BASIC_H_INCLUDED

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

#endif
