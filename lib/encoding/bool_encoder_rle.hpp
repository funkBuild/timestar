#ifndef BOOL_ENCODER_RLE_H_INCLUDED
#define BOOL_ENCODER_RLE_H_INCLUDED

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <cstdint>
#include <vector>

class BoolEncoderRLE {
public:
    static AlignedBuffer encode(const std::vector<bool>& values);
    static size_t encodeInto(const std::vector<bool>& values, AlignedBuffer& target);
    static void decode(Slice& encoded, size_t nToSkip, size_t length, std::vector<bool>& out);

private:
    static void writeVarint(AlignedBuffer& buf, uint64_t value);
    static uint64_t readVarint(Slice& slice);
};

#endif
