#pragma once

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <cstdint>
#include <vector>

class BoolEncoder {
private:
    template <class T>
    static void encodeBool(const std::vector<bool>& values, size_t& offset, AlignedBuffer& buffer);
    static void encodeBool(unsigned int length, const std::vector<bool>& values, size_t& offset, AlignedBuffer& buffer);

public:
    static AlignedBuffer encode(const std::vector<bool>& values);

    // Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
    // Returns the number of bytes written to the target buffer.
    static size_t encodeInto(const std::vector<bool>& values, AlignedBuffer& target);

    static void decode(Slice& encoded, size_t nToSkip, size_t length, std::vector<bool>& out);
};
