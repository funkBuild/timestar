#ifndef STRING_ENCODER_H_INCLUDED
#define STRING_ENCODER_H_INCLUDED

#include <vector>
#include <string>
#include <cstdint>

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

class StringEncoder {
private:
    // Write variable-length integer for string lengths
    static void writeVarInt(AlignedBuffer& buffer, uint32_t value);
    
    // Read variable-length integer
    static uint32_t readVarInt(Slice& slice);
    
public:
    StringEncoder() = default;
    
    // Encode a vector of strings with Snappy compression
    // Format: [header][compressed_data]
    // Header: magic_number(4) | uncompressed_size(4) | compressed_size(4) | count(4)
    // Data (before compression): [length_prefix][string_data] for each string
    static AlignedBuffer encode(const std::vector<std::string>& values);
    
    // Decode strings from compressed buffer
    static void decode(AlignedBuffer& encoded, size_t count, std::vector<std::string>& out);
    
    // Decode from a Slice (for TSM reading)
    static void decode(Slice& encoded, size_t count, std::vector<std::string>& out);
};

#endif // STRING_ENCODER_H_INCLUDED