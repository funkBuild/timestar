#ifndef SIMPLE16_H_INCLUDED
#define SIMPLE16_H_INCLUDED

#include <vector>
#include <cstdint>
#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

/**
 * Simple16 - Integer compression using 16 packing schemes
 * 
 * Simple16 is an evolution of Simple8b that provides better compression
 * and can handle full 64-bit values. It uses 16 different packing schemes
 * (4 bits for selector) but stores values more efficiently.
 * 
 * Key differences from Simple8b:
 * - Can encode full 64-bit values (scheme 15)
 * - More flexible packing schemes
 * - Better compression for mixed-size integers
 */
class Simple16 {
public:
    Simple16(){};

    static AlignedBuffer encode(std::vector<uint64_t> &values);
    static std::vector<uint64_t> decode(Slice &encoded, unsigned int size);

    template<uint64_t n, uint64_t bits>
    static bool canPack(std::vector<uint64_t> &values, size_t offset);

    template<uint64_t selector, uint64_t n, uint64_t bits>
    static uint64_t pack(std::vector<uint64_t> &values, size_t &offset);

    template<uint64_t n, uint64_t bits>
    static inline void unpack(uint64_t value, std::vector<uint64_t> &out);
    
    // Special handling for 64-bit values
    static void packLarge(std::vector<uint64_t> &values, size_t &offset, AlignedBuffer &buffer);
    static void unpackLarge(Slice &encoded, std::vector<uint64_t> &out);

    // Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
    // Returns the number of bytes written.
    static size_t encodeInto(std::vector<uint64_t> &values, AlignedBuffer &target);
};

#endif