#ifndef SIMPLE8B_H_INCLUDED
#define SIMPLE8B_H_INCLUDED

#include <vector>
#include <cstdint>

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

class Simple8B {
public:

  static AlignedBuffer encode(const std::vector<uint64_t> &values);
  static std::vector<uint64_t> decode(Slice &encoded, unsigned int size);

  template<uint64_t n, uint64_t bits>
  static bool canPack(const std::vector<uint64_t> &values, size_t offset);

  template<uint64_t selector, uint64_t n, uint64_t bits>
  static uint64_t pack(const std::vector<uint64_t> &values, size_t &offset);

  template<uint64_t n, uint64_t bits>
  static inline void unpack(uint64_t value, std::vector<uint64_t> &out);
};

#endif
