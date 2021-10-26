#ifndef __SIMPLE8B_H_INCLUDED__
#define __SIMPLE8B_H_INCLUDED__

#include <vector>
#include <cstdint>

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

class Simple8B {
private:
public:
  Simple8B(){};

  static AlignedBuffer encode(std::vector<uint64_t> &values);
  static std::vector<uint64_t> decode(Slice &encoded, unsigned int size);

  template<uint64_t n, uint64_t bits>
  static bool canPack(std::vector<uint64_t> &values, int offset);

  template<uint64_t selector, uint64_t n, uint64_t bits>
  static uint64_t pack(std::vector<uint64_t> &values, int &offset);

  template<uint64_t n, uint64_t bits>
  static inline void unpack(uint64_t value, std::vector<uint64_t> &out);
};

#endif
