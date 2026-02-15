#ifndef BOOL_ENCODER_H_INCLUDED
#define BOOL_ENCODER_H_INCLUDED

#include <vector>
#include <cstdint>

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

class BoolEncoder {
private:
  template <class T>
  static void encodeBool(const std::vector<bool> &values, size_t &offset, AlignedBuffer &buffer);
  static void encodeBool(unsigned int length, const std::vector<bool> &values, size_t &offset, AlignedBuffer &buffer);

public:
  BoolEncoder();

  static AlignedBuffer encode(const std::vector<bool> &values);
  static void decode(Slice &encoded, size_t nToSkip, size_t length, std::vector<bool> &out);
};

#endif
