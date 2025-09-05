#ifndef __FLOAT_ENCODER_H_INCLUDED__
#define __FLOAT_ENCODER_H_INCLUDED__

#include <vector>
#include <cstdint>

#include "compressed_buffer.hpp"
#include "slice_buffer.hpp"

class FloatEncoder {
private:


public:
  FloatEncoder(){};

  static CompressedBuffer encode(const std::vector<double> &values);
  static void decode(CompressedSlice &encoded, size_t nToSkip, size_t length, std::vector<double> &out);
};

#endif
