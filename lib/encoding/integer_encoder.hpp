#ifndef __INTEGER_ENCODER_H_INCLUDED__
#define __INTEGER_ENCODER_H_INCLUDED__

#include <vector>
#include <cstdint>

#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

class IntegerEncoder {
private:


public:
  IntegerEncoder();

  static AlignedBuffer encode(const std::vector<uint64_t> &values);
  static std::pair<size_t, size_t> decode(Slice &encoded, unsigned int timestampSize, std::vector<uint64_t> &values, uint64_t startTime = 0, uint64_t maxTime = UINT64_MAX);

};

#endif
