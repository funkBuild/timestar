#include "tsxor_encoder.hpp"
#include "util.hpp"

#include <iostream>
#include <cassert>
#include <cmath>


CompressedBuffer TsxorEncoder::encode(std::vector<double> &values){
  CompressedBuffer buffer;
  Window window;

  int countA = 0, countB = 0, countC = 0;

  for (int i = 0; i < values.size(); i++)
  {   
    uint64_t val = *((uint64_t *)&values[i]);

    if (window.contains(val))
    {
      auto offset = window.getIndexOf(val);
      uint8_t *bytes = (uint8_t *)&offset;
      buffer.write<8>(bytes[0]);

      countA++;
    }
    else
    {
      uint64_t candidate = window.getCandidate(val);
      uint64_t xor_value = candidate ^ val;
      auto offset = window.getIndexOf(candidate);

      //WRITE 1
      offset |= 0x80;
      uint8_t *bytes = (uint8_t *)&offset;
      buffer.write<8>(bytes[0]);

      auto lzb = getLeadingZeroBits(xor_value);
      const auto tzb = getTrailingZeroBits(xor_value);


      if(lzb > 31)
        lzb = 31;

      uint64_t data_bits = 8 * sizeof(uint64_t) - lzb - tzb;

      buffer.writeFixed<0b11, 2>();
      buffer.write<5>(lzb);
      buffer.write<6>(data_bits);

      buffer.write(xor_value >> tzb, data_bits);
    }
  }

  return std::move(buffer);
}