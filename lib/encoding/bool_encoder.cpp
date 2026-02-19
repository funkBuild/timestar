#include "bool_encoder.hpp"

#include <iostream>

// Timestamp encoding - http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

AlignedBuffer BoolEncoder::encode(const std::vector<bool> &values){
  AlignedBuffer buffer;

  const size_t valuesLength = values.size();
  size_t offset = 0;

  while(offset < valuesLength){
    unsigned int numValuesLeft = valuesLength - offset;

    if(numValuesLeft >= 64){
      encodeBool<uint64_t>(values, offset, buffer);
    } else if(numValuesLeft >= 32){
      encodeBool<uint32_t>(values, offset, buffer);
    } else if(numValuesLeft >= 16){
      encodeBool<uint16_t>(values, offset, buffer);
    } else if(numValuesLeft >= 8){
      encodeBool<uint8_t>(values, offset, buffer);
    } else {
      encodeBool(numValuesLeft, values, offset, buffer);
    }
  }

  return buffer;
}

size_t BoolEncoder::encodeInto(const std::vector<bool> &values, AlignedBuffer &target) {
  const size_t startPos = target.size();
  const size_t valuesLength = values.size();
  size_t offset = 0;

  while (offset < valuesLength) {
    unsigned int numValuesLeft = valuesLength - offset;

    if (numValuesLeft >= 64) {
      encodeBool<uint64_t>(values, offset, target);
    } else if (numValuesLeft >= 32) {
      encodeBool<uint32_t>(values, offset, target);
    } else if (numValuesLeft >= 16) {
      encodeBool<uint16_t>(values, offset, target);
    } else if (numValuesLeft >= 8) {
      encodeBool<uint8_t>(values, offset, target);
    } else {
      encodeBool(numValuesLeft, values, offset, target);
    }
  }

  return target.size() - startPos;
}

void BoolEncoder::decode(Slice &encoded, size_t nToSkip, size_t length, std::vector<bool> &out){
  size_t totalBits = nToSkip + length;
  size_t bytesNeeded = (totalBits + 7) / 8;

  for (size_t byteIdx = 0; byteIdx < bytesNeeded; byteIdx++) {
    uint8_t byte = encoded.read<uint8_t>();

    for (int i = 0; i < 8; i++) {
      if (nToSkip > 0) {
        nToSkip--;
      } else if (length > 0) {
        bool value = ((byte >> i) & 0x1) != 0;
        out.push_back(value);
        length--;
      }

      if (nToSkip == 0 && length == 0)
        break;
    }

    if (length == 0)
      break;
  }
}

template <class T>
void BoolEncoder::encodeBool(const std::vector<bool> &values, size_t &offset, AlignedBuffer &buffer){
  T value = 0;

  for(unsigned int i = 0; i < 8*sizeof(T); i++){
    value |= ((uint64_t)values[offset + i]) << i;
  }

  offset += 8*sizeof(T);
  buffer.write(value);
}

void BoolEncoder::encodeBool(unsigned int length, const std::vector<bool> &values, size_t &offset, AlignedBuffer &buffer){
  uint8_t value = 0;

  for(unsigned int i = 0; i < length; i++){
    value |= ((unsigned int)values[offset + i]) << i;
  }

  offset += length;
  buffer.write(value);
}