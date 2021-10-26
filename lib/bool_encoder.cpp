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

void BoolEncoder::decode(Slice &encoded, size_t nToSkip, size_t length, std::vector<bool> &out){
  //TODO: Optimize, find a better way of reading the packed bits into a vector<bool>
  //TODO: Optimize, use encode strategy to process 64-bits at a time
  size_t byteOffset = 0;
  size_t numValuesLeft = nToSkip + length;

  while(numValuesLeft-- > 0){
    uint8_t byte = encoded.read<uint8_t>();

    for(int i=0; i < 8; i++){
      if(nToSkip == 0) {
        bool value = ((byte >> i) & 0x1) != 0;
        out.push_back(value);

        length--;
        if(length == 0)
          break;
      } else {
        nToSkip--;
      }
    }

    if(length == 0)
      break;
  }

  return;
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