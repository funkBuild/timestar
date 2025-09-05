#include "simple8b.hpp"
#include "simple8b_exception.hpp"
#include "util.hpp"

#include <iostream>

AlignedBuffer Simple8B::encode(std::vector<uint64_t> &values){
  int offset = 0;
  AlignedBuffer buffer;

  while(offset < values.size()){
    if(canPack<240, 0>(values, offset)) {
      uint64_t val = pack<0, 240, 0>(values, offset);
      buffer.write(val);

    } else if(canPack<120, 0>(values, offset)) {
      uint64_t val = pack<1, 120, 0>(values, offset);
      buffer.write(val);

    } else if(canPack<60, 1>(values, offset)) {
      uint64_t val = pack<2, 60, 1>(values, offset);
      buffer.write(val);

    } else if(canPack<30, 2>(values, offset)) {
      uint64_t val = pack<3, 30, 2>(values, offset);
      buffer.write(val);

    } else if(canPack<20, 3>(values, offset)) {
      uint64_t val = pack<4, 20, 3>(values, offset);
      buffer.write(val);

    } else if(canPack<15, 4>(values, offset)) {
      uint64_t val = pack<5, 15, 4>(values, offset);
      buffer.write(val);
    
    } else if(canPack<12, 5>(values, offset)) {
      uint64_t val = pack<6, 12, 5>(values, offset);
      buffer.write(val);
    
    } else if(canPack<10, 6>(values, offset)) {
      uint64_t val = pack<7, 10, 6>(values, offset);
      buffer.write(val);

    } else if(canPack<8, 7>(values, offset)) {
      uint64_t val = pack<8, 8, 7>(values, offset);
      buffer.write(val);

    } else if(canPack<7, 8>(values, offset)) {
      uint64_t val = pack<9, 7, 8>(values, offset);
      buffer.write(val);

    } else if(canPack<6, 10>(values, offset)) {
      uint64_t val = pack<10, 6, 10>(values, offset);
      buffer.write(val);
    
    } else if(canPack<5, 12>(values, offset)) {
      uint64_t val = pack<11, 5, 12>(values, offset);
      buffer.write(val);
    
    } else if(canPack<4, 15>(values, offset)) {
      uint64_t val = pack<12, 4, 15>(values, offset);
      buffer.write(val);
    
    } else if(canPack<3, 20>(values, offset)) {
      uint64_t val = pack<13, 3, 20>(values, offset);
      buffer.write(val);
    
    } else if(canPack<2, 30>(values, offset)) {
      uint64_t val = pack<14, 2, 30>(values, offset);
      buffer.write(val);
    
    } else if(canPack<1, 60>(values, offset)) {
      uint64_t val = pack<15, 1, 60>(values, offset);
      buffer.write(val);
    
    } else {
      // Value cannot be encoded with any scheme - it exceeds 60 bits
      throw Simple8BValueTooLargeException(values[offset], offset);
    }
  }

  return std::move(buffer);
}

std::vector<uint64_t> Simple8B::decode(Slice &encoded, unsigned int size){
  std::vector<uint64_t> values;
  values.reserve(size);

  const size_t length = encoded.length<uint64_t>();

  for(unsigned int i=0; i < length; i++){
    uint64_t packedValue = encoded.read<uint64_t>();
    uint64_t selector = packedValue >> 60;

    //TODO: Handling of bad selector values
    switch(selector){
      case 0:
        unpack<240, 0>(packedValue, values);
        break;
      case 1:
        unpack<120, 0>(packedValue, values);
        break;
      case 2:
        unpack<60, 1>(packedValue, values);
        break;
      case 3:
        unpack<30, 2>(packedValue, values);
        break;
      case 4:
        unpack<20, 3>(packedValue, values);
        break;
      case 5:
        unpack<15, 4>(packedValue, values);
        break;
      case 6:
        unpack<12, 5>(packedValue, values);
        break;
      case 7:
        unpack<10, 6>(packedValue, values);
        break;
      case 8:
        unpack<8, 7>(packedValue, values);
        break;
      case 9:
        unpack<7, 8>(packedValue, values);
        break;
      case 10:
        unpack<6, 10>(packedValue, values);
        break;
      case 11:
        unpack<5, 12>(packedValue, values);
        break;
      case 12:
        unpack<4, 15>(packedValue, values);
        break;
      case 13:
        unpack<3, 20>(packedValue, values);
        break;
      case 14:
        unpack<2, 30>(packedValue, values);
        break;
      case 15:
        unpack<1, 60>(packedValue, values);
        break;
    }
  }

  return values;
}

template<uint64_t n, uint64_t bits>
bool Simple8B::canPack(std::vector<uint64_t> &values, int offset){
  int remaining = values.size() - offset;
  if(remaining < n)
    return false;

  if(bits == 0) {
    // Selector 0,1 are special and use 0 bits to encode runs of 1's
    return false;
  }

  uint64_t max = (1ull << bits) - 1;

  for(int i = offset; i < (offset+n); i++){
    if(values[i] > max)
      return false;
  }

  return true;
}

template<uint64_t selector, uint64_t n, uint64_t bits>
uint64_t Simple8B::pack(std::vector<uint64_t> &values, int &offset){
  uint64_t out = selector << 60;

  for(unsigned int i = 0; i < n; i++){
    out |= values[offset + i] << (i*bits);
  }

  offset += n;

  return out;
}

template<uint64_t n, uint64_t bits>
void Simple8B::unpack(uint64_t value, std::vector<uint64_t> &out){
  const uint64_t mask = (1ull << bits) - 1;
  unsigned int shiftAmount = 0;

  // TODO: Validate fold expression

  loop<int, n>([&out, value] (auto i) {
    constexpr int shiftAmount = i * bits;

    uint64_t v = (value >> shiftAmount) & mask;
    out.push_back(v);
  });

/*
  for(int i=0; i < n; i++){
    uint64_t v = (value >> shiftAmount) & mask;
    out.push_back(v);
    shiftAmount += bits;
  }
  */
}
