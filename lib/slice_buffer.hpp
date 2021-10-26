#ifndef __SLICE_BUFFER_H_INCLUDED__
#define __SLICE_BUFFER_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <iostream>
#include <memory>
#include <cstring>

class Slice {
public:
  uint8_t *data;
  size_t length_;
  size_t offset = 0;

  Slice(uint8_t *_data, size_t _length) : data(_data), length_(_length) {};

  template <class T>
  T read(){
    T value;

    std::memcpy(&value, data + offset, sizeof(T));
    offset += sizeof(T);

    return value;
  }

  template <class T>
  size_t length(){
    return length_ / sizeof(T);
  }
};

class CompressedSlice: public Slice {
private:
  int bitOffset = 0;
public:
  CompressedSlice(uint8_t *_data, size_t _length) : Slice(_data, _length) {};

  template <typename T>
  T read(const int bits){
    if(bitOffset > 63){
      offset++;
      bitOffset = 0;
    }

    const int leftover_bits = bits - (64 - bitOffset);
    const int bits_read = leftover_bits > 0 ? bits - leftover_bits : bits;
    const uint64_t mask = bits_read == 64 ? 0xffffffffffffffff : (1ull << bits_read) - 1;

    uint64_t value = data[offset] >> bitOffset;
    value &= mask;

    if(leftover_bits > 0) {
      offset++;
      bitOffset = leftover_bits;

      const uint64_t mask = leftover_bits == 64 ? 0xffffffffffffffff : (1ull << leftover_bits) - 1;
      value |= (data[offset] & mask) << bits_read;
    } else {
      bitOffset += bits;
    }

    return value;
  };

  template <typename T, int bits>
  T readFixed(){
    if(bitOffset > 63){
      offset++;
      bitOffset = 0;
    }

    const int leftover_bits = bits - (64 - bitOffset);
    const int bits_read = leftover_bits > 0 ? bits - leftover_bits : bits;
    const uint64_t mask = bits_read == 64 ? 0xffffffffffffffff : (1ull << bits_read) - 1;

    uint64_t value = data[offset] >> bitOffset;
    value &= mask;

    if(leftover_bits > 0) {
      offset++;
      bitOffset = leftover_bits;

      const uint64_t mask = leftover_bits == 64 ? 0xffffffffffffffff : (1ull << leftover_bits) - 1;
      value |= (data[offset] & mask) << bits_read;
    } else {
      bitOffset += bits;
    }
    
    return value;
  };

  bool readBit(){
    if(bitOffset > 63){
      offset++;
      bitOffset = 1;

      return (data[offset] & 1) == 1;
    }

    bool value = ((data[offset] >> bitOffset) & 1) == 1;
    bitOffset++;

    return value;
  }
};

class SliceBuffer {
private:

public:
  std::vector<uint8_t> data;

  SliceBuffer(unsigned int initialSize = 0){
    if(initialSize > 0)
      data.resize(initialSize);
  };

  Slice getSlice(size_t offset, size_t length){
    Slice slice(&data[offset], length);

    return std::move(slice);
  }

  CompressedSlice getCompressedSlice(size_t offset, size_t length){
    CompressedSlice slice(&data[offset], length);

    return std::move(slice);
  }

};

#endif
