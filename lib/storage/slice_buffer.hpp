#ifndef SLICE_BUFFER_H_INCLUDED
#define SLICE_BUFFER_H_INCLUDED

#include <vector>
#include <cstdint>
#include <iostream>
#include <memory>
#include <cstring>
#include <stdexcept>

class CompressedSlice {
private:
  std::unique_ptr<uint64_t[]> alignedStorage_;
  size_t length_;
  size_t offset = 0;
  int bitOffset = 0;

  void boundsCheck(size_t wordIndex) const {
    if (wordIndex >= length_) {
      throw std::runtime_error("CompressedSlice - attempted to read beyond buffer bounds");
    }
  }

public:
  const uint64_t *data;

  CompressedSlice(const uint8_t *_data, size_t _length)
    : alignedStorage_(std::make_unique<uint64_t[]>((_length + 7) / 8)),
      length_((_length + 7) / 8),
      data(alignedStorage_.get())
  {
    std::memcpy(alignedStorage_.get(), _data, _length);
    // Zero out any padding bytes in the last word
    size_t remainder = _length % 8;
    if (remainder != 0) {
      auto* bytePtr = reinterpret_cast<uint8_t*>(alignedStorage_.get()) + _length;
      std::memset(bytePtr, 0, 8 - remainder);
    }
  }

  template <typename T>
  T read(const int bits){
    if(bitOffset > 63){
      offset++;
      bitOffset = 0;
    }

    boundsCheck(offset);

    const int leftover_bits = bits - (64 - bitOffset);
    const int bits_read = leftover_bits > 0 ? bits - leftover_bits : bits;
    const uint64_t mask = bits_read == 64 ? 0xffffffffffffffff : (1ull << bits_read) - 1;

    uint64_t value = data[offset] >> bitOffset;
    value &= mask;

    if(leftover_bits > 0) {
      offset++;
      bitOffset = leftover_bits;

      boundsCheck(offset);

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

    boundsCheck(offset);

    const int leftover_bits = bits - (64 - bitOffset);
    const int bits_read = leftover_bits > 0 ? bits - leftover_bits : bits;
    const uint64_t mask = bits_read == 64 ? 0xffffffffffffffff : (1ull << bits_read) - 1;

    uint64_t value = data[offset] >> bitOffset;
    value &= mask;

    if(leftover_bits > 0) {
      offset++;
      bitOffset = leftover_bits;

      boundsCheck(offset);

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

      boundsCheck(offset);

      return (data[offset] & 1) == 1;
    }

    boundsCheck(offset);

    bool value = ((data[offset] >> bitOffset) & 1) == 1;
    bitOffset++;

    return value;
  }
};

class Slice {
public:
  const uint8_t *data;
  size_t length_;
  size_t offset = 0;

  Slice(const uint8_t *_data, size_t _length) : data(_data), length_(_length) {};

  template <class T>
  T read(){
    if (offset + sizeof(T) > length_) {
      throw std::runtime_error("Slice::read() - attempted to read beyond buffer bounds");
    }

    T value;
    std::memcpy(&value, data + offset, sizeof(T));
    offset += sizeof(T);

    return value;
  }

  template <class T>
  size_t length(){
    return (length_ - offset) / sizeof(T);
  }

  size_t remaining(){
    return length_ - offset;
  }

  std::string readString(size_t byteLength){
    if (offset + byteLength > length_) {
      throw std::runtime_error("Slice::readString() - attempted to read beyond buffer bounds");
    }

    std::string thisString((char *)(data + offset), byteLength);

    offset += byteLength;

    return thisString;
  }

  size_t bytesLeft(){
    return length_ - offset;
  }

  Slice getSlice(size_t byteLength){
    if (offset + byteLength > length_) {
      throw std::runtime_error("Slice::getSlice() - attempted to get slice beyond buffer bounds");
    }

    const uint8_t* ref = data + offset;
    Slice slice(ref, byteLength);

    offset += byteLength;

    return slice;
  }

  CompressedSlice getCompressedSlice(size_t byteLength){
    if (offset + byteLength > length_) {
      throw std::runtime_error("Slice::getCompressedSlice() - attempted to get slice beyond buffer bounds");
    }

    const uint8_t* ref = data + offset;
    CompressedSlice slice(ref, byteLength);

    offset += byteLength;

    return slice;
  }

  template <class T>
  T read(size_t customOffset){
    if (customOffset + sizeof(T) > length_) {
      throw std::runtime_error("Slice::read(offset) - attempted to read beyond buffer bounds");
    }

    T value;
    std::memcpy(&value, data + customOffset, sizeof(T));

    return value;
  }
};

#endif
