#ifndef COMPRESSED_BUFFER_H_INCLUDED
#define COMPRESSED_BUFFER_H_INCLUDED

#include <vector>
#include <cstdint>
#include <iostream>
#include <algorithm>
#include <stdexcept>
#include <string>

class CompressedBuffer {
private:
  size_t offset = 0;
  int bitOffset = 0;
  bool initialized = false;

  static constexpr size_t INITIAL_CAPACITY = 128;
  static constexpr size_t GROWTH_FACTOR = 2;

  inline void ensure_capacity(size_t words_needed) {
    size_t required = offset + words_needed + 1;
    if (data.capacity() < required) {
      size_t new_capacity = std::max(data.capacity() * GROWTH_FACTOR, required);
      data.reserve(new_capacity);
    }
  }

  inline void boundsCheck() const {
    if (offset >= data.size()) {
      throw std::out_of_range("CompressedBuffer: read past end of buffer at offset " +
          std::to_string(offset) + ", size " + std::to_string(data.size()));
    }
  }

public:
  std::vector<uint64_t> data;

  CompressedBuffer(size_t initialSize = 0) {
    if (initialSize > 0) {
      data.reserve(std::max(initialSize, INITIAL_CAPACITY));
      data.resize(initialSize);
    } else {
      data.reserve(INITIAL_CAPACITY);
    }
  };

  void write(uint64_t value, int bits);
  template <int bits>
  void write(uint64_t value);
  template <uint64_t value, int bits>
  void writeFixed();
  void rewind() {
    offset = 0;
    bitOffset = 0;
    initialized = true;
  };
  template <typename T>
  T read(int bits);
  template <typename T, int bits>
  T readFixed();
  bool isAtEnd() const { return offset >= data.size(); }
  size_t size() const { return dataByteSize(); }
  bool readBit();
  size_t dataByteSize() const { return data.size() * sizeof(uint64_t); }
  size_t capacity() const { return data.capacity() * sizeof(uint64_t); }

  void reserve(size_t words) {
    data.reserve(words);
  }

  void shrink_to_fit() {
    if (data.size() < data.capacity() * 0.75) {
      data.shrink_to_fit();
    }
  }

  void printOffsets(){
    // Debug output - consider using logger if needed
    // std::cout << "buf offset=" << std::dec << offset << " bitOffset=" << bitOffset << std::endl;
  }

  void printCurrentValue(){
    // Debug output - consider using logger if needed
    // std::cout << "buf value=" << std::hex << data[offset] << std::endl;
  }
};

#endif
