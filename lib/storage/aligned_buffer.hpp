#ifndef ALIGNED_BUFFER_H_INCLUDED
#define ALIGNED_BUFFER_H_INCLUDED

#include <vector>
#include <cstdint>
#include <iostream>
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>

#include "compressed_buffer.hpp"

class AlignedBuffer {
private:
  static constexpr size_t INITIAL_CAPACITY = 4096;
  static constexpr size_t GROWTH_FACTOR = 2;

  size_t current_size = 0;

  // Ensure we have enough capacity
  void ensure_capacity(size_t required);

public:
  std::vector<uint8_t> data;

  AlignedBuffer(size_t initialSize = 0) {
    if (initialSize > 0) {
      data.reserve(std::max(initialSize, INITIAL_CAPACITY));
      data.resize(initialSize);
      current_size = initialSize;
    } else {
      data.reserve(INITIAL_CAPACITY);
    }
  };

  template <class T>
  void write(T value);
  void write(const std::string &value);
  void write(const CompressedBuffer &value);
  void write(const AlignedBuffer &value);
  void write(std::_Bit_reference value);

  // Bulk write raw bytes (for compressed data, etc.)
  void write_bytes(const char* bytes, size_t count);

  // New optimized bulk write for arrays
  template<typename T>
  void write_array(const T* values, size_t count) {
    const size_t bytesToAdd = sizeof(T) * count;
    const size_t new_size = current_size + bytesToAdd;

    ensure_capacity(new_size);

    std::memcpy(data.data() + current_size, values, bytesToAdd);
    current_size = new_size;
  }

  // Read 8 bytes as uint64_t using memcpy to avoid strict aliasing violation
  uint64_t read64(size_t offset) const {
    if (offset + sizeof(uint64_t) > current_size) {
      throw std::runtime_error("AlignedBuffer::read64 out of bounds: offset=" +
                               std::to_string(offset) + " size=" +
                               std::to_string(current_size));
    }
    uint64_t val;
    std::memcpy(&val, data.data() + offset, sizeof(uint64_t));
    return val;
  }

  // Read a single byte
  uint8_t read8(size_t offset) const {
    if (offset >= current_size) {
      throw std::runtime_error("AlignedBuffer::read8 out of bounds: offset=" +
                               std::to_string(offset) + " size=" +
                               std::to_string(current_size));
    }
    return data[offset];
  }

  size_t size() const { return current_size; }
  size_t capacity() const { return data.capacity(); }

  // Reserve capacity upfront when size is known
  void reserve(size_t capacity) {
    data.reserve(capacity);
  }

  // Clear the buffer but keep allocated memory
  void clear() {
    current_size = 0;
  }

  // Shrink to fit after operations
  void shrink_to_fit() {
    data.resize(current_size);
    data.shrink_to_fit();
  }
};

std::ofstream& operator<<(std::ofstream& os, const AlignedBuffer& buf);


#endif
