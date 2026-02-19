#include "aligned_buffer.hpp"

#include <cstring>
#include <iostream>
#include <fstream>
#include <algorithm>

// Static constants for optimization
constexpr size_t AlignedBuffer::INITIAL_CAPACITY;
constexpr size_t AlignedBuffer::GROWTH_FACTOR;

template <class T>
void AlignedBuffer::write(T value){
  const size_t bytesToAdd = sizeof(T);
  const size_t new_size = current_size + bytesToAdd;

  ensure_capacity(new_size);

  // Direct memory copy at the correct position
  std::memcpy(data.data() + current_size, &value, bytesToAdd);
  current_size = new_size;
};

void AlignedBuffer::write(const std::string &value){
  const size_t bytesToAdd = value.length();
  const size_t new_size = current_size + bytesToAdd;

  ensure_capacity(new_size);

  std::memcpy(data.data() + current_size, value.data(), bytesToAdd);
  current_size = new_size;
};

void AlignedBuffer::write(const CompressedBuffer &value){
  const size_t bytesToAdd = value.data.size() * sizeof(uint64_t);
  const size_t new_size = current_size + bytesToAdd;

  ensure_capacity(new_size);

  std::memcpy(data.data() + current_size, value.data.data(), bytesToAdd);
  current_size = new_size;
};

void AlignedBuffer::write(const AlignedBuffer &value){
  const size_t bytesToAdd = value.current_size;
  const size_t new_size = current_size + bytesToAdd;

  ensure_capacity(new_size);

  std::memcpy(data.data() + current_size, value.data.data(), bytesToAdd);
  current_size = new_size;
};

// The data type of std::vector<bool>
void AlignedBuffer::write(std::_Bit_reference value){
  const size_t new_size = current_size + 1;
  ensure_capacity(new_size);
  uint8_t byte = value ? 1 : 0;
  std::memcpy(data.data() + current_size, &byte, 1);
  current_size = new_size;
};

// Bulk write raw bytes
void AlignedBuffer::write_bytes(const char* bytes, size_t count) {
  const size_t new_size = current_size + count;

  ensure_capacity(new_size);

  std::memcpy(data.data() + current_size, bytes, count);
  current_size = new_size;
}

void AlignedBuffer::ensure_capacity(size_t required) {
  if (required > data.size()) {
    if (required > data.capacity()) {
      // Use growth factor for better amortized performance
      size_t new_capacity = std::max(data.capacity() * GROWTH_FACTOR, required);
      data.reserve(new_capacity);
    }
    // Grow the vector's logical size so the storage is accessible.
    // With dma_default_init_allocator, resize() default-initializes new bytes
    // (no-op for uint8_t), avoiding redundant zeroing before memcpy.
    data.resize(required);
  }
}


std::ofstream& operator<<(std::ofstream& os, const AlignedBuffer& buf)
{
  if (buf.size() > 0) {
    os.write(reinterpret_cast<const char*>(buf.data.data()), buf.size());
  }
  return os;
}


template void AlignedBuffer::write<uint8_t>(uint8_t value);
template void AlignedBuffer::write<uint16_t>(uint16_t value);
template void AlignedBuffer::write<uint32_t>(uint32_t value);
template void AlignedBuffer::write<uint64_t>(uint64_t value);
template void AlignedBuffer::write<double>(double value);
