#include "aligned_buffer.hpp"

#include <cstring>
#include <iostream>
#include <fstream>

template <class T>
void AlignedBuffer::write(T value){
  const int bytesToAdd = sizeof(T);
  data.resize(data.size() + bytesToAdd);

  memcpy(&data.back() - bytesToAdd + 1, (uint8_t*)&value, bytesToAdd);
};

void AlignedBuffer::write(std::string &value){
  const int bytesToAdd = value.length();
  data.resize(data.size() + bytesToAdd);

  memcpy(&data.back() - bytesToAdd + 1, (uint8_t*)value.data(), bytesToAdd);
};

void AlignedBuffer::write(CompressedBuffer &value){
  const int bytesToAdd = value.dataByteSize();
  data.resize(data.size() + bytesToAdd);

  memcpy(&data.back() - bytesToAdd + 1, (uint8_t*)&value.data[0], bytesToAdd);
};

void AlignedBuffer::write(AlignedBuffer &value){
  const int bytesToAdd = value.size();
  data.resize(data.size() + bytesToAdd);

  memcpy(&data.back() - bytesToAdd + 1, (uint8_t*)&value.data[0], bytesToAdd);
};

// The data type of std::vector<bool>
void AlignedBuffer::write(std::_Bit_reference value){
  data.push_back((uint8_t)(value ? 1: 0));
};


std::ofstream& operator<<(std::ofstream& os, const AlignedBuffer& buf)
{
  os.write((const char*)&buf.data[0], buf.data.size());

  return os;
}


template void AlignedBuffer::write<uint8_t>(uint8_t value);
template void AlignedBuffer::write<uint16_t>(uint16_t value);
template void AlignedBuffer::write<uint32_t>(uint32_t value);
template void AlignedBuffer::write<uint64_t>(uint64_t value);
template void AlignedBuffer::write<double>(double value);
