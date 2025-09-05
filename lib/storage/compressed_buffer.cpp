#include "compressed_buffer.hpp"

#include <iostream>
#include <cstring>

void CompressedBuffer::write(uint64_t value, int bits){
  if(bitOffset > 63 || offset < 0 ){
    offset++;
    bitOffset = 0;
    data.push_back(0);
  }

  const int leftover_bits = bits - (64 - bitOffset);

  data[offset] |= value << bitOffset;

  if(leftover_bits > 0) {
    offset++;
    bitOffset = leftover_bits;
    data.push_back(value >> (bits - leftover_bits));
  } else {
    bitOffset += bits;
  }
}

template <int bits>
void CompressedBuffer::write(uint64_t value){
  if(bitOffset > 63 || offset < 0 ){
    offset++;
    bitOffset = 0;
    data.push_back(0);
  }

  const int leftover_bits = bits - (64 - bitOffset);

  data[offset] |= value << bitOffset;

  if(leftover_bits > 0) {
    offset++;
    bitOffset = leftover_bits;
    data.push_back(value >> (bits - leftover_bits));
  } else {
    bitOffset += bits;
  }
}

template <uint64_t value, int bits>
void CompressedBuffer::writeFixed(){
  if(bitOffset > 63 || offset < 0 ){
    offset++;
    bitOffset = 0;
    data.push_back(0);
  }

  // Happy path for single bit writes
  if(bits == 1) {
    if(value == 1)
      data[offset] |= value << bitOffset;
    bitOffset++;
    return;
  }

  const int leftover_bits = bits - (64 - bitOffset);

  data[offset] |= value << bitOffset;

  if(leftover_bits > 0) {
    offset++;
    bitOffset = leftover_bits;
    data.push_back(value >> (bits - leftover_bits));
  } else {
    bitOffset += bits;
  }
}

template <typename T>
T CompressedBuffer::read(const int bits){
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
}


template <typename T, int bits>
T CompressedBuffer::readFixed(){
  if(bitOffset > 63){
    offset++;
    bitOffset = 0;
  }

  const int leftover_bits = bits - (64 - bitOffset);
  const int bits_read = leftover_bits > 0 ? bits - leftover_bits : bits;
  const uint64_t mask = bits_read == 64 ? 0xffffffffffffffff : (1ull << bits_read) - 1;

//  std::cout << "READ" << std::endl;
//  std::cout << "bitOffset=" << bitOffset << " leftover_bits=" << leftover_bits << std::endl;
//  std::cout << "offset=" << offset << std::endl;


  uint64_t value = data[offset] >> bitOffset;
  value &= mask;

// std::cout << "value=" << data[offset] << std::endl << std::endl;


  if(leftover_bits > 0) {
    offset++;
    bitOffset = leftover_bits;

    const uint64_t mask = leftover_bits == 64 ? 0xffffffffffffffff : (1ull << leftover_bits) - 1;
    value |= (data[offset] & mask) << bits_read;
  } else {
    bitOffset += bits;
  }
  
  return value;
}

bool CompressedBuffer::readBit(){
  if(bitOffset > 63){
    offset++;
    bitOffset = 0;
  }

  bool value = ((data[offset] >> bitOffset) & 1) == 1;

  bitOffset++;

  return value;
}

template void CompressedBuffer::write<6>(uint64_t value);
template void CompressedBuffer::write<5>(uint64_t value);
template void CompressedBuffer::write<8>(uint64_t value);
template void CompressedBuffer::write<64>(uint64_t value);


template void CompressedBuffer::writeFixed<0b01, 2>();
template void CompressedBuffer::writeFixed<0b11, 2>();
template void CompressedBuffer::writeFixed<0b0, 1>();

template uint8_t  CompressedBuffer::read<uint8_t>(int bits);
template uint64_t CompressedBuffer::read<uint64_t>(int bits);
template double   CompressedBuffer::read<double>(int bits);
template bool     CompressedBuffer::read<bool>(int bits);

template uint64_t CompressedBuffer::readFixed<uint64_t, 64>();
template uint64_t CompressedBuffer::readFixed<uint64_t, 5>();
template uint64_t CompressedBuffer::readFixed<uint64_t, 6>();
