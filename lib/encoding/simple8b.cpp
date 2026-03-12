#include "simple8b.hpp"
#include "simple8b_exception.hpp"
#include "util.hpp"

#include <stdexcept>

AlignedBuffer Simple8B::encode(const std::vector<uint64_t> &values){
  size_t offset = 0;
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

  return buffer;
}

std::vector<uint64_t> Simple8B::decode(Slice &encoded, unsigned int size){
  std::vector<uint64_t> values;
  values.reserve(size);

  const size_t length = encoded.length<uint64_t>();

  for(unsigned int i=0; i < length; i++){
    uint64_t packedValue = encoded.read<uint64_t>();
    uint64_t selector = packedValue >> 60;

    // Validate selector is in valid range (0-15)
    if (selector > 15) {
      throw std::runtime_error("Simple8B decode: invalid selector value " +
                               std::to_string(selector) + " at position " + std::to_string(i));
    }

    switch(selector){
      case 0:
        throw std::runtime_error("Simple8B decode: unexpected selector 0 at position " +
                                 std::to_string(i) + " (encoder never produces selector 0, data may be corrupt)");
      case 1:
        throw std::runtime_error("Simple8B decode: unexpected selector 1 at position " +
                                 std::to_string(i) + " (encoder never produces selector 1, data may be corrupt)");
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

  // The last packed word may contain padding values beyond the expected count.
  // Trim to the exact expected size.
  if (values.size() > size) {
    values.resize(size);
  }

  return values;
}

template<uint64_t n, uint64_t bits>
bool Simple8B::canPack(const std::vector<uint64_t> &values, size_t offset){
  if (offset >= values.size())
    return false;

  size_t remaining = values.size() - offset;
  if(remaining < n)
    return false;

  if constexpr (bits == 0) {
    // Selector 0,1 are reserved for run-length encoding of zeros/ones
    // but not currently implemented in this encoder. Always return false
    // to skip these selectors and use higher-bit selectors instead.
    return false;
  }

  constexpr uint64_t max = (1ull << bits) - 1;

  for(size_t i = offset; i < offset + n; i++){
    if(values[i] > max)
      return false;
  }

  return true;
}

template<uint64_t selector, uint64_t n, uint64_t bits>
uint64_t Simple8B::pack(const std::vector<uint64_t> &values, size_t &offset){
  uint64_t out = selector << 60;

  // Compute mask for the bit width - defensive masking to prevent overflow
  // into adjacent bit fields even if canPack validation was bypassed
  constexpr uint64_t mask = (bits == 0) ? 0ULL : ((1ULL << bits) - 1);

  for(unsigned int i = 0; i < n; i++){
    // Mask the value before shifting to prevent overflow into selector or adjacent values
    out |= (values[offset + i] & mask) << (i*bits);
  }

  offset += n;

  return out;
}

template<uint64_t n, uint64_t bits>
void Simple8B::unpack(uint64_t value, std::vector<uint64_t> &out){
  const uint64_t mask = (1ull << bits) - 1;

  loop<int, n>([&out, value] (auto i) {
    constexpr int shiftAmount = i * bits;

    uint64_t v = (value >> shiftAmount) & mask;
    out.push_back(v);
  });
}

// Explicit template instantiations for all canPack specializations
template bool Simple8B::canPack<240, 0>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<120, 0>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<60, 1>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<30, 2>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<20, 3>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<15, 4>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<12, 5>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<10, 6>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<8, 7>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<7, 8>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<6, 10>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<5, 12>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<4, 15>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<3, 20>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<2, 30>(const std::vector<uint64_t>&, size_t);
template bool Simple8B::canPack<1, 60>(const std::vector<uint64_t>&, size_t);
