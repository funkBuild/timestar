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
  // Owning storage used only when the data must be copied (non-aligned or
  // caller-owned temporary buffer).  When null, data_ points directly into
  // an externally-owned buffer (zero-copy mode).
  std::unique_ptr<uint64_t[]> alignedStorage_;
  const uint8_t *data_;   // raw byte pointer to the data (owned or borrowed)
  size_t length_;          // number of logical uint64_t words
  size_t offset = 0;
  int bitOffset = 0;

  void boundsCheck(size_t wordIndex) const {
    if (wordIndex >= length_) {
      throw std::runtime_error("CompressedSlice - attempted to read beyond buffer bounds");
    }
  }

  // Load a uint64_t from the word at `wordIndex` using memcpy.
  // This is safe for any alignment and compiles to a single mov on x86-64.
  inline uint64_t loadWord(size_t wordIndex) const {
    uint64_t word;
    std::memcpy(&word, data_ + wordIndex * 8, sizeof(uint64_t));
    return word;
  }

public:
  // Legacy public member kept for ABI compatibility; points to data_ reinterpreted.
  // DEPRECATED: external code should not access this directly.
  const uint64_t *data;

  // Copying constructor: allocates aligned storage and copies the input data.
  // Handles non-multiple-of-8 lengths by zero-padding the last word.
  CompressedSlice(const uint8_t *_data, size_t _length)
    : alignedStorage_(std::make_unique<uint64_t[]>((_length + 7) / 8)),
      data_(reinterpret_cast<const uint8_t*>(alignedStorage_.get())),
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

  // Zero-copy constructor: borrows the given pointer without allocating or
  // copying.  The caller MUST ensure the pointed-to buffer outlives this
  // CompressedSlice.  `byteLength` must be a multiple of 8; this is
  // always the case for float-encoded value data in TSM blocks.
  struct ZeroCopy {};
  CompressedSlice(ZeroCopy, const uint8_t *_data, size_t _byteLength)
    : alignedStorage_(nullptr),
      data_(_data),
      length_(_byteLength / 8),
      data(reinterpret_cast<const uint64_t*>(_data))  // may be unaligned, kept for compat
  {
    // Zero-copy mode requires the byte length to be a multiple of 8 so that
    // we don't need to worry about padding the last word.
    if (_byteLength % 8 != 0) {
      throw std::runtime_error(
        "CompressedSlice zero-copy requires byteLength to be a multiple of 8, got " +
        std::to_string(_byteLength));
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

    uint64_t value = loadWord(offset) >> bitOffset;
    value &= mask;

    if(leftover_bits > 0) {
      offset++;
      bitOffset = leftover_bits;

      boundsCheck(offset);

      const uint64_t mask = leftover_bits == 64 ? 0xffffffffffffffff : (1ull << leftover_bits) - 1;
      value |= (loadWord(offset) & mask) << bits_read;
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

    uint64_t value = loadWord(offset) >> bitOffset;
    value &= mask;

    if(leftover_bits > 0) {
      offset++;
      bitOffset = leftover_bits;

      boundsCheck(offset);

      const uint64_t mask = leftover_bits == 64 ? 0xffffffffffffffff : (1ull << leftover_bits) - 1;
      value |= (loadWord(offset) & mask) << bits_read;
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

      return (loadWord(offset) & 1) == 1;
    }

    boundsCheck(offset);

    bool value = ((loadWord(offset) >> bitOffset) & 1) == 1;
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
    offset += byteLength;

    // Zero-copy path: when the byte length is a multiple of 8, we can
    // borrow the pointer directly into the underlying buffer, avoiding
    // an allocation and memcpy.  This is the common case for float-encoded
    // value data in TSM blocks (CompressedBuffer always produces a
    // multiple-of-8 byte stream).
    if (byteLength % 8 == 0) {
      return CompressedSlice(CompressedSlice::ZeroCopy{}, ref, byteLength);
    }

    // Fallback: copy into aligned storage (handles non-multiple-of-8 lengths
    // by zero-padding the last word).
    return CompressedSlice(ref, byteLength);
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
