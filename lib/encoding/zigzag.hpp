#ifndef __ZIGZAG_H_INCLUDED__
#define __ZIGZAG_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <algorithm>

// https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers

class ZigZag {
private:
public:
  ZigZag(){};

  static std::vector<uint64_t> zigzagEncodeVector( std::vector<int64_t> input )
  {
    std::vector<uint64_t> output;
    output.resize(input.size());

    std::transform(input.begin(), input.end(), output.begin(), [](int64_t x) { return zigzagEncode(x); });

    return std::move(output);
  }

  static std::vector<int64_t> zigzagDecodeVector( std::vector<uint64_t> input )
  {
    std::vector<int64_t> output;
    output.resize(input.size());

    std::transform(input.begin(), input.end(), output.begin(), [](uint64_t x) { return zigzagDecode(x); });

    return std::move(output);
  }

  static inline uint64_t zigzagEncode( int64_t x )
  {
    return ( ( uint64_t ) x << 1 ) ^ -( ( uint64_t ) x >> 63 );
  }

  static inline int64_t zigzagDecode( uint64_t y )
  {
    return ( int64_t ) ( ( y >> 1 ) ^ -( y & 0x1 ) );
  }
};

#endif
