#include "integer_encoder.hpp"
#include "zigzag.hpp"
#include "simple8b.hpp"
#include "slice_buffer.hpp"

#include <iostream>

// Timestamp encoding - http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

AlignedBuffer IntegerEncoder::encode(const std::vector<uint64_t> &values){
  std::vector<uint64_t> encoded;
  encoded.reserve(values.size());

  uint64_t start_value = values[0];
  encoded.push_back(start_value);

  int64_t delta = values[1] - values[0];
  uint64_t first_delta = ZigZag::zigzagEncode(delta);

  encoded.push_back(first_delta);

  for(unsigned int i = 2; i < values.size(); i++){
    int64_t D = (values[i] - values[i-1]) - (values[i-1] - values[i-2]);
    uint64_t encD = ZigZag::zigzagEncode(D);
    encoded.push_back(encD);
  }

  return Simple8B::encode(encoded);
}

std::pair<size_t, size_t> IntegerEncoder::decode(Slice &encoded, unsigned int timestampSize, std::vector<uint64_t> &values, uint64_t minTime, uint64_t maxTime){
  std::vector<uint64_t> deltaValues = Simple8B::decode(encoded, timestampSize);

  size_t nSkipped = 0, nAdded = 0;
  uint64_t last_decoded = deltaValues[0];

  if(last_decoded < minTime) {
    nSkipped++;
  } else {
    nAdded++;
    values.push_back(last_decoded);
  }

  int64_t delta = ZigZag::zigzagDecode(deltaValues[1]);
  last_decoded += delta;

  if(last_decoded < minTime) {
    nSkipped++;
  } else {
    nAdded++;
    values.push_back(last_decoded);
  }

  for(unsigned int i = 2; i < deltaValues.size(); i++){
    int64_t encD = ZigZag::zigzagDecode(deltaValues[i]);

    delta += encD;
    last_decoded += delta;

    if(last_decoded < minTime) {
      nSkipped++;
      continue;
    }

    if(last_decoded > maxTime)
      return {nSkipped, nAdded};

    nAdded++;
    values.push_back(last_decoded);
  }

  return {nSkipped, nAdded};
}
