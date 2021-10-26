#include <gtest/gtest.h>
#include <iostream>
#include <random>

#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "simple8b.hpp"

float get_random()
{
    static std::default_random_engine e;
    static std::uniform_real_distribution<> dis(0, 1); // range 0 - 1
    return dis(e);
}
/*
TEST(FloatTest, BasicAssertions) {
  const unsigned int test_length = 1000000;

  std::vector<double> v;
  double tmp = 0;

  for(unsigned int i=0; i < test_length; i++){
    tmp += get_random();
    v.push_back(tmp);
  }

  auto buf = FloatEncoder::encode(v);
  buf.rewind();
  std::vector<double> decoded;
  FloatEncoder::decode(buf, 0, v.size(), decoded);

    EXPECT_EQ(decoded.size(), v.size());


  for(unsigned int i=0; i < std::min(decoded.size(), v.size()); i++){
    //std::cout << i << " -- " << v[i] << " :: " << decoded[i] << std::endl;
    EXPECT_DOUBLE_EQ(v[i], decoded[i]);
  }
}

TEST(Simple8Test, BasicAssertions) {
  const unsigned int test_length = 1000000;

  std::vector<uint64_t> v;
  uint64_t tmp = 0;

  for(unsigned int i=0; i < test_length; i++){
    tmp += 10;
    v.push_back(tmp);
  }

  auto buf = Simple8B::encode(v);
  auto decoded = Simple8B::decode(buf);

  EXPECT_EQ(decoded.size(), v.size());

  for(unsigned int i=0; i < std::min(decoded.size(), v.size()); i++){
   // std::cout << i << " -- " << v[i] << " :: " << decoded[i] << std::endl;
    EXPECT_EQ(v[i], decoded[i]);
  }
}


TEST(IntegerTest, BasicAssertions) {
  const unsigned int test_length = 1000000;

  std::vector<uint64_t> v;
  uint64_t tmp = 0;

  for(unsigned int i=0; i < test_length; i++){
    tmp += 10000 * get_random();
    v.push_back(tmp);
  }

  auto buf = IntegerEncoder::encode(v);
  // buf.rewind();
  std::vector<uint64_t> decoded;
  IntegerEncoder::decode(buf, decoded);

  EXPECT_EQ(decoded.size(), v.size());


  for(unsigned int i=0; i < std::min(decoded.size(), v.size()); i++){
    EXPECT_EQ(v[i], decoded[i]);
  }
}
*/