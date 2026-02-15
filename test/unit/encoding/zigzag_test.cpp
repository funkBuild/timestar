#include <gtest/gtest.h>
#include "zigzag.hpp"
#include <vector>
#include <cstdint>
#include <climits>

TEST(ZigZagTest, EncodeZero) {
    EXPECT_EQ(ZigZag::zigzagEncode(0), 0u);
}

TEST(ZigZagTest, EncodePositive) {
    EXPECT_EQ(ZigZag::zigzagEncode(1), 2u);
    EXPECT_EQ(ZigZag::zigzagEncode(2), 4u);
    EXPECT_EQ(ZigZag::zigzagEncode(100), 200u);
}

TEST(ZigZagTest, EncodeNegative) {
    EXPECT_EQ(ZigZag::zigzagEncode(-1), 1u);
    EXPECT_EQ(ZigZag::zigzagEncode(-2), 3u);
    EXPECT_EQ(ZigZag::zigzagEncode(-100), 199u);
}

TEST(ZigZagTest, EncodeMax) {
    uint64_t encoded = ZigZag::zigzagEncode(INT64_MAX);
    int64_t decoded = ZigZag::zigzagDecode(encoded);
    EXPECT_EQ(decoded, INT64_MAX);
}

TEST(ZigZagTest, EncodeMin) {
    uint64_t encoded = ZigZag::zigzagEncode(INT64_MIN);
    int64_t decoded = ZigZag::zigzagDecode(encoded);
    EXPECT_EQ(decoded, INT64_MIN);
}

TEST(ZigZagTest, DecodeRoundtrip) {
    std::vector<int64_t> testValues = {
        0, 1, -1, 2, -2, 127, -128, 1000, -1000, INT64_MAX, INT64_MIN
    };

    for (int64_t val : testValues) {
        uint64_t encoded = ZigZag::zigzagEncode(val);
        int64_t decoded = ZigZag::zigzagDecode(encoded);
        EXPECT_EQ(decoded, val) << "Roundtrip failed for value " << val;
    }
}

TEST(ZigZagTest, EncodingOrder) {
    // ZigZag maps: 0->0, -1->1, 1->2, -2->3, 2->4
    EXPECT_EQ(ZigZag::zigzagEncode(0), 0u);
    EXPECT_EQ(ZigZag::zigzagEncode(-1), 1u);
    EXPECT_EQ(ZigZag::zigzagEncode(1), 2u);
    EXPECT_EQ(ZigZag::zigzagEncode(-2), 3u);
    EXPECT_EQ(ZigZag::zigzagEncode(2), 4u);
}

TEST(ZigZagTest, VectorRoundtrip) {
    std::vector<int64_t> input = {0, 1, -1, 100, -100, 50000, -50000, INT64_MAX, INT64_MIN};

    std::vector<uint64_t> encoded = ZigZag::zigzagEncodeVector(input);
    ASSERT_EQ(encoded.size(), input.size());

    std::vector<int64_t> decoded = ZigZag::zigzagDecodeVector(encoded);
    ASSERT_EQ(decoded.size(), input.size());

    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i;
    }
}

TEST(ZigZagTest, EmptyVector) {
    std::vector<int64_t> input;
    std::vector<uint64_t> encoded = ZigZag::zigzagEncodeVector(input);
    EXPECT_TRUE(encoded.empty());

    std::vector<int64_t> decoded = ZigZag::zigzagDecodeVector(encoded);
    EXPECT_TRUE(decoded.empty());
}

TEST(ZigZagTest, LargeVectorRoundtrip) {
    std::vector<int64_t> input(10000);
    for (int i = 0; i < 10000; i++) {
        input[i] = static_cast<int64_t>(i) - 5000;
    }

    std::vector<uint64_t> encoded = ZigZag::zigzagEncodeVector(input);
    ASSERT_EQ(encoded.size(), input.size());

    std::vector<int64_t> decoded = ZigZag::zigzagDecodeVector(encoded);
    ASSERT_EQ(decoded.size(), input.size());

    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i;
    }
}
