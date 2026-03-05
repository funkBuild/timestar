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

// Boundary tests: verify exact encoded values for INT64_MIN and INT64_MAX.
// ZigZag maps: encode(n) = (uint64_t(n) << 1) ^ uint64_t(n >> 63)
// encode(INT64_MAX) = (0xFFFFFFFFFFFFFFFE) ^ 0 = UINT64_MAX - 1
// encode(INT64_MIN) = (0x0000000000000000) ^ 0xFFFFFFFFFFFFFFFF = UINT64_MAX

TEST(ZigZagTest, BoundaryEncodeMaxExactValue) {
    // INT64_MAX (9223372036854775807) must encode to UINT64_MAX - 1 (18446744073709551614)
    uint64_t encoded = ZigZag::zigzagEncode(INT64_MAX);
    EXPECT_EQ(encoded, UINT64_MAX - 1u)
        << "encode(INT64_MAX) should equal UINT64_MAX - 1 = " << (UINT64_MAX - 1u)
        << " but got " << encoded;
}

TEST(ZigZagTest, BoundaryEncodeMinExactValue) {
    // INT64_MIN (-9223372036854775808) must encode to UINT64_MAX (18446744073709551615).
    // A naive signed-shift implementation (n << 1) would invoke UB due to signed overflow;
    // the implementation must use static_cast<uint64_t>(n) << 1 to avoid UB.
    uint64_t encoded = ZigZag::zigzagEncode(INT64_MIN);
    EXPECT_EQ(encoded, UINT64_MAX)
        << "encode(INT64_MIN) should equal UINT64_MAX = " << UINT64_MAX
        << " but got " << encoded;
}

TEST(ZigZagTest, BoundaryDecodeUint64MaxGivesInt64Min) {
    // decode(UINT64_MAX) must return INT64_MIN without crashing or wrapping wrongly.
    // UINT64_MAX is the encoded form of INT64_MIN.
    int64_t decoded = ZigZag::zigzagDecode(UINT64_MAX);
    EXPECT_EQ(decoded, INT64_MIN)
        << "decode(UINT64_MAX) should equal INT64_MIN but got " << decoded;
}

TEST(ZigZagTest, BoundaryDecodeUint64MaxMinus1GivesInt64Max) {
    // decode(UINT64_MAX - 1) must return INT64_MAX.
    // UINT64_MAX - 1 is the encoded form of INT64_MAX.
    int64_t decoded = ZigZag::zigzagDecode(UINT64_MAX - 1u);
    EXPECT_EQ(decoded, INT64_MAX)
        << "decode(UINT64_MAX - 1) should equal INT64_MAX but got " << decoded;
}

TEST(ZigZagTest, BoundaryRoundtripInt64Min) {
    // Full round-trip for INT64_MIN: encode then decode must recover the original value.
    int64_t original = INT64_MIN;
    uint64_t encoded = ZigZag::zigzagEncode(original);
    int64_t decoded = ZigZag::zigzagDecode(encoded);
    EXPECT_EQ(encoded, UINT64_MAX) << "Encoded INT64_MIN should be UINT64_MAX";
    EXPECT_EQ(decoded, original)   << "Round-trip of INT64_MIN failed";
}

TEST(ZigZagTest, BoundaryRoundtripInt64Max) {
    // Full round-trip for INT64_MAX: encode then decode must recover the original value.
    int64_t original = INT64_MAX;
    uint64_t encoded = ZigZag::zigzagEncode(original);
    int64_t decoded = ZigZag::zigzagDecode(encoded);
    EXPECT_EQ(encoded, UINT64_MAX - 1u) << "Encoded INT64_MAX should be UINT64_MAX - 1";
    EXPECT_EQ(decoded, original)        << "Round-trip of INT64_MAX failed";
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
