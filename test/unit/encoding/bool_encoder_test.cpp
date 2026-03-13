#include "bool_encoder.hpp"

#include "slice_buffer.hpp"

#include <gtest/gtest.h>

#include <random>
#include <vector>

TEST(BoolEncoderTest, AllTrue) {
    std::vector<bool> values(100, true);
    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], true) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, AllFalse) {
    std::vector<bool> values(100, false);
    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], false) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, Alternating) {
    std::vector<bool> values(128);
    for (size_t i = 0; i < 128; i++) {
        values[i] = (i % 2 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, ExactBoundary8) {
    std::vector<bool> values = {true, false, true, true, false, false, true, false};

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, ExactBoundary16) {
    std::vector<bool> values(16);
    for (size_t i = 0; i < 16; i++) {
        values[i] = (i % 3 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, ExactBoundary32) {
    std::vector<bool> values(32);
    for (size_t i = 0; i < 32; i++) {
        values[i] = (i % 5 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, ExactBoundary64) {
    std::vector<bool> values(64);
    for (size_t i = 0; i < 64; i++) {
        values[i] = (i % 7 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, PartialByte) {
    // 5 values: tests the partial-byte path where numValuesLeft < 8
    std::vector<bool> values = {true, false, true, false, true};

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, NonPowerOf8) {
    // 70 values: 64 via uint64_t + 6 via partial byte
    std::vector<bool> values(70);
    for (size_t i = 0; i < 70; i++) {
        values[i] = (i % 3 != 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, DecodeWithSkip) {
    // Encode 20 values, decode with nToSkip=5 and length=10
    std::vector<bool> values(20);
    for (size_t i = 0; i < 20; i++) {
        values[i] = (i % 2 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 5, 10, decoded);

    ASSERT_EQ(decoded.size(), 10u);
    for (size_t i = 0; i < 10; i++) {
        EXPECT_EQ(decoded[i], values[i + 5])
            << "Mismatch at decoded index " << i << " (original index " << (i + 5) << ")";
    }
}

TEST(BoolEncoderTest, DecodeSkipAll) {
    // Encode 10 values, decode with nToSkip=10 and length=0
    std::vector<bool> values(10, true);

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 10, 0, decoded);

    EXPECT_EQ(decoded.size(), 0u);
}

TEST(BoolEncoderTest, LargeDataset) {
    // 1000 random bools
    std::mt19937 gen(42);
    std::uniform_int_distribution<int> dist(0, 1);

    std::vector<bool> values(1000);
    for (size_t i = 0; i < 1000; i++) {
        values[i] = (dist(gen) == 1);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, SingleValue) {
    // Single true
    {
        std::vector<bool> values = {true};
        AlignedBuffer encoded = BoolEncoder::encode(values);
        EXPECT_GT(encoded.size(), 0u);

        Slice slice(encoded.data.data(), encoded.size());
        std::vector<bool> decoded;
        BoolEncoder::decode(slice, 0, 1, decoded);

        ASSERT_EQ(decoded.size(), 1u);
        EXPECT_EQ(decoded[0], true);
    }

    // Single false
    {
        std::vector<bool> values = {false};
        AlignedBuffer encoded = BoolEncoder::encode(values);
        EXPECT_GT(encoded.size(), 0u);

        Slice slice(encoded.data.data(), encoded.size());
        std::vector<bool> decoded;
        BoolEncoder::decode(slice, 0, 1, decoded);

        ASSERT_EQ(decoded.size(), 1u);
        EXPECT_EQ(decoded[0], false);
    }
}

TEST(BoolEncoderTest, Mixed73) {
    // 73 values: 64 via uint64_t + 8 via uint8_t + 1 via partial byte
    std::vector<bool> values(73);
    for (size_t i = 0; i < 73; i++) {
        // Known pattern: true for multiples of 3, false otherwise
        values[i] = (i % 3 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

// Edge-case tests: sizes that are NOT multiples of 64 (or of any power-of-2
// word width used internally).  These exercise the partial-batch paths and
// verify there is no out-of-bounds read in encodeBool<T>().

TEST(BoolEncoderTest, SevenBools) {
    // 7 values: all handled by the partial-byte (< 8) overload.
    std::vector<bool> values = {true, false, true, true, false, true, false};

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, ThirtyThreeBools) {
    // 33 values: 32 via uint32_t + 1 via partial byte.
    std::vector<bool> values(33);
    for (size_t i = 0; i < 33; i++) {
        values[i] = (i % 4 != 1);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, SixtyThreeBools) {
    // 63 values: 32 via uint32_t + 16 via uint16_t + 8 via uint8_t + 7 via
    // partial byte — exercises every branching tier.
    std::vector<bool> values(63);
    for (size_t i = 0; i < 63; i++) {
        values[i] = (i % 5 < 3);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, SixtyFiveBools) {
    // 65 values: 64 via uint64_t + 1 via partial byte.
    std::vector<bool> values(65);
    for (size_t i = 0; i < 65; i++) {
        values[i] = (i % 2 == 0);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, OneTwentyNineBools) {
    // 129 values: 128 via two uint64_t passes + 1 via partial byte.
    std::vector<bool> values(129);
    for (size_t i = 0; i < 129; i++) {
        values[i] = (i % 3 != 2);
    }

    AlignedBuffer encoded = BoolEncoder::encode(values);
    EXPECT_GT(encoded.size(), 0u);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderTest, EncodeIntoEdgeSizes) {
    // Verify encodeInto() (used in WAL path) produces identical results to
    // encode() for the same non-multiple-of-64 sizes.
    for (size_t n : {1u, 7u, 33u, 63u, 65u, 129u}) {
        std::vector<bool> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = (i % 2 == 0);
        }

        AlignedBuffer encoded1 = BoolEncoder::encode(values);

        AlignedBuffer encoded2;
        BoolEncoder::encodeInto(values, encoded2);

        ASSERT_EQ(encoded1.size(), encoded2.size()) << "encode() vs encodeInto() size mismatch for n=" << n;
        ASSERT_EQ(0, std::memcmp(encoded1.data.data(), encoded2.data.data(), encoded1.size()))
            << "encode() vs encodeInto() content mismatch for n=" << n;

        // Also verify round-trip for encodeInto output.
        Slice slice(encoded2.data.data(), encoded2.size());
        std::vector<bool> decoded;
        BoolEncoder::decode(slice, 0, n, decoded);
        ASSERT_EQ(decoded.size(), n) << "decode size mismatch for n=" << n;
        for (size_t i = 0; i < n; i++) {
            EXPECT_EQ(decoded[i], values[i]) << "Round-trip mismatch at index " << i << " for n=" << n;
        }
    }
}
