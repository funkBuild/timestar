#include <gtest/gtest.h>
#include "bool_encoder.hpp"
#include "slice_buffer.hpp"
#include <vector>
#include <random>

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
        EXPECT_EQ(decoded[i], values[i + 5]) << "Mismatch at decoded index " << i
                                              << " (original index " << (i + 5) << ")";
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
