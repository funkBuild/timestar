#include <gtest/gtest.h>
#include <cstdint>
#include <limits>

#include "../../../lib/storage/aligned_buffer.hpp"

class AlignedBufferTest : public ::testing::Test {
protected:
    AlignedBuffer buf;
};

// ---------- read64 correctness ----------

TEST_F(AlignedBufferTest, Read64ReturnsCorrectValue) {
    uint64_t expected = 0xDEADBEEFCAFEBABEULL;
    buf.write(expected);
    EXPECT_EQ(buf.read64(0), expected);
}

TEST_F(AlignedBufferTest, Read64MultipleValues) {
    uint64_t v1 = 1;
    uint64_t v2 = std::numeric_limits<uint64_t>::max();
    uint64_t v3 = 0x0102030405060708ULL;

    buf.write(v1);
    buf.write(v2);
    buf.write(v3);

    EXPECT_EQ(buf.read64(0), v1);
    EXPECT_EQ(buf.read64(8), v2);
    EXPECT_EQ(buf.read64(16), v3);
}

TEST_F(AlignedBufferTest, Read64Zero) {
    uint64_t zero = 0;
    buf.write(zero);
    EXPECT_EQ(buf.read64(0), 0ULL);
}

// ---------- read64 bounds checking ----------

TEST_F(AlignedBufferTest, Read64ThrowsOnEmptyBuffer) {
    EXPECT_THROW(buf.read64(0), std::runtime_error);
}

TEST_F(AlignedBufferTest, Read64ThrowsWhenOffsetTooLarge) {
    uint64_t val = 42;
    buf.write(val);
    // Buffer has 8 bytes (indices 0-7). Reading 8 bytes starting at offset 1
    // would require bytes 1-8, but byte 8 does not exist.
    EXPECT_THROW(buf.read64(1), std::runtime_error);
}

TEST_F(AlignedBufferTest, Read64ThrowsAtExactBoundary) {
    uint64_t val = 42;
    buf.write(val);
    // Offset 8 is exactly at the end; needs bytes 8-15 but only 0-7 exist.
    EXPECT_THROW(buf.read64(8), std::runtime_error);
}

TEST_F(AlignedBufferTest, Read64SucceedsAtLastValidOffset) {
    uint64_t v1 = 111;
    uint64_t v2 = 222;
    buf.write(v1);
    buf.write(v2);
    // Buffer is 16 bytes. Last valid offset for read64 is 8.
    EXPECT_NO_THROW(buf.read64(8));
    EXPECT_EQ(buf.read64(8), v2);
}

// ---------- read8 correctness ----------

TEST_F(AlignedBufferTest, Read8ReturnsCorrectValue) {
    uint8_t val = 0xAB;
    buf.write(val);
    EXPECT_EQ(buf.read8(0), 0xAB);
}

TEST_F(AlignedBufferTest, Read8MultipleBytes) {
    uint8_t a = 0x01, b = 0x02, c = 0xFF;
    buf.write(a);
    buf.write(b);
    buf.write(c);

    EXPECT_EQ(buf.read8(0), 0x01);
    EXPECT_EQ(buf.read8(1), 0x02);
    EXPECT_EQ(buf.read8(2), 0xFF);
}

// ---------- read8 bounds checking ----------

TEST_F(AlignedBufferTest, Read8ThrowsOnEmptyBuffer) {
    EXPECT_THROW(buf.read8(0), std::runtime_error);
}

TEST_F(AlignedBufferTest, Read8ThrowsWhenOffsetEqualsSize) {
    uint8_t val = 0x42;
    buf.write(val);
    // Buffer has 1 byte at index 0. Index 1 is out of bounds.
    EXPECT_THROW(buf.read8(1), std::runtime_error);
}

// ---------- write / read roundtrip ----------

TEST_F(AlignedBufferTest, WriteReadRoundtripUint64) {
    for (uint64_t i = 0; i < 100; ++i) {
        buf.write(i);
    }
    EXPECT_EQ(buf.size(), 100 * sizeof(uint64_t));
    for (uint64_t i = 0; i < 100; ++i) {
        EXPECT_EQ(buf.read64(i * sizeof(uint64_t)), i);
    }
}

TEST_F(AlignedBufferTest, WriteReadRoundtripUint8) {
    for (uint8_t i = 0; i < 255; ++i) {
        buf.write(i);
    }
    EXPECT_EQ(buf.size(), 255u);
    for (uint8_t i = 0; i < 255; ++i) {
        EXPECT_EQ(buf.read8(i), i);
    }
}

TEST_F(AlignedBufferTest, WriteReadRoundtripDouble) {
    double val = 3.14159265358979;
    buf.write(val);
    // Read back the raw bytes as uint64_t and compare bitwise
    uint64_t raw;
    std::memcpy(&raw, &val, sizeof(double));
    EXPECT_EQ(buf.read64(0), raw);
}

TEST_F(AlignedBufferTest, WriteArrayReadRoundtrip) {
    std::vector<uint64_t> values = {10, 20, 30, 40, 50};
    buf.write_array(values.data(), values.size());

    EXPECT_EQ(buf.size(), values.size() * sizeof(uint64_t));
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(buf.read64(i * sizeof(uint64_t)), values[i]);
    }
}

// ---------- size and clear interaction ----------

TEST_F(AlignedBufferTest, ClearResetsSize) {
    buf.write<uint64_t>(42);
    EXPECT_EQ(buf.size(), sizeof(uint64_t));
    buf.clear();
    EXPECT_EQ(buf.size(), 0u);
    // After clear, reads should throw
    EXPECT_THROW(buf.read64(0), std::runtime_error);
    EXPECT_THROW(buf.read8(0), std::runtime_error);
}

TEST_F(AlignedBufferTest, InitialSizeConstructor) {
    AlignedBuffer sized(16);
    EXPECT_EQ(sized.size(), 16u);
    // Should be able to read (values are zero-initialized by vector::resize)
    EXPECT_NO_THROW(sized.read64(0));
    EXPECT_NO_THROW(sized.read64(8));
    EXPECT_THROW(sized.read64(9), std::runtime_error);
}
