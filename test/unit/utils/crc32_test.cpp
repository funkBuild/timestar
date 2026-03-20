#include "../../lib/utils/crc32.hpp"
#include <gtest/gtest.h>
#include <cstring>
#include <vector>

// CRC32 class is in the global namespace (defined after detail:: in crc32.hpp)

TEST(CRC32UnitTest, EmptyInput) {
    uint8_t dummy = 0;
    uint32_t crc = CRC32::compute(&dummy, 0);
    // CRC32C of empty input is 0
    EXPECT_EQ(crc, 0u);
}

TEST(CRC32UnitTest, SingleByte) {
    uint8_t data = 0x00;
    uint32_t crc = CRC32::compute(&data, 1);
    EXPECT_NE(crc, 0u);  // Non-trivial hash for single byte
}

TEST(CRC32UnitTest, KnownVector_Zeros) {
    // 4 zero bytes
    std::vector<uint8_t> data(4, 0x00);
    uint32_t crc = CRC32::compute(data.data(), data.size());
    EXPECT_NE(crc, 0u);
}

TEST(CRC32UnitTest, Deterministic) {
    std::string data = "Hello, TimeStar!";
    uint32_t crc1 = CRC32::compute(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    uint32_t crc2 = CRC32::compute(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    EXPECT_EQ(crc1, crc2);
}

TEST(CRC32UnitTest, DifferentInputsDifferentCRC) {
    std::string data1 = "hello";
    std::string data2 = "world";
    uint32_t crc1 = CRC32::compute(reinterpret_cast<const uint8_t*>(data1.data()), data1.size());
    uint32_t crc2 = CRC32::compute(reinterpret_cast<const uint8_t*>(data2.data()), data2.size());
    EXPECT_NE(crc1, crc2);
}

TEST(CRC32UnitTest, SingleBitFlipDetected) {
    std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    uint32_t original = CRC32::compute(data.data(), data.size());

    // Flip one bit
    data[3] ^= 0x01;
    uint32_t flipped = CRC32::compute(data.data(), data.size());
    EXPECT_NE(original, flipped);
}

TEST(CRC32UnitTest, IncrementalUpdate) {
    std::string full = "Hello, World!";
    uint32_t fullCRC = CRC32::compute(reinterpret_cast<const uint8_t*>(full.data()), full.size());

    // Compute in two parts using update
    uint32_t partialCRC = CRC32::update(0, reinterpret_cast<const uint8_t*>(full.data()), 7);
    uint32_t incrementalCRC = CRC32::update(partialCRC, reinterpret_cast<const uint8_t*>(full.data() + 7), 6);

    EXPECT_EQ(fullCRC, incrementalCRC);
}

TEST(CRC32UnitTest, CharAndUint8Parity) {
    std::string str = "test data for CRC";
    uint32_t crcFromStr = CRC32::compute(reinterpret_cast<const uint8_t*>(str.data()), str.size());

    std::vector<uint8_t> bytes(str.begin(), str.end());
    uint32_t crcFromBytes = CRC32::compute(bytes.data(), bytes.size());

    EXPECT_EQ(crcFromStr, crcFromBytes);
}

TEST(CRC32UnitTest, LargeInput) {
    // Verify the slicing-by-8 path is exercised (needs > 8 bytes)
    std::vector<uint8_t> data(4096, 0xAA);
    uint32_t crc = CRC32::compute(data.data(), data.size());
    EXPECT_NE(crc, 0u);

    // Same data should produce same CRC
    uint32_t crc2 = CRC32::compute(data.data(), data.size());
    EXPECT_EQ(crc, crc2);
}

TEST(CRC32UnitTest, AllAlignmentResidues) {
    // Test with sizes 1-16 to exercise all alignment residue paths
    std::vector<uint8_t> data(16);
    for (size_t i = 0; i < 16; ++i) data[i] = static_cast<uint8_t>(i + 1);

    uint32_t prev = 0;
    for (size_t len = 1; len <= 16; ++len) {
        uint32_t crc = CRC32::compute(data.data(), len);
        EXPECT_NE(crc, prev) << "CRC should differ for length " << len;
        prev = crc;
    }
}
