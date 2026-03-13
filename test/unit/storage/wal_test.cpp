#include "../../../lib/storage/wal.hpp"

#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

class WALTest : public ::testing::Test {
protected:
    std::string testDir = "./test_wal_files";
    fs::path savedCwd;

    void SetUp() override {
        savedCwd = fs::current_path();
        // If a previous test left us inside the test directory, step out first
        if (fs::current_path().filename() == "test_wal_files") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }
        // Remove stale test directory from previous runs, then recreate
        fs::remove_all(testDir);
        fs::create_directories(testDir);
        fs::current_path(testDir);
    }

    void TearDown() override {
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }
};

TEST_F(WALTest, SequenceNumberToFilename) {
    EXPECT_EQ(WAL::sequenceNumberToFilename(1), "shard_0/0000000001.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(42), "shard_0/0000000042.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(999), "shard_0/0000000999.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(12345678), "shard_0/0012345678.wal");
}

// ---------------------------------------------------------------------------
// CRC32 unit tests
// ---------------------------------------------------------------------------

TEST(CRC32Test, EmptyInput) {
    // CRC32 of empty data is 0x00000000
    uint32_t crc = CRC32::compute(static_cast<const uint8_t*>(nullptr), 0);
    EXPECT_EQ(crc, 0x00000000);
}

TEST(CRC32Test, KnownTestVector) {
    // CRC32 of "123456789" is 0xCBF43926 (standard test vector)
    const std::string data = "123456789";
    uint32_t crc = CRC32::compute(data.data(), data.size());
    EXPECT_EQ(crc, 0xCBF43926);
}

TEST(CRC32Test, SingleByte) {
    // CRC32 of a single 'A' byte (0x41)
    uint8_t byte = 0x41;
    uint32_t crc = CRC32::compute(&byte, 1);
    // Known CRC32 of single byte 'A' = 0xD3D99E8B
    EXPECT_EQ(crc, 0xD3D99E8B);
}

TEST(CRC32Test, Deterministic) {
    // Same input always produces same CRC
    const std::string data = "Hello, World!";
    uint32_t crc1 = CRC32::compute(data.data(), data.size());
    uint32_t crc2 = CRC32::compute(data.data(), data.size());
    EXPECT_EQ(crc1, crc2);
}

TEST(CRC32Test, DifferentInputsDifferentCRC) {
    const std::string data1 = "Hello, World!";
    const std::string data2 = "Hello, World?";
    uint32_t crc1 = CRC32::compute(data1.data(), data1.size());
    uint32_t crc2 = CRC32::compute(data2.data(), data2.size());
    EXPECT_NE(crc1, crc2);
}

TEST(CRC32Test, SingleBitFlipDetected) {
    // Flipping a single bit in the input must change the CRC
    std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0x04, 0x05};
    uint32_t original_crc = CRC32::compute(data.data(), data.size());

    // Flip one bit in the middle byte
    data[2] ^= 0x01;
    uint32_t flipped_crc = CRC32::compute(data.data(), data.size());

    EXPECT_NE(original_crc, flipped_crc);
}

TEST(CRC32Test, CharOverload) {
    // The char* overload should produce the same result as uint8_t*
    const char* cdata = "test data";
    const uint8_t* udata = reinterpret_cast<const uint8_t*>(cdata);
    size_t len = 9;

    EXPECT_EQ(CRC32::compute(cdata, len), CRC32::compute(udata, len));
}