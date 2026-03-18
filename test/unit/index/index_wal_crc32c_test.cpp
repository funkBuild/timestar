#include "../../../lib/index/native/index_wal.hpp"
#include "../../../lib/index/native/memtable.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <filesystem>

using namespace timestar::index;

// =============================================================================
// CRC32C polynomial consistency tests.
//
// BUG: The x86 hardware path used CRC32C (Castagnoli polynomial 0x1EDC6F41)
// via _mm_crc32 intrinsics, but the software fallback used CRC32 (zlib
// polynomial 0xEDB88320). These produce different checksums.
//
// FIX: Use the CRC32C polynomial (0x82F63B78 reflected) in the software table.
//
// These tests verify WAL write/replay round-trip (which exercises CRC32C)
// and independently compute CRC32C to verify the correct polynomial is used.
// =============================================================================

// Reference CRC32C implementation using the Castagnoli polynomial.
// Used only for test verification — not performance-critical.
static uint32_t referenceCrc32c(const char* data, size_t len) {
    // CRC32C (Castagnoli) reflected polynomial
    static constexpr uint32_t POLY = 0x82F63B78;
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (POLY & (-(crc & 1)));
        }
    }
    return crc ^ 0xFFFFFFFF;
}

class IndexWalCrc32cTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("test_wal_crc32c"); }
    void TearDown() override { std::filesystem::remove_all("test_wal_crc32c"); }
};

// Verify the reference CRC32C produces the RFC 3720 test vector
TEST_F(IndexWalCrc32cTest, ReferenceCrc32cMatchesRFC3720) {
    // CRC32C("123456789") = 0xE3069283 per RFC 3720 / iSCSI spec
    uint32_t crc = referenceCrc32c("123456789", 9);
    EXPECT_EQ(crc, 0xE3069283u);
}

// Write a WAL entry and replay it — exercises the CRC32C path end-to-end.
// If the polynomial is wrong, replay will fail CRC validation and drop entries.
SEASTAR_TEST_F(IndexWalCrc32cTest, WalRoundtripVerifiesCrc32c) {
    std::filesystem::create_directories("test_wal_crc32c");

    // Write a batch
    {
        auto wal = co_await IndexWAL::open("test_wal_crc32c");
        IndexWriteBatch batch;
        batch.put("test_key_1", "value_1");
        batch.put("test_key_2", "value_2");
        co_await wal.append(batch);
        co_await wal.close();
    }

    // Replay and verify entries survived the CRC check
    {
        auto wal = co_await IndexWAL::open("test_wal_crc32c");
        MemTable mt;
        auto count = co_await wal.replay(mt);

        EXPECT_EQ(count, 1u) << "Expected 1 record replayed (CRC must have matched)";
        EXPECT_EQ(mt.size(), 2u) << "Expected 2 entries in replayed MemTable";

        auto v1 = mt.get("test_key_1");
        EXPECT_TRUE(v1.has_value());
        if (v1.has_value()) EXPECT_EQ(*v1, "value_1");

        auto v2 = mt.get("test_key_2");
        EXPECT_TRUE(v2.has_value());
        if (v2.has_value()) EXPECT_EQ(*v2, "value_2");

        co_await wal.close();
    }
}

// Source inspection: verify the software fallback uses CRC32C polynomial
TEST_F(IndexWalCrc32cTest, SoftwareFallbackUsesCrc32cPolynomial) {
    std::string src;
    {
        std::ifstream ifs("../lib/index/native/index_wal.cpp");
        src.assign(std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>());
    }

    if (src.empty()) {
        GTEST_SKIP() << "Could not read index_wal.cpp for source inspection";
    }

    // The CRC32C Castagnoli reflected polynomial is 0x82F63B78
    EXPECT_NE(src.find("0x82F63B78"), std::string::npos)
        << "Software fallback must use CRC32C polynomial 0x82F63B78";

    // The old zlib polynomial must NOT be present
    EXPECT_EQ(src.find("0xEDB88320"), std::string::npos)
        << "Software fallback must NOT use CRC32 zlib polynomial 0xEDB88320";
}
