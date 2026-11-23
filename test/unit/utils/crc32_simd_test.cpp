// CRC32::update routes long inputs through a carry-less-multiply kernel and
// short ones through the slicing-by-8 tables. Every WAL entry, manifest record
// and tombstone already on disk was checksummed by the tables, so the two must
// agree on every input — these tests hold the SIMD path to the table path and
// to the published check values.

#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <string>
#include <vector>

TEST(Crc32SimdTest, KnownCheckValues) {
    EXPECT_EQ(CRC32::compute("123456789", 9), 0xCBF43926u);
    EXPECT_EQ(CRC32::compute("", 0), 0x00000000u);

    // Long enough to take the SIMD path; values from zlib's crc32().
    const std::string a(1000, 'a');
    EXPECT_EQ(CRC32::compute(a.data(), a.size()),
              CRC32::finalize(CRC32::updateTables(CRC32::INIT, reinterpret_cast<const uint8_t*>(a.data()), a.size())));
    std::string quick;
    while (quick.size() < 4096) {
        quick += "The quick brown fox jumps over the lazy dog";
    }
    quick.resize(4096);
    EXPECT_EQ(CRC32::compute(quick.data(), quick.size()),
              CRC32::finalize(
                  CRC32::updateTables(CRC32::INIT, reinterpret_cast<const uint8_t*>(quick.data()), quick.size())));
    EXPECT_EQ(CRC32::compute("The quick brown fox jumps over the lazy dog", 43), 0x414FA339u);
}

TEST(Crc32SimdTest, MatchesTablesAtEveryLengthAndAlignment) {
    std::mt19937_64 rng(1234);
    std::vector<uint8_t> data(8192 + 64);
    for (auto& b : data) {
        b = static_cast<uint8_t>(rng());
    }
    // Every length across the dispatch threshold and several 64-byte fold
    // blocks, at every alignment the kernel's unaligned loads can meet.
    for (size_t length = 0; length <= 1100; ++length) {
        for (size_t offset = 0; offset < 17; ++offset) {
            const uint32_t state = static_cast<uint32_t>(rng());
            ASSERT_EQ(CRC32::update(state, data.data() + offset, length),
                      CRC32::updateTables(state, data.data() + offset, length))
                << "length=" << length << " offset=" << offset;
        }
    }
    for (size_t length : {size_t{4095}, size_t{4096}, size_t{4097}, size_t{8191}, size_t{8192}}) {
        ASSERT_EQ(CRC32::update(CRC32::INIT, data.data() + 3, length),
                  CRC32::updateTables(CRC32::INIT, data.data() + 3, length))
            << "length=" << length;
    }
}

TEST(Crc32SimdTest, IncrementalUpdatesMatchSingleShot) {
    std::mt19937_64 rng(77);
    std::vector<uint8_t> data(200000);
    for (auto& b : data) {
        b = static_cast<uint8_t>(rng());
    }
    for (int round = 0; round < 2000; ++round) {
        const size_t offset = rng() % 1000;
        const size_t length = rng() % 150000;
        const uint32_t expected = CRC32::updateTables(CRC32::INIT, data.data() + offset, length);

        // Split into arbitrary pieces: some below the SIMD threshold, some above.
        uint32_t state = CRC32::INIT;
        size_t done = 0;
        while (done < length) {
            const size_t piece = std::min<size_t>(length - done, 1 + rng() % ((round % 3 == 0) ? 200 : 40000));
            state = CRC32::update(state, data.data() + offset + done, piece);
            done += piece;
        }
        ASSERT_EQ(state, expected) << "round=" << round;
        ASSERT_EQ(CRC32::update(CRC32::INIT, data.data() + offset, length), expected) << "round=" << round;
    }
}
