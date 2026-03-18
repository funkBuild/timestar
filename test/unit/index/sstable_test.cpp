#include "../../../lib/index/native/sstable.hpp"

#include "../../seastar_gtest.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <format>
#include <string>

using namespace timestar::index;

static std::string testDir() {
    auto dir = std::filesystem::temp_directory_path() / "timestar_sstable_test";
    std::filesystem::create_directories(dir);
    return dir.string();
}

class SSTableTest : public ::testing::Test {
public:
    void SetUp() override { dir_ = testDir(); }

    void TearDown() override { std::filesystem::remove_all(dir_); }

    std::string sstPath(const std::string& name = "test.sst") { return dir_ + "/" + name; }

    std::string dir_;
};

// ============================================================================
// SSTable Writer + Reader roundtrip tests
// ============================================================================

SEASTAR_TEST_F(SSTableTest, WriteAndReadSingle) {
    auto path = self->sstPath("single.sst");
    auto writer = co_await SSTableWriter::create(path);
    writer.add("hello", "world");
    auto meta = co_await writer.finish();

    EXPECT_EQ(meta.entryCount, 1u);
    EXPECT_EQ(meta.minKey, "hello");
    EXPECT_EQ(meta.maxKey, "hello");

    auto reader = co_await SSTableReader::open(path);
    auto val = reader->get("hello");
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(*val, "world");

    auto missing = reader->get("nothere");
    EXPECT_FALSE(missing.has_value());

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, WriteAndReadMany) {
    auto path = self->sstPath("many.sst");
    auto writer = co_await SSTableWriter::create(path, 512);  // Small blocks to force multiple
    const int N = 500;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    auto meta = co_await writer.finish();

    EXPECT_EQ(meta.entryCount, static_cast<uint64_t>(N));
    EXPECT_EQ(meta.minKey, "key:0000");
    EXPECT_EQ(meta.maxKey, std::format("key:{:04d}", N - 1));

    auto reader = co_await SSTableReader::open(path);

    // Point lookups
    for (int i = 0; i < N; i += 50) {
        auto val = reader->get(std::format("key:{:04d}", i));
        EXPECT_TRUE(val.has_value()) << "Missing key:" << i;
        EXPECT_EQ(*val, std::format("val:{:04d}", i));
    }

    // Non-existent keys
    auto missing = reader->get("key:9999");
    EXPECT_FALSE(missing.has_value());

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, IteratorFullScan) {
    auto path = self->sstPath("iter.sst");
    auto writer = co_await SSTableWriter::create(path, 512);
    const int N = 200;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("k:{:04d}", i), std::format("v:{:04d}", i));
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    auto it = reader->newIterator();
    it->seekToFirst();

    int count = 0;
    while (it->valid()) {
        EXPECT_EQ(it->key(), std::format("k:{:04d}", count));
        EXPECT_EQ(it->value(), std::format("v:{:04d}", count));
        ++count;
        it->next();
    }
    EXPECT_EQ(count, N);

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, IteratorSeek) {
    auto path = self->sstPath("seek.sst");
    auto writer = co_await SSTableWriter::create(path, 512);
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    auto it = reader->newIterator();

    // Seek to exact key
    it->seek("key:0050");
    EXPECT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key:0050");

    // Seek between keys
    it->seek("key:0050a");
    EXPECT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key:0051");

    // Seek before first
    it->seek("aaa");
    EXPECT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "key:0000");

    // Seek past last
    it->seek("zzz");
    EXPECT_FALSE(it->valid());

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, IteratorSeekAndIterate) {
    auto path = self->sstPath("seekiter.sst");
    auto writer = co_await SSTableWriter::create(path, 512);
    for (int i = 0; i < 100; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    auto it = reader->newIterator();

    it->seek("key:0095");
    int count = 0;
    while (it->valid()) {
        ++count;
        it->next();
    }
    EXPECT_EQ(count, 5);  // keys 95-99

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, BinaryKeysAndValues) {
    auto path = self->sstPath("binary.sst");
    auto writer = co_await SSTableWriter::create(path);

    // Keys with binary data (like the index key format)
    std::string key1 = std::string("\x05", 1) + std::string(16, '\x01');
    std::string key2 = std::string("\x05", 1) + std::string(16, '\x02');
    std::string val1 = std::string("measurement\0field\0tag", 21);
    std::string val2 = std::string("data\0with\0nulls", 15);

    writer.add(key1, val1);
    writer.add(key2, val2);
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    auto v1 = reader->get(key1);
    EXPECT_TRUE(v1.has_value());
    EXPECT_EQ(*v1, val1);

    auto v2 = reader->get(key2);
    EXPECT_TRUE(v2.has_value());
    EXPECT_EQ(*v2, val2);

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, EmptyValues) {
    auto path = self->sstPath("empty_vals.sst");
    auto writer = co_await SSTableWriter::create(path);
    writer.add("key1", "");
    writer.add("key2", "");
    writer.add("key3", "value3");
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    auto v1 = reader->get("key1");
    EXPECT_TRUE(v1.has_value());
    EXPECT_EQ(*v1, "");

    auto v3 = reader->get("key3");
    EXPECT_TRUE(v3.has_value());
    EXPECT_EQ(*v3, "value3");

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, BloomFilterRejects) {
    auto path = self->sstPath("bloom.sst");
    auto writer = co_await SSTableWriter::create(path, 16384, 15);
    for (int i = 0; i < 1000; ++i) {
        writer.add(std::format("exist:{:04d}", i), "v");
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);

    // Non-existent keys should mostly be rejected by bloom filter
    // (no disk read needed). We can't directly observe this, but we can
    // verify correctness.
    for (int i = 0; i < 100; ++i) {
        auto val = reader->get(std::format("noexist:{:04d}", i));
        EXPECT_FALSE(val.has_value());
    }

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, PrefixScan) {
    auto path = self->sstPath("prefix.sst");
    auto writer = co_await SSTableWriter::create(path, 512);

    // Write entries simulating index key patterns
    for (int i = 0; i < 10; ++i) {
        writer.add(std::format("\x05meas1:{:02d}", i), "data");
    }
    for (int i = 0; i < 5; ++i) {
        writer.add(std::format("\x05meas2:{:02d}", i), "data");
    }
    for (int i = 0; i < 3; ++i) {
        writer.add(std::format("\x06tag:{:02d}", i), "data");
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    auto it = reader->newIterator();

    // Scan prefix \x05meas1
    std::string prefix = "\x05meas1";
    it->seek(prefix);
    int count = 0;
    while (it->valid() && it->key().substr(0, prefix.size()) == prefix) {
        ++count;
        it->next();
    }
    EXPECT_EQ(count, 10);

    co_await reader->close();
}

// ============================================================================
// SSTable metadata v2 tests — write timestamp in extended footer
// ============================================================================

SEASTAR_TEST_F(SSTableTest, WriteTimestampRecorded) {
    auto path = self->sstPath("ts_recorded.sst");
    auto writer = co_await SSTableWriter::create(path);
    writer.add("key1", "val1");
    writer.add("key2", "val2");
    auto meta = co_await writer.finish();

    EXPECT_GT(meta.writeTimestamp, 0u);

    auto reader = co_await SSTableReader::open(path);
    EXPECT_GT(reader->metadata().writeTimestamp, 0u);
    EXPECT_EQ(reader->metadata().writeTimestamp, meta.writeTimestamp);

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, WriteTimestampReasonable) {
    auto beforeNs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    auto path = self->sstPath("ts_reasonable.sst");
    auto writer = co_await SSTableWriter::create(path);
    writer.add("key1", "val1");
    auto meta = co_await writer.finish();

    auto afterNs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    // writeTimestamp should be between before and after (within 10 seconds for safety)
    EXPECT_GE(meta.writeTimestamp, beforeNs);
    EXPECT_LE(meta.writeTimestamp, afterNs);

    // Verify through reader as well
    auto reader = co_await SSTableReader::open(path);
    EXPECT_GE(reader->metadata().writeTimestamp, beforeNs);
    EXPECT_LE(reader->metadata().writeTimestamp, afterNs);

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, ExtendedFooterRoundTrip) {
    auto path = self->sstPath("footer_roundtrip.sst");
    auto writer = co_await SSTableWriter::create(path, 512);
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    auto meta = co_await writer.finish();

    EXPECT_EQ(meta.entryCount, static_cast<uint64_t>(N));
    EXPECT_EQ(meta.minKey, "key:0000");
    EXPECT_EQ(meta.maxKey, std::format("key:{:04d}", N - 1));
    EXPECT_GT(meta.writeTimestamp, 0u);

    // Re-open and verify all metadata fields survived the round trip
    auto reader = co_await SSTableReader::open(path);
    const auto& rmeta = reader->metadata();
    EXPECT_EQ(rmeta.entryCount, static_cast<uint64_t>(N));
    EXPECT_EQ(rmeta.writeTimestamp, meta.writeTimestamp);
    EXPECT_EQ(rmeta.minKey, "key:0000");

    // Verify data is still readable
    auto val = reader->get("key:0050");
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(*val, "val:0050");

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, EntryCountCorrectInMetadata) {
    auto path = self->sstPath("entry_count.sst");
    auto writer = co_await SSTableWriter::create(path);

    const int N = 42;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("k:{:03d}", i), "v");
    }
    auto meta = co_await writer.finish();

    EXPECT_EQ(meta.entryCount, static_cast<uint64_t>(N));

    auto reader = co_await SSTableReader::open(path);
    EXPECT_EQ(reader->metadata().entryCount, static_cast<uint64_t>(N));

    // Verify by iterating
    auto it = reader->newIterator();
    it->seekToFirst();
    int count = 0;
    while (it->valid()) {
        ++count;
        it->next();
    }
    EXPECT_EQ(count, N);

    co_await reader->close();
}

// ============================================================================
// CRC32 checksum tests
// ============================================================================

SEASTAR_TEST_F(SSTableTest, CRC32ChecksumRoundTrip) {
    auto path = self->sstPath("crc_roundtrip.sst");
    auto writer = co_await SSTableWriter::create(path, 512);  // Small blocks to force multiple
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    auto meta = co_await writer.finish();

    EXPECT_EQ(meta.entryCount, static_cast<uint64_t>(N));

    // Open and read back — CRC32 checksums are validated implicitly on every block read
    auto reader = co_await SSTableReader::open(path);

    // Point lookups (each triggers block decompression with CRC validation)
    for (int i = 0; i < N; ++i) {
        auto val = reader->get(std::format("key:{:04d}", i));
        EXPECT_TRUE(val.has_value()) << "Missing key:" << i;
        if (val.has_value()) {
            EXPECT_EQ(*val, std::format("val:{:04d}", i));
        }
    }

    // Full iteration (validates CRC on every block boundary)
    auto it = reader->newIterator();
    it->seekToFirst();
    int count = 0;
    while (it->valid()) {
        EXPECT_EQ(it->key(), std::format("key:{:04d}", count));
        EXPECT_EQ(it->value(), std::format("val:{:04d}", count));
        ++count;
        it->next();
    }
    EXPECT_EQ(count, N);

    co_await reader->close();
}

// ============================================================================
// Two-level summary index tests
// ============================================================================

SEASTAR_TEST_F(SSTableTest, SummaryIndexLargeFile) {
    // Write 10,000 entries with small blocks (128 bytes) to force many blocks,
    // triggering summary index construction. Verify all entries found via get().
    auto path = self->sstPath("summary_large.sst");
    auto writer = co_await SSTableWriter::create(path, 128);  // Very small blocks
    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("key:{:06d}", i), std::format("val:{:06d}", i));
    }
    auto meta = co_await writer.finish();
    EXPECT_EQ(meta.entryCount, static_cast<uint64_t>(N));

    auto reader = co_await SSTableReader::open(path);

    // The file should have many blocks — enough to trigger summary construction
    EXPECT_GT(reader->blockCount(), 128u) << "Expected enough blocks for summary index";

    // Verify every entry is findable via point lookup (uses summary-accelerated findBlock)
    for (int i = 0; i < N; ++i) {
        auto val = reader->get(std::format("key:{:06d}", i));
        EXPECT_TRUE(val.has_value()) << "Missing key:" << i;
        if (val.has_value()) {
            EXPECT_EQ(*val, std::format("val:{:06d}", i));
        }
    }

    // Verify non-existent keys return nullopt
    EXPECT_FALSE(reader->get("key:999999").has_value());
    EXPECT_FALSE(reader->get("aaa").has_value());
    EXPECT_FALSE(reader->get("zzz").has_value());

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, SummaryIndexCorrectness) {
    // Write entries with known keys, verify findBlock returns correct block
    // for various key patterns: before first, after last, exact match, between blocks.
    auto path = self->sstPath("summary_correct.sst");
    auto writer = co_await SSTableWriter::create(path, 128);  // Small blocks
    const int N = 5000;
    for (int i = 0; i < N; ++i) {
        // Use wide spacing in key values to test "between" lookups
        writer.add(std::format("k:{:08d}", i * 10), std::format("v:{:08d}", i * 10));
    }
    auto meta = co_await writer.finish();
    EXPECT_EQ(meta.entryCount, static_cast<uint64_t>(N));

    auto reader = co_await SSTableReader::open(path);
    EXPECT_GT(reader->blockCount(), 128u) << "Expected enough blocks for summary index";

    // Test 1: Exact match lookups at various positions
    for (int i = 0; i < N; i += 100) {
        auto val = reader->get(std::format("k:{:08d}", i * 10));
        EXPECT_TRUE(val.has_value()) << "Missing key at i=" << i;
        if (val.has_value()) {
            EXPECT_EQ(*val, std::format("v:{:08d}", i * 10));
        }
    }

    // Test 2: Keys between existing entries should return nullopt
    // (key:00000005 doesn't exist — only multiples of 10)
    EXPECT_FALSE(reader->get("k:00000005").has_value());
    EXPECT_FALSE(reader->get("k:00000015").has_value());
    EXPECT_FALSE(reader->get("k:00025005").has_value());

    // Test 3: Key before first entry
    EXPECT_FALSE(reader->get("a:00000000").has_value());

    // Test 4: Key after last entry
    EXPECT_FALSE(reader->get("z:99999999").has_value());

    // Test 5: Iterator seek still works with summary index
    auto it = reader->newIterator();
    it->seek("k:00005000");
    EXPECT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "k:00005000");

    // Seek between keys — should land on next key
    it->seek("k:00005001");
    EXPECT_TRUE(it->valid());
    EXPECT_EQ(it->key(), "k:00005010");

    // Test 6: Full iteration correctness with summary index
    it->seekToFirst();
    int count = 0;
    while (it->valid()) {
        EXPECT_EQ(it->key(), std::format("k:{:08d}", count * 10));
        ++count;
        it->next();
    }
    EXPECT_EQ(count, N);

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, SummaryIndexNotBuiltForSmallFiles) {
    // Verify that summary is not built for small files (< SUMMARY_INTERVAL * 2 blocks)
    auto path = self->sstPath("summary_small.sst");
    auto writer = co_await SSTableWriter::create(path, 16384);  // Large blocks = fewer blocks
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);

    // With large block size and only 100 entries, we should have very few blocks
    // Summary should NOT be built (fewer than SUMMARY_INTERVAL * 2 blocks)
    // But lookups should still work correctly
    for (int i = 0; i < N; ++i) {
        auto val = reader->get(std::format("key:{:04d}", i));
        EXPECT_TRUE(val.has_value()) << "Missing key:" << i;
        if (val.has_value()) {
            EXPECT_EQ(*val, std::format("val:{:04d}", i));
        }
    }

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, SummaryIndexContainsCheck) {
    // Verify contains() works correctly with summary index
    auto path = self->sstPath("summary_contains.sst");
    auto writer = co_await SSTableWriter::create(path, 128);
    const int N = 8000;
    for (int i = 0; i < N; ++i) {
        writer.add(std::format("c:{:06d}", i), "x");
    }
    co_await writer.finish();

    auto reader = co_await SSTableReader::open(path);
    EXPECT_GT(reader->blockCount(), 128u);

    // contains() uses findBlock() internally, so this tests summary acceleration
    for (int i = 0; i < N; i += 200) {
        EXPECT_TRUE(reader->contains(std::format("c:{:06d}", i))) << "Missing at i=" << i;
    }
    EXPECT_FALSE(reader->contains("c:999999"));
    EXPECT_FALSE(reader->contains("aaa"));

    co_await reader->close();
}

SEASTAR_TEST_F(SSTableTest, CRC32CorruptionDetected) {
    auto path = self->sstPath("crc_corrupt.sst");
    auto writer = co_await SSTableWriter::create(path, 512);
    for (int i = 0; i < 50; ++i) {
        writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
    }
    co_await writer.finish();

    // Corrupt one byte in the middle of the first data block.
    // Data blocks start at offset 0, so corrupting byte 10 is safely inside
    // the compressed data region (past the 4-byte size prefix).
    {
        int fd = ::open(path.c_str(), O_RDWR);
        EXPECT_GE(fd, 0) << "Failed to open SSTable for corruption";
        if (fd < 0) co_return;
        char bad = 0xFF;
        ssize_t wr = ::pwrite(fd, &bad, 1, 10);  // offset 10: inside compressed data
        EXPECT_EQ(wr, 1);
        ::close(fd);
    }

    // Re-open and attempt to read — should detect CRC mismatch
    auto reader = co_await SSTableReader::open(path);
    EXPECT_THROW(reader->get("key:0000"), std::runtime_error);

    co_await reader->close();
}

// ============================================================================
// Bug fix tests: abort() cleanup and truncated file detection
// ============================================================================

SEASTAR_TEST_F(SSTableTest, AbortDeletesPartialFile) {
    auto path = self->sstPath("abort_cleanup.sst");

    // Create a writer, add some data, then abort without finishing
    auto writer = co_await SSTableWriter::create(path);
    writer.add("key1", "val1");
    writer.add("key2", "val2");

    // The file should exist after create()
    EXPECT_TRUE(std::filesystem::exists(path));

    co_await writer.abort();

    // After abort(), the partial file must be removed
    EXPECT_FALSE(std::filesystem::exists(path));
}

SEASTAR_TEST_F(SSTableTest, AbortEmptyWriterDeletesFile) {
    auto path = self->sstPath("abort_empty.sst");

    // Create a writer but add nothing — abort immediately
    auto writer = co_await SSTableWriter::create(path);
    EXPECT_TRUE(std::filesystem::exists(path));

    co_await writer.abort();

    EXPECT_FALSE(std::filesystem::exists(path));
}

SEASTAR_TEST_F(SSTableTest, TruncatedFileThrowsOnOpen) {
    auto path = self->sstPath("truncated.sst");

    // Write a valid SSTable first to get a realistic file
    {
        auto writer = co_await SSTableWriter::create(path);
        for (int i = 0; i < 50; ++i) {
            writer.add(std::format("key:{:04d}", i), std::format("val:{:04d}", i));
        }
        co_await writer.finish();
    }

    // Truncate the file to just a few bytes (less than the 64-byte footer)
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_TRUNC);
        EXPECT_GE(fd, 0) << "Failed to open file for truncation";
        if (fd < 0) co_return;
        // Write 10 garbage bytes — too small for a valid SSTable
        const char garbage[] = "TRUNCATED!";
        [[maybe_unused]] auto n = ::write(fd, garbage, 10);
        ::close(fd);
    }

    // Opening a truncated file must throw (file too small for footer)
    EXPECT_THROW(co_await SSTableReader::open(path), std::runtime_error);
}

SEASTAR_TEST_F(SSTableTest, TruncatedMetadataThrowsOnOpen) {
    // Craft a file with a valid footer that references an index region
    // containing data that claims more entries than actually fit.
    // This exercises the index parser's bounds checks.
    auto path = self->sstPath("truncated_meta.sst");

    auto encodeFixed64 = [](char* buf, uint64_t v) {
        for (int i = 0; i < 8; ++i)
            buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    };
    auto encodeFixed32 = [](char* buf, uint32_t v) {
        buf[0] = static_cast<char>(v & 0xff);
        buf[1] = static_cast<char>((v >> 8) & 0xff);
        buf[2] = static_cast<char>((v >> 16) & 0xff);
        buf[3] = static_cast<char>((v >> 24) & 0xff);
    };

    // Build a 16-byte "index" region: numEntries=5, then only 12 bytes of
    // data (not enough for even one full entry: 4+key+12 minimum).
    char indexData[16];
    std::memset(indexData, 0, 16);
    encodeFixed32(indexData, 5);  // Claim 5 entries, but only 12 bytes follow

    // Build the footer referencing this index region
    char footer[64];
    std::memset(footer, 0, 64);
    encodeFixed64(footer + 0, 0);             // bloomOffset (no bloom)
    encodeFixed64(footer + 8, 0);             // bloomSize = 0
    encodeFixed64(footer + 16, 0);            // indexOffset = 0 (start of file)
    encodeFixed64(footer + 24, 16);           // indexSize = 16
    encodeFixed64(footer + 32, 5);            // entryCount
    encodeFixed64(footer + 40, 0);            // writeTimestamp
    encodeFixed32(footer + 56, 0x54534958);   // SSTABLE_MAGIC
    encodeFixed32(footer + 60, 1);            // SSTABLE_VERSION

    // Write: 16 bytes index data + 64 bytes footer = 80 bytes total
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        EXPECT_GE(fd, 0);
        if (fd < 0) co_return;
        [[maybe_unused]] auto n1 = ::write(fd, indexData, 16);
        [[maybe_unused]] auto n2 = ::write(fd, footer, 64);
        ::close(fd);
    }

    // The index parser will see numEntries=5 but run out of data after
    // the first partial entry, throwing "SSTable index entry truncated"
    EXPECT_THROW(co_await SSTableReader::open(path), std::runtime_error);
}
