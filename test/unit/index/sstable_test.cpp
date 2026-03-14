#include "../../../lib/index/native/sstable.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

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
