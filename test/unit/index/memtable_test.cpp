#include "../../../lib/index/native/memtable.hpp"

#include <gtest/gtest.h>

#include <format>
#include <string>

using namespace timestar::index;

TEST(MemTableTest, EmptyTable) {
    MemTable mt;
    EXPECT_TRUE(mt.empty());
    EXPECT_EQ(mt.size(), 0u);
    EXPECT_EQ(mt.get("key"), std::nullopt);
    EXPECT_FALSE(mt.contains("key"));
}

TEST(MemTableTest, PutAndGet) {
    MemTable mt;
    mt.put("hello", "world");
    EXPECT_FALSE(mt.empty());
    EXPECT_EQ(mt.size(), 1u);

    auto val = mt.get("hello");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "world");
}

TEST(MemTableTest, PutOverwrite) {
    MemTable mt;
    mt.put("key", "value1");
    mt.put("key", "value2");
    EXPECT_EQ(mt.size(), 1u);

    auto val = mt.get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "value2");
}

TEST(MemTableTest, DeleteKey) {
    MemTable mt;
    mt.put("key", "value");
    mt.remove("key");

    EXPECT_EQ(mt.get("key"), std::nullopt);
    EXPECT_TRUE(mt.contains("key"));
    EXPECT_TRUE(mt.isTombstone("key"));
}

TEST(MemTableTest, DeleteNonExistent) {
    MemTable mt;
    mt.remove("key");
    EXPECT_TRUE(mt.contains("key"));
    EXPECT_TRUE(mt.isTombstone("key"));
    EXPECT_EQ(mt.get("key"), std::nullopt);
}

TEST(MemTableTest, PutAfterDelete) {
    MemTable mt;
    mt.put("key", "v1");
    mt.remove("key");
    mt.put("key", "v2");

    auto val = mt.get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "v2");
    EXPECT_FALSE(mt.isTombstone("key"));
}

TEST(MemTableTest, EmptyValue) {
    MemTable mt;
    mt.put("key", "");
    auto val = mt.get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "");
    EXPECT_FALSE(mt.isTombstone("key"));
}

TEST(MemTableTest, BinaryKeys) {
    MemTable mt;
    std::string key1 = std::string("\x05", 1) + std::string(16, '\x01');
    std::string key2 = std::string("\x05", 1) + std::string(16, '\x02');
    mt.put(key1, "val1");
    mt.put(key2, "val2");

    EXPECT_EQ(*mt.get(key1), "val1");
    EXPECT_EQ(*mt.get(key2), "val2");
}

TEST(MemTableTest, MemoryUsageGrows) {
    MemTable mt;
    size_t initial = mt.approximateMemoryUsage();
    EXPECT_EQ(initial, 0u);

    mt.put("key1", "value1");
    EXPECT_GT(mt.approximateMemoryUsage(), initial);

    size_t after1 = mt.approximateMemoryUsage();
    mt.put("key2", "value2");
    EXPECT_GT(mt.approximateMemoryUsage(), after1);
}

// ============================================================================
// Iterator tests
// ============================================================================

TEST(MemTableIteratorTest, EmptyTable) {
    MemTable mt;
    auto it = mt.newIterator();
    it.seekToFirst();
    EXPECT_FALSE(it.valid());
}

TEST(MemTableIteratorTest, SeekToFirst) {
    MemTable mt;
    mt.put("cherry", "3");
    mt.put("apple", "1");
    mt.put("banana", "2");

    auto it = mt.newIterator();
    it.seekToFirst();
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), "apple");
    EXPECT_EQ(it.value(), "1");
}

TEST(MemTableIteratorTest, FullIteration) {
    MemTable mt;
    for (int i = 0; i < 50; ++i) {
        mt.put(std::format("key:{:03d}", i), std::format("val:{:03d}", i));
    }

    auto it = mt.newIterator();
    it.seekToFirst();
    int count = 0;
    std::string prev;
    while (it.valid()) {
        EXPECT_GT(std::string(it.key()), prev) << "Keys not sorted at index " << count;
        prev = std::string(it.key());
        ++count;
        it.next();
    }
    EXPECT_EQ(count, 50);
}

TEST(MemTableIteratorTest, SeekExact) {
    MemTable mt;
    mt.put("aaa", "1");
    mt.put("bbb", "2");
    mt.put("ccc", "3");

    auto it = mt.newIterator();
    it.seek("bbb");
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), "bbb");
    EXPECT_EQ(it.value(), "2");
}

TEST(MemTableIteratorTest, SeekBetween) {
    MemTable mt;
    mt.put("aaa", "1");
    mt.put("ccc", "3");

    auto it = mt.newIterator();
    it.seek("bbb");
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), "ccc");
}

TEST(MemTableIteratorTest, SeekPastEnd) {
    MemTable mt;
    mt.put("aaa", "1");

    auto it = mt.newIterator();
    it.seek("zzz");
    EXPECT_FALSE(it.valid());
}

TEST(MemTableIteratorTest, IterateWithTombstones) {
    MemTable mt;
    mt.put("aaa", "1");
    mt.put("bbb", "2");
    mt.remove("bbb");
    mt.put("ccc", "3");

    auto it = mt.newIterator();
    it.seekToFirst();

    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), "aaa");
    EXPECT_FALSE(it.isTombstone());
    it.next();

    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), "bbb");
    EXPECT_TRUE(it.isTombstone());
    it.next();

    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), "ccc");
    EXPECT_FALSE(it.isTombstone());
}

TEST(MemTableIteratorTest, PrefixScan) {
    MemTable mt;
    // Build keys with embedded null bytes using std::string constructor
    std::string k1 = std::string("\x05m1", 3) + std::string("\x00""f1", 3);
    std::string k2 = std::string("\x05m1", 3) + std::string("\x00""f2", 3);
    std::string k3 = std::string("\x05m2", 3) + std::string("\x00""f1", 3);
    std::string k4 = std::string("\x06tag", 4);
    mt.put(k1, "d1");
    mt.put(k2, "d2");
    mt.put(k3, "d3");
    mt.put(k4, "s1");

    std::string prefix = std::string("\x05m1", 3);
    auto it = mt.newIterator();
    it.seek(prefix);

    int count = 0;
    while (it.valid() && it.key().substr(0, prefix.size()) == prefix) {
        ++count;
        it.next();
    }
    EXPECT_EQ(count, 2);
}
