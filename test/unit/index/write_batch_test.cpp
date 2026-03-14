#include "../../../lib/index/native/memtable.hpp"
#include "../../../lib/index/native/write_batch.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace timestar::index;

TEST(WriteBatchTest, EmptyBatch) {
    IndexWriteBatch batch;
    EXPECT_TRUE(batch.empty());
    EXPECT_EQ(batch.count(), 0u);
}

TEST(WriteBatchTest, PutOperations) {
    IndexWriteBatch batch;
    batch.put("key1", "val1");
    batch.put("key2", "val2");
    EXPECT_EQ(batch.count(), 2u);
    EXPECT_FALSE(batch.empty());
    EXPECT_GT(batch.approximateSize(), 0u);
}

TEST(WriteBatchTest, DeleteOperations) {
    IndexWriteBatch batch;
    batch.remove("key1");
    EXPECT_EQ(batch.count(), 1u);
    EXPECT_EQ(batch.ops()[0].type, IndexWriteBatch::OpType::Delete);
}

TEST(WriteBatchTest, Clear) {
    IndexWriteBatch batch;
    batch.put("key1", "val1");
    batch.put("key2", "val2");
    batch.clear();
    EXPECT_TRUE(batch.empty());
    EXPECT_EQ(batch.count(), 0u);
    EXPECT_EQ(batch.approximateSize(), 0u);
}

TEST(WriteBatchTest, ApplyToMemTable) {
    MemTable mt;
    mt.put("existing", "old_value");

    IndexWriteBatch batch;
    batch.put("key1", "val1");
    batch.put("key2", "val2");
    batch.put("existing", "new_value");
    batch.remove("key_to_delete");
    batch.applyTo(mt);

    EXPECT_EQ(*mt.get("key1"), "val1");
    EXPECT_EQ(*mt.get("key2"), "val2");
    EXPECT_EQ(*mt.get("existing"), "new_value");
    EXPECT_TRUE(mt.isTombstone("key_to_delete"));
}

TEST(WriteBatchTest, AtomicBatchApply) {
    // Simulates the index pattern: multiple related keys in one batch
    MemTable mt;
    IndexWriteBatch batch;

    // Series metadata write (like getOrCreateSeriesId)
    batch.put("\x05" "series_id_bytes", "measurement\0field\0tags");
    batch.put("\x0A" "weather\0series_id", "");
    batch.put("\x0C" "weather\0temp\0series_id", "");
    batch.put("\x06" "weather\0loc\0us-west\0series_id", "series_id");
    batch.put("\x02" "weather", "encoded_fields");
    batch.put("\x03" "weather", "encoded_tags");

    batch.applyTo(mt);
    EXPECT_EQ(mt.size(), 6u);

    // All keys should be present
    EXPECT_TRUE(mt.contains("\x05" "series_id_bytes"));
    EXPECT_TRUE(mt.contains("\x0A" "weather\0series_id"));
    EXPECT_TRUE(mt.contains("\x02" "weather"));
}

// ============================================================================
// Serialization tests
// ============================================================================

TEST(WriteBatchSerializationTest, RoundtripPutOnly) {
    IndexWriteBatch original;
    original.put("key1", "value1");
    original.put("key2", "value2");

    std::string serialized;
    original.serializeTo(serialized);
    EXPECT_GT(serialized.size(), 0u);

    auto restored = IndexWriteBatch::deserializeFrom(serialized);
    EXPECT_EQ(restored.count(), 2u);

    // Apply both to separate MemTables and compare
    MemTable mt1, mt2;
    original.applyTo(mt1);
    restored.applyTo(mt2);

    EXPECT_EQ(*mt1.get("key1"), *mt2.get("key1"));
    EXPECT_EQ(*mt1.get("key2"), *mt2.get("key2"));
}

TEST(WriteBatchSerializationTest, RoundtripMixed) {
    IndexWriteBatch original;
    original.put("key1", "value1");
    original.remove("key2");
    original.put("key3", "value3");

    std::string serialized;
    original.serializeTo(serialized);

    auto restored = IndexWriteBatch::deserializeFrom(serialized);
    EXPECT_EQ(restored.count(), 3u);

    MemTable mt;
    restored.applyTo(mt);
    EXPECT_EQ(*mt.get("key1"), "value1");
    EXPECT_TRUE(mt.isTombstone("key2"));
    EXPECT_EQ(*mt.get("key3"), "value3");
}

TEST(WriteBatchSerializationTest, RoundtripBinaryData) {
    IndexWriteBatch original;
    std::string binKey = std::string("\x05", 1) + std::string(16, '\x00');
    std::string binVal = "measurement\0field\0tagcount";
    original.put(binKey, binVal);

    std::string serialized;
    original.serializeTo(serialized);

    auto restored = IndexWriteBatch::deserializeFrom(serialized);
    EXPECT_EQ(restored.count(), 1u);

    MemTable mt;
    restored.applyTo(mt);
    EXPECT_EQ(*mt.get(binKey), binVal);
}

TEST(WriteBatchSerializationTest, RoundtripEmpty) {
    IndexWriteBatch original;
    std::string serialized;
    original.serializeTo(serialized);

    auto restored = IndexWriteBatch::deserializeFrom(serialized);
    EXPECT_EQ(restored.count(), 0u);
}

TEST(WriteBatchSerializationTest, DeserializeTruncated) {
    EXPECT_THROW(IndexWriteBatch::deserializeFrom("x"), std::runtime_error);
}

TEST(WriteBatchSerializationTest, DeserializeCorrupt) {
    // Valid header saying 100 ops but no data
    std::string bad;
    bad.push_back(100);
    bad.push_back(0);
    bad.push_back(0);
    bad.push_back(0);
    EXPECT_THROW(IndexWriteBatch::deserializeFrom(bad), std::runtime_error);
}
