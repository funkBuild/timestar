#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/storage/memory_store.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

// Tests documenting MemoryStore behavior relevant to WAL recovery durability.
// These are synchronous unit tests that verify properties of MemoryStore
// without requiring the Seastar event loop.

class WALRecoveryDurabilityTest : public ::testing::Test {
protected:
    std::shared_ptr<MemoryStore> store;

    void SetUp() override { store = std::make_shared<MemoryStore>(42); }
};

// A freshly constructed MemoryStore has no WAL.
// This documents the baseline state before initWAL() or initFromWAL() is called.
TEST_F(WALRecoveryDurabilityTest, FreshMemoryStoreHasNoWAL) {
    EXPECT_EQ(store->getWAL(), nullptr);
}

// A freshly constructed MemoryStore is empty.
TEST_F(WALRecoveryDurabilityTest, FreshMemoryStoreIsEmpty) {
    EXPECT_TRUE(store->isEmpty());
}

// insertMemory() works without a WAL being initialized.
// This is the code path used during WAL recovery: data is loaded into memory
// via initFromWAL -> WALReader -> insertMemory, but no new WAL is created.
TEST_F(WALRecoveryDurabilityTest, InsertMemoryWorksWithoutWAL) {
    EXPECT_EQ(store->getWAL(), nullptr);

    TimeStarInsert<double> insert("cpu", "usage");
    insert.addValue(1000, 50.0);
    insert.addValue(2000, 60.0);
    insert.addValue(3000, 70.0);

    // insertMemory does not require a WAL
    store->insertMemory(std::move(insert));

    EXPECT_FALSE(store->isEmpty());
    EXPECT_EQ(store->getWAL(), nullptr);  // WAL still not initialized

    // Verify data is accessible
    SeriesId128 seriesId = insert.seriesId128();
    auto result = store->querySeries<double>(seriesId);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->values.size(), 3);
    EXPECT_DOUBLE_EQ(result->values[0], 50.0);
    EXPECT_DOUBLE_EQ(result->values[2], 70.0);
}

// After insertMemory without WAL, the store correctly reports non-empty state
// and the series type is correct.
TEST_F(WALRecoveryDurabilityTest, RecoveredStoreHasCorrectSeriesType) {
    EXPECT_EQ(store->getWAL(), nullptr);

    TimeStarInsert<double> floatInsert("temperature", "value");
    floatInsert.addValue(1000, 23.5);
    store->insertMemory(std::move(floatInsert));

    TimeStarInsert<bool> boolInsert("door", "open");
    boolInsert.addValue(1000, true);
    store->insertMemory(std::move(boolInsert));

    TimeStarInsert<std::string> stringInsert("logs", "message");
    stringInsert.addValue(1000, "hello world");
    store->insertMemory(std::move(stringInsert));

    // All three series should exist with correct types, despite no WAL
    EXPECT_EQ(store->getSeriesType(floatInsert.seriesId128()).value(), TSMValueType::Float);
    EXPECT_EQ(store->getSeriesType(boolInsert.seriesId128()).value(), TSMValueType::Boolean);
    EXPECT_EQ(store->getSeriesType(stringInsert.seriesId128()).value(), TSMValueType::String);
}

// Document that a WAL-less store's isEmpty() returns true initially,
// meaning the recovery code path correctly checks store emptiness
// before attempting TSM conversion.
TEST_F(WALRecoveryDurabilityTest, EmptyStoreIsDetectedBeforeConversion) {
    // Before any data is loaded, isEmpty() should return true.
    // In the recovery flow, this determines whether convertWalToTsm is called.
    EXPECT_TRUE(store->isEmpty());
    EXPECT_EQ(store->getWAL(), nullptr);

    // After inserting data (simulating WAL replay), isEmpty is false
    TimeStarInsert<double> insert("metric", "value");
    insert.addValue(1000, 1.0);
    store->insertMemory(std::move(insert));

    EXPECT_FALSE(store->isEmpty());
}

// Verify that multiple inserts accumulate correctly in a WAL-less store,
// simulating a WAL recovery that replays multiple WAL entries.
TEST_F(WALRecoveryDurabilityTest, MultipleInsertsAccumulateWithoutWAL) {
    EXPECT_EQ(store->getWAL(), nullptr);

    // Simulate replaying multiple WAL entries
    for (int i = 0; i < 100; i++) {
        TimeStarInsert<double> insert("sensor", "reading");
        insert.addValue(static_cast<uint64_t>(i) * 1000, static_cast<double>(i));
        store->insertMemory(std::move(insert));
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("sensor reading");
    auto result = store->querySeries<double>(seriesId);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->values.size(), 100);
    EXPECT_EQ(result->timestamps.size(), 100);

    // WAL should still be null
    EXPECT_EQ(store->getWAL(), nullptr);
}
