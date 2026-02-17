#include <gtest/gtest.h>
#include <algorithm>
#include <numeric>
#include <random>
#include <vector>

#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

// Tests for InMemorySeries::insert maintaining sorted order.
// These tests verify that timestamps and values remain correctly paired
// and sorted after various insert patterns.

class MemoryStoreSortedTest : public ::testing::Test {
protected:
    // Helper: verify that timestamps are sorted and values are correctly paired.
    // Expects that value == timestamp * factor (for double series).
    template <typename T>
    void verifySorted(const InMemorySeries<T>& series) {
        ASSERT_EQ(series.timestamps.size(), series.values.size());
        for (size_t i = 1; i < series.timestamps.size(); ++i) {
            EXPECT_LE(series.timestamps[i - 1], series.timestamps[i])
                << "Timestamps not sorted at index " << i
                << ": " << series.timestamps[i - 1] << " > " << series.timestamps[i];
        }
    }

    // Helper: verify that each value equals its timestamp cast to double
    // (used when we set value = static_cast<double>(timestamp))
    void verifyPairing(const InMemorySeries<double>& series) {
        for (size_t i = 0; i < series.timestamps.size(); ++i) {
            EXPECT_DOUBLE_EQ(series.values[i], static_cast<double>(series.timestamps[i]))
                << "Value/timestamp mismatch at index " << i
                << ": timestamp=" << series.timestamps[i]
                << " value=" << series.values[i];
        }
    }
};

// --- Test: In-order inserts remain sorted ---
TEST_F(MemoryStoreSortedTest, InOrderInsertsRemainSorted) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert("test", "field");
    insert.addValue(1000, 1000.0);
    insert.addValue(2000, 2000.0);
    insert.addValue(3000, 3000.0);
    insert.addValue(4000, 4000.0);
    insert.addValue(5000, 5000.0);

    series.insert(std::move(insert));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 5);
}

// --- Test: Out-of-order single batch results in sorted data ---
TEST_F(MemoryStoreSortedTest, OutOfOrderSingleBatchGetsSorted) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert("test", "field");
    insert.addValue(5000, 5000.0);
    insert.addValue(1000, 1000.0);
    insert.addValue(3000, 3000.0);
    insert.addValue(2000, 2000.0);
    insert.addValue(4000, 4000.0);

    series.insert(std::move(insert));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 5);
}

// --- Test: Multiple sequential batches with in-order data ---
TEST_F(MemoryStoreSortedTest, MultipleInOrderBatches) {
    InMemorySeries<double> series;

    // First batch: 1000-3000
    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(1000, 1000.0);
    insert1.addValue(2000, 2000.0);
    insert1.addValue(3000, 3000.0);
    series.insert(std::move(insert1));

    // Second batch: 4000-6000 (after first batch)
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(4000, 4000.0);
    insert2.addValue(5000, 5000.0);
    insert2.addValue(6000, 6000.0);
    series.insert(std::move(insert2));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 6);
}

// --- Test: Second batch arrives before first batch's timestamps ---
TEST_F(MemoryStoreSortedTest, LateArrivingBatch) {
    InMemorySeries<double> series;

    // First batch: recent data 4000-6000
    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(4000, 4000.0);
    insert1.addValue(5000, 5000.0);
    insert1.addValue(6000, 6000.0);
    series.insert(std::move(insert1));

    // Second batch: late-arriving data 1000-3000
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(1000, 1000.0);
    insert2.addValue(2000, 2000.0);
    insert2.addValue(3000, 3000.0);
    series.insert(std::move(insert2));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 6);

    // Verify the order is correct
    EXPECT_EQ(series.timestamps[0], 1000);
    EXPECT_EQ(series.timestamps[5], 6000);
    EXPECT_DOUBLE_EQ(series.values[0], 1000.0);
    EXPECT_DOUBLE_EQ(series.values[5], 6000.0);
}

// --- Test: Interleaved timestamps from different batches ---
TEST_F(MemoryStoreSortedTest, InterleavedBatches) {
    InMemorySeries<double> series;

    // First batch: odd timestamps
    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(1000, 1000.0);
    insert1.addValue(3000, 3000.0);
    insert1.addValue(5000, 5000.0);
    series.insert(std::move(insert1));

    // Second batch: even timestamps (interleaves with first)
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(2000, 2000.0);
    insert2.addValue(4000, 4000.0);
    insert2.addValue(6000, 6000.0);
    series.insert(std::move(insert2));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 6);

    // Verify exact order
    for (size_t i = 0; i < 6; ++i) {
        EXPECT_EQ(series.timestamps[i], (i + 1) * 1000);
    }
}

// --- Test: Empty insert does nothing ---
TEST_F(MemoryStoreSortedTest, EmptyInsertNoOp) {
    InMemorySeries<double> series;

    // Insert some data first
    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(1000, 1000.0);
    insert1.addValue(2000, 2000.0);
    series.insert(std::move(insert1));

    // Empty insert
    TSDBInsert<double> emptyInsert("test", "field");
    series.insert(std::move(emptyInsert));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 2);
}

// --- Test: Empty insert into empty series ---
TEST_F(MemoryStoreSortedTest, EmptyInsertIntoEmptySeries) {
    InMemorySeries<double> series;

    TSDBInsert<double> emptyInsert("test", "field");
    series.insert(std::move(emptyInsert));

    EXPECT_EQ(series.timestamps.size(), 0);
    EXPECT_EQ(series.values.size(), 0);
}

// --- Test: Single-element insert ---
TEST_F(MemoryStoreSortedTest, SingleElementInsert) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert("test", "field");
    insert.addValue(42000, 42000.0);
    series.insert(std::move(insert));

    EXPECT_EQ(series.timestamps.size(), 1);
    EXPECT_EQ(series.timestamps[0], 42000);
    EXPECT_DOUBLE_EQ(series.values[0], 42000.0);
}

// --- Test: Single element inserted before existing data ---
TEST_F(MemoryStoreSortedTest, SingleElementBeforeExisting) {
    InMemorySeries<double> series;

    // Insert data at 5000
    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(5000, 5000.0);
    series.insert(std::move(insert1));

    // Insert single element before it
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(1000, 1000.0);
    series.insert(std::move(insert2));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 2);
    EXPECT_EQ(series.timestamps[0], 1000);
    EXPECT_EQ(series.timestamps[1], 5000);
}

// --- Test: Duplicate timestamps ---
TEST_F(MemoryStoreSortedTest, DuplicateTimestamps) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(1000, 1.0);
    insert1.addValue(2000, 2.0);
    insert1.addValue(3000, 3.0);
    series.insert(std::move(insert1));

    // Insert with duplicate timestamps
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(2000, 20.0);
    insert2.addValue(3000, 30.0);
    insert2.addValue(4000, 40.0);
    series.insert(std::move(insert2));

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 6);

    // All timestamps should be in non-decreasing order (duplicates allowed)
    for (size_t i = 1; i < series.timestamps.size(); ++i) {
        EXPECT_LE(series.timestamps[i - 1], series.timestamps[i]);
    }
}

// --- Test: Large random batch is correctly sorted ---
TEST_F(MemoryStoreSortedTest, LargeRandomBatch) {
    InMemorySeries<double> series;

    // Generate 1000 random timestamps
    std::vector<uint64_t> timestamps(1000);
    std::iota(timestamps.begin(), timestamps.end(), 1);  // 1 to 1000

    std::mt19937 rng(42);  // Deterministic seed
    std::shuffle(timestamps.begin(), timestamps.end(), rng);

    TSDBInsert<double> insert("test", "field");
    for (auto ts : timestamps) {
        insert.addValue(ts, static_cast<double>(ts));
    }
    series.insert(std::move(insert));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 1000);
}

// --- Test: Multiple random batches merge correctly ---
TEST_F(MemoryStoreSortedTest, MultipleRandomBatches) {
    InMemorySeries<double> series;

    std::mt19937 rng(12345);

    // Insert 5 batches of 200 random points each
    for (int batch = 0; batch < 5; ++batch) {
        std::vector<uint64_t> timestamps(200);
        for (int i = 0; i < 200; ++i) {
            timestamps[i] = rng() % 10000 + 1;
        }

        TSDBInsert<double> insert("test", "field");
        for (auto ts : timestamps) {
            insert.addValue(ts, static_cast<double>(ts));
        }
        series.insert(std::move(insert));
    }

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 1000);
}

// --- Test: Boolean values stay paired after sorting ---
TEST_F(MemoryStoreSortedTest, BoolValuesStayPaired) {
    InMemorySeries<bool> series;

    TSDBInsert<bool> insert("status", "online");
    insert.addValue(5000, false);
    insert.addValue(1000, true);
    insert.addValue(3000, true);
    insert.addValue(2000, false);
    insert.addValue(4000, true);

    series.insert(std::move(insert));

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 5);

    // Verify pairing: timestamp -> expected value
    // After sorting by timestamp: 1000->true, 2000->false, 3000->true, 4000->true, 5000->false
    EXPECT_EQ(series.timestamps[0], 1000);
    EXPECT_EQ(series.values[0], true);
    EXPECT_EQ(series.timestamps[1], 2000);
    EXPECT_EQ(series.values[1], false);
    EXPECT_EQ(series.timestamps[2], 3000);
    EXPECT_EQ(series.values[2], true);
    EXPECT_EQ(series.timestamps[3], 4000);
    EXPECT_EQ(series.values[3], true);
    EXPECT_EQ(series.timestamps[4], 5000);
    EXPECT_EQ(series.values[4], false);
}

// --- Test: String values stay paired after sorting ---
TEST_F(MemoryStoreSortedTest, StringValuesStayPaired) {
    InMemorySeries<std::string> series;

    TSDBInsert<std::string> insert("logs", "message");
    insert.addValue(3000, "third");
    insert.addValue(1000, "first");
    insert.addValue(4000, "fourth");
    insert.addValue(2000, "second");

    series.insert(std::move(insert));

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 4);

    EXPECT_EQ(series.timestamps[0], 1000);
    EXPECT_EQ(series.values[0], "first");
    EXPECT_EQ(series.timestamps[1], 2000);
    EXPECT_EQ(series.values[1], "second");
    EXPECT_EQ(series.timestamps[2], 3000);
    EXPECT_EQ(series.values[2], "third");
    EXPECT_EQ(series.timestamps[3], 4000);
    EXPECT_EQ(series.values[3], "fourth");
}

// --- Test: Multiple out-of-order string batches ---
TEST_F(MemoryStoreSortedTest, MultipleStringBatchesOutOfOrder) {
    InMemorySeries<std::string> series;

    // First batch: later timestamps
    TSDBInsert<std::string> insert1("logs", "message");
    insert1.addValue(5000, "five");
    insert1.addValue(6000, "six");
    series.insert(std::move(insert1));

    // Second batch: earlier timestamps
    TSDBInsert<std::string> insert2("logs", "message");
    insert2.addValue(1000, "one");
    insert2.addValue(2000, "two");
    series.insert(std::move(insert2));

    // Third batch: middle timestamps
    TSDBInsert<std::string> insert3("logs", "message");
    insert3.addValue(3000, "three");
    insert3.addValue(4000, "four");
    series.insert(std::move(insert3));

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 6);

    EXPECT_EQ(series.values[0], "one");
    EXPECT_EQ(series.values[1], "two");
    EXPECT_EQ(series.values[2], "three");
    EXPECT_EQ(series.values[3], "four");
    EXPECT_EQ(series.values[4], "five");
    EXPECT_EQ(series.values[5], "six");
}

// --- Test: insertMemory through MemoryStore maintains order ---
TEST_F(MemoryStoreSortedTest, InsertMemoryMaintainsOrder) {
    MemoryStore store(1);

    // Insert out-of-order data through MemoryStore
    TSDBInsert<double> insert1("temp", "value");
    insert1.addTag("loc", "west");
    insert1.addValue(5000, 5000.0);
    insert1.addValue(3000, 3000.0);
    insert1.addValue(1000, 1000.0);
    store.insertMemory(std::move(insert1));

    // Insert more out-of-order data
    TSDBInsert<double> insert2("temp", "value");
    insert2.addTag("loc", "west");
    insert2.addValue(4000, 4000.0);
    insert2.addValue(2000, 2000.0);
    insert2.addValue(6000, 6000.0);
    store.insertMemory(std::move(insert2));

    // Verify via querySeries
    SeriesId128 seriesId = insert1.seriesId128();
    auto result = store.querySeries<double>(seriesId);
    ASSERT_NE(result, nullptr);

    EXPECT_EQ(result->timestamps.size(), 6);
    for (size_t i = 1; i < result->timestamps.size(); ++i) {
        EXPECT_LE(result->timestamps[i - 1], result->timestamps[i])
            << "Timestamps not sorted at index " << i;
    }

    // Verify pairing
    for (size_t i = 0; i < result->timestamps.size(); ++i) {
        EXPECT_DOUBLE_EQ(result->values[i], static_cast<double>(result->timestamps[i]));
    }
}

// --- Test: Boundary case where new batch starts exactly at last timestamp ---
TEST_F(MemoryStoreSortedTest, NewBatchStartsAtExactBoundary) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(1000, 1000.0);
    insert1.addValue(2000, 2000.0);
    insert1.addValue(3000, 3000.0);
    series.insert(std::move(insert1));

    // New batch starts at exactly the last timestamp
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(3000, 30000.0);  // Duplicate timestamp, different value
    insert2.addValue(4000, 4000.0);
    insert2.addValue(5000, 5000.0);
    series.insert(std::move(insert2));

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 6);
}

// --- Test: Three batches where middle batch is out of order internally ---
TEST_F(MemoryStoreSortedTest, MiddleBatchInternallyUnsorted) {
    InMemorySeries<double> series;

    // First batch: sorted
    TSDBInsert<double> insert1("test", "field");
    insert1.addValue(1000, 1000.0);
    insert1.addValue(2000, 2000.0);
    series.insert(std::move(insert1));

    // Second batch: internally unsorted, timestamps between first and third
    TSDBInsert<double> insert2("test", "field");
    insert2.addValue(5000, 5000.0);
    insert2.addValue(3000, 3000.0);
    insert2.addValue(4000, 4000.0);
    series.insert(std::move(insert2));

    // Third batch: sorted, after all
    TSDBInsert<double> insert3("test", "field");
    insert3.addValue(6000, 6000.0);
    insert3.addValue(7000, 7000.0);
    series.insert(std::move(insert3));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 7);
}

// --- Test: All same timestamps ---
TEST_F(MemoryStoreSortedTest, AllSameTimestamps) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert("test", "field");
    insert.addValue(1000, 1.0);
    insert.addValue(1000, 2.0);
    insert.addValue(1000, 3.0);
    series.insert(std::move(insert));

    verifySorted(series);
    EXPECT_EQ(series.timestamps.size(), 3);
    // All timestamps should be 1000
    for (auto ts : series.timestamps) {
        EXPECT_EQ(ts, 1000);
    }
}

// --- Test: Reverse-order data ---
TEST_F(MemoryStoreSortedTest, ReverseOrderData) {
    InMemorySeries<double> series;

    TSDBInsert<double> insert("test", "field");
    insert.addValue(5000, 5000.0);
    insert.addValue(4000, 4000.0);
    insert.addValue(3000, 3000.0);
    insert.addValue(2000, 2000.0);
    insert.addValue(1000, 1000.0);

    series.insert(std::move(insert));

    verifySorted(series);
    verifyPairing(series);
    EXPECT_EQ(series.timestamps.size(), 5);
}
