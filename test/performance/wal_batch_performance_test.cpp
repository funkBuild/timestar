#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <vector>
#include "../../lib/storage/wal.hpp"
#include "../../lib/storage/memory_store.hpp"
#include "../../lib/core/timestar_value.hpp"
#include "../test_helpers.hpp"
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

class WALBatchPerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
        std::filesystem::create_directories("shard_0");
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }
};

TEST_F(WALBatchPerformanceTest, CompareSingleVsBatchInserts) {
    seastar::async([]() {
        const int NUM_SERIES = 100;
        const int POINTS_PER_SERIES = 1000;

        // Declare duration variables in outer scope so they can be compared across blocks
        std::chrono::milliseconds duration1{0};
        std::chrono::milliseconds duration2{0};
        std::chrono::milliseconds duration3{0};

        // Test 1: Single inserts with immediate flush (old behavior)
        {
            WAL wal1(1000);
            MemoryStore store1(1000);
            wal1.init(&store1).get();
            wal1.setImmediateFlush(true);  // Old behavior

            auto start = std::chrono::high_resolution_clock::now();

            for (int s = 0; s < NUM_SERIES; s++) {
                TimeStarInsert<double> insert("metric", "value" + std::to_string(s));
                insert.tags = {{"host", "server" + std::to_string(s % 10)}};

                for (int i = 0; i < POINTS_PER_SERIES; i++) {
                    insert.timestamps.push_back(1000000 + i * 1000);
                    insert.values.push_back(100.0 + i);
                }

                wal1.insert(insert).get();
            }

            wal1.finalFlush().get();
            wal1.close().get();

            auto end = std::chrono::high_resolution_clock::now();
            duration1 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "Single inserts with immediate flush: " << duration1.count() << " ms" << std::endl;

            // Verify file size
            auto size1 = wal1.getCurrentSize();
            std::cout << "  WAL size: " << size1 << " bytes" << std::endl;
        }

        // Test 2: Single inserts with batched flush (new behavior)
        {
            WAL wal2(1001);
            MemoryStore store2(1001);
            wal2.init(&store2).get();
            wal2.setImmediateFlush(false);  // New behavior - batch flushes

            auto start = std::chrono::high_resolution_clock::now();

            for (int s = 0; s < NUM_SERIES; s++) {
                TimeStarInsert<double> insert("metric", "value" + std::to_string(s));
                insert.tags = {{"host", "server" + std::to_string(s % 10)}};

                for (int i = 0; i < POINTS_PER_SERIES; i++) {
                    insert.timestamps.push_back(1000000 + i * 1000);
                    insert.values.push_back(100.0 + i);
                }

                wal2.insert(insert).get();
            }

            wal2.finalFlush().get();
            wal2.close().get();

            auto end = std::chrono::high_resolution_clock::now();
            duration2 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "Single inserts with batched flush: " << duration2.count() << " ms" << std::endl;

            // Verify file size
            auto size2 = wal2.getCurrentSize();
            std::cout << "  WAL size: " << size2 << " bytes" << std::endl;

            // Batched flush should not be slower than immediate flush.
            // Under system load both can finish in single-digit ms, so allow
            // the batched path to be up to 2x + 1ms to avoid timing flakes.
            EXPECT_LE(duration2.count(), duration1.count() * 2 + 1);
        }

        // Test 3: Batch inserts (new batch API)
        {
            WAL wal3(1002);
            MemoryStore store3(1002);
            wal3.init(&store3).get();

            auto start = std::chrono::high_resolution_clock::now();

            // Batch 10 series at a time
            for (int batch = 0; batch < NUM_SERIES / 10; batch++) {
                std::vector<TimeStarInsert<double>> batchInserts;

                for (int s = 0; s < 10; s++) {
                    int seriesIdx = batch * 10 + s;
                    TimeStarInsert<double> insert("metric", "value" + std::to_string(seriesIdx));
                    insert.tags = {{"host", "server" + std::to_string(seriesIdx % 10)}};

                    for (int i = 0; i < POINTS_PER_SERIES; i++) {
                        insert.timestamps.push_back(1000000 + i * 1000);
                        insert.values.push_back(100.0 + i);
                    }

                    batchInserts.push_back(std::move(insert));
                }

                wal3.insertBatch(batchInserts).get();
            }

            wal3.finalFlush().get();
            wal3.close().get();

            auto end = std::chrono::high_resolution_clock::now();
            duration3 = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "Batch inserts (10 series per batch): " << duration3.count() << " ms" << std::endl;

            // Verify file size
            auto size3 = wal3.getCurrentSize();
            std::cout << "  WAL size: " << size3 << " bytes" << std::endl;

            // Batch API should be comparable to single inserts with batched flush.
            // Both approaches have similar I/O characteristics (buffered writes),
            // so under system load their timings are nearly identical. Allow the
            // batch duration to be up to 2x the single-batched duration to avoid
            // flaky failures from timing jitter.
            EXPECT_LE(duration3.count(), duration2.count() * 2 + 1);
        }

        std::cout << "\nOptimization Summary:" << std::endl;
        std::cout << "- Batched flush reduces I/O operations" << std::endl;
        std::cout << "- Larger block size (64KB) improves SSD performance" << std::endl;
        std::cout << "- Batch insert API reduces overhead for multiple series" << std::endl;
    }).get();
}

TEST_F(WALBatchPerformanceTest, VerifyDataIntegrity) {
    seastar::async([]() {
        // Write data with batch API
        {
            WAL wal(1003);
            MemoryStore store(1003);
            wal.init(&store).get();

            std::vector<TimeStarInsert<double>> batchInserts;

            for (int s = 0; s < 5; s++) {
                TimeStarInsert<double> insert("test_metric", "field" + std::to_string(s));
                insert.tags = {{"tag1", "value" + std::to_string(s)}};

                for (int i = 0; i < 10; i++) {
                    insert.timestamps.push_back(1000 + i);
                    insert.values.push_back(s * 10.0 + i);
                }

                batchInserts.push_back(insert);
            }

            wal.insertBatch(batchInserts).get();
            wal.close().get();
        }

        // Read back and verify
        {
            MemoryStore recoveredStore(1003);
            WALReader reader(WAL::sequenceNumberToFilename(1003));
            reader.readAll(&recoveredStore).get();

            // Verify all series were recovered
            for (int s = 0; s < 5; s++) {
                TimeStarInsert<double> expected("test_metric", "field" + std::to_string(s));
                expected.tags = {{"tag1", "value" + std::to_string(s)}};
                std::string seriesKey = expected.seriesKey();

                SeriesId128 queryId = SeriesId128::fromSeriesKey(seriesKey);
                auto result = recoveredStore.querySeries<double>(queryId);
                ASSERT_NE(result, nullptr);

                const auto& series = *result;
                EXPECT_EQ(series.timestamps.size(), 10);
                EXPECT_EQ(series.values.size(), 10);

                for (int i = 0; i < 10; i++) {
                    EXPECT_EQ(series.timestamps[i], 1000 + i);
                    EXPECT_DOUBLE_EQ(series.values[i], s * 10.0 + i);
                }
            }
        }

        std::cout << "Data integrity verified - batch inserts correctly persisted and recovered" << std::endl;
    }).get();
}
