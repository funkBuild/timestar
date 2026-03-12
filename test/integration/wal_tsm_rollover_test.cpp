#include <gtest/gtest.h>
#include "../seastar_gtest.hpp"
#include "../test_helpers.hpp"
#include <filesystem>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include "../../lib/core/engine.hpp"
#include "../../lib/storage/tsm_file_manager.hpp"
#include "../../lib/storage/wal_file_manager.hpp"
#include "../../lib/query/query_runner.hpp"
#include "../../lib/utils/logger.hpp"

namespace fs = std::filesystem;

class WALTSMRolloverTest : public ::testing::Test {
public:
    void SetUp() override {
        // Clean up all shard directories and let Engine create its own
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }

    size_t countTSMFiles() {
        size_t count = 0;
        std::string tsmDir = "shard_0/tsm";
        if (!fs::exists(tsmDir)) return 0;
        for (const auto& entry : fs::directory_iterator(tsmDir)) {
            if (entry.path().extension() == ".tsm") {
                count++;
            }
        }
        return count;
    }

    size_t countWALFiles() {
        size_t count = 0;
        // WAL files are stored directly in the shard directory (e.g., shard_0/0000000001.wal)
        std::string walDir = "shard_0";
        if (!fs::exists(walDir)) return 0;
        for (const auto& entry : fs::directory_iterator(walDir)) {
            if (entry.path().extension() == ".wal") {
                count++;
            }
        }
        return count;
    }
};

SEASTAR_TEST_F(WALTSMRolloverTest, TestWALToTSMRollover) {
    co_await seastar::async([self]() {
        std::cout << "\n=== Testing WAL to TSM Rollover ===" << std::endl;

        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Initially should have 1 WAL file and 0 TSM files
        EXPECT_EQ(self->countWALFiles(), 1) << "Should start with 1 WAL file";
        EXPECT_EQ(self->countTSMFiles(), 0) << "Should start with 0 TSM files";
        std::cout << "Initial state: " << self->countWALFiles() << " WAL files, "
                 << self->countTSMFiles() << " TSM files" << std::endl;

        // The WAL threshold is 16MB. The WAL uses compressed encoding (XOR for
        // floats, delta for timestamps), so we need to write enough data to
        // exceed 16MB of actual compressed WAL data. Use many distinct series
        // with large point counts to ensure sufficient volume.
        const size_t POINTS_PER_INSERT = 5000;
        const size_t MAX_SERIES = 500;

        std::cout << "Writing data to trigger WAL rollover..." << std::endl;
        std::cout << "  Max series: " << MAX_SERIES << std::endl;
        std::cout << "  Points per insert: " << POINTS_PER_INSERT << std::endl;

        uint64_t baseTime = 1000000000;
        bool rolloverDetected = false;

        for (size_t s = 0; s < MAX_SERIES; s++) {
            TimeStarInsert<double> insert("test_metric", "field_" + std::to_string(s));
            insert.tags = {
                {"host", "server_" + std::to_string(s % 10)},
                {"region", "region_" + std::to_string(s % 5)}
            };

            for (size_t i = 0; i < POINTS_PER_INSERT; i++) {
                uint64_t timestamp = baseTime + s * POINTS_PER_INSERT * 1000 + i * 1000;
                // Use varied values to reduce compression ratio
                double value = s * 100.0 + i * 0.7 + (i % 7) * 3.14159;
                insert.addValue(timestamp, value);
            }

            engine->insert(insert).get();

            // Check for rollover periodically
            if (s % 20 == 19) {
                size_t tsmCount = self->countTSMFiles();
                if (tsmCount > 0) {
                    std::cout << "  WAL rollover detected after " << (s + 1) << " series! "
                             << "TSM files: " << tsmCount << std::endl;
                    rolloverDetected = true;
                    break;
                }
            }
        }

        // The background TSM conversion runs asynchronously after rollover.
        // If we haven't observed a TSM file yet, poll briefly to let the
        // conversion finish before asserting.
        if (!rolloverDetected) {
            for (int wait = 0; wait < 100 && self->countTSMFiles() == 0; ++wait) {
                seastar::sleep(std::chrono::milliseconds(50)).get();
            }
        }

        // Now we should have at least 1 TSM file
        size_t finalTsmCount = self->countTSMFiles();
        size_t finalWalCount = self->countWALFiles();

        std::cout << "\nFinal state:" << std::endl;
        std::cout << "  WAL files: " << finalWalCount << std::endl;
        std::cout << "  TSM files: " << finalTsmCount << std::endl;

        EXPECT_GE(finalTsmCount, 1) << "Should have created at least 1 TSM file after exceeding threshold";

        // Verify data integrity - query some of the data back
        std::cout << "\nVerifying data integrity..." << std::endl;

        for (size_t s = 0; s < std::min(size_t(3), MAX_SERIES); s++) {
            TimeStarInsert<double> queryKey("test_metric", "field_" + std::to_string(s));
            queryKey.tags = {
                {"host", "server_" + std::to_string(s % 10)},
                {"region", "region_" + std::to_string(s % 5)}
            };
            std::string seriesKey = queryKey.seriesKey();

            uint64_t queryStart = baseTime + s * POINTS_PER_INSERT * 1000;
            uint64_t queryEnd = queryStart + 10000000;
            auto resultOpt = engine->query(seriesKey, queryStart, queryEnd).get();

            if (resultOpt.has_value()) {
                size_t pointCount = std::visit([](auto&& res) {
                    return res.timestamps.size();
                }, *resultOpt);

                std::cout << "  Series " << s << ": " << pointCount << " points recovered" << std::endl;
                EXPECT_GT(pointCount, 0) << "Should have data for series " << s;
            }
        }

        std::cout << "\n=== WAL to TSM Rollover Test Complete ===" << std::endl;
    });
}

SEASTAR_TEST_F(WALTSMRolloverTest, TestMultipleRollovers) {
    co_await seastar::async([self]() {
        std::cout << "\n=== Testing Multiple WAL Rollovers ===" << std::endl;

        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        const size_t TARGET_ROLLOVERS = 3;
        size_t rollovers_seen = 0;
        size_t last_tsm_count = 0;

        std::cout << "Target: " << TARGET_ROLLOVERS << " rollovers" << std::endl;

        // Write data in large rounds to trigger multiple rollovers.
        // Each round writes enough distinct series with enough points
        // to exceed the 16MB WAL threshold even after compression.
        const size_t MAX_ROUNDS = 10;
        const size_t SERIES_PER_ROUND = 300;
        const size_t POINTS_PER_SERIES = 5000;

        for (size_t round = 0; round < MAX_ROUNDS && rollovers_seen < TARGET_ROLLOVERS; round++) {
            std::cout << "\nRound " << (round + 1) << ":" << std::endl;

            for (size_t s = 0; s < SERIES_PER_ROUND; s++) {
                TimeStarInsert<double> insert("metric_round_" + std::to_string(round),
                                        "field_" + std::to_string(s));
                insert.tags = {{"round", std::to_string(round)}};

                for (size_t i = 0; i < POINTS_PER_SERIES; i++) {
                    uint64_t ts = 1000000ULL + round * 10000000ULL + s * POINTS_PER_SERIES + i;
                    double val = s * 1000.0 + i * 1.1 + (i % 13) * 2.718;
                    insert.addValue(ts, val);
                }

                engine->insert(insert).get();
            }

            size_t current_tsm_count = self->countTSMFiles();
            if (current_tsm_count > last_tsm_count) {
                size_t new_files = current_tsm_count - last_tsm_count;
                rollovers_seen += new_files;
                std::cout << "  Rollover detected! "
                         << "TSM files: " << last_tsm_count << " -> " << current_tsm_count
                         << " (rollovers seen: " << rollovers_seen << ")" << std::endl;
                last_tsm_count = current_tsm_count;
            }
        }

        size_t final_tsm_count = self->countTSMFiles();
        std::cout << "\nFinal TSM count: " << final_tsm_count << std::endl;
        std::cout << "Rollovers observed: " << rollovers_seen << std::endl;

        EXPECT_GE(rollovers_seen, TARGET_ROLLOVERS)
            << "Should have seen at least " << TARGET_ROLLOVERS << " rollovers";
        EXPECT_GE(final_tsm_count, TARGET_ROLLOVERS)
            << "Should have at least " << TARGET_ROLLOVERS << " TSM files";

        std::cout << "\n=== Multiple Rollovers Test Complete ===" << std::endl;
    });
}

SEASTAR_TEST_F(WALTSMRolloverTest, TestBatchedWritesWithRollover) {
    co_await seastar::async([self]() {
        std::cout << "\n=== Testing Batched Writes with Rollover ===" << std::endl;

        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        std::cout << "Using batched write optimizations..." << std::endl;

        // Write enough data to exceed 16MB WAL threshold (compressed).
        // The WAL uses XOR encoding for floats and delta encoding for timestamps,
        // which can achieve high compression ratios. Use many series with large
        // point counts and longer tag values to ensure sufficient volume.
        const size_t BATCH_SIZE = 5000;   // Points per series
        const size_t NUM_SERIES = 1000;   // Enough series to exceed threshold

        auto start = std::chrono::high_resolution_clock::now();

        bool rolloverDetected = false;
        for (size_t s = 0; s < NUM_SERIES; s++) {
            TimeStarInsert<double> insert("batch_metric", "series_" + std::to_string(s));
            insert.tags = {
                {"batch_test", "true"},
                {"host", "server_" + std::to_string(s % 10)},
                {"region", "region_" + std::to_string(s % 5)}
            };

            for (size_t i = 0; i < BATCH_SIZE; i++) {
                double val = s * 1000.0 + i * 0.9 + (i % 11) * 1.414;
                insert.addValue(2000000000ULL + s * BATCH_SIZE * 1000 + i * 1000, val);
            }

            engine->insert(insert).get();

            if (s % 20 == 19) {
                size_t tsmCount = self->countTSMFiles();
                if (tsmCount > 0) {
                    std::cout << "  Rollover after " << (s + 1) << " series" << std::endl;
                    rolloverDetected = true;
                    break;
                }
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // Wait for background TSM conversion if it hasn't completed yet
        if (!rolloverDetected) {
            for (int wait = 0; wait < 100 && self->countTSMFiles() == 0; ++wait) {
                seastar::sleep(std::chrono::milliseconds(50)).get();
            }
        }

        size_t finalTsmCount = self->countTSMFiles();
        size_t finalWalCount = self->countWALFiles();

        std::cout << "\nResults with optimized batching:" << std::endl;
        std::cout << "  Write time: " << duration.count() << " ms" << std::endl;
        std::cout << "  TSM files created: " << finalTsmCount << std::endl;
        std::cout << "  WAL files: " << finalWalCount << std::endl;

        EXPECT_GE(finalTsmCount, 1) << "Should have created TSM files with batched writes";

        // Verify a sample of data
        TimeStarInsert<double> queryKey("batch_metric", "series_0");
        queryKey.tags = {
            {"batch_test", "true"},
            {"host", "server_0"},
            {"region", "region_0"}
        };
        std::string seriesKey = queryKey.seriesKey();

        auto resultOpt = engine->query(seriesKey, 2000000000, 2001000000).get();

        size_t pointCount = 0;
        if (resultOpt.has_value()) {
            pointCount = std::visit([](auto&& res) {
                return res.timestamps.size();
            }, *resultOpt);
        }

        std::cout << "  Data verification: " << pointCount << " points recovered for series_0" << std::endl;
        EXPECT_GT(pointCount, 0) << "Should have data after rollover";

        std::cout << "\n=== Batched Writes with Rollover Test Complete ===" << std::endl;
    });
}
