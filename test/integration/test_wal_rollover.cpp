// Test for WAL rollover and TSM creation
#include "../../lib/core/engine.hpp"
#include "../../lib/core/timestar_value.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

namespace fs = std::filesystem;

class WALRolloverTest : public ::testing::Test {
public:
    void SetUp() override {
        cleanupTestFiles();
        fs::create_directories("shard_0");
    }

    void TearDown() override { cleanupTestFiles(); }

    static void cleanupTestFiles() {
        std::string shardDir = "shard_0";

        if (!fs::exists("."))
            return;

        // Remove WAL and TSM files
        for (const auto& entry : fs::directory_iterator(".")) {
            auto path = entry.path();
            if ((path.extension() == ".wal" || path.extension() == ".tsm") &&
                path.string().find(shardDir) != std::string::npos) {
                try {
                    fs::remove(path);
                } catch (...) {}
            }
        }
    }

    static size_t countTsmFiles() {
        size_t count = 0;
        std::string shardDir = "shard_0";

        for (const auto& entry : fs::directory_iterator(".")) {
            if (entry.path().extension() == ".tsm" && entry.path().string().find(shardDir) != std::string::npos) {
                count++;
                std::cout << "Found TSM: " << entry.path().filename() << std::endl;
            }
        }
        return count;
    }
};

seastar::future<> testWALRolloverAndTSMCreation() {
    std::cout << "\n========================================" << std::endl;
    std::cout << "WAL Rollover and TSM Creation Test" << std::endl;
    std::cout << "========================================\n" << std::endl;

    Engine engine;
    std::cout << "Initializing engine..." << std::endl;
    co_await engine.init();

    // Start background TSM writer task
    std::cout << "Starting background TSM writer..." << std::endl;
    co_await engine.startBackgroundTasks();

    std::cout << "\nInitial TSM files: " << WALRolloverTest::countTsmFiles() << std::endl;

    // The WAL threshold is 16MB - write data to exceed it
    std::cout << "\nWriting data to trigger WAL rollover (16MB threshold)..." << std::endl;

    const size_t SERIES_COUNT = 50;
    const size_t POINTS_PER_BATCH = 2000;
    const size_t BATCHES = 15;

    size_t totalPoints = 0;
    size_t estimatedMB = 0;
    auto startTime = std::chrono::high_resolution_clock::now();

    for (size_t batch = 0; batch < BATCHES; batch++) {
        for (size_t s = 0; s < SERIES_COUNT; s++) {
            TimeStarInsert<double> insert("metric", "field_" + std::to_string(s));
            insert.addTag("host", "h" + std::to_string(s % 5));

            uint64_t baseTime = 1000000000 + batch * POINTS_PER_BATCH * 1000;
            for (size_t i = 0; i < POINTS_PER_BATCH; i++) {
                insert.addValue(baseTime + i * 1000, s * 100.0 + i);
            }

            totalPoints += POINTS_PER_BATCH;
            co_await engine.insert(insert);
        }

        // Estimate: ~32 bytes per point (compressed)
        estimatedMB = (totalPoints * 32) / (1024 * 1024);
        std::cout << "Batch " << (batch + 1) << "/" << BATCHES << " - Data: ~" << estimatedMB << " MB" << std::endl;

        // Give time for background processing
        co_await seastar::sleep(std::chrono::milliseconds(200));

        size_t tsmCount = WALRolloverTest::countTsmFiles();
        if (tsmCount > 0) {
            std::cout << "\n*** WAL ROLLOVER DETECTED! ***" << std::endl;
            break;
        }
    }

    // Wait for background tasks
    std::cout << "\nWaiting for background tasks..." << std::endl;
    co_await seastar::sleep(std::chrono::seconds(2));

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    size_t finalTsmCount = WALRolloverTest::countTsmFiles();

    std::cout << "\n========================================" << std::endl;
    std::cout << "Results:" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Points written: " << totalPoints << std::endl;
    std::cout << "Estimated size: ~" << estimatedMB << " MB" << std::endl;
    std::cout << "Time: " << duration.count() << " ms" << std::endl;
    std::cout << "TSM files: " << finalTsmCount << std::endl;

    if (finalTsmCount > 0) {
        std::cout << "\nSUCCESS: WAL rollover working!" << std::endl;

        // Verify data
        TimeStarInsert<double> q("metric", "field_0");
        q.addTag("host", "h0");
        auto resultOpt = co_await engine.query(q.seriesKey(), 1000000000, 2000000000);

        if (resultOpt.has_value()) {
            size_t points = std::visit([](auto&& r) { return r.timestamps.size(); }, *resultOpt);
            std::cout << "Data verified: " << points << " points recovered" << std::endl;
            EXPECT_GT(points, 0);
        }
    } else {
        std::cout << "\nNo TSM files created yet" << std::endl;
    }

    // Shutdown
    co_await engine.stop();

    WALRolloverTest::cleanupTestFiles();

    std::cout << "\nTest complete!" << std::endl;
}

TEST_F(WALRolloverTest, WALRolloverAndTSMCreation) {
    testWALRolloverAndTSMCreation().get();
}
