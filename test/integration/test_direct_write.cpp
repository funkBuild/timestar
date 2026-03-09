#include <gtest/gtest.h>
#include <iostream>
#include <seastar/core/sleep.hh>
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include <filesystem>

class DirectWriteTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_0");
    }

    void TearDown() override {
        std::filesystem::remove_all("shard_0");
    }
};

seastar::future<> testDirectWriteLargeTimestamp() {
    Engine engine;
    co_await engine.init();

    std::cout << "Testing direct engine write..." << std::endl;

    // Test 1: Write with current timestamp (61-bit)
    uint64_t timestamp = 1756565110829708288ULL;  // 61-bit timestamp
    std::cout << "Writing timestamp: " << timestamp
              << " (requires " << (64 - __builtin_clzll(timestamp)) << " bits)" << std::endl;

    TimeStarInsert<double> insert("test", "value");
    insert.addTag("host", "server-01");
    insert.addValue(timestamp, 42.0);

    try {
        co_await engine.insert(std::move(insert));
        std::cout << "Write succeeded!" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Write failed: " << e.what() << std::endl;
    }

    // Test 2: Write with very large timestamp
    timestamp = UINT64_MAX;  // Max 64-bit
    std::cout << "\nWriting max timestamp: " << timestamp
              << " (requires " << (64 - __builtin_clzll(timestamp)) << " bits)" << std::endl;

    TimeStarInsert<double> insert2("test", "value");
    insert2.addTag("host", "server-02");
    insert2.addValue(timestamp, 99.0);

    try {
        co_await engine.insert(std::move(insert2));
        std::cout << "Write succeeded!" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Write failed: " << e.what() << std::endl;
    }

    co_await engine.stop();

    std::cout << "\nTest complete!" << std::endl;
}

TEST_F(DirectWriteTest, LargeTimestampWrite) {
    testDirectWriteLargeTimestamp().get();
}
