#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>

#include "../../../lib/core/engine.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/query/query_runner.hpp"

#include <seastar/core/app_template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

class WritePathIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test data
        for (int i = 0; i < 4; i++) {
            std::filesystem::remove_all("shard_" + std::to_string(i));
        }
    }
    
    void TearDown() override {
        // Clean up test data
        for (int i = 0; i < 4; i++) {
            std::filesystem::remove_all("shard_" + std::to_string(i));
        }
    }
};

// End-to-end test: Write data through Engine and read it back
seastar::future<> testWriteAndRead() {
    Engine engine;
    co_await engine.init();
    co_await engine.startBackgroundTasks();
    
    // Test 1: Write float data
    TSDBInsert<double> tempInsert("temperature", "value");
    tempInsert.addTag("location", "us-west");
    tempInsert.addTag("sensor", "temp-01");
    
    uint64_t baseTime = 1638202821000000000;
    for (int i = 0; i < 10; i++) {
        tempInsert.addValue(baseTime + i * 1000000000, 20.0 + i * 0.5);
    }
    
    co_await engine.insert(tempInsert);
    
    // Test 2: Write boolean data
    TSDBInsert<bool> statusInsert("temperature", "is_active");
    statusInsert.addTag("location", "us-west");
    statusInsert.addTag("sensor", "temp-01");
    
    for (int i = 0; i < 10; i++) {
        statusInsert.addValue(baseTime + i * 1000000000, i % 2 == 0);
    }
    
    co_await engine.insert(statusInsert);
    
    // Test 3: Write string data
    TSDBInsert<std::string> messageInsert("temperature", "status");
    messageInsert.addTag("location", "us-west");
    messageInsert.addTag("sensor", "temp-01");
    
    for (int i = 0; i < 10; i++) {
        messageInsert.addValue(baseTime + i * 1000000000, 
                             "Status " + std::to_string(i));
    }
    
    co_await engine.insert(messageInsert);
    
    // Force memory store to flush to TSM
    co_await engine.rolloverMemoryStore();
    
    // Small delay to ensure flush completes
    co_await seastar::sleep(std::chrono::milliseconds(100));
    
    // Read data back using query
    std::string seriesKey = tempInsert.seriesKey();
    auto result = co_await engine.query(seriesKey, baseTime, baseTime + 10 * 1000000000);
    
    // Verify float data
    if (std::holds_alternative<QueryResult<double>>(result)) {
        auto& floatResult = std::get<QueryResult<double>>(result);
        EXPECT_EQ(floatResult.timestamps.size(), 10);
        EXPECT_EQ(floatResult.values.size(), 10);
        
        for (int i = 0; i < 10; i++) {
            EXPECT_EQ(floatResult.timestamps[i], baseTime + i * 1000000000);
            EXPECT_DOUBLE_EQ(floatResult.values[i], 20.0 + i * 0.5);
        }
        std::cout << "Float data verified: " << floatResult.values.size() << " points" << std::endl;
    } else {
        FAIL() << "Expected float result";
    }
    
    // Query boolean data
    seriesKey = statusInsert.seriesKey();
    result = co_await engine.query(seriesKey, baseTime, baseTime + 10 * 1000000000);
    
    if (std::holds_alternative<QueryResult<bool>>(result)) {
        auto& boolResult = std::get<QueryResult<bool>>(result);
        EXPECT_EQ(boolResult.timestamps.size(), 10);
        EXPECT_EQ(boolResult.values.size(), 10);
        
        for (int i = 0; i < 10; i++) {
            EXPECT_EQ(boolResult.timestamps[i], baseTime + i * 1000000000);
            EXPECT_EQ(boolResult.values[i], i % 2 == 0);
        }
        std::cout << "Boolean data verified: " << boolResult.values.size() << " points" << std::endl;
    } else {
        FAIL() << "Expected boolean result";
    }
    
    // Query string data
    seriesKey = messageInsert.seriesKey();
    result = co_await engine.query(seriesKey, baseTime, baseTime + 10 * 1000000000);
    
    if (std::holds_alternative<QueryResult<std::string>>(result)) {
        auto& stringResult = std::get<QueryResult<std::string>>(result);
        EXPECT_EQ(stringResult.timestamps.size(), 10);
        EXPECT_EQ(stringResult.values.size(), 10);
        
        for (int i = 0; i < 10; i++) {
            EXPECT_EQ(stringResult.timestamps[i], baseTime + i * 1000000000);
            EXPECT_EQ(stringResult.values[i], "Status " + std::to_string(i));
        }
        std::cout << "String data verified: " << stringResult.values.size() << " points" << std::endl;
    } else {
        FAIL() << "Expected string result";
    }
    
    co_await engine.stop();
    
    std::cout << "End-to-end write and read test passed!" << std::endl;
}

TEST_F(WritePathIntegrationTest, WriteAndRead) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testWriteAndRead().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Test failed with exception: " << e.what() << std::endl;
            }
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}

// Test metadata queries through Engine
seastar::future<> testMetadataQueries() {
    Engine engine;
    co_await engine.init();
    
    // Insert data for multiple measurements
    TSDBInsert<double> tempInsert("temperature", "value");
    tempInsert.addTag("location", "us-west");
    tempInsert.addTag("sensor", "temp-01");
    tempInsert.addValue(1638202821000000000, 25.5);
    co_await engine.insert(tempInsert);
    
    TSDBInsert<double> tempInsert2("temperature", "humidity");
    tempInsert2.addTag("location", "us-west");
    tempInsert2.addTag("sensor", "temp-01");
    tempInsert2.addValue(1638202821000000000, 65.0);
    co_await engine.insert(tempInsert2);
    
    TSDBInsert<double> pressureInsert("pressure", "value");
    pressureInsert.addTag("location", "us-east");
    pressureInsert.addTag("sensor", "press-01");
    pressureInsert.addValue(1638202821000000000, 1013.25);
    co_await engine.insert(pressureInsert);
    
    // Test field queries
    auto tempFields = co_await engine.getMeasurementFields("temperature");
    EXPECT_EQ(tempFields.size(), 2);
    EXPECT_TRUE(tempFields.count("value") > 0);
    EXPECT_TRUE(tempFields.count("humidity") > 0);
    
    auto pressureFields = co_await engine.getMeasurementFields("pressure");
    EXPECT_EQ(pressureFields.size(), 1);
    EXPECT_TRUE(pressureFields.count("value") > 0);
    
    // Test tag queries
    auto tempTags = co_await engine.getMeasurementTags("temperature");
    EXPECT_EQ(tempTags.size(), 2);
    EXPECT_TRUE(tempTags.count("location") > 0);
    EXPECT_TRUE(tempTags.count("sensor") > 0);
    
    // Test tag value queries
    auto locations = co_await engine.getTagValues("temperature", "location");
    EXPECT_EQ(locations.size(), 1);
    EXPECT_TRUE(locations.count("us-west") > 0);
    
    co_await engine.stop();
    
    std::cout << "Metadata queries test passed!" << std::endl;
}

TEST_F(WritePathIntegrationTest, MetadataQueries) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testMetadataQueries().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Test failed with exception: " << e.what() << std::endl;
            }
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}

// Test persistence across restarts
seastar::future<> testPersistence() {
    // Phase 1: Write data
    {
        Engine engine;
        co_await engine.init();
        
        TSDBInsert<double> insert("cpu", "usage");
        insert.addTag("host", "server-01");
        insert.addTag("cpu", "cpu0");
        
        uint64_t baseTime = 1638202821000000000;
        for (int i = 0; i < 100; i++) {
            insert.addValue(baseTime + i * 1000000000, 50.0 + i * 0.1);
        }
        
        co_await engine.insert(insert);
        co_await engine.rolloverMemoryStore();
        co_await seastar::sleep(std::chrono::milliseconds(100));
        co_await engine.stop();
    }
    
    // Phase 2: Read data after restart
    {
        Engine engine;
        co_await engine.init();
        
        // Query using structured query
        auto result = co_await engine.queryBySeries("cpu",
            {{"host", "server-01"}, {"cpu", "cpu0"}},
            "usage",
            1638202821000000000,
            1638202821000000000 + 100 * 1000000000);
        
        if (std::holds_alternative<QueryResult<double>>(result)) {
            auto& floatResult = std::get<QueryResult<double>>(result);
            EXPECT_EQ(floatResult.timestamps.size(), 100);
            
            for (int i = 0; i < 100; i++) {
                EXPECT_DOUBLE_EQ(floatResult.values[i], 50.0 + i * 0.1);
            }
            std::cout << "Data persisted correctly: " << floatResult.values.size() << " points" << std::endl;
        } else {
            FAIL() << "Expected float result after restart";
        }
        
        co_await engine.stop();
    }
    
    std::cout << "Persistence test passed!" << std::endl;
}

TEST_F(WritePathIntegrationTest, Persistence) {
    seastar::app_template app;
    
    auto exitCode = app.run(0, nullptr, [&] {
        return testPersistence().then([&] {
            seastar::engine().exit(0);
        }).handle_exception([&](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                std::cerr << "Test failed with exception: " << e.what() << std::endl;
            }
            seastar::engine().exit(1);
        });
    });
    
    EXPECT_EQ(exitCode, 0);
}