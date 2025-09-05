/*
 * Google Test + Seastar integration for write path integration tests
 * 
 * Tests the complete write path through Engine including WAL, memory store, and TSM files
 */

#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/query/query_result.hpp"
#include <filesystem>
#include <functional>
#include <variant>

using namespace seastar;

// Wrapper to run Seastar code within Google Test
static int run_in_seastar(std::function<seastar::future<>()> func) {
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    
    seastar::app_template app;
    try {
        return app.run(argc, argv, [func = std::move(func)]() {
            return func().handle_exception([](std::exception_ptr ep) {
                try {
                    std::rethrow_exception(ep);
                } catch (const std::exception& e) {
                    std::cerr << "Test failed with exception: " << e.what() << std::endl;
                    return make_exception_future<>(ep);
                }
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Failed to run Seastar app: " << e.what() << std::endl;
        return 1;
    }
}

class WritePathIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test data
        std::filesystem::remove_all("shard_0");
    }
    
    void TearDown() override {
        // Clean up test data
        std::filesystem::remove_all("shard_0");
    }
};

TEST_F(WritePathIntegrationTest, BasicWriteAndQuery) {
    int result = run_in_seastar([]() -> seastar::future<> {
        Engine engine;
        
        co_await engine.init();
        co_await engine.startBackgroundTasks();
        
        // Insert float data
        TSDBInsert<double> tempInsert("weather", "temperature");
        tempInsert.addTag("location", "us-midwest");
        tempInsert.addTag("host", "server-01");
        
        int64_t baseTime = 1638360000000000000LL; // Dec 1, 2021
        tempInsert.addValue(baseTime, 82.5);
        
        co_await engine.insert(tempInsert);
        
        // Insert boolean data
        TSDBInsert<bool> statusInsert("system", "status");
        statusInsert.addTag("host", "server-01");
        statusInsert.addValue(baseTime + 1000000000, true);
        
        co_await engine.insert(statusInsert);
        
        // Insert string data
        TSDBInsert<std::string> messageInsert("logs", "message");
        messageInsert.addTag("host", "server-01");
        messageInsert.addValue(baseTime + 2000000000, "System started successfully");
        
        co_await engine.insert(messageInsert);
        
        // Wait a bit for data to be processed
        // Note: Engine doesn't have public rolloverMemoryStore method
        co_await seastar::sleep(std::chrono::milliseconds(200));
        
        // Query float data
        std::string seriesKey = "weather,host=server-01,location=us-midwest,temperature";
        auto result = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);
        
        // Check if we got a float result
        if (std::holds_alternative<QueryResult<double>>(result)) {
            auto& floatResult = std::get<QueryResult<double>>(result);
            EXPECT_EQ(floatResult.timestamps.size(), 1);
            EXPECT_EQ(floatResult.values.size(), 1);
            EXPECT_EQ(floatResult.timestamps[0], baseTime);
            EXPECT_DOUBLE_EQ(floatResult.values[0], 82.5);
        } else {
            ADD_FAILURE() << "Expected float result for temperature query";
        }
        
        // Query boolean data
        seriesKey = "system,host=server-01,status";
        result = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);
        
        if (std::holds_alternative<QueryResult<bool>>(result)) {
            auto& boolResult = std::get<QueryResult<bool>>(result);
            EXPECT_EQ(boolResult.timestamps.size(), 1);
            EXPECT_EQ(boolResult.values.size(), 1);
            EXPECT_EQ(boolResult.values[0], true);
        } else {
            ADD_FAILURE() << "Expected bool result for status query";
        }
        
        // Query string data
        seriesKey = "logs,host=server-01,message";
        result = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);
        
        if (std::holds_alternative<QueryResult<std::string>>(result)) {
            auto& stringResult = std::get<QueryResult<std::string>>(result);
            EXPECT_EQ(stringResult.timestamps.size(), 1);
            EXPECT_EQ(stringResult.values.size(), 1);
            EXPECT_EQ(stringResult.values[0], "System started successfully");
        } else {
            ADD_FAILURE() << "Expected string result for message query";
        }
        
        co_await engine.stop();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(WritePathIntegrationTest, MetadataIndexing) {
    int result = run_in_seastar([]() -> seastar::future<> {
        Engine engine;
        
        co_await engine.init();
        
        // Insert data with different measurements and tags
        TSDBInsert<double> tempInsert("temperature", "value");
        tempInsert.addTag("location", "room1");
        tempInsert.addValue(1638360000000000000LL, 22.5);
        co_await engine.insert(tempInsert);
        
        tempInsert.tags["location"] = "room2";
        tempInsert.timestamps.clear();
        tempInsert.values.clear();
        tempInsert.addValue(1638360000000000000LL, 23.0);
        co_await engine.insert(tempInsert);
        
        TSDBInsert<double> pressureInsert("pressure", "value");
        pressureInsert.addTag("location", "room1");
        pressureInsert.addValue(1638360000000000000LL, 1013.25);
        co_await engine.insert(pressureInsert);
        
        // Test measurement fields
        auto tempFields = co_await engine.getMeasurementFields("temperature");
        EXPECT_EQ(tempFields.size(), 1);
        EXPECT_TRUE(tempFields.find("value") != tempFields.end());
        
        auto pressureFields = co_await engine.getMeasurementFields("pressure");
        EXPECT_EQ(pressureFields.size(), 1);
        EXPECT_TRUE(pressureFields.find("value") != pressureFields.end());
        
        // Test measurement tags
        auto tempTags = co_await engine.getMeasurementTags("temperature");
        EXPECT_EQ(tempTags.size(), 1);
        EXPECT_TRUE(tempTags.find("location") != tempTags.end());
        
        // Test tag values
        auto locations = co_await engine.getTagValues("temperature", "location");
        EXPECT_EQ(locations.size(), 2);
        EXPECT_TRUE(locations.find("room1") != locations.end());
        EXPECT_TRUE(locations.find("room2") != locations.end());
        
        co_await engine.stop();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(WritePathIntegrationTest, MultipleSeriesQuery) {
    int result = run_in_seastar([]() -> seastar::future<> {
        Engine engine;
        
        co_await engine.init();
        co_await engine.startBackgroundTasks();
        
        int64_t baseTime = 1638360000000000000LL;
        
        // Insert data for multiple series
        for (int i = 0; i < 5; i++) {
            TSDBInsert<double> insert("metrics", "cpu");
            insert.addTag("host", "server-" + std::to_string(i));
            insert.addValue(baseTime + i * 1000000000, 50.0 + i * 10);
            co_await engine.insert(insert);
        }
        
        // Query each series
        for (int i = 0; i < 5; i++) {
            std::string seriesKey = "metrics,host=server-" + std::to_string(i) + ",cpu";
            auto result = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);
            
            if (std::holds_alternative<QueryResult<double>>(result)) {
                auto& floatResult = std::get<QueryResult<double>>(result);
                EXPECT_EQ(floatResult.values.size(), 1);
                EXPECT_DOUBLE_EQ(floatResult.values[0], 50.0 + i * 10);
            } else {
                ADD_FAILURE() << "Expected float result for cpu metric query";
            }
        }
        
        co_await engine.stop();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}