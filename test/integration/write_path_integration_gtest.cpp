/*
 * Google Test + Seastar integration for write path integration tests
 *
 * Tests the complete write path through Engine including WAL, memory store, and TSM files
 */

#include <gtest/gtest.h>
#include "../seastar_gtest.hpp"
#include "../test_helpers.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/query_result.hpp"
#include <filesystem>
#include <functional>
#include <variant>

class WritePathIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }
};

SEASTAR_TEST_F(WritePathIntegrationTest, BasicWriteAndQuery) {
    Engine engine;

    co_await engine.init();
    co_await engine.startBackgroundTasks();

    // Insert float data
    TimeStarInsert<double> tempInsert("weather", "temperature");
    tempInsert.addTag("location", "us-midwest");
    tempInsert.addTag("host", "server-01");

    int64_t baseTime = 1638360000000000000LL; // Dec 1, 2021
    tempInsert.addValue(baseTime, 82.5);

    co_await engine.insert(tempInsert);

    // Insert boolean data
    TimeStarInsert<bool> statusInsert("system", "status");
    statusInsert.addTag("host", "server-01");
    statusInsert.addValue(baseTime + 1000000000, true);

    co_await engine.insert(statusInsert);

    // Insert string data
    TimeStarInsert<std::string> messageInsert("logs", "message");
    messageInsert.addTag("host", "server-01");
    messageInsert.addValue(baseTime + 2000000000, "System started successfully");

    co_await engine.insert(messageInsert);

    // Wait a bit for data to be processed
    co_await seastar::sleep(std::chrono::milliseconds(200));

    // Query float data - use EXPECT (non-fatal) instead of ASSERT (fatal with return) in coroutines
    std::string seriesKey = tempInsert.seriesKey();
    auto result = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);

    EXPECT_TRUE(result.has_value()) << "Expected query result for temperature";
    if (result.has_value() && std::holds_alternative<QueryResult<double>>(*result)) {
        auto& floatResult = std::get<QueryResult<double>>(*result);
        EXPECT_EQ(floatResult.timestamps.size(), 1);
        EXPECT_EQ(floatResult.values.size(), 1);
        EXPECT_EQ(floatResult.timestamps[0], baseTime);
        EXPECT_DOUBLE_EQ(floatResult.values[0], 82.5);
    } else if (result.has_value()) {
        ADD_FAILURE() << "Expected float result for temperature query";
    }

    // Query boolean data
    seriesKey = statusInsert.seriesKey();
    auto result2 = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);

    EXPECT_TRUE(result2.has_value()) << "Expected query result for status";
    if (result2.has_value() && std::holds_alternative<QueryResult<bool>>(*result2)) {
        auto& boolResult = std::get<QueryResult<bool>>(*result2);
        EXPECT_EQ(boolResult.timestamps.size(), 1);
        EXPECT_EQ(boolResult.values.size(), 1);
        EXPECT_EQ(boolResult.values[0], true);
    } else if (result2.has_value()) {
        ADD_FAILURE() << "Expected bool result for status query";
    }

    // Query string data
    seriesKey = messageInsert.seriesKey();
    auto result3 = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);

    EXPECT_TRUE(result3.has_value()) << "Expected query result for message";
    if (result3.has_value() && std::holds_alternative<QueryResult<std::string>>(*result3)) {
        auto& stringResult = std::get<QueryResult<std::string>>(*result3);
        EXPECT_EQ(stringResult.timestamps.size(), 1);
        EXPECT_EQ(stringResult.values.size(), 1);
        EXPECT_EQ(stringResult.values[0], "System started successfully");
    } else if (result3.has_value()) {
        ADD_FAILURE() << "Expected string result for message query";
    }

    co_await engine.stop();
}

SEASTAR_TEST_F(WritePathIntegrationTest, MetadataIndexing) {
    Engine engine;

    co_await engine.init();

    // Insert data with different measurements and tags
    TimeStarInsert<double> tempInsert("temperature", "value");
    tempInsert.addTag("location", "room1");
    tempInsert.addValue(1638360000000000000LL, 22.5);
    co_await engine.insert(tempInsert);
    co_await engine.indexMetadata(tempInsert);

    // Create a new insert for room2 (don't reuse - seriesKey cache issue)
    TimeStarInsert<double> tempInsert2("temperature", "value");
    tempInsert2.addTag("location", "room2");
    tempInsert2.addValue(1638360000000000000LL, 23.0);
    co_await engine.insert(tempInsert2);
    co_await engine.indexMetadata(tempInsert2);

    TimeStarInsert<double> pressureInsert("pressure", "value");
    pressureInsert.addTag("location", "room1");
    pressureInsert.addValue(1638360000000000000LL, 1013.25);
    co_await engine.insert(pressureInsert);
    co_await engine.indexMetadata(pressureInsert);

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
}

SEASTAR_TEST_F(WritePathIntegrationTest, MultipleSeriesQuery) {
    Engine engine;

    co_await engine.init();
    co_await engine.startBackgroundTasks();

    int64_t baseTime = 1638360000000000000LL;

    // Insert data for multiple series
    for (int i = 0; i < 5; i++) {
        TimeStarInsert<double> insert("metrics", "cpu");
        insert.addTag("host", "server-" + std::to_string(i));
        insert.addValue(baseTime + i * 1000000000LL, 50.0 + i * 10);
        co_await engine.insert(insert);
        co_await engine.indexMetadata(insert);
    }

    // Wait for data to settle
    co_await seastar::sleep(std::chrono::milliseconds(100));

    // Query each series
    for (int i = 0; i < 5; i++) {
        TimeStarInsert<double> queryKey("metrics", "cpu");
        queryKey.addTag("host", "server-" + std::to_string(i));
        std::string seriesKey = queryKey.seriesKey();
        auto result = co_await engine.query(seriesKey, baseTime, baseTime + 10000000000LL);

        EXPECT_TRUE(result.has_value()) << "Expected query result for series " << i;
        if (result.has_value() && std::holds_alternative<QueryResult<double>>(*result)) {
            auto& floatResult = std::get<QueryResult<double>>(*result);
            EXPECT_EQ(floatResult.values.size(), 1) << "Series " << i << " (key=" << seriesKey << ") expected 1 value";
            if (floatResult.values.size() == 1) {
                EXPECT_DOUBLE_EQ(floatResult.values[0], 50.0 + i * 10);
            }
        } else if (result.has_value()) {
            ADD_FAILURE() << "Expected float result for cpu metric query";
        }
    }

    co_await engine.stop();
}
