#include <gtest/gtest.h>
#include <filesystem>
#include <chrono>

#include "../../../lib/core/engine.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/query_runner.hpp"
#include "../test_helpers.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

class WritePathEndToEndTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }
};

// End-to-end test: Write data through Engine and read it back
seastar::future<> testWriteAndRead() {
    Engine engine;
    co_await engine.init();
    co_await engine.startBackgroundTasks();

    std::exception_ptr eptr;
    try {
        // Test 1: Write float data
        TimeStarInsert<double> tempInsert("temperature", "value");
        tempInsert.addTag("location", "us-west");
        tempInsert.addTag("sensor", "temp-01");

        uint64_t baseTime = 1638202821000000000;
        for (int i = 0; i < 10; i++) {
            tempInsert.addValue(baseTime + static_cast<uint64_t>(i) * 1000000000, 20.0 + i * 0.5);
        }

        co_await engine.insert(tempInsert);
        co_await engine.indexMetadata(tempInsert);

        // Test 2: Write boolean data
        TimeStarInsert<bool> statusInsert("temperature", "is_active");
        statusInsert.addTag("location", "us-west");
        statusInsert.addTag("sensor", "temp-01");

        for (int i = 0; i < 10; i++) {
            statusInsert.addValue(baseTime + static_cast<uint64_t>(i) * 1000000000, i % 2 == 0);
        }

        co_await engine.insert(statusInsert);
        co_await engine.indexMetadata(statusInsert);

        // Test 3: Write string data
        TimeStarInsert<std::string> messageInsert("temperature", "status");
        messageInsert.addTag("location", "us-west");
        messageInsert.addTag("sensor", "temp-01");

        for (int i = 0; i < 10; i++) {
            messageInsert.addValue(baseTime + static_cast<uint64_t>(i) * 1000000000,
                                 "Status " + std::to_string(i));
        }

        co_await engine.insert(messageInsert);
        co_await engine.indexMetadata(messageInsert);

        // Wait for data to settle
        co_await seastar::sleep(std::chrono::milliseconds(100));

        // Read data back using query
        std::string seriesKey = tempInsert.seriesKey();
        auto result = co_await engine.query(seriesKey, baseTime, baseTime + 10LL * 1000000000);

        // Verify float data (using EXPECT instead of ASSERT to avoid return in coroutine)
        EXPECT_TRUE(result.has_value()) << "Expected query result for float series";
        if (result.has_value() && std::holds_alternative<QueryResult<double>>(*result)) {
            auto& floatResult = std::get<QueryResult<double>>(*result);
            EXPECT_EQ(floatResult.timestamps.size(), 10);
            EXPECT_EQ(floatResult.values.size(), 10);

            for (int i = 0; i < 10; i++) {
                EXPECT_EQ(floatResult.timestamps[i], baseTime + static_cast<uint64_t>(i) * 1000000000);
                EXPECT_DOUBLE_EQ(floatResult.values[i], 20.0 + i * 0.5);
            }
            std::cout << "Float data verified: " << floatResult.values.size() << " points" << std::endl;
        } else if (result.has_value()) {
            ADD_FAILURE() << "Expected float result";
        }

        // Query boolean data
        seriesKey = statusInsert.seriesKey();
        auto result2 = co_await engine.query(seriesKey, baseTime, baseTime + 10LL * 1000000000);

        EXPECT_TRUE(result2.has_value()) << "Expected query result for bool series";
        if (result2.has_value() && std::holds_alternative<QueryResult<bool>>(*result2)) {
            auto& boolResult = std::get<QueryResult<bool>>(*result2);
            EXPECT_EQ(boolResult.timestamps.size(), 10);
            EXPECT_EQ(boolResult.values.size(), 10);

            for (int i = 0; i < 10; i++) {
                EXPECT_EQ(boolResult.timestamps[i], baseTime + static_cast<uint64_t>(i) * 1000000000);
                EXPECT_EQ(boolResult.values[i], i % 2 == 0);
            }
            std::cout << "Boolean data verified: " << boolResult.values.size() << " points" << std::endl;
        } else if (result2.has_value()) {
            ADD_FAILURE() << "Expected boolean result";
        }

        // Query string data
        seriesKey = messageInsert.seriesKey();
        auto result3 = co_await engine.query(seriesKey, baseTime, baseTime + 10LL * 1000000000);

        EXPECT_TRUE(result3.has_value()) << "Expected query result for string series";
        if (result3.has_value() && std::holds_alternative<QueryResult<std::string>>(*result3)) {
            auto& stringResult = std::get<QueryResult<std::string>>(*result3);
            EXPECT_EQ(stringResult.timestamps.size(), 10);
            EXPECT_EQ(stringResult.values.size(), 10);

            for (int i = 0; i < 10; i++) {
                EXPECT_EQ(stringResult.timestamps[i], baseTime + static_cast<uint64_t>(i) * 1000000000);
                EXPECT_EQ(stringResult.values[i], "Status " + std::to_string(i));
            }
            std::cout << "String data verified: " << stringResult.values.size() << " points" << std::endl;
        } else if (result3.has_value()) {
            ADD_FAILURE() << "Expected string result";
        }

        // Test assertions above verify correctness via EXPECT_EQ/EXPECT_TRUE
    } catch (...) {
        eptr = std::current_exception();
    }

    co_await engine.stop();
    if (eptr) std::rethrow_exception(eptr);
}

TEST_F(WritePathEndToEndTest, WriteAndRead) {
    testWriteAndRead().get();
}

// Test metadata queries through Engine
seastar::future<> testMetadataQueries() {
    Engine engine;
    co_await engine.init();

    std::exception_ptr eptr;
    try {
        // Insert data for multiple measurements
        TimeStarInsert<double> tempInsert("temperature", "value");
        tempInsert.addTag("location", "us-west");
        tempInsert.addTag("sensor", "temp-01");
        tempInsert.addValue(1638202821000000000, 25.5);
        co_await engine.insert(tempInsert);
        co_await engine.indexMetadata(tempInsert);

        TimeStarInsert<double> tempInsert2("temperature", "humidity");
        tempInsert2.addTag("location", "us-west");
        tempInsert2.addTag("sensor", "temp-01");
        tempInsert2.addValue(1638202821000000000, 65.0);
        co_await engine.insert(tempInsert2);
        co_await engine.indexMetadata(tempInsert2);

        TimeStarInsert<double> pressureInsert("pressure", "value");
        pressureInsert.addTag("location", "us-east");
        pressureInsert.addTag("sensor", "press-01");
        pressureInsert.addValue(1638202821000000000, 1013.25);
        co_await engine.insert(pressureInsert);
        co_await engine.indexMetadata(pressureInsert);

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

        // Test assertions above verify correctness via EXPECT_EQ/EXPECT_TRUE
    } catch (...) {
        eptr = std::current_exception();
    }

    co_await engine.stop();
    if (eptr) std::rethrow_exception(eptr);
}

TEST_F(WritePathEndToEndTest, MetadataQueries) {
    testMetadataQueries().get();
}

// Test persistence across restarts
seastar::future<> testPersistence() {
    // Phase 1: Write data
    {
        Engine engine;
        co_await engine.init();

        std::exception_ptr eptr;
        try {
            TimeStarInsert<double> insert("cpu", "usage");
            insert.addTag("host", "server-01");
            insert.addTag("cpu", "cpu0");

            uint64_t baseTime = 1638202821000000000;
            for (int i = 0; i < 100; i++) {
                insert.addValue(baseTime + static_cast<uint64_t>(i) * 1000000000, 50.0 + i * 0.1);
            }

            co_await engine.insert(insert);
            co_await engine.indexMetadata(insert);
            co_await engine.rolloverMemoryStore();
            co_await seastar::sleep(std::chrono::milliseconds(100));
        } catch (...) {
            eptr = std::current_exception();
        }

        co_await engine.stop();
        if (eptr) std::rethrow_exception(eptr);
    }

    // Phase 2: Read data after restart
    {
        Engine engine;
        co_await engine.init();

        std::exception_ptr eptr;
        try {
            // Query using structured query
            auto result = co_await engine.queryBySeries("cpu",
                {{"host", "server-01"}, {"cpu", "cpu0"}},
                "usage",
                1638202821000000000,
                1638202821000000000 + 100LL * 1000000000);

            if (std::holds_alternative<QueryResult<double>>(result)) {
                auto& floatResult = std::get<QueryResult<double>>(result);
                EXPECT_EQ(floatResult.timestamps.size(), 100);

                for (int i = 0; i < 100; i++) {
                    EXPECT_DOUBLE_EQ(floatResult.values[i], 50.0 + i * 0.1);
                }
                std::cout << "Data persisted correctly: " << floatResult.values.size() << " points" << std::endl;
            } else {
                ADD_FAILURE() << "Expected float result after restart";
            }

            // Test assertions above verify correctness via EXPECT_EQ/EXPECT_DOUBLE_EQ
        } catch (...) {
            eptr = std::current_exception();
        }

        co_await engine.stop();
        if (eptr) std::rethrow_exception(eptr);
    }
}

TEST_F(WritePathEndToEndTest, Persistence) {
    testPersistence().get();
}
