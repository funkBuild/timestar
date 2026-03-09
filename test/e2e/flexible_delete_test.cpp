#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <chrono>

#include "engine.hpp"
#include "timestar_value.hpp"
#include "../test_helpers.hpp"

class FlexibleDeleteTest : public ::testing::Test {
protected:
    std::unique_ptr<Engine> engine;

    void SetUp() override {
        cleanTestShardDirectories();
        engine = std::make_unique<Engine>();

        // Initialize engine in Seastar context
        seastar::async([this] {
            engine->init().get();
        }).get();
    }

    void TearDown() override {
        if (engine) {
            try {
                seastar::async([this] {
                    engine->stop().get();
                }).get();
            } catch (...) {
                // Swallow errors during cleanup to avoid segfaults
            }
        }
        cleanTestShardDirectories();
    }

    // Helper to insert test data
    // NOTE: Engine::insert() does NOT index metadata (indexing is handled at
    // the HTTP handler level). For tests that rely on deleteByPattern() or
    // other index-based lookups, we must explicitly call indexMetadata() after
    // each insert to populate the LevelDB index.
    seastar::future<> insertTestData() {
        // Insert data for multiple series with different tags and fields

        // Series 1: cpu,host=server01,region=us-east.usage
        TimeStarInsert<double> insert1("cpu", "usage");
        insert1.tags = {{"host", "server01"}, {"region", "us-east"}};
        insert1.timestamps = {1000, 2000, 3000, 4000, 5000};
        insert1.values = {10.5, 20.5, 30.5, 40.5, 50.5};
        co_await engine->insert(insert1);
        co_await engine->indexMetadata(insert1);

        // Series 2: cpu,host=server01,region=us-east.temperature
        TimeStarInsert<double> insert2("cpu", "temperature");
        insert2.tags = {{"host", "server01"}, {"region", "us-east"}};
        insert2.timestamps = {1000, 2000, 3000, 4000, 5000};
        insert2.values = {60.0, 65.0, 70.0, 75.0, 80.0};
        co_await engine->insert(insert2);
        co_await engine->indexMetadata(insert2);

        // Series 3: cpu,host=server02,region=us-east.usage
        TimeStarInsert<double> insert3("cpu", "usage");
        insert3.tags = {{"host", "server02"}, {"region", "us-east"}};
        insert3.timestamps = {1000, 2000, 3000, 4000, 5000};
        insert3.values = {15.5, 25.5, 35.5, 45.5, 55.5};
        co_await engine->insert(insert3);
        co_await engine->indexMetadata(insert3);

        // Series 4: cpu,host=server01,region=us-west.usage
        TimeStarInsert<double> insert4("cpu", "usage");
        insert4.tags = {{"host", "server01"}, {"region", "us-west"}};
        insert4.timestamps = {1000, 2000, 3000, 4000, 5000};
        insert4.values = {12.5, 22.5, 32.5, 42.5, 52.5};
        co_await engine->insert(insert4);
        co_await engine->indexMetadata(insert4);

        // Series 5: memory,host=server01,region=us-east.usage
        TimeStarInsert<double> insert5("memory", "usage");
        insert5.tags = {{"host", "server01"}, {"region", "us-east"}};
        insert5.timestamps = {1000, 2000, 3000, 4000, 5000};
        insert5.values = {1024.0, 2048.0, 3072.0, 4096.0, 5120.0};
        co_await engine->insert(insert5);
        co_await engine->indexMetadata(insert5);
    }
};

TEST_F(FlexibleDeleteTest, DeleteByMeasurementOnly) {
    seastar::async([this] {
        // Insert test data
        insertTestData().get();

        // Delete all cpu measurement data
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();

        // Should delete 4 cpu series
        EXPECT_EQ(result.seriesDeleted, 4);
        EXPECT_EQ(result.deletedSeries.size(), 4);

        // Verify memory series still exists
        auto memResult = engine->query("memory,host=server01,region=us-east usage", 0, UINT64_MAX).get();
        EXPECT_TRUE(memResult.has_value());
    }).get();
}

TEST_F(FlexibleDeleteTest, DeleteByMeasurementAndTags) {
    seastar::async([this] {
        insertTestData().get();

        // Delete all series for cpu measurement with host=server01
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.tags = {{"host", "server01"}};
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();

        // Should delete 3 series (server01 in us-east and us-west, with usage and temperature)
        EXPECT_EQ(result.seriesDeleted, 3);

        // Verify server02 series still exists
        auto server02Result = engine->query("cpu,host=server02,region=us-east usage", 0, UINT64_MAX).get();
        EXPECT_TRUE(server02Result.has_value());
    }).get();
}

TEST_F(FlexibleDeleteTest, DeleteByMeasurementTagsAndFields) {
    seastar::async([this] {
        insertTestData().get();

        // Delete only temperature field for cpu measurement with host=server01
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.tags = {{"host", "server01"}};
        request.fields = {"temperature"};
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();

        // Should delete only 1 series (temperature field)
        EXPECT_EQ(result.seriesDeleted, 1);
        EXPECT_EQ(result.deletedSeries[0], "cpu,host=server01,region=us-east temperature");

        // Verify usage field still exists
        auto usageResult = engine->query("cpu,host=server01,region=us-east usage", 0, UINT64_MAX).get();
        EXPECT_TRUE(usageResult.has_value());
    }).get();
}

TEST_F(FlexibleDeleteTest, DeleteWithTimeRange) {
    seastar::async([this] {
        insertTestData().get();

        // Delete partial time range for all cpu series
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.startTime = 2000;
        request.endTime = 3000;

        auto result = engine->deleteByPattern(request).get();

        // Should affect 4 series
        EXPECT_EQ(result.seriesDeleted, 4);

        // Query remaining data and verify points outside range still exist
        auto queryResult = engine->query("cpu,host=server01,region=us-east usage", 0, UINT64_MAX).get();
        if (queryResult.has_value() && std::holds_alternative<QueryResult<double>>(*queryResult)) {
            auto& doubleResult = std::get<QueryResult<double>>(*queryResult);
            // Should have points at 1000, 4000, 5000 (deleted 2000, 3000)
            EXPECT_EQ(doubleResult.timestamps.size(), 3);
            EXPECT_EQ(doubleResult.timestamps[0], 1000);
            EXPECT_EQ(doubleResult.timestamps[1], 4000);
            EXPECT_EQ(doubleResult.timestamps[2], 5000);
        }
    }).get();
}

TEST_F(FlexibleDeleteTest, DeleteMultipleFields) {
    seastar::async([this] {
        insertTestData().get();

        // Delete both usage and temperature fields for specific tags
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.tags = {{"host", "server01"}, {"region", "us-east"}};
        request.fields = {"usage", "temperature"};
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();

        // Should delete 2 series
        EXPECT_EQ(result.seriesDeleted, 2);

        // Verify other regions still exist
        auto westResult = engine->query("cpu,host=server01,region=us-west usage", 0, UINT64_MAX).get();
        EXPECT_TRUE(westResult.has_value());
    }).get();
}

TEST_F(FlexibleDeleteTest, DeleteNonExistentPattern) {
    seastar::async([this] {
        insertTestData().get();

        // Try to delete non-existent measurement
        Engine::DeleteRequest request;
        request.measurement = "nonexistent";
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();

        // Should delete nothing
        EXPECT_EQ(result.seriesDeleted, 0);
        EXPECT_TRUE(result.deletedSeries.empty());
    }).get();
}

// Note: This test is integrated into the main test runner in main.cpp
// which already handles Seastar initialization
