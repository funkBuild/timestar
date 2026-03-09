#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <chrono>
#include <filesystem>

#include "engine.hpp"
#include "timestar_value.hpp"
#include "query_result.hpp"
#include "../test_helpers.hpp"

namespace fs = std::filesystem;

class DeleteE2ETest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }

    // Helper to insert test data
    seastar::future<> insertTestData(Engine* engine) {
        // Insert data for series: cpu,host=server01,region=us-east usage
        TimeStarInsert<double> insert1("cpu", "usage");
        insert1.tags = {{"host", "server01"}, {"region", "us-east"}};
        insert1.timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        insert1.values = {10.5, 20.5, 30.5, 40.5, 50.5, 60.5, 70.5, 80.5, 90.5, 100.5};
        co_await engine->insert(insert1);

        // Insert data for series: cpu,host=server01,region=us-east temperature
        TimeStarInsert<double> insert2("cpu", "temperature");
        insert2.tags = {{"host", "server01"}, {"region", "us-east"}};
        insert2.timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        insert2.values = {60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0, 68.0, 69.0};
        co_await engine->insert(insert2);

        // Insert data for series: cpu,host=server02,region=us-east usage
        TimeStarInsert<double> insert3("cpu", "usage");
        insert3.tags = {{"host", "server02"}, {"region", "us-east"}};
        insert3.timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        insert3.values = {15.5, 25.5, 35.5, 45.5, 55.5, 65.5, 75.5, 85.5, 95.5, 105.5};
        co_await engine->insert(insert3);

        // Insert data for series: memory,host=server01,region=us-east usage
        TimeStarInsert<double> insert4("memory", "usage");
        insert4.tags = {{"host", "server01"}, {"region", "us-east"}};
        insert4.timestamps = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000};
        insert4.values = {1024, 2048, 3072, 4096, 5120, 6144, 7168, 8192, 9216, 10240};
        co_await engine->insert(insert4);

        // Small delay to ensure data is written
        co_await seastar::sleep(std::chrono::milliseconds(100));
    }

    // Helper to check if query result is empty
    bool isQueryResultEmpty(const std::optional<VariantQueryResult>& result) {
        if (!result.has_value()) {
            return true;
        }
        if (std::holds_alternative<QueryResult<double>>(*result)) {
            const auto& doubleResult = std::get<QueryResult<double>>(*result);
            return doubleResult.timestamps.empty();
        } else if (std::holds_alternative<QueryResult<bool>>(*result)) {
            const auto& boolResult = std::get<QueryResult<bool>>(*result);
            return boolResult.timestamps.empty();
        } else if (std::holds_alternative<QueryResult<std::string>>(*result)) {
            const auto& stringResult = std::get<QueryResult<std::string>>(*result);
            return stringResult.timestamps.empty();
        }
        return true;
    }

    // Helper to verify query results
    void verifyQueryResult(const std::optional<VariantQueryResult>& result,
                          const std::vector<uint64_t>& expectedTimestamps,
                          const std::vector<double>& expectedValues) {

        ASSERT_TRUE(result.has_value()) << "Expected query result but got nullopt";

        if (std::holds_alternative<QueryResult<double>>(*result)) {
            const auto& doubleResult = std::get<QueryResult<double>>(*result);
            ASSERT_EQ(doubleResult.timestamps.size(), expectedTimestamps.size());
            ASSERT_EQ(doubleResult.values.size(), expectedValues.size());

            for (size_t i = 0; i < expectedTimestamps.size(); ++i) {
                EXPECT_EQ(doubleResult.timestamps[i], expectedTimestamps[i]);
                EXPECT_DOUBLE_EQ(doubleResult.values[i], expectedValues[i]);
            }
        } else {
            FAIL() << "Expected QueryResult<double> but got different type";
        }
    }
};

TEST_F(DeleteE2ETest, DeleteTimeRangeVerifyQuery) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert test data
        insertTestData(engine).get();

        // Query before deletion to verify data exists
        std::string seriesKey = "cpu,host=server01,region=us-east usage";
        auto beforeResult = engine->query(seriesKey, 0, UINT64_MAX).get();
        verifyQueryResult(beforeResult,
            {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
            {10.5, 20.5, 30.5, 40.5, 50.5, 60.5, 70.5, 80.5, 90.5, 100.5});

        // Delete middle time range (3000-7000)
        bool deleted = engine->deleteRange(seriesKey, 3000, 7000).get();
        EXPECT_TRUE(deleted);

        // Query after deletion - should only return points outside deleted range
        auto afterResult = engine->query(seriesKey, 0, UINT64_MAX).get();
        verifyQueryResult(afterResult,
            {1000, 2000, 8000, 9000, 10000},
            {10.5, 20.5, 80.5, 90.5, 100.5});
    }).join().get();
}

TEST_F(DeleteE2ETest, DeleteByPatternVerifyQuery) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert test data
        insertTestData(engine).get();

        // Query cpu series before deletion
        std::string cpuSeries1 = "cpu,host=server01,region=us-east usage";
        std::string cpuSeries2 = "cpu,host=server01,region=us-east temperature";
        std::string memorySeries = "memory,host=server01,region=us-east usage";

        auto cpuBefore1 = engine->query(cpuSeries1, 0, UINT64_MAX).get();
        ASSERT_FALSE(isQueryResultEmpty(cpuBefore1));

        auto cpuBefore2 = engine->query(cpuSeries2, 0, UINT64_MAX).get();
        ASSERT_FALSE(isQueryResultEmpty(cpuBefore2));

        // Delete all cpu measurement data with host=server01
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.tags = {{"host", "server01"}};
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();
        EXPECT_EQ(result.seriesDeleted, 2); // usage and temperature fields

        // Query after deletion - cpu series should be empty
        auto cpuAfter1 = engine->query(cpuSeries1, 0, UINT64_MAX).get();
        EXPECT_TRUE(isQueryResultEmpty(cpuAfter1));

        auto cpuAfter2 = engine->query(cpuSeries2, 0, UINT64_MAX).get();
        EXPECT_TRUE(isQueryResultEmpty(cpuAfter2));

        // Memory series should still have data
        auto memoryAfter = engine->query(memorySeries, 0, UINT64_MAX).get();
        verifyQueryResult(memoryAfter,
            {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
            {1024, 2048, 3072, 4096, 5120, 6144, 7168, 8192, 9216, 10240});
    }).join().get();
}

TEST_F(DeleteE2ETest, DeleteSpecificFieldVerifyQuery) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert test data
        insertTestData(engine).get();

        // Delete only temperature field for cpu measurement
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.tags = {{"host", "server01"}, {"region", "us-east"}};
        request.fields = {"temperature"};
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();
        EXPECT_EQ(result.seriesDeleted, 1);

        // Query after deletion
        std::string usageSeries = "cpu,host=server01,region=us-east usage";
        std::string tempSeries = "cpu,host=server01,region=us-east temperature";

        // Usage field should still have data
        auto usageResult = engine->query(usageSeries, 0, UINT64_MAX).get();
        verifyQueryResult(usageResult,
            {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
            {10.5, 20.5, 30.5, 40.5, 50.5, 60.5, 70.5, 80.5, 90.5, 100.5});

        // Temperature field should be deleted
        auto tempResult = engine->query(tempSeries, 0, UINT64_MAX).get();
        EXPECT_TRUE(isQueryResultEmpty(tempResult));
    }).join().get();
}

TEST_F(DeleteE2ETest, PartialDeleteVerifyTimeRange) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert test data
        insertTestData(engine).get();

        // Delete partial time range for all fields of server01
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.tags = {{"host", "server01"}};
        request.startTime = 4000;
        request.endTime = 7000;

        auto result = engine->deleteByPattern(request).get();
        EXPECT_GT(result.seriesDeleted, 0);

        // Query the affected series
        std::string seriesKey = "cpu,host=server01,region=us-east usage";

        // Query full range
        auto fullResult = engine->query(seriesKey, 0, UINT64_MAX).get();
        verifyQueryResult(fullResult,
            {1000, 2000, 3000, 8000, 9000, 10000},
            {10.5, 20.5, 30.5, 80.5, 90.5, 100.5});

        // Query deleted range - should return empty
        auto deletedRangeResult = engine->query(seriesKey, 4000, 7000).get();
        EXPECT_TRUE(isQueryResultEmpty(deletedRangeResult));

        // Query partial overlap with deleted range
        auto overlapResult = engine->query(seriesKey, 2000, 8000).get();
        verifyQueryResult(overlapResult,
            {2000, 3000, 8000},
            {20.5, 30.5, 80.5});
    }).join().get();
}

TEST_F(DeleteE2ETest, DeleteNonExistentDataVerifyNoChange) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert test data
        insertTestData(engine).get();

        // Try to delete non-existent measurement
        Engine::DeleteRequest request;
        request.measurement = "nonexistent";
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();
        EXPECT_EQ(result.seriesDeleted, 0);

        // Verify existing data is unchanged
        std::string seriesKey = "cpu,host=server01,region=us-east usage";
        auto queryResult = engine->query(seriesKey, 0, UINT64_MAX).get();
        verifyQueryResult(queryResult,
            {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
            {10.5, 20.5, 30.5, 40.5, 50.5, 60.5, 70.5, 80.5, 90.5, 100.5});
    }).join().get();
}

TEST_F(DeleteE2ETest, DeleteAllMeasurementDataVerifyEmpty) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert test data
        insertTestData(engine).get();

        // Delete all cpu measurement data
        Engine::DeleteRequest request;
        request.measurement = "cpu";
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();
        EXPECT_EQ(result.seriesDeleted, 3); // server01 usage & temp, server02 usage

        // Query all cpu series - should be empty
        std::vector<std::string> cpuSeries = {
            "cpu,host=server01,region=us-east usage",
            "cpu,host=server01,region=us-east temperature",
            "cpu,host=server02,region=us-east usage"
        };

        for (const auto& series : cpuSeries) {
            auto result = engine->query(series, 0, UINT64_MAX).get();
            EXPECT_TRUE(isQueryResultEmpty(result))
                << "Series " << series << " should be empty after deletion";
        }

        // Memory series should still exist
        std::string memorySeries = "memory,host=server01,region=us-east usage";
        auto memoryResult = engine->query(memorySeries, 0, UINT64_MAX).get();
        ASSERT_FALSE(isQueryResultEmpty(memoryResult));
    }).join().get();
}

TEST_F(DeleteE2ETest, DeleteNonExistentSeriesIsGracefulNoOp) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Do NOT insert any data -- engine is empty.

        // deleteByPattern on a measurement that was never written should
        // return 0 deleted series without throwing or crashing.
        Engine::DeleteRequest request;
        request.measurement = "totally_missing";
        request.tags = {{"host", "ghost"}};
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        auto result = engine->deleteByPattern(request).get();
        EXPECT_EQ(result.seriesDeleted, 0);
        EXPECT_TRUE(result.deletedSeries.empty());
    }).join().get();
}

TEST_F(DeleteE2ETest, DeleteRangeNonExistentSeriesKeyIsGraceful) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Engine is empty -- no data has been inserted.
        // deleteRange on a series key that was never written should
        // return false (nothing deleted) without throwing.
        std::string seriesKey = "nonexistent,host=nowhere usage";
        bool deleted = engine->deleteRange(seriesKey, 0, UINT64_MAX).get();
        EXPECT_FALSE(deleted);
    }).join().get();
}

TEST_F(DeleteE2ETest, QueryNonExistentSeriesReturnsEmptyAfterInsert) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // Insert some data so the engine is not completely empty
        insertTestData(engine).get();

        // Query a series key that was never written -- should return
        // empty/nullopt, not crash.
        std::string seriesKey = "disk,host=server99,region=eu-west iops";
        auto result = engine->query(seriesKey, 0, UINT64_MAX).get();
        EXPECT_TRUE(isQueryResultEmpty(result));
    }).join().get();
}

// Note: This test file needs to be compiled with the Seastar test framework
// The actual test execution is handled by the main test runner
