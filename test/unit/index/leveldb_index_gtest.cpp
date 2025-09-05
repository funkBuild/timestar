/*
 * Google Test + Seastar integration for LevelDBIndex tests
 * 
 * This test file uses the wrapper approach to test async LevelDB index operations
 */

#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>
#include <functional>

// Need to include these after filesystem to avoid conflicts
#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"

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

class LevelDBIndexAsyncTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test index
        std::filesystem::remove_all("shard_999");
        std::filesystem::remove_all("shard_998");
    }
    
    void TearDown() override {
        // Clean up test index
        std::filesystem::remove_all("shard_999");
        std::filesystem::remove_all("shard_998");
    }
};

TEST_F(LevelDBIndexAsyncTest, BasicIndexOperations) {
    int result = run_in_seastar([]() -> seastar::future<> {
        LevelDBIndex index(999); // Use shard 999 for testing
        
        co_await index.open();
        
        // Test 1: Create a series and get ID
        TSDBInsert<double> tempInsert("weather", "temperature");
        tempInsert.addTag("location", "us-midwest");
        tempInsert.addTag("host", "server-01");
        
        uint64_t seriesId1 = co_await index.indexInsert(tempInsert);
        EXPECT_GT(seriesId1, 0);
        
        // Test 2: Same series should return same ID
        uint64_t seriesId2 = co_await index.indexInsert(tempInsert);
        EXPECT_EQ(seriesId1, seriesId2);
        
        // Test 3: Different field should get different ID
        TSDBInsert<double> humidityInsert("weather", "humidity");
        humidityInsert.addTag("location", "us-midwest");
        humidityInsert.addTag("host", "server-01");
        
        uint64_t seriesId3 = co_await index.indexInsert(humidityInsert);
        EXPECT_GT(seriesId3, 0);
        EXPECT_NE(seriesId1, seriesId3);
        
        // Test 4: Check measurement fields
        auto fields = co_await index.getFields("weather");
        EXPECT_EQ(fields.size(), 2);
        EXPECT_TRUE(fields.find("temperature") != fields.end());
        EXPECT_TRUE(fields.find("humidity") != fields.end());
        
        // Test 5: Check measurement tags
        auto tags = co_await index.getTags("weather");
        EXPECT_EQ(tags.size(), 2);
        EXPECT_TRUE(tags.find("location") != tags.end());
        EXPECT_TRUE(tags.find("host") != tags.end());
        
        // Test 6: Check tag values
        auto locations = co_await index.getTagValues("weather", "location");
        EXPECT_EQ(locations.size(), 1);
        EXPECT_TRUE(locations.find("us-midwest") != locations.end());
        
        co_await index.close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(LevelDBIndexAsyncTest, SeriesIdGeneration) {
    int result = run_in_seastar([]() -> seastar::future<> {
        LevelDBIndex index(998); // Use shard 998 for this test
        
        co_await index.open();
        
        // Test multiple series with same measurement, different tags
        std::string measurement = "cpu_usage";
        
        std::map<std::string, std::string> tags1 = {{"host", "server-01"}, {"cpu", "cpu0"}};
        std::map<std::string, std::string> tags2 = {{"host", "server-01"}, {"cpu", "cpu1"}};
        std::map<std::string, std::string> tags3 = {{"host", "server-02"}, {"cpu", "cpu0"}};
        
        uint64_t id1 = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
        uint64_t id2 = co_await index.getOrCreateSeriesId(measurement, tags2, "idle");
        uint64_t id3 = co_await index.getOrCreateSeriesId(measurement, tags3, "idle");
        
        // All should have different IDs
        EXPECT_NE(id1, id2);
        EXPECT_NE(id1, id3);
        EXPECT_NE(id2, id3);
        
        // Test same series returns same ID
        uint64_t id1_again = co_await index.getOrCreateSeriesId(measurement, tags1, "idle");
        EXPECT_EQ(id1, id1_again);
        
        // Test different field gets different ID
        uint64_t id4 = co_await index.getOrCreateSeriesId(measurement, tags1, "user");
        EXPECT_NE(id1, id4);
        
        co_await index.close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(LevelDBIndexAsyncTest, Persistence) {
    // Use a shared pointer to pass data between runs
    auto sharedData = std::make_shared<uint64_t>(0);
    std::string measurement = "test_measurement";
    std::map<std::string, std::string> tags = {{"host", "test-host"}};
    std::string field = "test_field";
    
    // Create series and close
    int result1 = run_in_seastar([sharedData, measurement, tags, field]() -> seastar::future<> {
        auto index = std::make_unique<LevelDBIndex>(999);
        return index->open().then([index = std::move(index), sharedData, measurement, tags, field]() mutable {
            return index->getOrCreateSeriesId(measurement, tags, field).then([index = std::move(index), sharedData](uint64_t seriesId) mutable {
                *sharedData = seriesId;
                EXPECT_GT(*sharedData, 0);
                return index->close();
            });
        });
    });
    EXPECT_EQ(result1, 0);
    
    // Reopen and verify
    int result2 = run_in_seastar([sharedData, measurement, tags, field]() -> seastar::future<> {
        auto index = std::make_unique<LevelDBIndex>(999);
        return index->open().then([index = std::move(index), sharedData, measurement, tags, field]() mutable {
            return index->getOrCreateSeriesId(measurement, tags, field).then([index = std::move(index), sharedData, measurement, field](uint64_t newId) mutable {
                EXPECT_EQ(*sharedData, newId);
                
                // Check metadata persisted
                return index->getFields(measurement).then([index = std::move(index), field](std::set<std::string> fields) mutable {
                    EXPECT_EQ(fields.size(), 1);
                    EXPECT_TRUE(fields.find(field) != fields.end());
                    return index->close();
                });
            });
        });
    });
    EXPECT_EQ(result2, 0);
}