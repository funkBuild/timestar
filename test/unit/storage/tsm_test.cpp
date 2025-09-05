#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include <chrono>

#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/compressed_buffer.hpp"
#include "../../../lib/encoding/float_encoder.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/core/tsdb_value.hpp"

namespace fs = std::filesystem;

class TSMTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_files";
    
    void SetUp() override {
        fs::create_directories(testDir);
    }
    
    void TearDown() override {
        fs::remove_all(testDir);
    }
    
    std::string getTestFilePath(const std::string& filename) {
        return testDir + "/" + filename;
    }
};

TEST_F(TSMTest, BasicTSMWriterFloat) {
    std::string filename = getTestFilePath("test_float.tsm");
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    uint64_t baseTime = 1600000000000;
    for (int i = 0; i < 1000; i++) {
        timestamps.push_back(baseTime + i * 1000);
        values.push_back(100.0 + sin(i * 0.1) * 10.0);
    }
    
    writer.writeSeries(TSMValueType::Float, "temperature.sensor1", timestamps, values);
    writer.writeIndex();
    writer.close();
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

TEST_F(TSMTest, BasicTSMWriterBoolean) {
    std::string filename = getTestFilePath("test_bool.tsm");
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<bool> values;
    
    uint64_t baseTime = 1600000000000;
    for (int i = 0; i < 500; i++) {
        timestamps.push_back(baseTime + i * 1000);
        values.push_back(i % 2 == 0);
    }
    
    writer.writeSeries(TSMValueType::Boolean, "status.online", timestamps, values);
    writer.writeIndex();
    writer.close();
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

TEST_F(TSMTest, WriteMultipleSeries) {
    std::string filename = getTestFilePath("test_multi.tsm");
    
    TSMWriter writer(filename);
    
    uint64_t baseTime = 1600000000000;
    
    // Series 1: Temperature
    std::vector<uint64_t> tempTimestamps;
    std::vector<double> tempValues;
    for (int i = 0; i < 100; i++) {
        tempTimestamps.push_back(baseTime + i * 1000);
        tempValues.push_back(20.0 + i * 0.1);
    }
    writer.writeSeries(TSMValueType::Float, "temperature.room1", tempTimestamps, tempValues);
    
    // Series 2: Humidity
    std::vector<uint64_t> humidTimestamps;
    std::vector<double> humidValues;
    for (int i = 0; i < 100; i++) {
        humidTimestamps.push_back(baseTime + i * 1000);
        humidValues.push_back(45.0 + i * 0.2);
    }
    writer.writeSeries(TSMValueType::Float, "humidity.room1", humidTimestamps, humidValues);
    
    // Series 3: Door status
    std::vector<uint64_t> doorTimestamps;
    std::vector<bool> doorValues;
    for (int i = 0; i < 50; i++) {
        doorTimestamps.push_back(baseTime + i * 5000);
        doorValues.push_back(i % 3 == 0);
    }
    writer.writeSeries(TSMValueType::Boolean, "door.status", doorTimestamps, doorValues);
    
    writer.writeIndex();
    writer.close();
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

TEST_F(TSMTest, LargeDataset) {
    std::string filename = getTestFilePath("test_large.tsm");
    
    const int NUM_POINTS = 100000;
    uint64_t baseTime = 1600000000000;
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    std::default_random_engine generator;
    std::normal_distribution<double> distribution(100.0, 10.0);
    
    for (int i = 0; i < NUM_POINTS; i++) {
        timestamps.push_back(baseTime + i * 100);
        values.push_back(distribution(generator));
    }
    
    writer.writeSeries(TSMValueType::Float, "metrics.large", timestamps, values);
    writer.writeIndex();
    writer.close();
    
    // Verify file was created and has reasonable size
    EXPECT_TRUE(fs::exists(filename));
    auto fileSize = fs::file_size(filename);
    EXPECT_GT(fileSize, 1000); // Should be at least 1KB
    
    // Check compression ratio (rough estimate)
    // Uncompressed would be ~1.6MB (100k * 16 bytes per point)
    // Good compression should get it under 1MB
    EXPECT_LT(fileSize, 1000000);
}

TEST_F(TSMTest, BlockBoundaries) {
    std::string filename = getTestFilePath("test_blocks.tsm");
    
    // Test data that spans multiple blocks (MaxPointsPerBlock = 10000)
    const int NUM_POINTS = 25000; // Should create 3 blocks
    
    TSMWriter writer(filename);
    
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    uint64_t baseTime = 1600000000000;
    for (int i = 0; i < NUM_POINTS; i++) {
        timestamps.push_back(baseTime + i * 1000);
        values.push_back(i * 1.0);
    }
    
    writer.writeSeries(TSMValueType::Float, "metrics.blocks", timestamps, values);
    writer.writeIndex();
    writer.close();
    
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

// Note: Tests that require reading TSM files would need Seastar runtime
// Example structure for read tests:

/*
TEST_F(TSMTest, ReadTSMFile) {
    // This would require Seastar app context to run properly
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    app.run(1, argv, [this]() -> seastar::future<> {
        std::string filename = getTestFilePath("test_read.tsm");
        
        // Write test data first
        {
            TSMWriter writer(filename);
            std::vector<uint64_t> timestamps = {1000, 2000, 3000};
            std::vector<double> values = {1.0, 2.0, 3.0};
            writer.writeSeries(TSMValueType::Float, "test.series", timestamps, values);
            writer.writeIndex();
            writer.close();
        }
        
        // Read back
        TSM tsm(filename);
        co_await tsm.open();
        co_await tsm.readIndex();
        
        // Verify series type
        std::string seriesKey = "test.series";
        auto seriesType = tsm.getSeriesType(seriesKey);
        EXPECT_TRUE(seriesType.has_value());
        EXPECT_EQ(seriesType.value(), TSMValueType::Float);
        
        co_return;
    });
}
*/