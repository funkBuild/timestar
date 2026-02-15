#include <gtest/gtest.h>
#include <filesystem>
#include <random>
#include <chrono>

#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/compressed_buffer.hpp"
#include "../../../lib/encoding/float_encoder.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

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
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("temperature.sensor1");
    writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
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
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("status.online");
    writer.writeSeries(TSMValueType::Boolean, seriesId, timestamps, values);
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
    SeriesId128 tempSeriesId = SeriesId128::fromSeriesKey("temperature.room1");
    writer.writeSeries(TSMValueType::Float, tempSeriesId, tempTimestamps, tempValues);
    
    // Series 2: Humidity
    std::vector<uint64_t> humidTimestamps;
    std::vector<double> humidValues;
    for (int i = 0; i < 100; i++) {
        humidTimestamps.push_back(baseTime + i * 1000);
        humidValues.push_back(45.0 + i * 0.2);
    }
    SeriesId128 humidSeriesId = SeriesId128::fromSeriesKey("humidity.room1");
    writer.writeSeries(TSMValueType::Float, humidSeriesId, humidTimestamps, humidValues);
    
    // Series 3: Door status
    std::vector<uint64_t> doorTimestamps;
    std::vector<bool> doorValues;
    for (int i = 0; i < 50; i++) {
        doorTimestamps.push_back(baseTime + i * 5000);
        doorValues.push_back(i % 3 == 0);
    }
    SeriesId128 doorSeriesId = SeriesId128::fromSeriesKey("door.status");
    writer.writeSeries(TSMValueType::Boolean, doorSeriesId, doorTimestamps, doorValues);
    
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
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("metrics.large");
    writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
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
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey("metrics.blocks");
    writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
    writer.writeIndex();
    writer.close();
    
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
}

// TSM Reference Counting Tests

TEST_F(TSMTest, ReferenceCountingBasic) {
    std::string filename = getTestFilePath("0_9.tsm");  // tier=0, seq=9

    // Write a minimal file
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.ref");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    // Test reference counting (synchronous operations)
    TSM tsm(filename);

    // Initial ref count should be 0
    EXPECT_EQ(tsm.getRefCount(), 0);

    // Add references
    tsm.addRef();
    EXPECT_EQ(tsm.getRefCount(), 1);

    tsm.addRef();
    EXPECT_EQ(tsm.getRefCount(), 2);

    // Release references
    tsm.releaseRef();
    EXPECT_EQ(tsm.getRefCount(), 1);

    tsm.releaseRef();
    EXPECT_EQ(tsm.getRefCount(), 0);
}

TEST_F(TSMTest, MarkForDeletion) {
    std::string filename = getTestFilePath("0_10.tsm");  // tier=0, seq=10

    // Write a minimal file
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.mark");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);

    // Initially not scheduled for deletion
    EXPECT_FALSE(tsm.scheduleDeletionFlag);

    // Mark for deletion with no references
    tsm.markForDeletion();
    EXPECT_TRUE(tsm.scheduleDeletionFlag);
}

TEST_F(TSMTest, MarkForDeletionWithReferences) {
    std::string filename = getTestFilePath("0_11.tsm");  // tier=0, seq=11

    // Write a minimal file
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.mark_refs");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);

    // Add a reference
    tsm.addRef();
    EXPECT_EQ(tsm.getRefCount(), 1);

    // Mark for deletion
    tsm.markForDeletion();

    // Should not be scheduled yet because there's a reference
    EXPECT_FALSE(tsm.scheduleDeletionFlag);

    // Release the reference
    tsm.releaseRef();

    // Now should be scheduled for deletion
    EXPECT_TRUE(tsm.scheduleDeletionFlag);
}

// getValueType tests

TEST_F(TSMTest, GetValueTypeDouble) {
    EXPECT_EQ(TSM::getValueType<double>(), TSMValueType::Float);
}

TEST_F(TSMTest, GetValueTypeBool) {
    EXPECT_EQ(TSM::getValueType<bool>(), TSMValueType::Boolean);
}

TEST_F(TSMTest, GetValueTypeString) {
    EXPECT_EQ(TSM::getValueType<std::string>(), TSMValueType::String);
}

// rankAsInteger tests

TEST_F(TSMTest, RankAsIntegerTier0) {
    std::string filename = getTestFilePath("0_42.tsm");
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.rank0");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    // tierNum=0, seqNum=42 -> (0 << 60) + 42 = 42
    EXPECT_EQ(tsm.rankAsInteger(), 42);
}

TEST_F(TSMTest, RankAsIntegerTier1) {
    std::string filename = getTestFilePath("1_5.tsm");
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.rank1");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    // tierNum=1, seqNum=5 -> (1 << 60) + 5
    uint64_t expected = (uint64_t(1) << 60) + 5;
    EXPECT_EQ(tsm.rankAsInteger(), expected);
}

TEST_F(TSMTest, RankAsIntegerMaxValidTier) {
    std::string filename = getTestFilePath("15_1.tsm");
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.rank15");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    // tierNum=15, seqNum=1 -> (15 << 60) + 1
    uint64_t expected = (uint64_t(15) << 60) + 1;
    EXPECT_EQ(tsm.rankAsInteger(), expected);
}

TEST_F(TSMTest, RankAsIntegerOverflowTier) {
    std::string filename = getTestFilePath("16_1.tsm");
    {
        TSMWriter writer(filename);
        std::vector<uint64_t> timestamps = {1000};
        std::vector<double> values = {1.0};
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("test.rank16");
        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(filename);
    // tierNum=16 should throw overflow_error
    EXPECT_THROW(tsm.rankAsInteger(), std::overflow_error);
}