#include <gtest/gtest.h>
#include <filesystem>
#include <vector>
#include <string>

#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/tsdb_value.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace fs = std::filesystem;

class TSMStringTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_string_files";
    
    void SetUp() override {
        fs::create_directories(testDir);
    }
    
    void TearDown() override {
        fs::remove_all(testDir);
    }
    
    std::string getTestFilePath(const std::string& filename) {
        return testDir + "/" + filename;
    }
    
    // Helper function for safe TSM operations with automatic cleanup
    template<typename Func>
    int runTSMTest(const std::string& filename, Func&& func) {
        seastar::app_template app;
        char* argv[] = {(char*)"test"};
        
        return app.run(1, argv, [filename, func = std::forward<Func>(func)]() -> seastar::future<int> {
            auto tsm = seastar::make_shared<TSM>(filename);
            bool fileOpened = false;
            
            try {
                co_await tsm->open();
                fileOpened = true;
                co_await func(tsm);
            } catch (...) {
                // Will close outside try/catch
                throw;
            }
            
            if (fileOpened) {
                co_await tsm->close();
            }
            
            co_return 0;
        });
    }
};

TEST_F(TSMStringTest, WriteAndReadStringData) {
    std::string filename = getTestFilePath("0_1001.tsm");
    
    // Write string data
    {
        TSMWriter writer(filename);
        
        std::vector<uint64_t> timestamps;
        std::vector<std::string> values;
        
        uint64_t baseTime = 1600000000000;
        values.push_back("sensor_starting");
        timestamps.push_back(baseTime);
        
        values.push_back("sensor_running");
        timestamps.push_back(baseTime + 1000);
        
        values.push_back("sensor_warning");
        timestamps.push_back(baseTime + 2000);
        
        values.push_back("sensor_error");
        timestamps.push_back(baseTime + 3000);
        
        values.push_back("sensor_stopped");
        timestamps.push_back(baseTime + 4000);
        
        writer.writeSeries(TSMValueType::String, "device.status", timestamps, values);
        writer.writeIndex();
        writer.close();
    }
    
    // Verify file was created
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
    
    // Read and verify string data using seastar
    EXPECT_EQ(runTSMTest(filename, [](seastar::shared_ptr<TSM> tsm) -> seastar::future<> {
        // Read the string series
        TSMResult<std::string> result(0);
        co_await tsm->template readSeries<std::string>("device.status", 0, UINT64_MAX, result);
        
        // Verify we got the expected values
        auto [timestamps, values] = result.getAllData();
        EXPECT_EQ(timestamps.size(), 5);
        EXPECT_EQ(values.size(), 5);
        
        if (values.size() >= 5) {
            EXPECT_EQ(values[0], "sensor_starting");
            EXPECT_EQ(values[1], "sensor_running");
            EXPECT_EQ(values[2], "sensor_warning");
            EXPECT_EQ(values[3], "sensor_error");
            EXPECT_EQ(values[4], "sensor_stopped");
        }
        
        co_return;
    }), 0);
}

TEST_F(TSMStringTest, WriteMixedDataTypes) {
    std::string filename = getTestFilePath("0_1002.tsm");
    
    {
        TSMWriter writer(filename);
        
        uint64_t baseTime = 1600000000000;
        
        // Write float data
        std::vector<uint64_t> floatTimestamps;
        std::vector<double> floatValues;
        for (int i = 0; i < 10; i++) {
            floatTimestamps.push_back(baseTime + i * 1000);
            floatValues.push_back(20.0 + i * 0.5);
        }
        writer.writeSeries(TSMValueType::Float, "temperature", floatTimestamps, floatValues);
        
        // Write string data
        std::vector<uint64_t> stringTimestamps;
        std::vector<std::string> stringValues;
        stringValues.push_back("normal");
        stringTimestamps.push_back(baseTime);
        stringValues.push_back("warning");
        stringTimestamps.push_back(baseTime + 5000);
        stringValues.push_back("critical");
        stringTimestamps.push_back(baseTime + 10000);
        
        writer.writeSeries(TSMValueType::String, "status", stringTimestamps, stringValues);
        
        // Write boolean data
        std::vector<uint64_t> boolTimestamps;
        std::vector<bool> boolValues;
        for (int i = 0; i < 10; i++) {
            boolTimestamps.push_back(baseTime + i * 1000);
            boolValues.push_back(i % 2 == 0);
        }
        writer.writeSeries(TSMValueType::Boolean, "enabled", boolTimestamps, boolValues);
        
        writer.writeIndex();
        writer.close();
    }
    
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
    
    // Read and verify mixed data types using seastar
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    EXPECT_EQ(app.run(1, argv, [filename]() -> seastar::future<int> {
        auto tsm = seastar::make_shared<TSM>(filename);
        co_await tsm->open();
        
        // Read float series
        TSMResult<double> floatResult(0);
        co_await tsm->readSeries<double>("temperature", 0, UINT64_MAX, floatResult);
        auto [floatTimestamps, floatValues] = floatResult.getAllData();
        EXPECT_EQ(floatValues.size(), 10);
        if (!floatValues.empty()) {
            EXPECT_DOUBLE_EQ(floatValues[0], 20.0);
            EXPECT_DOUBLE_EQ(floatValues[9], 24.5);
        }
        
        // Read string series
        TSMResult<std::string> stringResult(0);
        co_await tsm->readSeries<std::string>("status", 0, UINT64_MAX, stringResult);
        auto [stringTimestamps, stringValues] = stringResult.getAllData();
        EXPECT_EQ(stringValues.size(), 3);
        if (stringValues.size() >= 3) {
            EXPECT_EQ(stringValues[0], "normal");
            EXPECT_EQ(stringValues[1], "warning");
            EXPECT_EQ(stringValues[2], "critical");
        }
        
        // Read boolean series
        TSMResult<bool> boolResult(0);
        co_await tsm->readSeries<bool>("enabled", 0, UINT64_MAX, boolResult);
        auto [boolTimestamps, boolValues] = boolResult.getAllData();
        EXPECT_EQ(boolValues.size(), 10);
        if (boolValues.size() >= 2) {
            EXPECT_TRUE(boolValues[0]);   // 0 % 2 == 0
            EXPECT_FALSE(boolValues[1]);  // 1 % 2 != 0
        }
        
        co_return 0;
    }), 0);
}

TEST_F(TSMStringTest, LargeStringDataset) {
    std::string filename = getTestFilePath("0_1003.tsm");
    
    {
        TSMWriter writer(filename);
        
        std::vector<uint64_t> timestamps;
        std::vector<std::string> values;
        
        uint64_t baseTime = 1600000000000;
        
        // Generate log-like messages
        for (int i = 0; i < 1000; i++) {
            timestamps.push_back(baseTime + i * 100);
            
            std::string logMsg = "[" + std::to_string(i) + "] ";
            if (i % 10 == 0) {
                logMsg += "ERROR: Failed to connect to database server at 192.168.1." + std::to_string(i % 256);
            } else if (i % 5 == 0) {
                logMsg += "WARNING: High memory usage detected: " + std::to_string(80 + i % 20) + "%";
            } else {
                logMsg += "INFO: Request processed successfully in " + std::to_string(10 + i % 90) + "ms";
            }
            values.push_back(logMsg);
        }
        
        writer.writeSeries(TSMValueType::String, "application.logs", timestamps, values);
        writer.writeIndex();
        writer.close();
    }
    
    auto fileSize = fs::file_size(filename);
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fileSize, 1000); // Should be at least 1KB
    
    // Calculate uncompressed size
    size_t uncompressedSize = 0;
    for (int i = 0; i < 1000; i++) {
        uncompressedSize += 8; // timestamp
        uncompressedSize += 4; // string length prefix
        uncompressedSize += 50; // approximate string length
    }
    
    // Should achieve compression
    EXPECT_LT(fileSize, uncompressedSize);
    
    // Read and verify large string dataset using seastar
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    EXPECT_EQ(app.run(1, argv, [filename]() -> seastar::future<int> {
        auto tsm = seastar::make_shared<TSM>(filename);
        co_await tsm->open();
        
        // Read the large string series
        TSMResult<std::string> result(0);
        co_await tsm->readSeries<std::string>("application.logs", 0, UINT64_MAX, result);
        
        // Verify we got the expected count
        auto [timestamps, values] = result.getAllData();
        EXPECT_EQ(timestamps.size(), 1000);
        EXPECT_EQ(values.size(), 1000);
        
        // Verify some specific entries
        if (values.size() >= 100) {
            // Check first entry (0 % 10 == 0, so it's an ERROR)
            EXPECT_TRUE(values[0].starts_with("[0] ERROR: Failed to connect"));
            
            // Check an error entry (every 10th)
            EXPECT_TRUE(values[10].starts_with("[10] ERROR: Failed to connect"));
            
            // Check a warning entry (every 5th that's not an error)
            EXPECT_TRUE(values[5].starts_with("[5] WARNING: High memory usage"));
            
            // Check a regular info entry
            EXPECT_TRUE(values[1].starts_with("[1] INFO: Request processed"));
        }
        
        co_return 0;
    }), 0);
}

TEST_F(TSMStringTest, EmptyAndSpecialStrings) {
    std::string filename = getTestFilePath("0_1004.tsm");
    
    {
        TSMWriter writer(filename);
        
        std::vector<uint64_t> timestamps;
        std::vector<std::string> values;
        
        uint64_t baseTime = 1600000000000;
        
        // Mix of empty, special character, and normal strings
        values.push_back("");
        timestamps.push_back(baseTime);
        
        values.push_back("Line1\nLine2\nLine3");
        timestamps.push_back(baseTime + 1000);
        
        values.push_back("\t\t\tTabbed");
        timestamps.push_back(baseTime + 2000);
        
        values.push_back("Path: /usr/local/bin");
        timestamps.push_back(baseTime + 3000);
        
        values.push_back("");
        timestamps.push_back(baseTime + 4000);
        
        values.push_back("JSON: {\"key\":\"value\"}");
        timestamps.push_back(baseTime + 5000);
        
        writer.writeSeries(TSMValueType::String, "special.strings", timestamps, values);
        writer.writeIndex();
        writer.close();
    }
    
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 0);
    
    // Read and verify special character strings with seastar
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    EXPECT_EQ(app.run(1, argv, [filename]() -> seastar::future<int> {
        auto tsm = seastar::make_shared<TSM>(filename);
        co_await tsm->open();
        
        // Read the special strings series
        TSMResult<std::string> result(0);
        co_await tsm->readSeries<std::string>("special.strings", 0, UINT64_MAX, result);
        
        auto [timestamps, values] = result.getAllData();
        EXPECT_EQ(timestamps.size(), 6);
        EXPECT_EQ(values.size(), 6);
        
        // Verify special character handling
        if (values.size() >= 6) {
            EXPECT_EQ(values[0], "");  // Empty string
            EXPECT_EQ(values[1], "Line1\nLine2\nLine3");  // Newlines
            EXPECT_EQ(values[2], "\t\t\tTabbed");  // Tabs
            EXPECT_EQ(values[3], "Path: /usr/local/bin");  // Path
            EXPECT_EQ(values[4], "");  // Another empty string
            EXPECT_EQ(values[5], "JSON: {\"key\":\"value\"}");  // JSON with quotes
        }
        
        co_return 0;
    }), 0);
}

TEST_F(TSMStringTest, StringBlockBoundaries) {
    std::string filename = getTestFilePath("0_1005.tsm");
    
    {
        TSMWriter writer(filename);
        
        // Create enough strings to span multiple blocks (MaxPointsPerBlock = 10000)
        std::vector<uint64_t> timestamps;
        std::vector<std::string> values;
        
        uint64_t baseTime = 1600000000000;
        
        for (int i = 0; i < 25000; i++) {
            timestamps.push_back(baseTime + i * 100);
            values.push_back("Entry_" + std::to_string(i));
        }
        
        writer.writeSeries(TSMValueType::String, "large.dataset", timestamps, values);
        writer.writeIndex();
        writer.close();
    }
    
    EXPECT_TRUE(fs::exists(filename));
    EXPECT_GT(fs::file_size(filename), 10000); // Should be reasonably large
    
    // Test reading large dataset that spans multiple blocks
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    EXPECT_EQ(app.run(1, argv, [filename]() -> seastar::future<int> {
        auto tsm = seastar::make_shared<TSM>(filename);
        co_await tsm->open();
        
        // Read the entire large dataset
        TSMResult<std::string> result(0);
        co_await tsm->readSeries<std::string>("large.dataset", 0, UINT64_MAX, result);
        
        auto [timestamps, values] = result.getAllData();
        EXPECT_EQ(timestamps.size(), 25000);
        EXPECT_EQ(values.size(), 25000);
        
        // Verify some entries across different blocks
        if (values.size() >= 25000) {
            EXPECT_EQ(values[0], "Entry_0");
            EXPECT_EQ(values[1000], "Entry_1000");
            EXPECT_EQ(values[10000], "Entry_10000");  // Different block
            EXPECT_EQ(values[20000], "Entry_20000");  // Different block
            EXPECT_EQ(values[24999], "Entry_24999");  // Last entry
        }
        
        // Verify timestamp ordering
        for (size_t i = 1; i < timestamps.size(); i++) {
            EXPECT_GT(timestamps[i], timestamps[i-1]); // Should be sorted
        }
        
        co_return 0;
    }), 0);
}

TEST_F(TSMStringTest, MemoryStoreWithStrings) {
    // Test that memory store can handle string inserts
    auto store = std::make_shared<MemoryStore>(1);
    
    TSDBInsert<std::string> insert("events", "description");
    insert.addValue(1000, "System started");
    insert.addValue(2000, "User logged in");
    insert.addValue(3000, "File uploaded");
    insert.addValue(4000, "Task completed");
    insert.addValue(5000, "System shutdown");
    
    store->insertMemory(insert);
    
    // Verify series exists
    auto seriesKey = insert.seriesKey();
    auto seriesType = store->getSeriesType(seriesKey);
    
    ASSERT_TRUE(seriesType.has_value());
    EXPECT_EQ(seriesType.value(), TSMValueType::String);
    
    // Verify data was inserted
    auto it = store->series.find(seriesKey);
    ASSERT_NE(it, store->series.end());
    
    auto& seriesData = std::get<InMemorySeries<std::string>>(it->second);
    EXPECT_EQ(seriesData.timestamps.size(), 5);
    EXPECT_EQ(seriesData.values.size(), 5);
    EXPECT_EQ(seriesData.values[0], "System started");
    EXPECT_EQ(seriesData.values[4], "System shutdown");
}

TEST_F(TSMStringTest, StringSeriesCompression) {
    std::string filename = getTestFilePath("0_1006.tsm");
    uint64_t baseTime = 1600000000000;
    
    {
        TSMWriter writer(filename);
        
        std::vector<uint64_t> timestamps;
        std::vector<std::string> values;
        
        // Create repetitive strings that should compress well
        for (int i = 0; i < 1000; i++) {
            timestamps.push_back(baseTime + i * 100);
            
            // Repetitive pattern
            std::string status;
            switch(i % 4) {
                case 0: status = "SENSOR_OK: All systems operational"; break;
                case 1: status = "SENSOR_OK: All systems operational"; break;
                case 2: status = "SENSOR_WARN: Temperature slightly elevated"; break;
                case 3: status = "SENSOR_OK: All systems operational"; break;
            }
            values.push_back(status);
        }
        
        writer.writeSeries(TSMValueType::String, "sensor.status", timestamps, values);
        writer.writeIndex();
        writer.close();
    }
    
    auto fileSize = fs::file_size(filename);
    EXPECT_TRUE(fs::exists(filename));
    
    // With repetitive data, should achieve good compression
    // Uncompressed would be ~40KB (1000 * ~40 bytes per string)
    EXPECT_LT(fileSize, 20000); // Should be less than 20KB with compression
    
    // Test time-range reading with seastar
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    EXPECT_EQ(app.run(1, argv, [&]() -> seastar::future<int> {
        auto tsm = seastar::make_shared<TSM>(filename);
        co_await tsm->open();
        
        // Read first half of the data (first 500 entries)
        uint64_t endTime = baseTime + 499 * 100; // Exclude the 500th entry
        TSMResult<std::string> result(0);
        co_await tsm->readSeries<std::string>("sensor.status", baseTime, endTime, result);
        
        auto [timestamps, values] = result.getAllData();
        EXPECT_EQ(timestamps.size(), 500); // Should be exactly 500
        EXPECT_EQ(timestamps.size(), values.size());
        
        // Verify all timestamps are in range
        for (auto timestamp : timestamps) {
            EXPECT_GE(timestamp, baseTime);
            EXPECT_LE(timestamp, endTime);
        }
        
        // Verify some values match expected pattern
        if (!values.empty()) {
            // Most should be "SENSOR_OK: All systems operational"
            int okCount = 0;
            for (const auto& value : values) {
                if (value == "SENSOR_OK: All systems operational") {
                    okCount++;
                }
            }
            EXPECT_GT(okCount, values.size() * 0.6); // At least 60% should be OK status
        }
        
        co_return 0;
    }), 0);
}