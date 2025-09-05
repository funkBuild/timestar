#include <gtest/gtest.h>
#include <filesystem>
#include <memory>

#include "../../../lib/storage/wal.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/tsdb_value.hpp"

namespace fs = std::filesystem;

class WALTest : public ::testing::Test {
protected:
    std::string testDir = "./test_wal_files";
    
    void SetUp() override {
        fs::create_directories(testDir);
        // Change to test directory for WAL files
        fs::current_path(testDir);
    }
    
    void TearDown() override {
        // Change back to parent directory
        fs::current_path("..");
        fs::remove_all(testDir);
    }
};

TEST_F(WALTest, SequenceNumberToFilename) {
    EXPECT_EQ(WAL::sequenceNumberToFilename(1), "shard_0/0000000001.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(42), "shard_0/0000000042.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(999), "shard_0/0000000999.wal");
    EXPECT_EQ(WAL::sequenceNumberToFilename(12345678), "shard_0/0012345678.wal");
}

// Note: The following tests would require Seastar runtime to work properly.
// They are provided as examples of how to test WAL functionality.
// In practice, these would need to be run within a Seastar application context.

/*
Example test structure for WAL write and recovery:

TEST_F(WALTest, WriteAndRecoverFloatData) {
    seastar::app_template app;
    char* argv[] = {(char*)"test"};
    
    app.run(1, argv, []() -> seastar::future<> {
        unsigned int sequenceNumber = 1;
        auto store = std::make_shared<MemoryStore>(sequenceNumber);
        
        // Write to WAL
        {
            WAL wal(sequenceNumber);
            co_await wal.init(store.get());
            
            TSDBInsert<double> insert("temperature", "sensor1");
            insert.addValue(1000, 20.5);
            insert.addValue(2000, 21.0);
            insert.addValue(3000, 21.5);
            
            co_await wal.insert(insert);
            co_await wal.close();
        }
        
        // Create new store and recover from WAL
        auto recoveredStore = std::make_shared<MemoryStore>(sequenceNumber);
        {
            std::string walFile = WAL::sequenceNumberToFilename(sequenceNumber);
            WALReader reader(walFile);
            co_await reader.readAll(recoveredStore.get());
        }
        
        // Verify recovered data
        std::string seriesKey = "temperature sensor1";
        auto it = recoveredStore->series.find(seriesKey);
        if (it != recoveredStore->series.end()) {
            auto& seriesData = std::get<InMemorySeries<double>>(it->second);
            EXPECT_EQ(seriesData.values.size(), 3);
            EXPECT_DOUBLE_EQ(seriesData.values[0], 20.5);
        }
        
        co_return;
    });
}
*/