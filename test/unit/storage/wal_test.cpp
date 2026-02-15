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