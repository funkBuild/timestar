#include "../../../lib/storage/shard_store_startup.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>

namespace fs = std::filesystem;

namespace {

using timestar::ShardStoreInspection;
using timestar::ShardStoreStartup;
using timestar::ShardStoreStartupStatus;
using timestar::StorageLayout;

class ShardStoreStartupTest : public ::testing::Test {
protected:
    fs::path testDir;

    void SetUp() override {
        static std::atomic_uint64_t sequence{0};
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        testDir = fs::temp_directory_path() / (std::string("timestar_shard_startup_") + std::to_string(::getpid()) +
                                               "_" + std::to_string(sequence.fetch_add(1)) + "_" + info->name());
        fs::remove_all(testDir);
        fs::create_directories(testDir);
    }

    void TearDown() override { fs::remove_all(testDir); }

    void writeFile(const fs::path& relativePath, const std::string& contents) {
        const auto path = testDir / relativePath;
        if (!path.parent_path().empty())
            fs::create_directories(path.parent_path());
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output) << path;
        output << contents;
    }

    // Create `count` populated shard directories (tsm + native_index present).
    void createShards(unsigned count) {
        for (unsigned shard = 0; shard < count; ++shard) {
            fs::create_directories(testDir / ("shard_" + std::to_string(shard)) / "tsm");
            fs::create_directories(testDir / ("shard_" + std::to_string(shard)) / "native_index");
        }
    }

    ShardStoreInspection inspect(unsigned count) const {
        return ShardStoreStartup{StorageLayout(testDir)}.inspect(count);
    }

    std::set<std::string> rootEntries() const {
        std::set<std::string> names;
        for (const auto& entry : fs::directory_iterator(testDir))
            names.insert(entry.path().filename().string());
        return names;
    }
};

TEST_F(ShardStoreStartupTest, FreshEmptyRootStartsAndRecordsCountOnCommit) {
    ShardStoreStartup startup{StorageLayout(testDir)};
    const auto inspection = startup.inspect(4);

    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::FreshStore);
    EXPECT_TRUE(inspection.canStart());
    EXPECT_TRUE(inspection.isFresh());
    // Inspection alone writes nothing.
    EXPECT_TRUE(rootEntries().empty());

    createShards(4);
    startup.commitAfterInitialization(inspection);
    EXPECT_TRUE(fs::exists(testDir / "shard_count.meta"));

    // A restart at the same count now matches.
    const auto restart = inspect(4);
    EXPECT_EQ(restart.status, ShardStoreStartupStatus::MatchingShardCount);
    EXPECT_EQ(restart.previousShardCount, 4u);
}

TEST_F(ShardStoreStartupTest, MatchingRecordedCountStartsNormally) {
    createShards(2);
    writeFile("shard_count.meta", "2\n");

    const auto inspection = inspect(2);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::MatchingShardCount);
    EXPECT_TRUE(inspection.canStart());
}

TEST_F(ShardStoreStartupTest, ChangedCountWithRecordedMetaFailsClosed) {
    createShards(4);
    writeFile("shard_count.meta", "4\n");

    const auto before = rootEntries();
    const auto inspection = inspect(2);

    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::UnsafeShardCountChange);
    EXPECT_FALSE(inspection.canStart());
    EXPECT_EQ(inspection.previousShardCount, 4u);
    EXPECT_EQ(inspection.requestedShardCount, 2u);
    // The failing inspection renamed or created nothing.
    EXPECT_EQ(rootEntries(), before);

    const auto message = ShardStoreStartup{StorageLayout(testDir)}.failureMessage(inspection);
    EXPECT_NE(message.find(testDir.string()), std::string::npos) << message;
    EXPECT_NE(message.find('4'), std::string::npos) << message;
    EXPECT_NE(message.find('2'), std::string::npos) << message;
}

TEST_F(ShardStoreStartupTest, ChangedCountInferredFromShardDirsFailsClosedWithoutMeta) {
    createShards(4);  // No shard_count.meta on disk.

    const auto inspection = inspect(8);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::UnsafeShardCountChange);
    EXPECT_EQ(inspection.previousShardCount, 4u);
}

TEST_F(ShardStoreStartupTest, MatchingCountInferredFromShardDirsStartsWithoutMeta) {
    createShards(4);

    const auto inspection = inspect(4);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::MatchingShardCount);
    EXPECT_EQ(inspection.previousShardCount, 4u);
}

TEST_F(ShardStoreStartupTest, RecordedCountThatDisagreesWithShardDirsFailsClosed) {
    // A committed store always has exactly shard_0..shard_{N-1}. A meta that
    // records more shards than exist on disk is a partial/corrupt store; even
    // when the requested count matches the meta, startup must refuse rather than
    // let Engine create the missing shards fresh beside the populated ones.
    createShards(2);
    writeFile("shard_count.meta", "4\n");

    const auto inspection = inspect(4);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_FALSE(inspection.canStart());
}

TEST_F(ShardStoreStartupTest, RecordedCountWithNoShardDirsFailsClosed) {
    writeFile("shard_count.meta", "2\n");  // meta present, but no shard data at all

    const auto inspection = inspect(2);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_FALSE(inspection.canStart());
}

TEST_F(ShardStoreStartupTest, MalformedMetaOverShardDataFailsClosed) {
    createShards(2);
    writeFile("shard_count.meta", "not-a-number\n");

    const auto inspection = inspect(2);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::InvalidMetadata);
    EXPECT_FALSE(inspection.canStart());
}

TEST_F(ShardStoreStartupTest, MalformedMetaWithoutShardDataIsTreatedAsFresh) {
    writeFile("shard_count.meta", "garbage");

    const auto inspection = inspect(2);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::FreshStore);
}

TEST_F(ShardStoreStartupTest, NonContiguousShardDirsAreInvalidWithoutMeta) {
    fs::create_directories(testDir / "shard_0" / "tsm");
    fs::create_directories(testDir / "shard_2" / "tsm");  // gap at shard_1

    const auto inspection = inspect(3);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::InvalidMetadata);
}

TEST_F(ShardStoreStartupTest, InterruptedRebalanceStateFileFailsClosed) {
    createShards(2);
    writeFile("shard_count.meta", "2\n");
    writeFile("rebalance.state", "1 2 4\n");  // phase InProgress, 2 -> 4

    const auto before = rootEntries();
    const auto inspection = inspect(2);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::InterruptedLegacyRebalance);
    EXPECT_FALSE(inspection.canStart());
    EXPECT_EQ(rootEntries(), before);
}

TEST_F(ShardStoreStartupTest, LeftoverRebalanceStagingDirectoryFailsClosed) {
    createShards(2);
    writeFile("shard_count.meta", "2\n");
    fs::create_directories(testDir / "shard_1_new" / "tsm");

    const auto inspection = inspect(2);
    EXPECT_EQ(inspection.status, ShardStoreStartupStatus::InterruptedLegacyRebalance);
}

TEST_F(ShardStoreStartupTest, CommitIsRejectedForARefusedInspection) {
    createShards(4);
    writeFile("shard_count.meta", "4\n");
    ShardStoreStartup startup{StorageLayout(testDir)};
    const auto inspection = startup.inspect(2);
    ASSERT_FALSE(inspection.canStart());
    EXPECT_THROW(startup.commitAfterInitialization(inspection), std::logic_error);
}

TEST_F(ShardStoreStartupTest, SessionInspectsAtConstructionAndCommitsOnce) {
    createShards(2);
    writeFile("shard_count.meta", "2\n");

    timestar::ShardStoreStartupSession session(StorageLayout(testDir), 2);
    EXPECT_TRUE(session.canStart());
    EXPECT_EQ(session.inspection().status, ShardStoreStartupStatus::MatchingShardCount);
    session.commitEngineInitialization();
    session.commitEngineInitialization();  // idempotent, no throw
    EXPECT_TRUE(fs::exists(testDir / "shard_count.meta"));
}

}  // namespace
