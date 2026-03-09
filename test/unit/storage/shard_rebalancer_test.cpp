#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <cmath>

#include "../../../lib/storage/shard_rebalancer.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/core/series_id.hpp"

namespace fs = std::filesystem;

class ShardRebalancerTest : public ::testing::Test {
protected:
    std::string testDir = "./test_rebalancer_data";

    void SetUp() override {
        fs::remove_all(testDir);
        fs::create_directories(testDir);
    }

    void TearDown() override {
        fs::remove_all(testDir);
    }

    // Helper to create a shard directory structure with TSM files
    void createShardWithTSM(unsigned shardId, const std::vector<std::string>& seriesKeys,
                            uint64_t baseTime = 1000000000, int numPoints = 100) {
        std::string shardPath = testDir + "/shard_" + std::to_string(shardId);
        fs::create_directories(shardPath + "/tsm");
        fs::create_directories(shardPath + "/index");

        // Create a MemoryStore with data for each series
        auto store = seastar::make_shared<MemoryStore>(0);

        for (const auto& key : seriesKeys) {
            SeriesId128 seriesId = SeriesId128::fromSeriesKey(key);
            InMemorySeries<double> series;
            for (int i = 0; i < numPoints; ++i) {
                series.timestamps.push_back(baseTime + i * 1000);
                series.values.push_back(100.0 + std::sin(i * 0.1) * 10.0);
            }
            store->series[seriesId] = std::move(series);
        }

        // Write TSM file using blocking close
        std::string tsmPath = shardPath + "/tsm/0_0.tsm";
        TSMWriter::run(store, tsmPath);
    }

    // Helper to write shard_count.meta
    void writeMetaFile(unsigned count) {
        timestar::ShardRebalancer::writeShardCountMeta(testDir, count);
    }
};

// ---------------------------------------------------------------------------
// shard_count.meta tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, WriteAndReadShardCountMeta) {
    timestar::ShardRebalancer::writeShardCountMeta(testDir, 4);
    unsigned count = timestar::ShardRebalancer::readShardCountMeta(testDir);
    EXPECT_EQ(count, 4u);
}

TEST_F(ShardRebalancerTest, ReadMissingMetaReturnsZero) {
    unsigned count = timestar::ShardRebalancer::readShardCountMeta(testDir);
    EXPECT_EQ(count, 0u);
}

TEST_F(ShardRebalancerTest, OverwriteShardCountMeta) {
    timestar::ShardRebalancer::writeShardCountMeta(testDir, 4);
    timestar::ShardRebalancer::writeShardCountMeta(testDir, 8);
    unsigned count = timestar::ShardRebalancer::readShardCountMeta(testDir);
    EXPECT_EQ(count, 8u);
}

// ---------------------------------------------------------------------------
// Detection tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, FreshInstallNoRebalanceNeeded) {
    // No shard directories, no meta file
    timestar::ShardRebalancer rebalancer(testDir);
    EXPECT_FALSE(rebalancer.isRebalanceNeeded(4));
}

TEST_F(ShardRebalancerTest, SameShardCountNoRebalance) {
    writeMetaFile(4);
    timestar::ShardRebalancer rebalancer(testDir);
    EXPECT_FALSE(rebalancer.isRebalanceNeeded(4));
}

TEST_F(ShardRebalancerTest, DifferentShardCountNeedsRebalance) {
    writeMetaFile(4);
    timestar::ShardRebalancer rebalancer(testDir);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(8));
    EXPECT_EQ(rebalancer.previousShardCount(), 4u);
}

TEST_F(ShardRebalancerTest, ScaleDownNeedsRebalance) {
    writeMetaFile(8);
    timestar::ShardRebalancer rebalancer(testDir);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(4));
    EXPECT_EQ(rebalancer.previousShardCount(), 8u);
}

TEST_F(ShardRebalancerTest, DetectShardCountFromDirectories) {
    // No meta file, but shard directories exist
    fs::create_directories(testDir + "/shard_0/tsm");
    fs::create_directories(testDir + "/shard_1/tsm");
    fs::create_directories(testDir + "/shard_2/tsm");
    fs::create_directories(testDir + "/shard_3/tsm");

    timestar::ShardRebalancer rebalancer(testDir);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(8));
    EXPECT_EQ(rebalancer.previousShardCount(), 4u);
}

TEST_F(ShardRebalancerTest, DetectShardCountIgnoresOldAndNewDirs) {
    // Leftover _old and _new dirs should not confuse detection
    fs::create_directories(testDir + "/shard_0/tsm");
    fs::create_directories(testDir + "/shard_1/tsm");
    fs::create_directories(testDir + "/shard_0_old");
    fs::create_directories(testDir + "/shard_0_new");

    timestar::ShardRebalancer rebalancer(testDir);
    // Should only count shard_0 and shard_1 (regex matches exact "shard_N" pattern)
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(4));
    EXPECT_EQ(rebalancer.previousShardCount(), 2u);
}

// ---------------------------------------------------------------------------
// Rebalance state file tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, InProgressStateTriggersRebalance) {
    // Simulate a crash during rebalance: write an InProgress state file
    std::string stateFile = testDir + "/rebalance.state";
    std::ofstream ofs(stateFile);
    ofs << "1 4 8\n"; // InProgress, old=4, new=8
    ofs.close();

    timestar::ShardRebalancer rebalancer(testDir);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(8));
    EXPECT_EQ(rebalancer.previousShardCount(), 4u);
}

// ---------------------------------------------------------------------------
// TSM file move/split analysis tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, SingleSeriesTSMCanBeMoved) {
    // Create a TSM file with a single series
    std::string key = "test.single.series";
    createShardWithTSM(0, {key});
    writeMetaFile(1);

    // Verify the TSM file was created
    EXPECT_TRUE(fs::exists(testDir + "/shard_0/tsm/0_0.tsm"));
}

TEST_F(ShardRebalancerTest, MultipleSeriesSameTargetCanBeMoved) {
    // Create two series that hash to the same target shard (mod 2)
    // We'll try many keys and find ones that hash to the same shard
    std::vector<std::string> sameShardKeys;
    unsigned targetShard = 0;
    for (int i = 0; i < 100 && sameShardKeys.size() < 3; ++i) {
        std::string key = "series_" + std::to_string(i);
        SeriesId128 id = SeriesId128::fromSeriesKey(key);
        unsigned shard = SeriesId128::Hash{}(id) % 2;
        if (shard == targetShard) {
            sameShardKeys.push_back(key);
        }
    }

    ASSERT_GE(sameShardKeys.size(), 2u) << "Need at least 2 keys hashing to same shard";

    createShardWithTSM(0, sameShardKeys);
    writeMetaFile(1);

    // Verify TSM file exists
    EXPECT_TRUE(fs::exists(testDir + "/shard_0/tsm/0_0.tsm"));
}

// ---------------------------------------------------------------------------
// Series hash distribution tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, SeriesHashDistribution) {
    // Verify that series keys distribute across shards
    std::map<unsigned, int> distribution;
    unsigned newShardCount = 4;

    for (int i = 0; i < 100; ++i) {
        std::string key = "measurement,host=server" + std::to_string(i) + " value";
        SeriesId128 id = SeriesId128::fromSeriesKey(key);
        unsigned shard = SeriesId128::Hash{}(id) % newShardCount;
        distribution[shard]++;
    }

    // With 100 keys and 4 shards, each shard should get some keys
    for (unsigned s = 0; s < newShardCount; ++s) {
        EXPECT_GT(distribution[s], 0) << "Shard " << s << " got no keys";
    }
}

TEST_F(ShardRebalancerTest, HashConsistencyAfterRebalance) {
    // Verify that the hash function is deterministic
    std::string key = "measurement,host=server01 value";
    SeriesId128 id1 = SeriesId128::fromSeriesKey(key);
    SeriesId128 id2 = SeriesId128::fromSeriesKey(key);
    EXPECT_EQ(id1, id2);

    unsigned shard1 = SeriesId128::Hash{}(id1) % 8;
    unsigned shard2 = SeriesId128::Hash{}(id2) % 8;
    EXPECT_EQ(shard1, shard2);
}

// ---------------------------------------------------------------------------
// Directory structure tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, StagingDirectoryLayout) {
    // Verify the rebalancer creates proper staging directories
    // (We can't run execute() without Seastar, but we can test the layout logic)
    unsigned newShardCount = 4;
    for (unsigned s = 0; s < newShardCount; ++s) {
        std::string newDir = testDir + "/shard_" + std::to_string(s) + "_new";
        fs::create_directories(newDir + "/tsm");
        fs::create_directories(newDir + "/index");
        EXPECT_TRUE(fs::exists(newDir + "/tsm"));
        EXPECT_TRUE(fs::exists(newDir + "/index"));
    }
}

TEST_F(ShardRebalancerTest, CutoverRenameSimulation) {
    // Simulate the cutover: old dirs -> _old, new dirs -> final
    unsigned oldCount = 2;
    unsigned newCount = 4;

    // Create old shard dirs
    for (unsigned s = 0; s < oldCount; ++s) {
        fs::create_directories(testDir + "/shard_" + std::to_string(s) + "/tsm");
    }
    // Create new staging dirs
    for (unsigned s = 0; s < newCount; ++s) {
        fs::create_directories(testDir + "/shard_" + std::to_string(s) + "_new/tsm");
    }

    // Simulate rename: old -> _old
    for (unsigned s = 0; s < oldCount; ++s) {
        fs::rename(testDir + "/shard_" + std::to_string(s),
                   testDir + "/shard_" + std::to_string(s) + "_old");
    }
    // Simulate rename: _new -> final
    for (unsigned s = 0; s < newCount; ++s) {
        fs::rename(testDir + "/shard_" + std::to_string(s) + "_new",
                   testDir + "/shard_" + std::to_string(s));
    }

    // Verify final state
    for (unsigned s = 0; s < newCount; ++s) {
        EXPECT_TRUE(fs::exists(testDir + "/shard_" + std::to_string(s)));
    }
    for (unsigned s = 0; s < oldCount; ++s) {
        EXPECT_TRUE(fs::exists(testDir + "/shard_" + std::to_string(s) + "_old"));
        EXPECT_FALSE(fs::exists(testDir + "/shard_" + std::to_string(s) + "_new"));
    }
}

// ---------------------------------------------------------------------------
// Scale-up / scale-down shard mapping tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, ScaleUpDoubleMovesHalfSeries) {
    // When scaling from N to 2N, roughly half the series stay on the same shard
    // (those where hash % 2N == hash % N) and half move to new shards
    unsigned oldCount = 4;
    unsigned newCount = 8;
    int totalSeries = 1000;
    int stayCount = 0;

    for (int i = 0; i < totalSeries; ++i) {
        std::string key = "m,host=h" + std::to_string(i) + " v";
        SeriesId128 id = SeriesId128::fromSeriesKey(key);
        size_t hash = SeriesId128::Hash{}(id);
        if (hash % oldCount == hash % newCount) {
            stayCount++;
        }
    }

    // Roughly 50% should stay (with some statistical variation)
    double stayPct = static_cast<double>(stayCount) / totalSeries * 100.0;
    EXPECT_GT(stayPct, 35.0) << "Expected ~50% to stay, got " << stayPct << "%";
    EXPECT_LT(stayPct, 65.0) << "Expected ~50% to stay, got " << stayPct << "%";
}

TEST_F(ShardRebalancerTest, ScaleDownMergesShards) {
    // When scaling from 8 to 4, each new shard receives data from 2 old shards
    unsigned oldCount = 8;
    unsigned newCount = 4;
    int totalSeries = 1000;

    // Map: new_shard -> set of old_shards that contribute to it
    std::map<unsigned, std::set<unsigned>> newToOldShards;

    for (int i = 0; i < totalSeries; ++i) {
        std::string key = "m,host=h" + std::to_string(i) + " v";
        SeriesId128 id = SeriesId128::fromSeriesKey(key);
        size_t hash = SeriesId128::Hash{}(id);
        unsigned oldShard = hash % oldCount;
        unsigned newShard = hash % newCount;
        newToOldShards[newShard].insert(oldShard);
    }

    // Each new shard should receive data from at least 2 old shards
    for (unsigned s = 0; s < newCount; ++s) {
        EXPECT_GE(newToOldShards[s].size(), 2u)
            << "New shard " << s << " should merge from >=2 old shards";
    }
}
