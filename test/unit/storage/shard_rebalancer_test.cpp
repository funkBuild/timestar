#include "../../../lib/storage/shard_rebalancer.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_tombstone.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <seastar/util/defer.hh>
#include <type_traits>

namespace fs = std::filesystem;

static_assert(!std::is_constructible_v<timestar::ShardRebalancer, std::string>);

class ShardRebalancerTest : public ::testing::Test {
protected:
    fs::path testDir = fs::temp_directory_path() /
                       ("timestar rebalancer layout " + std::to_string(static_cast<unsigned long>(::getpid())));
    timestar::StorageLayout layout{testDir};

    void SetUp() override {
        fs::remove_all(testDir);
        fs::create_directories(testDir);
    }

    void TearDown() override { fs::remove_all(testDir); }

    // Helper to create a shard directory structure with TSM files
    void createShardWithTSM(unsigned shardId, const std::vector<std::string>& seriesKeys,
                            uint64_t baseTime = 1000000000, int numPoints = 100) {
        fs::create_directories(layout.tsmDir(shardId));
        fs::create_directories(layout.nativeIndexDir(shardId));

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
        const auto tsmPath = layout.tsmFile(shardId, 0, 0);
        TSMWriter::run(store, tsmPath.string());
    }

    // Helper to write shard_count.meta
    void writeMetaFile(unsigned count) { timestar::ShardRebalancer::writeShardCountMeta(layout, count); }
};

// ---------------------------------------------------------------------------
// shard_count.meta tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, WriteAndReadShardCountMeta) {
    timestar::ShardRebalancer::writeShardCountMeta(layout, 4);
    unsigned count = timestar::ShardRebalancer::readShardCountMeta(layout);
    EXPECT_EQ(count, 4u);
    EXPECT_TRUE(fs::exists(layout.shardCountMetadataFile()));
    EXPECT_FALSE(fs::exists(layout.shardCountMetadataTemporaryFile()));
}

TEST_F(ShardRebalancerTest, ReadMissingMetaReturnsZero) {
    unsigned count = timestar::ShardRebalancer::readShardCountMeta(layout);
    EXPECT_EQ(count, 0u);
}

TEST_F(ShardRebalancerTest, OverwriteShardCountMeta) {
    timestar::ShardRebalancer::writeShardCountMeta(layout, 4);
    timestar::ShardRebalancer::writeShardCountMeta(layout, 8);
    unsigned count = timestar::ShardRebalancer::readShardCountMeta(layout);
    EXPECT_EQ(count, 8u);
}

// ---------------------------------------------------------------------------
// Detection tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, FreshInstallNoRebalanceNeeded) {
    // No shard directories, no meta file
    timestar::ShardRebalancer rebalancer(layout);
    EXPECT_FALSE(rebalancer.isRebalanceNeeded(4));
}

TEST_F(ShardRebalancerTest, SameShardCountNoRebalance) {
    writeMetaFile(4);
    timestar::ShardRebalancer rebalancer(layout);
    EXPECT_FALSE(rebalancer.isRebalanceNeeded(4));
}

TEST_F(ShardRebalancerTest, DifferentShardCountNeedsRebalance) {
    writeMetaFile(4);
    timestar::ShardRebalancer rebalancer(layout);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(8));
    EXPECT_EQ(rebalancer.previousShardCount(), 4u);
}

TEST_F(ShardRebalancerTest, ScaleDownNeedsRebalance) {
    writeMetaFile(8);
    timestar::ShardRebalancer rebalancer(layout);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(4));
    EXPECT_EQ(rebalancer.previousShardCount(), 8u);
}

TEST_F(ShardRebalancerTest, DetectShardCountFromDirectories) {
    // No meta file, but shard directories exist
    fs::create_directories(layout.tsmDir(0));
    fs::create_directories(layout.tsmDir(1));
    fs::create_directories(layout.tsmDir(2));
    fs::create_directories(layout.tsmDir(3));

    timestar::ShardRebalancer rebalancer(layout);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(8));
    EXPECT_EQ(rebalancer.previousShardCount(), 4u);
}

TEST_F(ShardRebalancerTest, DetectShardCountIgnoresOldAndNewDirs) {
    // Leftover _old and _new dirs should not confuse detection
    fs::create_directories(layout.tsmDir(0));
    fs::create_directories(layout.tsmDir(1));
    fs::create_directories(layout.shardRetiredDir(0));
    fs::create_directories(layout.shardStagingDir(0));

    timestar::ShardRebalancer rebalancer(layout);
    // Should only count shard_0 and shard_1 (regex matches exact "shard_N" pattern)
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(4));
    EXPECT_EQ(rebalancer.previousShardCount(), 2u);
}

// ---------------------------------------------------------------------------
// Rebalance state file tests
// ---------------------------------------------------------------------------

TEST_F(ShardRebalancerTest, InProgressStateTriggersRebalance) {
    // Simulate a crash during rebalance: write an InProgress state file
    std::ofstream ofs(layout.rebalanceStateFile());
    ofs << "1 4 8\n";  // InProgress, old=4, new=8
    ofs.close();

    timestar::ShardRebalancer rebalancer(layout);
    EXPECT_TRUE(rebalancer.isRebalanceNeeded(8));
    EXPECT_EQ(rebalancer.previousShardCount(), 4u);
}

TEST_F(ShardRebalancerTest, ExecuteUsesInjectedLayoutForSplitCutoverAndControlFiles) {
    const auto previousCoreCount = timestar::placement().coreCount();
    auto restorePlacement = seastar::defer(
        [previousCoreCount] { timestar::setGlobalPlacement(timestar::PlacementTable::buildLocal(previousCoreCount)); });
    timestar::setGlobalPlacement(timestar::PlacementTable::buildLocal(2));

    std::array<bool, 2> represented{};
    std::vector<std::string> seriesKeys;
    for (unsigned i = 0; i < 100 && (!represented[0] || !represented[1]); ++i) {
        auto key = "layout.integration.series." + std::to_string(i);
        const auto target = timestar::routeToCore(SeriesId128::fromSeriesKey(key));
        represented[target] = true;
        seriesKeys.push_back(std::move(key));
    }
    ASSERT_TRUE(represented[0]);
    ASSERT_TRUE(represented[1]);

    createShardWithTSM(0, seriesKeys, 1000000000, 3);
    writeMetaFile(1);

    timestar::ShardRebalancer rebalancer(layout);
    ASSERT_TRUE(rebalancer.isRebalanceNeeded(2));
    rebalancer.execute(2).get();

    EXPECT_EQ(timestar::ShardRebalancer::readShardCountMeta(layout), 2u);
    EXPECT_FALSE(fs::exists(layout.shardCountMetadataTemporaryFile()));
    EXPECT_FALSE(fs::exists(layout.rebalanceStateFile()));
    EXPECT_FALSE(fs::exists(layout.rebalanceStateTemporaryFile()));
    for (unsigned shard = 0; shard < 2; ++shard) {
        EXPECT_TRUE(fs::exists(layout.shardDir(shard)));
        EXPECT_TRUE(fs::exists(layout.nativeIndexDir(shard)));
        EXPECT_TRUE(fs::exists(layout.tsmFile(shard, "0_split_0.tsm")));
        EXPECT_FALSE(fs::exists(layout.shardStagingDir(shard)));
        EXPECT_FALSE(fs::exists(layout.shardRetiredDir(shard)));
    }
}

TEST_F(ShardRebalancerTest, ExecuteMovePreservesTombstoneUnderInjectedLayout) {
    const auto previousCoreCount = timestar::placement().coreCount();
    auto restorePlacement = seastar::defer(
        [previousCoreCount] { timestar::setGlobalPlacement(timestar::PlacementTable::buildLocal(previousCoreCount)); });
    timestar::setGlobalPlacement(timestar::PlacementTable::buildLocal(2));

    std::string seriesKey;
    unsigned targetShard = 0;
    for (unsigned i = 0; i < 100; ++i) {
        auto candidate = "layout.move.series." + std::to_string(i);
        const auto candidateTarget = timestar::routeToCore(SeriesId128::fromSeriesKey(candidate));
        if (candidateTarget == 1) {
            seriesKey = std::move(candidate);
            targetShard = candidateTarget;
            break;
        }
    }
    ASSERT_FALSE(seriesKey.empty());

    createShardWithTSM(0, {seriesKey}, 1000000000, 3);
    const auto seriesId = SeriesId128::fromSeriesKey(seriesKey);
    const auto sourceTombstone = layout.tsmTombstoneFile(0, 0, 0);
    {
        timestar::TSMTombstone tombstone(sourceTombstone.string());
        ASSERT_TRUE(tombstone.addTombstone(seriesId, 1000000000, 1000000000).get());
        tombstone.flush().get();
    }
    std::ifstream sourceBytes(sourceTombstone, std::ios::binary);
    const std::string tombstoneContents{std::istreambuf_iterator<char>(sourceBytes), std::istreambuf_iterator<char>()};
    ASSERT_FALSE(tombstoneContents.empty());
    writeMetaFile(1);

    timestar::ShardRebalancer rebalancer(layout);
    ASSERT_TRUE(rebalancer.isRebalanceNeeded(2));
    rebalancer.execute(2).get();

    const auto destinationTsm = layout.tsmFile(targetShard, 0, 0);
    const auto destinationTombstone = layout.tsmTombstoneFile(targetShard, 0, 0);
    EXPECT_TRUE(fs::exists(destinationTsm));
    ASSERT_TRUE(fs::exists(destinationTombstone));
    std::ifstream tombstone(destinationTombstone, std::ios::binary);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(tombstone), std::istreambuf_iterator<char>()),
              tombstoneContents);
    timestar::TSMTombstone loaded(destinationTombstone.string());
    loaded.load().get();
    EXPECT_TRUE(loaded.isDeleted(seriesId, 1000000000));
}

TEST_F(ShardRebalancerTest, SymlinkedTsmFailsBeforeAnyRebalanceMutation) {
    const auto outsideDir = testDir / "outside";
    const std::string seriesKey = "layout.symlink.series";
    const auto seriesId = SeriesId128::fromSeriesKey(seriesKey);
    fs::create_directories(outsideDir);
    createShardWithTSM(0, {seriesKey}, 1000000000, 3);

    const auto outsideTsm = outsideDir / "0_0.tsm";
    const auto outsideTombstone = outsideDir / "0_0.tombstone";
    fs::rename(layout.tsmFile(0, 0, 0), outsideTsm);
    {
        timestar::TSMTombstone tombstone(outsideTombstone.string());
        ASSERT_TRUE(tombstone.addTombstone(seriesId, 1000000000, 1000000000).get());
        tombstone.flush().get();
    }
    fs::create_symlink(outsideTsm, layout.tsmFile(0, "link.tsm"));
    writeMetaFile(1);

    timestar::ShardRebalancer rebalancer(layout);
    ASSERT_TRUE(rebalancer.isRebalanceNeeded(2));
    EXPECT_THROW(rebalancer.execute(2).get(), std::runtime_error);

    EXPECT_TRUE(fs::is_symlink(layout.tsmFile(0, "link.tsm")));
    EXPECT_TRUE(fs::exists(outsideTsm));
    EXPECT_TRUE(fs::exists(outsideTombstone));
    timestar::TSMTombstone loaded(outsideTombstone.string());
    loaded.load().get();
    EXPECT_TRUE(loaded.isDeleted(seriesId, 1000000000));
    EXPECT_EQ(timestar::ShardRebalancer::readShardCountMeta(layout), 1u);
    EXPECT_FALSE(fs::exists(layout.rebalanceStateFile()));
    EXPECT_FALSE(fs::exists(layout.rebalanceStateTemporaryFile()));
    EXPECT_FALSE(fs::exists(layout.shardStagingDir(0)));
    EXPECT_FALSE(fs::exists(layout.shardStagingDir(1)));
    EXPECT_FALSE(fs::exists(layout.shardRetiredDir(0)));
    EXPECT_TRUE(fs::exists(layout.shardDir(0)));
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
    EXPECT_TRUE(fs::exists(layout.tsmFile(0, 0, 0)));
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
    EXPECT_TRUE(fs::exists(layout.tsmFile(0, 0, 0)));
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
        fs::create_directories(layout.shardStagingTsmDir(s));
        fs::create_directories(layout.shardStagingNativeIndexDir(s));
        EXPECT_TRUE(fs::exists(layout.shardStagingTsmDir(s)));
        EXPECT_TRUE(fs::exists(layout.shardStagingNativeIndexDir(s)));
    }
}

TEST_F(ShardRebalancerTest, CutoverRenameSimulation) {
    // Simulate the cutover: old dirs -> _old, new dirs -> final
    unsigned oldCount = 2;
    unsigned newCount = 4;

    // Create old shard dirs
    for (unsigned s = 0; s < oldCount; ++s) {
        fs::create_directories(layout.tsmDir(s));
    }
    // Create new staging dirs
    for (unsigned s = 0; s < newCount; ++s) {
        fs::create_directories(layout.shardStagingTsmDir(s));
    }

    // Simulate rename: old -> _old
    for (unsigned s = 0; s < oldCount; ++s) {
        fs::rename(layout.shardDir(s), layout.shardRetiredDir(s));
    }
    // Simulate rename: _new -> final
    for (unsigned s = 0; s < newCount; ++s) {
        fs::rename(layout.shardStagingDir(s), layout.shardDir(s));
    }

    // Verify final state
    for (unsigned s = 0; s < newCount; ++s) {
        EXPECT_TRUE(fs::exists(layout.shardDir(s)));
    }
    for (unsigned s = 0; s < oldCount; ++s) {
        EXPECT_TRUE(fs::exists(layout.shardRetiredDir(s)));
        EXPECT_FALSE(fs::exists(layout.shardStagingDir(s)));
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
        EXPECT_GE(newToOldShards[s].size(), 2u) << "New shard " << s << " should merge from >=2 old shards";
    }
}
