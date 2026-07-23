#include "../../../lib/storage/storage_layout.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <limits>
#include <string>
#include <type_traits>

namespace fs = std::filesystem;

namespace {

static_assert(std::is_copy_constructible_v<timestar::StorageLayout>);
static_assert(!std::is_copy_assignable_v<timestar::StorageLayout>);
static_assert(!std::is_move_assignable_v<timestar::StorageLayout>);

TEST(StorageLayoutTest, EmptyRootIsRejectedInsteadOfSilentlySelectingTheWorkingDirectory) {
    EXPECT_THROW(timestar::StorageLayout(fs::path{}), std::invalid_argument);
    EXPECT_THROW(timestar::StorageLayout(fs::path(std::string("data\0outside", 12))), std::invalid_argument);
}

TEST(StorageLayoutTest, DefaultRootExactlyPreservesLegacyNames) {
    const timestar::StorageLayout layout(".");

    EXPECT_EQ(layout.root(), ".");
    EXPECT_EQ(layout.shardDir(0), "shard_0");
    EXPECT_EQ(layout.shardDir(17), "shard_17");
    EXPECT_EQ(layout.shardStagingDir(2), "shard_2_new");
    EXPECT_EQ(layout.shardStagingTsmDir(2), "shard_2_new/tsm");
    EXPECT_EQ(layout.shardStagingNativeIndexDir(2), "shard_2_new/native_index");
    EXPECT_EQ(layout.shardRetiredDir(2), "shard_2_old");
    EXPECT_EQ(layout.walFile(3, 42), "shard_3/0000000042.wal");
    EXPECT_EQ(layout.tsmDir(3), "shard_3/tsm");
    EXPECT_EQ(layout.tsmFile(3, "0_wal_8_0000000042.tsm"), "shard_3/tsm/0_wal_8_0000000042.tsm");
    EXPECT_EQ(layout.tsmTombstoneFile(3, "0_wal_8_0000000042.tsm"), "shard_3/tsm/0_wal_8_0000000042.tombstone");
    EXPECT_EQ(layout.tsmFile(3, 2, 99), "shard_3/tsm/2_99.tsm");
    EXPECT_EQ(layout.tsmTemporaryFile(3, 2, 99), "shard_3/tsm/2_99.tsm.tmp");
    EXPECT_EQ(layout.tsmTombstoneFile(3, 2, 99), "shard_3/tsm/2_99.tombstone");
    EXPECT_EQ(layout.compactedTsmFile(3, 2, 99, 81), "shard_3/tsm/2_99_d81.tsm");
    EXPECT_EQ(layout.compactedTsmTemporaryFile(3, 2, 99, 81), "shard_3/tsm/2_99_d81.tsm.tmp");
    EXPECT_EQ(layout.compactedTsmTombstoneFile(3, 2, 99, 81), "shard_3/tsm/2_99_d81.tombstone");
    EXPECT_EQ(layout.shardStagingTsmFile(3, "2_99.tsm"), "shard_3_new/tsm/2_99.tsm");
    EXPECT_EQ(layout.shardStagingTombstoneFile(3, "2_99.tsm"), "shard_3_new/tsm/2_99.tombstone");
    EXPECT_EQ(layout.rebalanceWalTsmFile(3, 8, "0000000042"), "shard_3_new/tsm/0_wal_8_0000000042.tsm");
    EXPECT_EQ(layout.rebalanceCollisionTsmFile(3, 11), "shard_3_new/tsm/0_rebal_11.tsm");
    EXPECT_EQ(layout.rebalanceSplitTsmFile(3, 12), "shard_3_new/tsm/0_split_12.tsm");
    EXPECT_EQ(layout.nativeIndexDir(3), "shard_3/native_index");
    EXPECT_EQ(layout.nativeManifestFile(3), "shard_3/native_index/MANIFEST");
    EXPECT_EQ(layout.nativeManifestTemporaryFile(3), "shard_3/native_index/MANIFEST.tmp");
    EXPECT_EQ(layout.nativeWalDir(3), "shard_3/native_index/wal");
    EXPECT_EQ(layout.nativeWalFile(3, 7), "shard_3/native_index/wal/idx_000007.wal");
    EXPECT_EQ(layout.nativeSstableFile(3, 7), "shard_3/native_index/idx_000007.sst");
    EXPECT_EQ(layout.placementFile(), "placement.json");
    EXPECT_EQ(layout.vshardDataDir(), "vshards");
    EXPECT_EQ(layout.vshardDir(0), "vshards/0000");
    EXPECT_EQ(layout.vshardDir(4095), "vshards/4095");
    EXPECT_THROW((void)layout.vshardDir(4096), std::out_of_range);
    EXPECT_EQ(layout.shardCountMetadataFile(), "shard_count.meta");
    EXPECT_EQ(layout.shardCountMetadataTemporaryFile(), "shard_count.meta.tmp");
    EXPECT_EQ(layout.rebalanceStateFile(), "rebalance.state");
    EXPECT_EQ(layout.rebalanceStateTemporaryFile(), "rebalance.state.tmp");
}

TEST(StorageLayoutTest, RelativeRootWithSpacesIsPreservedLexically) {
    const timestar::StorageLayout layout("tenant data/primary");

    EXPECT_EQ(layout.root(), "tenant data/primary");
    EXPECT_EQ(layout.shardDir(1), "tenant data/primary/shard_1");
    EXPECT_EQ(layout.walFile(1, 10000000000ULL), "tenant data/primary/shard_1/10000000000.wal");
    EXPECT_EQ(layout.nativeSstableFile(1, 1000000), "tenant data/primary/shard_1/native_index/idx_1000000.sst");
    EXPECT_EQ(layout.shardStagingTsmDir(1), "tenant data/primary/shard_1_new/tsm");
    EXPECT_EQ(layout.rebalanceSplitTsmFile(1, 44), "tenant data/primary/shard_1_new/tsm/0_split_44.tsm");
    EXPECT_EQ(layout.placementFile(), "tenant data/primary/placement.json");
    EXPECT_EQ(layout.vshardDir(27), "tenant data/primary/vshards/0027");
}

TEST(StorageLayoutTest, AnchoredRootIsAbsoluteAndStableAtTheOwnershipBoundary) {
    const timestar::StorageLayout relative("tenant data/primary");
    const auto anchored = relative.anchored();

    EXPECT_EQ(anchored.root(), fs::absolute(relative.root()).lexically_normal());
    EXPECT_TRUE(anchored.root().is_absolute());
    EXPECT_EQ(anchored.anchored(), anchored);
}

TEST(StorageLayoutTest, AbsoluteAndFilesystemRootsComposeWithoutDuplicateSeparators) {
    const timestar::StorageLayout absolute("/var/lib/timestar///");
    const timestar::StorageLayout filesystemRoot("/");

    EXPECT_EQ(absolute.root(), "/var/lib/timestar");
    EXPECT_EQ(absolute.tsmFile(4, 0, 1), "/var/lib/timestar/shard_4/tsm/0_1.tsm");
    EXPECT_EQ(filesystemRoot.shardDir(4), "/shard_4");
    EXPECT_EQ(filesystemRoot.rebalanceStateFile(), "/rebalance.state");
}

TEST(StorageLayoutTest, LexicalNormalizationIsPurePathComposition) {
    const fs::path unnormalized = "missing-parent/child/../data";
    const auto normalized = unnormalized.lexically_normal();

    const timestar::StorageLayout layout(unnormalized);

    EXPECT_EQ(layout.root(), normalized);
    EXPECT_EQ(layout.nativeIndexDir(9), normalized / "shard_9/native_index");
}

TEST(StorageLayoutTest, StagingBasenamesCannotEscapeTheirShard) {
    const timestar::StorageLayout layout("data");
    const fs::path nulFilename(std::string("safe\0.tsm", 9));

    EXPECT_THROW((void)layout.tsmFile(1, "../outside.tsm"), std::invalid_argument);
    EXPECT_THROW((void)layout.tsmFile(1, nulFilename), std::invalid_argument);
    EXPECT_THROW((void)layout.shardStagingTsmFile(1, "../outside.tsm"), std::invalid_argument);
    EXPECT_THROW((void)layout.shardStagingTsmFile(1, nulFilename), std::invalid_argument);
    EXPECT_THROW((void)layout.shardStagingTsmFile(1, "nested/file.tsm"), std::invalid_argument);
    EXPECT_THROW((void)layout.shardStagingTsmFile(1, "not-tsm.wal"), std::invalid_argument);
    EXPECT_THROW((void)layout.rebalanceWalTsmFile(1, 2, "../outside"), std::invalid_argument);
    EXPECT_THROW((void)layout.rebalanceWalTsmFile(1, 2, ""), std::invalid_argument);
}

TEST(StorageLayoutTest, NumericNamesCoverBoundariesWithoutTruncation) {
    const timestar::StorageLayout layout("data");
    const auto maximumShard = std::numeric_limits<unsigned>::max();
    const auto maximumSequence = std::numeric_limits<uint64_t>::max();

    EXPECT_EQ(layout.shardDir(maximumShard), fs::path("data") / ("shard_" + std::to_string(maximumShard)));
    EXPECT_EQ(layout.walFile(0, maximumSequence),
              fs::path("data/shard_0") / (std::to_string(maximumSequence) + ".wal"));
    EXPECT_EQ(layout.tsmFile(0, maximumSequence, maximumSequence),
              fs::path("data/shard_0/tsm") /
                  (std::to_string(maximumSequence) + "_" + std::to_string(maximumSequence) + ".tsm"));
}

TEST(StorageLayoutTest, CanonicalShardDirectoryParsingSharesTheLayoutGrammar) {
    const timestar::StorageLayout layout("/var/lib/timestar");

    EXPECT_EQ(layout.parseShardDirName("shard_0"), 0u);
    EXPECT_EQ(layout.parseShardDirName("shard_17"), 17u);
    EXPECT_EQ(layout.parseShardDirName("shard_4294967295"), std::numeric_limits<unsigned>::max());
    EXPECT_FALSE(layout.parseShardDirName("shard_00").has_value());
    EXPECT_FALSE(layout.parseShardDirName("shard_").has_value());
    EXPECT_FALSE(layout.parseShardDirName("shard_1_new").has_value());
    EXPECT_FALSE(layout.parseShardDirName("other_1").has_value());
    EXPECT_TRUE(layout.isShardNamespaceEntry("shard_invalid"));
    EXPECT_FALSE(layout.isShardNamespaceEntry("placement.json"));
}

TEST(StorageLayoutTest, CanonicalVShardDirectoryParsingRejectsAliasesAndEscapes) {
    const timestar::StorageLayout layout("/var/lib/timestar");

    EXPECT_EQ(layout.parseVShardDirName("0000"), 0u);
    EXPECT_EQ(layout.parseVShardDirName("0027"), 27u);
    EXPECT_EQ(layout.parseVShardDirName("4095"), 4095u);
    EXPECT_FALSE(layout.parseVShardDirName("0").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("00000").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("+001").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("-001").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("4096").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("65536").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("../0001").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("vshards/0001").has_value());
    EXPECT_FALSE(layout.parseVShardDirName("0001/").has_value());
}

TEST(StorageLayoutTest, DecommissionedWorkerArtifactNamesAreRecognizedExactly) {
    const timestar::StorageLayout layout("data");

    EXPECT_TRUE(layout.isDecommissionedWorkerArtifactName("workers.json"));
    EXPECT_TRUE(layout.isDecommissionedWorkerArtifactName("workers.json.tmp"));
    EXPECT_TRUE(layout.isDecommissionedWorkerArtifactName("vshard_ownership"));
    EXPECT_TRUE(layout.isDecommissionedWorkerArtifactName("vshard_ownership.manifest"));
    EXPECT_TRUE(layout.isDecommissionedWorkerArtifactName("vshard_ownership.manifest.tmp"));
    EXPECT_TRUE(layout.isDecommissionedWorkerArtifactName("vshard_ownership.initializing"));

    EXPECT_FALSE(layout.isDecommissionedWorkerArtifactName("vshards"));
    EXPECT_FALSE(layout.isDecommissionedWorkerArtifactName("vshard_ownershipX"));
    EXPECT_FALSE(layout.isDecommissionedWorkerArtifactName("workers.jsonX"));
    EXPECT_FALSE(layout.isDecommissionedWorkerArtifactName("workers"));
    EXPECT_FALSE(layout.isDecommissionedWorkerArtifactName("placement.json"));
    EXPECT_FALSE(layout.isDecommissionedWorkerArtifactName(""));
}

TEST(StorageLayoutTest, EquivalentLexicalRootsProduceEqualValues) {
    const timestar::StorageLayout first("data/./tenant/../primary");
    const timestar::StorageLayout second("data/primary///");

    EXPECT_EQ(first, second);
}

}  // namespace
