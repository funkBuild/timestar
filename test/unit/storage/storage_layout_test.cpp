#include "../../../lib/storage/storage_layout.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <limits>
#include <string>
#include <type_traits>

namespace fs = std::filesystem;

namespace {

static_assert(std::is_copy_constructible_v<timestar::StorageLayout>);

TEST(StorageLayoutTest, EmptyRootIsRejectedInsteadOfSilentlySelectingTheWorkingDirectory) {
    EXPECT_THROW(timestar::StorageLayout(fs::path{}), std::invalid_argument);
    EXPECT_THROW(timestar::StorageLayout(fs::path(std::string("data\0outside", 12))), std::invalid_argument);
}

TEST(StorageLayoutTest, DefaultRootProducesCanonicalV1Names) {
    const timestar::StorageLayout layout(".");

    EXPECT_EQ(layout.root(), ".");
    EXPECT_EQ(layout.shardDir(0), "shard_0");
    EXPECT_EQ(layout.shardDir(17), "shard_17");
    EXPECT_EQ(layout.walFile(3, 42), "shard_3/0000000042.wal");
    EXPECT_EQ(layout.walCreationTemporaryFile(3, 42), "shard_3/0000000042.wal.creating");
    EXPECT_EQ(layout.tsmDir(3), "shard_3/tsm");
    EXPECT_EQ(layout.tsmFile(3, "2_99.tsm"), "shard_3/tsm/2_99.tsm");
    EXPECT_EQ(layout.tsmTombstoneFile(3, "2_99.tsm"), "shard_3/tsm/2_99.tombstone");
    EXPECT_EQ(layout.tsmFile(3, 2, 99), "shard_3/tsm/2_99.tsm");
    EXPECT_EQ(layout.tsmTemporaryFile(3, 2, 99), "shard_3/tsm/2_99.tsm.tmp");
    EXPECT_EQ(layout.tsmTombstoneFile(3, 2, 99), "shard_3/tsm/2_99.tombstone");
    EXPECT_EQ(layout.compactedTsmFile(3, 2, 99, 81), "shard_3/tsm/2_99_d81.tsm");
    EXPECT_EQ(layout.compactedTsmTemporaryFile(3, 2, 99, 81), "shard_3/tsm/2_99_d81.tsm.tmp");
    EXPECT_EQ(layout.compactedTsmTombstoneFile(3, 2, 99, 81), "shard_3/tsm/2_99_d81.tombstone");
    EXPECT_EQ(layout.nativeIndexDir(3), "shard_3/native_index");
    EXPECT_EQ(layout.nativeManifestFile(3), "shard_3/native_index/MANIFEST");
    EXPECT_EQ(layout.nativeManifestTemporaryFile(3), "shard_3/native_index/MANIFEST.tmp");
    EXPECT_EQ(layout.nativeWalDir(3), "shard_3/native_index/wal");
    EXPECT_EQ(layout.nativeWalFile(3, 7), "shard_3/native_index/wal/idx_000007.wal");
    EXPECT_EQ(layout.nativeSstableFile(3, 7), "shard_3/native_index/idx_000007.sst");
    EXPECT_EQ(layout.placementFile(), "placement.json");
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
    EXPECT_EQ(layout.placementFile(), "tenant data/primary/placement.json");
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

TEST(StorageLayoutTest, TsmBasenamesCannotEscapeTheirShard) {
    const timestar::StorageLayout layout("data");
    const fs::path nulFilename(std::string("safe\0.tsm", 9));

    EXPECT_THROW((void)layout.tsmFile(1, "../outside.tsm"), std::invalid_argument);
    EXPECT_THROW((void)layout.tsmFile(1, nulFilename), std::invalid_argument);
    EXPECT_THROW((void)layout.tsmFile(1, "nested/file.tsm"), std::invalid_argument);
    EXPECT_THROW((void)layout.tsmFile(1, "not-tsm.wal"), std::invalid_argument);
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

TEST(StorageLayoutTest, EquivalentLexicalRootsProduceEqualValues) {
    const timestar::StorageLayout first("data/./tenant/../primary");
    const timestar::StorageLayout second("data/primary///");

    EXPECT_EQ(first, second);
}

}  // namespace
