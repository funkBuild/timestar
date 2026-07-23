#include "../../../lib/storage/vshard_snapshot_builder.hpp"

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/vshard_extent_map.hpp"
#include "../../../lib/storage/vshard_verification_hash.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using timestar::RevisionRange;
using timestar::VShardExtent;
using timestar::VShardExtentMap;
using timestar::VShardId;
using timestar::VShardSnapshotBuilder;
using timestar::VShardVerificationHash;

SeriesId128 sid(const std::string& k) {
    return SeriesId128::fromSeriesKey(k);
}

VShardExtentMap extentsFor(VShardId vs) {
    VShardExtentMap m;
    m.add(vs, VShardExtent{10, RevisionRange{1, 500, false}});
    m.add(vs, VShardExtent{20, RevisionRange{501, 900, false}});
    return m;
}

TEST(VShardSnapshotBuilderTest, BuildsValidManifestWithDerivedRevisionAndHash) {
    const VShardId vs{7};
    const auto s = sid("series.one");

    VShardSnapshotBuilder builder(vs);
    builder.addFloat(s, 100, 1.5);
    builder.addFloat(s, 200, 2.5);
    builder.addFloat(s, 300, 3.5);

    const auto extents = extentsFor(vs);
    const auto manifest = builder.build(extents, /*catalogHash=*/std::string(32, 'c'));

    EXPECT_TRUE(manifest.valid());
    EXPECT_EQ(manifest.vshard, vs);
    EXPECT_EQ(manifest.snapshotRevision, 900u) << "max revision across the VShard's extents";
    EXPECT_EQ(manifest.catalogHash, std::string(32, 'c'));
    ASSERT_EQ(manifest.dataExtents.size(), 2u);
    EXPECT_EQ(manifest.dataExtents[0].fileId, 10u);
    EXPECT_EQ(manifest.dataExtents[1].fileId, 20u);

    // The verification hash matches an independent hash fed the same points.
    VShardVerificationHash h;
    h.addFloat(s, 100, 1.5);
    h.addFloat(s, 200, 2.5);
    h.addFloat(s, 300, 3.5);
    EXPECT_EQ(manifest.verificationHash, h.digestHex());

    // The manifest round-trips through its own codec.
    const auto decoded = timestar::VShardSnapshotManifest::decode(manifest.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, manifest);
}

TEST(VShardSnapshotBuilderTest, NoExtentsGivesZeroRevisionAndEmptyExtents) {
    const VShardId vs{3};
    VShardSnapshotBuilder builder(vs);  // no points fed
    VShardExtentMap empty;
    const auto manifest = builder.build(empty, std::string(32, '0'));
    EXPECT_TRUE(manifest.valid());
    EXPECT_EQ(manifest.snapshotRevision, 0u);
    EXPECT_TRUE(manifest.dataExtents.empty());
}

TEST(VShardSnapshotBuilderTest, TombstoneIdsAreSortedAndDeduped) {
    const VShardId vs{1};
    VShardSnapshotBuilder builder(vs);
    const auto manifest = builder.build(extentsFor(vs), std::string(32, 'a'), {8, 3, 3, 15, 8});
    EXPECT_TRUE(manifest.valid()) << "tombstone ids must be strictly ascending after build";
    EXPECT_EQ(manifest.tombstoneObjectIds, (std::vector<uint64_t>{3, 8, 15}));
}

}  // namespace
