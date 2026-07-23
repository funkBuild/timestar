#include "../../../lib/storage/vshard_snapshot_manifest.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using timestar::RevisionRange;
using timestar::VShardExtent;
using timestar::VShardId;
using timestar::VShardSnapshotManifest;

VShardSnapshotManifest sample() {
    VShardSnapshotManifest m;
    m.vshard = VShardId{7};
    m.snapshotRevision = 1000;
    m.verificationHash = std::string(32, 'a');
    m.catalogHash = std::string(32, 'b');
    m.dataExtents = {
        VShardExtent{10, RevisionRange{1, 500, false}},
        VShardExtent{20, RevisionRange{501, 1000, false}},
    };
    m.tombstoneObjectIds = {3, 8, 15};
    return m;
}

TEST(VShardSnapshotManifestTest, SampleIsValid) {
    EXPECT_TRUE(sample().valid());
}

TEST(VShardSnapshotManifestTest, RoundTrips) {
    const auto m = sample();
    const auto decoded = VShardSnapshotManifest::decode(m.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, m);
    EXPECT_EQ(decoded->encode(), m.encode());
}

TEST(VShardSnapshotManifestTest, EmptyExtentsAndTombstonesRoundTrip) {
    VShardSnapshotManifest m;
    m.vshard = VShardId{0};
    m.snapshotRevision = 0;
    m.verificationHash = std::string(32, '0');
    m.catalogHash = std::string(32, 'f');
    const auto decoded = VShardSnapshotManifest::decode(m.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, m);
}

TEST(VShardSnapshotManifestTest, ValidRejectsBadHashShape) {
    auto m = sample();
    m.verificationHash = "tooshort";
    EXPECT_FALSE(m.valid());
    m = sample();
    m.catalogHash = std::string(32, 'z');  // 'z' is not hex
    EXPECT_FALSE(m.valid());
}

TEST(VShardSnapshotManifestTest, ValidRejectsUnorderedExtentsAndOverWatermark) {
    auto m = sample();
    std::swap(m.dataExtents[0], m.dataExtents[1]);  // fileId descending
    EXPECT_FALSE(m.valid());

    m = sample();
    m.dataExtents.back().revRange.maxRev = 2000;  // past snapshotRevision (1000)
    EXPECT_FALSE(m.valid());

    m = sample();
    m.tombstoneObjectIds = {5, 5};  // not strictly ascending
    EXPECT_FALSE(m.valid());
}

TEST(VShardSnapshotManifestTest, DecodeFailsClosedOnCorruptionAndStructure) {
    const std::string good = sample().encode();
    EXPECT_FALSE(VShardSnapshotManifest::decode("").has_value());
    EXPECT_FALSE(VShardSnapshotManifest::decode(good.substr(0, good.size() - 1)).has_value());

    std::string flipped = good;
    flipped[good.size() / 2] ^= 0xff;
    EXPECT_FALSE(VShardSnapshotManifest::decode(flipped).has_value());

    std::string badMagic = good;
    badMagic[0] ^= 0xff;
    EXPECT_FALSE(VShardSnapshotManifest::decode(badMagic).has_value());

    // A byte-valid but structurally invalid manifest is rejected by decode()
    // (via valid()): re-encode a manifest whose extent exceeds the watermark.
    VShardSnapshotManifest bad = sample();
    bad.snapshotRevision = 100;  // extents reference up to 1000 -> invalid
    // bad.encode() has a correct CRC but fails valid() on decode.
    EXPECT_FALSE(VShardSnapshotManifest::decode(bad.encode()).has_value());
}

}  // namespace
