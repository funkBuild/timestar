#include "../../../lib/storage/vshard_snapshot_manifest.hpp"

#include "../../../lib/utils/crc32.hpp"

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
    m = sample();
    m.verificationHash = std::string(32, 'A');  // uppercase is not canonical (lowercase only)
    EXPECT_FALSE(m.valid());
}

// A crafted oversized count must fail closed (nullopt), never throw bad_alloc
// out of decode() from a giant reserve.
TEST(VShardSnapshotManifestTest, DecodeRejectsOversizedCountWithoutThrowing) {
    // Build a byte-valid header + hashes, then a bogus huge extent count, with a
    // correct CRC so it passes the integrity check and reaches the loop.
    std::string body;
    auto putU32 = [&](uint32_t v) {
        for (int i = 0; i < 4; ++i)
            body.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };
    auto putU16 = [&](uint16_t v) {
        body.push_back(static_cast<char>(v & 0xff));
        body.push_back(static_cast<char>((v >> 8) & 0xff));
    };
    auto putU64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i)
            body.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
    };
    auto putStr = [&](const std::string& s) {
        putU32(static_cast<uint32_t>(s.size()));
        body += s;
    };
    putU32(0x56534e50);            // magic "VSNP"
    putU32(1);                     // version
    putU16(1);                     // vshard
    putU64(100);                   // snapshotRevision
    putStr(std::string(32, 'a'));  // verificationHash
    putStr(std::string(32, 'b'));  // catalogHash
    putU32(0xFFFFFFFFu);           // extentCount -> would reserve ~100GB if trusted

    // Append a correct CRC32 over the body so integrity passes.
    const uint32_t crc = CRC32::compute(body.data(), body.size());
    std::string bytes = body;
    for (int i = 0; i < 4; ++i)
        bytes.push_back(static_cast<char>((crc >> (i * 8)) & 0xff));

    EXPECT_NO_THROW({ EXPECT_FALSE(VShardSnapshotManifest::decode(bytes).has_value()); });
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
