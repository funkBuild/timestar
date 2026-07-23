#include "../../../lib/storage/vshard_manifest.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using timestar::VShardId;
using timestar::VShardManifest;
using timestar::VShardWatermarks;

TEST(VShardManifestTest, UnknownVShardIsZero) {
    VShardManifest m;
    EXPECT_EQ(m.size(), 0u);
    EXPECT_EQ(m.get(VShardId{42}), (VShardWatermarks{0, 0}));
}

TEST(VShardManifestTest, WatermarksAreMonotonic) {
    VShardManifest m;
    m.setApplied(VShardId{3}, 10);
    m.setReleased(VShardId{3}, 4);
    EXPECT_EQ(m.get(VShardId{3}), (VShardWatermarks{10, 4}));

    m.setApplied(VShardId{3}, 7);   // regression ignored
    m.setReleased(VShardId{3}, 2);  // regression ignored
    EXPECT_EQ(m.get(VShardId{3}), (VShardWatermarks{10, 4}));

    m.setApplied(VShardId{3}, 15);
    m.setReleased(VShardId{3}, 12);
    EXPECT_EQ(m.get(VShardId{3}), (VShardWatermarks{15, 12}));
}

TEST(VShardManifestTest, ReleasedNeverOutrunsApplied) {
    VShardManifest m;
    m.setApplied(VShardId{1}, 5);
    m.setReleased(VShardId{1}, 100);  // asks to release past what is applied
    EXPECT_EQ(m.get(VShardId{1}).releasedSeq, 5u) << "released is clamped to applied";

    // Raising applied lifts the clamped released up to the earlier intent.
    m.setApplied(VShardId{1}, 40);
    EXPECT_EQ(m.get(VShardId{1}).releasedSeq, 40u);
    m.setApplied(VShardId{1}, 200);
    EXPECT_EQ(m.get(VShardId{1}).releasedSeq, 100u) << "clamp lifts no further than the intent";
}

TEST(VShardManifestTest, EncodeIsDeterministicAndOrderIndependent) {
    VShardManifest a;
    a.setApplied(VShardId{9}, 3);
    a.setApplied(VShardId{1}, 7);
    a.setReleased(VShardId{1}, 2);

    VShardManifest b;  // same content, inserted in a different order
    b.setApplied(VShardId{1}, 7);
    b.setReleased(VShardId{1}, 2);
    b.setApplied(VShardId{9}, 3);

    EXPECT_EQ(a.encode(), b.encode()) << "serialisation is order-independent (sorted by VShard)";
}

TEST(VShardManifestTest, RoundTripsThroughEncodeDecode) {
    VShardManifest m;
    m.setApplied(VShardId{0}, 1);
    m.setApplied(VShardId{4095}, 999);
    m.setReleased(VShardId{4095}, 500);
    m.setApplied(VShardId{100}, 50);

    const std::string bytes = m.encode();
    const auto decoded = VShardManifest::decode(bytes);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->size(), 3u);
    EXPECT_EQ(decoded->get(VShardId{0}), (VShardWatermarks{1, 0}));
    EXPECT_EQ(decoded->get(VShardId{4095}), (VShardWatermarks{999, 500}));
    EXPECT_EQ(decoded->get(VShardId{100}), (VShardWatermarks{50, 0}));
    // Re-encoding the decoded manifest is byte-identical.
    EXPECT_EQ(decoded->encode(), bytes);
}

TEST(VShardManifestTest, EmptyManifestRoundTrips) {
    VShardManifest m;
    const auto decoded = VShardManifest::decode(m.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->size(), 0u);
}

TEST(VShardManifestTest, DecodeFailsClosedOnCorruption) {
    VShardManifest m;
    m.setApplied(VShardId{2}, 8);
    m.setReleased(VShardId{2}, 3);
    const std::string good = m.encode();

    EXPECT_FALSE(VShardManifest::decode("").has_value());
    EXPECT_FALSE(VShardManifest::decode("short").has_value());

    // Truncated (drop the last byte -> CRC no longer covers the body).
    EXPECT_FALSE(VShardManifest::decode(good.substr(0, good.size() - 1)).has_value());

    // Flip a payload byte (mid-body, before the trailing CRC) -> CRC mismatch.
    std::string flipped = good;
    flipped[good.size() / 2] ^= 0xff;
    EXPECT_FALSE(VShardManifest::decode(flipped).has_value());

    // Corrupt the magic.
    std::string badMagic = good;
    badMagic[0] ^= 0xff;
    EXPECT_FALSE(VShardManifest::decode(badMagic).has_value());
}

}  // namespace
