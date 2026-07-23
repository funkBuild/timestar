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
    EXPECT_EQ(m.get(VShardId{1}).releasedSeq, 5u) << "released is clamped to applied at set time";

    // Advancing applied does NOT retroactively raise the clamped released -- the
    // stored value is final, so reload stays behaviourally identical.
    m.setApplied(VShardId{1}, 40);
    EXPECT_EQ(m.get(VShardId{1}).releasedSeq, 5u) << "no retroactive lift";

    // The caller re-derives released each cycle; a fresh setReleased advances it,
    // clamped to the now-higher applied.
    m.setReleased(VShardId{1}, 100);
    EXPECT_EQ(m.get(VShardId{1}).releasedSeq, 40u);
}

// The persist boundary must not change behaviour: a decoded manifest must react
// to a later setApplied/setReleased exactly as the live one would (regression
// guard for the old clamp-on-encode intent-loss bug).
TEST(VShardManifestTest, ReloadIsBehaviourallyIdentical) {
    VShardManifest live;
    live.setApplied(VShardId{1}, 5);
    live.setReleased(VShardId{1}, 100);  // clamped to 5

    auto reloaded = VShardManifest::decode(live.encode());
    ASSERT_TRUE(reloaded.has_value());
    EXPECT_EQ(reloaded->get(VShardId{1}), live.get(VShardId{1}));

    // Apply the same further operations to both; they must stay identical.
    for (VShardManifest* m : {&live, &*reloaded}) {
        m->setApplied(VShardId{1}, 200);
        m->setReleased(VShardId{1}, 150);
    }
    EXPECT_EQ(reloaded->get(VShardId{1}), live.get(VShardId{1}));
    EXPECT_EQ(live.get(VShardId{1}), (VShardWatermarks{200, 150}));
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
