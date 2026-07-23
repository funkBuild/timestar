#include "../../../lib/storage/snapshot_pin_set.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

namespace {

using timestar::SnapshotPin;
using timestar::SnapshotPinSet;

constexpr uint64_t kNoFloor = std::numeric_limits<uint64_t>::max();

TEST(SnapshotPinSetTest, EmptyHasNoFloor) {
    SnapshotPinSet s;
    EXPECT_EQ(s.size(), 0u);
    EXPECT_EQ(s.gcFloor(100), kNoFloor);
    EXPECT_FALSE(s.isPinned(50, 100));
}

TEST(SnapshotPinSetTest, PinAndRenewPerHolder) {
    SnapshotPinSet s;
    s.pin("learnerA", SnapshotPin{100, 500});
    s.pin("learnerB", SnapshotPin{200, 600});
    EXPECT_EQ(s.size(), 2u);
    EXPECT_EQ(s.pinOf("learnerA"), (SnapshotPin{100, 500}));

    s.pin("learnerA", SnapshotPin{150, 700});  // renew: replaces, not adds
    EXPECT_EQ(s.size(), 2u);
    EXPECT_EQ(s.pinOf("learnerA"), (SnapshotPin{150, 700}));

    EXPECT_TRUE(s.release("learnerB"));
    EXPECT_FALSE(s.release("nobody"));
    EXPECT_EQ(s.size(), 1u);
}

TEST(SnapshotPinSetTest, GcFloorIsMinOverLivePins) {
    SnapshotPinSet s;
    s.pin("a", SnapshotPin{100, 1000});  // live at now=500
    s.pin("b", SnapshotPin{50, 1000});   // live, lower revision -> the floor
    s.pin("c", SnapshotPin{10, 200});    // EXPIRED at now=500 -> ignored

    EXPECT_EQ(s.gcFloor(500), 50u) << "min over live pins, expired 'c' excluded";
    EXPECT_TRUE(s.isPinned(50, 500));
    EXPECT_TRUE(s.isPinned(1000, 500));  // >= a live pin's revision
    EXPECT_FALSE(s.isPinned(49, 500));   // below every live pin
    EXPECT_FALSE(s.isPinned(10, 500));   // only 'c' protects it, and it's expired
}

TEST(SnapshotPinSetTest, LeaseExpiryReleasesTheFloor) {
    SnapshotPinSet s;
    s.pin("stuck", SnapshotPin{100, 1000});
    EXPECT_EQ(s.gcFloor(999), 100u);  // still live
    EXPECT_EQ(s.gcFloor(1000), kNoFloor) << "deadline is exclusive: at now==deadline the lease has lapsed";
    EXPECT_EQ(s.gcFloor(2000), kNoFloor);
}

TEST(SnapshotPinSetTest, EvictExpiredRemovesLapsedPins) {
    SnapshotPinSet s;
    s.pin("live", SnapshotPin{100, 1000});
    s.pin("dead", SnapshotPin{50, 300});
    EXPECT_EQ(s.evictExpired(500), 1u);  // only 'dead'
    EXPECT_EQ(s.size(), 1u);
    EXPECT_TRUE(s.pinOf("live").has_value());
    EXPECT_FALSE(s.pinOf("dead").has_value());
}

TEST(SnapshotPinSetTest, EncodeRoundTripsDeterministically) {
    SnapshotPinSet a;
    a.pin("z-holder", SnapshotPin{300, 900});
    a.pin("a-holder", SnapshotPin{100, 500});

    SnapshotPinSet b;  // inserted in a different order
    b.pin("a-holder", SnapshotPin{100, 500});
    b.pin("z-holder", SnapshotPin{300, 900});
    EXPECT_EQ(a.encode(), b.encode());

    const auto decoded = SnapshotPinSet::decode(a.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->size(), 2u);
    EXPECT_EQ(decoded->pinOf("a-holder"), (SnapshotPin{100, 500}));
    EXPECT_EQ(decoded->encode(), a.encode());
}

TEST(SnapshotPinSetTest, DecodeFailsClosedOnCorruption) {
    SnapshotPinSet s;
    s.pin("h", SnapshotPin{1, 2});
    const std::string good = s.encode();

    EXPECT_FALSE(SnapshotPinSet::decode("").has_value());
    EXPECT_FALSE(SnapshotPinSet::decode(good.substr(0, good.size() - 1)).has_value());

    std::string flipped = good;
    flipped[good.size() / 2] ^= 0xff;
    EXPECT_FALSE(SnapshotPinSet::decode(flipped).has_value());

    std::string badMagic = good;
    badMagic[0] ^= 0xff;
    EXPECT_FALSE(SnapshotPinSet::decode(badMagic).has_value());
}

}  // namespace
