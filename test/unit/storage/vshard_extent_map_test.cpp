#include "../../../lib/storage/vshard_extent_map.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

using timestar::RevisionRange;
using timestar::VShardExtent;
using timestar::VShardExtentMap;
using timestar::VShardId;

VShardExtent ext(uint64_t fileId, uint64_t minRev, uint64_t maxRev) {
    return VShardExtent{fileId, RevisionRange{minRev, maxRev, /*empty=*/false}};
}

TEST(VShardExtentMapTest, EmptyMap) {
    VShardExtentMap m;
    EXPECT_EQ(m.extentCount(), 0u);
    EXPECT_TRUE(m.extents(VShardId{3}).empty());
    EXPECT_TRUE(m.revRange(VShardId{3}).empty);
    EXPECT_TRUE(m.vshards().empty());
}

TEST(VShardExtentMapTest, ExtentsOrderedByFileIdAndRangeIsUnion) {
    VShardExtentMap m;
    m.add(VShardId{5}, ext(20, 100, 150));
    m.add(VShardId{5}, ext(10, 1, 50));  // earlier file
    m.add(VShardId{5}, ext(30, 200, 260));

    auto e = m.extents(VShardId{5});
    ASSERT_EQ(e.size(), 3u);
    EXPECT_EQ(e[0].fileId, 10u);
    EXPECT_EQ(e[1].fileId, 20u);
    EXPECT_EQ(e[2].fileId, 30u);

    const auto range = m.revRange(VShardId{5});
    EXPECT_FALSE(range.empty);
    EXPECT_EQ(range.minRev, 1u);
    EXPECT_EQ(range.maxRev, 260u);
}

TEST(VShardExtentMapTest, SameVShardFileMergesRange) {
    VShardExtentMap m;
    m.add(VShardId{1}, ext(7, 10, 20));
    m.add(VShardId{1}, ext(7, 5, 30));  // same file -> union
    auto e = m.extents(VShardId{1});
    ASSERT_EQ(e.size(), 1u);
    EXPECT_EQ(e[0].revRange.minRev, 5u);
    EXPECT_EQ(e[0].revRange.maxRev, 30u);
}

TEST(VShardExtentMapTest, VShardsAscendingAndCount) {
    VShardExtentMap m;
    m.add(VShardId{9}, ext(1, 1, 1));
    m.add(VShardId{2}, ext(1, 1, 1));
    m.add(VShardId{2}, ext(2, 2, 2));
    EXPECT_EQ(m.vshards(), (std::vector<VShardId>{VShardId{2}, VShardId{9}}));
    EXPECT_EQ(m.extentCount(), 3u);
}

TEST(VShardExtentMapTest, EncodeIsOrderIndependentAndRoundTrips) {
    VShardExtentMap a;
    a.add(VShardId{9}, ext(3, 30, 40));
    a.add(VShardId{1}, ext(2, 10, 20));
    a.add(VShardId{1}, ext(1, 1, 9));

    VShardExtentMap b;  // same content inserted in a different order
    b.add(VShardId{1}, ext(1, 1, 9));
    b.add(VShardId{9}, ext(3, 30, 40));
    b.add(VShardId{1}, ext(2, 10, 20));

    EXPECT_EQ(a.encode(), b.encode());

    const auto decoded = VShardExtentMap::decode(a.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->extentCount(), 3u);
    EXPECT_EQ(decoded->extents(VShardId{1}).size(), 2u);
    EXPECT_EQ(decoded->revRange(VShardId{1}).maxRev, 20u);
    EXPECT_EQ(decoded->revRange(VShardId{9}).minRev, 30u);
    EXPECT_EQ(decoded->encode(), a.encode());  // re-encode is byte-identical
}

TEST(VShardExtentMapTest, EmptyRoundTrips) {
    VShardExtentMap m;
    const auto decoded = VShardExtentMap::decode(m.encode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->extentCount(), 0u);
}

TEST(VShardExtentMapTest, DecodeFailsClosedOnCorruption) {
    VShardExtentMap m;
    m.add(VShardId{4}, ext(2, 5, 8));
    const std::string good = m.encode();

    EXPECT_FALSE(VShardExtentMap::decode("").has_value());
    EXPECT_FALSE(VShardExtentMap::decode("short").has_value());
    EXPECT_FALSE(VShardExtentMap::decode(good.substr(0, good.size() - 1)).has_value());  // truncation/CRC

    std::string flipped = good;
    flipped[good.size() / 2] ^= 0xff;  // mid-body -> CRC mismatch
    EXPECT_FALSE(VShardExtentMap::decode(flipped).has_value());

    std::string badMagic = good;
    badMagic[0] ^= 0xff;
    EXPECT_FALSE(VShardExtentMap::decode(badMagic).has_value());
}

}  // namespace
