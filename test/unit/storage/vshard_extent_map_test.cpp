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
    EXPECT_TRUE(m.extents(VShardId{3}).empty());
    EXPECT_TRUE(m.revRange(VShardId{3}).empty);
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

}  // namespace
