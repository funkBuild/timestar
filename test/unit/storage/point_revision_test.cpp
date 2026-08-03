#include "../../../lib/storage/point_revision.hpp"

#include <gtest/gtest.h>

namespace {

using timestar::RevisionRange;

TEST(PointRevisionTest, ExtendBuildsRange) {
    RevisionRange r;
    EXPECT_TRUE(r.empty);
    r.extend(10);
    EXPECT_FALSE(r.empty);
    EXPECT_EQ(r.minRev, 10u);
    EXPECT_EQ(r.maxRev, 10u);
    r.extend(4);
    r.extend(20);
    r.extend(12);
    EXPECT_EQ(r.minRev, 4u);
    EXPECT_EQ(r.maxRev, 20u);
}

TEST(PointRevisionTest, MergeIsUnionWithEmptyIdentity) {
    RevisionRange a;  // empty
    RevisionRange b;
    b.extend(3);
    b.extend(8);
    a.merge(b);  // empty.merge(x) == x
    EXPECT_EQ(a, b);

    RevisionRange empty;
    a.merge(empty);  // x.merge(empty) == x
    EXPECT_EQ(a, b);

    RevisionRange c;
    c.extend(1);
    c.extend(30);
    a.merge(c);
    EXPECT_EQ(a.minRev, 1u);
    EXPECT_EQ(a.maxRev, 30u);
}

}  // namespace
