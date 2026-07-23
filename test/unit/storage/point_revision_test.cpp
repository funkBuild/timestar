#include "../../../lib/storage/point_revision.hpp"

#include <gtest/gtest.h>

namespace {

using timestar::kFirstAssignedRevision;
using timestar::kMigratedRevision;
using timestar::lwwWinnerRevision;
using timestar::overlaps;
using timestar::RangeWinner;
using timestar::rangeWinner;
using timestar::RevisionRange;

TEST(PointRevisionTest, ReservedFloorAndLwwWinner) {
    EXPECT_EQ(kMigratedRevision, 0u);
    EXPECT_EQ(kFirstAssignedRevision, 1u);
    // Any assigned write beats migrated data at the same point.
    EXPECT_EQ(lwwWinnerRevision(kMigratedRevision, kFirstAssignedRevision), kFirstAssignedRevision);
    EXPECT_EQ(lwwWinnerRevision(5, 9), 9u);
    EXPECT_EQ(lwwWinnerRevision(9, 5), 9u);
    EXPECT_EQ(lwwWinnerRevision(7, 7), 7u);  // same write, either answer identical
}

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

TEST(PointRevisionTest, OverlapsRespectsEmptyAndBoundaries) {
    RevisionRange empty;
    RevisionRange one{5, 10, false};
    EXPECT_FALSE(overlaps(empty, one));
    EXPECT_FALSE(overlaps(one, empty));
    EXPECT_FALSE(overlaps(empty, empty));

    RevisionRange below{1, 4, false};    // strictly below one
    RevisionRange touch{10, 12, false};  // shares the boundary 10
    RevisionRange above{11, 20, false};  // strictly above one
    EXPECT_FALSE(overlaps(one, below));
    EXPECT_TRUE(overlaps(one, touch));  // inclusive ranges: shared endpoint overlaps
    EXPECT_FALSE(overlaps(one, above));
    EXPECT_TRUE(overlaps(one, one));
}

TEST(PointRevisionTest, RangeWinnerDisjointResolvesWithoutPerPoint) {
    RevisionRange low{1, 5, false};
    RevisionRange high{6, 9, false};
    EXPECT_EQ(rangeWinner(high, low), RangeWinner::A);  // A strictly above
    EXPECT_EQ(rangeWinner(low, high), RangeWinner::B);  // B strictly above

    // Migrated block [0,0] loses outright to any assigned block.
    RevisionRange migrated{0, 0, false};
    RevisionRange assigned{1, 100, false};
    EXPECT_EQ(rangeWinner(assigned, migrated), RangeWinner::A);
    EXPECT_EQ(rangeWinner(migrated, assigned), RangeWinner::B);
}

TEST(PointRevisionTest, RangeWinnerOverlapFallsBackToPerPoint) {
    RevisionRange a{1, 10, false};
    RevisionRange b{5, 15, false};
    EXPECT_EQ(rangeWinner(a, b), RangeWinner::Overlap);
    // Adjacency at a shared endpoint is an overlap (a tier-0 dup could sit there).
    RevisionRange c{10, 20, false};
    EXPECT_EQ(rangeWinner(a, c), RangeWinner::Overlap);
}

TEST(PointRevisionTest, RangeWinnerEmptySides) {
    RevisionRange empty;
    RevisionRange some{3, 7, false};
    EXPECT_EQ(rangeWinner(some, empty), RangeWinner::A);  // non-empty wins
    EXPECT_EQ(rangeWinner(empty, some), RangeWinner::B);
    EXPECT_EQ(rangeWinner(empty, empty), RangeWinner::Overlap);  // degenerate no-op
}

}  // namespace
