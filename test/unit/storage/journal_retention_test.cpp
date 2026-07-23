#include "../../../lib/storage/journal_retention.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace {

using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalRetention;
using timestar::VShardId;

JournalRecord rec(uint16_t vshard, uint64_t seq) {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
    r.kind = JournalRecordKind::Data;
    return r;
}

TEST(JournalRetentionTest, ReleasedWatermarkIsMonotonic) {
    JournalRetention r;
    EXPECT_EQ(r.released(VShardId{7}), 0u);  // unknown -> nothing released
    r.setReleased(VShardId{7}, 10);
    EXPECT_EQ(r.released(VShardId{7}), 10u);
    r.setReleased(VShardId{7}, 5);  // regression ignored
    EXPECT_EQ(r.released(VShardId{7}), 10u);
    r.setReleased(VShardId{7}, 12);
    EXPECT_EQ(r.released(VShardId{7}), 12u);
    // Independent per VShard.
    EXPECT_EQ(r.released(VShardId{8}), 0u);
}

TEST(JournalRetentionTest, FullyReleasedSegmentIsReclaimable) {
    JournalRetention r;
    r.setReleased(VShardId{1}, 3);
    r.setReleased(VShardId{2}, 5);
    const std::vector<JournalRecord> seg = {rec(1, 1), rec(1, 2), rec(1, 3), rec(2, 5)};

    const auto gc = r.planSegment(seg);
    EXPECT_TRUE(gc.reclaimable);
    EXPECT_TRUE(gc.liveRecordIndices.empty());
}

TEST(JournalRetentionTest, NothingReleasedMeansEveryRecordIsLive) {
    JournalRetention r;
    const std::vector<JournalRecord> seg = {rec(1, 1), rec(2, 1), rec(3, 1)};

    const auto gc = r.planSegment(seg);
    EXPECT_FALSE(gc.reclaimable);
    EXPECT_EQ(gc.liveRecordIndices, (std::vector<size_t>{0, 1, 2}));
}

TEST(JournalRetentionTest, LaggardVShardRecordsAreCopiedForward) {
    JournalRetention r;
    r.setReleased(VShardId{1}, 100);  // vshard 1 fully caught up
    r.setReleased(VShardId{2}, 4);    // vshard 2 laggard: only up to seq 4 released
    // Interleaved segment: vshard 1 (all released) and vshard 2 (some live).
    const std::vector<JournalRecord> seg = {
        rec(1, 98), rec(2, 4), rec(1, 99), rec(2, 5), rec(1, 100), rec(2, 6),
    };

    const auto gc = r.planSegment(seg);
    EXPECT_FALSE(gc.reclaimable);
    // Only the un-released vshard-2 records (seq 5, 6) at indices 3 and 5.
    EXPECT_EQ(gc.liveRecordIndices, (std::vector<size_t>{3, 5}));
}

TEST(JournalRetentionTest, BoundaryReleasedSeqIsReclaimed) {
    JournalRetention r;
    r.setReleased(VShardId{1}, 5);
    // seq == released is reclaimable; seq == released+1 is live.
    const std::vector<JournalRecord> seg = {rec(1, 5), rec(1, 6)};

    const auto gc = r.planSegment(seg);
    EXPECT_FALSE(gc.reclaimable);
    EXPECT_EQ(gc.liveRecordIndices, (std::vector<size_t>{1}));  // only seq 6
}

}  // namespace
