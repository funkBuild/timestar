#include "../../../lib/storage/journal_replay.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <stdexcept>
#include <vector>

namespace {

using timestar::assignCore;
using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalReplay;
using timestar::VShardId;

JournalRecord rec(uint16_t vshard, uint64_t seq) {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
    r.kind = JournalRecordKind::Data;
    return r;
}

TEST(JournalReplayTest, RejectsZeroCoreCount) {
    EXPECT_THROW(JournalReplay(0), std::invalid_argument);
}

TEST(JournalReplayTest, RoutesEachVShardToItsAssignedCoreInSequenceOrder) {
    JournalReplay r(4);
    // Interleave a few VShards' records.
    for (uint64_t s = 1; s <= 3; ++s)
        for (uint16_t v : {0, 1, 5, 4095})
            ASSERT_TRUE(r.ingest(rec(v, s)));
    EXPECT_FALSE(r.failed());

    for (uint16_t v : {0, 1, 5, 4095}) {
        const unsigned core = assignCore(VShardId{v}, 4);
        EXPECT_EQ(r.ownerCore(VShardId{v}), core);
        std::vector<uint64_t> seqs;
        for (const auto& rc : r.recordsForCore(core))
            if (rc.vshard.value() == v)
                seqs.push_back(rc.vshardSeq);
        EXPECT_EQ(seqs, (std::vector<uint64_t>{1, 2, 3})) << "vshard " << v;
    }
}

TEST(JournalReplayTest, SequenceGapFailsClosed) {
    JournalReplay r(2);
    EXPECT_TRUE(r.ingest(rec(7, 10)));  // baseline
    EXPECT_TRUE(r.ingest(rec(7, 11)));
    EXPECT_FALSE(r.ingest(rec(7, 13)));  // gap: expected 12
    EXPECT_TRUE(r.failed());
    EXPECT_NE(r.failureDetail().find("discontinuity"), std::string::npos);
    // After a failure, further ingest is refused.
    EXPECT_FALSE(r.ingest(rec(7, 12)));
}

TEST(JournalReplayTest, SequenceRegressionFailsClosed) {
    JournalReplay r(2);
    EXPECT_TRUE(r.ingest(rec(3, 5)));
    EXPECT_TRUE(r.ingest(rec(3, 6)));
    EXPECT_FALSE(r.ingest(rec(3, 6)));  // duplicate / regression
    EXPECT_TRUE(r.failed());
}

TEST(JournalReplayTest, OutOfRangeVShardFailsClosed) {
    JournalReplay r(2);
    JournalRecord bad;
    bad.vshard = VShardId{4096};  // constructible, but out of range
    bad.vshardSeq = 1;
    EXPECT_FALSE(r.ingest(bad));
    EXPECT_TRUE(r.failed());
}

TEST(JournalReplayTest, InterleavedVShardsEachKeepIndependentBaselines) {
    JournalReplay r(3);
    // Two VShards with different starting sequences (post-GC retained logs).
    EXPECT_TRUE(r.ingest(rec(1, 100)));
    EXPECT_TRUE(r.ingest(rec(2, 5)));
    EXPECT_TRUE(r.ingest(rec(1, 101)));
    EXPECT_TRUE(r.ingest(rec(2, 6)));
    EXPECT_TRUE(r.ingest(rec(1, 102)));
    EXPECT_FALSE(r.failed());
}

// The Task 1 replay-routing contract: a stream written under one core count
// replays correctly under another. Every VShard's records land entirely on one
// core (assignCore) and in identical sequence order, regardless of core count.
TEST(JournalReplayTest, ReplayIsCoreCountIndependent) {
    const std::vector<uint16_t> vshards = {0, 1, 2, 5, 100, 1023, 4095};
    std::vector<JournalRecord> stream;
    for (uint64_t s = 1; s <= 4; ++s)
        for (uint16_t v : vshards)
            stream.push_back(rec(v, s));

    auto perVShardSeqs = [&](unsigned coreCount) {
        JournalReplay r(coreCount);
        for (const auto& rc : stream)
            EXPECT_TRUE(r.ingest(rc));
        EXPECT_FALSE(r.failed());

        std::map<uint16_t, std::vector<uint64_t>> seqs;
        std::map<uint16_t, unsigned> coreOf;
        for (unsigned core = 0; core < coreCount; ++core) {
            for (const auto& rc : r.recordsForCore(core)) {
                seqs[rc.vshard.value()].push_back(rc.vshardSeq);
                // Every record of a VShard must be on the same core.
                auto [it, inserted] = coreOf.emplace(rc.vshard.value(), core);
                EXPECT_EQ(it->second, core)
                    << "vshard " << rc.vshard.value() << " split across cores at cc=" << coreCount;
            }
        }
        // Each VShard's owning core matches assignCore.
        for (const auto& [v, core] : coreOf)
            EXPECT_EQ(core, assignCore(VShardId{v}, coreCount));
        return seqs;
    };

    const auto under2 = perVShardSeqs(2);
    const auto under4 = perVShardSeqs(4);
    const auto under7 = perVShardSeqs(7);
    // Same per-VShard record sequences regardless of the replay core count.
    EXPECT_EQ(under2, under4);
    EXPECT_EQ(under4, under7);
    // Sanity: every VShard has its full [1,2,3,4] sequence.
    for (uint16_t v : vshards)
        EXPECT_EQ(under4.at(v), (std::vector<uint64_t>{1, 2, 3, 4}));
}

}  // namespace
