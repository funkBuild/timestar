#include "../../../lib/storage/journal_replay.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using timestar::assignCore;
using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalReplay;
using timestar::VShardId;

JournalRecord rec(uint16_t vshard, uint64_t seq, std::string payload = "") {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
    r.kind = JournalRecordKind::Data;
    r.payload = std::move(payload);
    return r;
}

TEST(JournalReplayTest, RejectsZeroCoreCount) {
    EXPECT_THROW(JournalReplay(0), std::invalid_argument);
}

TEST(JournalReplayTest, RoutesEachVShardToItsAssignedCoreInSequenceOrder) {
    JournalReplay r(4);
    for (uint64_t s = 1; s <= 3; ++s)
        for (uint16_t v : {0, 1, 5, 4095})
            ASSERT_TRUE(r.ingest(rec(v, s)));
    EXPECT_TRUE(r.finalize());
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

// The copy-forward scenario: a laggard VShard's older records arrive physically
// AFTER its newer records (GC relocated them to the tail). Sequence order, not
// physical order, is authoritative -- finalize sorts and it replays cleanly.
TEST(JournalReplayTest, ReorderedPhysicalInputIsSortedBySequence) {
    JournalReplay r(2);
    ASSERT_TRUE(r.ingest(rec(2, 6, "newest")));  // newest, physically first
    for (uint64_t s = 1; s <= 5; ++s)
        ASSERT_TRUE(r.ingest(rec(2, s, "old")));  // older, copied forward behind it
    EXPECT_TRUE(r.finalize());
    EXPECT_FALSE(r.failed());

    std::vector<uint64_t> seqs;
    for (const auto& rc : r.recordsForCore(r.ownerCore(VShardId{2})))
        seqs.push_back(rc.vshardSeq);
    EXPECT_EQ(seqs, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6}));
}

// A crash in the GC barrier->delete window leaves a byte-identical duplicate. It
// must be de-duplicated, not treated as corruption.
TEST(JournalReplayTest, IdenticalDuplicateIsDeduped) {
    JournalReplay r(2);
    ASSERT_TRUE(r.ingest(rec(3, 5, "p")));
    ASSERT_TRUE(r.ingest(rec(3, 6, "p")));
    ASSERT_TRUE(r.ingest(rec(3, 6, "p")));  // identical duplicate (copy-forward twin)
    ASSERT_TRUE(r.ingest(rec(3, 7, "p")));
    EXPECT_TRUE(r.finalize());
    EXPECT_FALSE(r.failed());

    std::vector<uint64_t> seqs;
    for (const auto& rc : r.recordsForCore(r.ownerCore(VShardId{3})))
        seqs.push_back(rc.vshardSeq);
    EXPECT_EQ(seqs, (std::vector<uint64_t>{5, 6, 7})) << "the duplicate 6 is dropped once";
}

// Two different records claiming the same sequence is genuine corruption.
TEST(JournalReplayTest, ConflictingDuplicateFailsClosed) {
    JournalReplay r(2);
    ASSERT_TRUE(r.ingest(rec(3, 5, "a")));
    ASSERT_TRUE(r.ingest(rec(3, 5, "DIFFERENT")));  // same seq, different bytes
    EXPECT_FALSE(r.finalize());
    EXPECT_TRUE(r.failed());
    EXPECT_NE(r.failureDetail().find("conflicting duplicate"), std::string::npos);
}

TEST(JournalReplayTest, SequenceGapFailsClosed) {
    JournalReplay r(2);
    ASSERT_TRUE(r.ingest(rec(7, 10)));
    ASSERT_TRUE(r.ingest(rec(7, 11)));
    ASSERT_TRUE(r.ingest(rec(7, 13)));  // gap: 12 missing
    EXPECT_FALSE(r.finalize());
    EXPECT_TRUE(r.failed());
    EXPECT_NE(r.failureDetail().find("discontinuity"), std::string::npos);
    // recordsForCore is empty on failure.
    EXPECT_TRUE(r.recordsForCore(r.ownerCore(VShardId{7})).empty());
}

TEST(JournalReplayTest, GapCoveredByALaterSnapshotIsAccepted) {
    JournalReplay r(2);
    ASSERT_TRUE(r.ingest(rec(7, 10, "obsolete-before-snapshot")));
    JournalRecord snapshot = rec(7, 13, "snapshot");  // released 11-12 were collected
    snapshot.kind = JournalRecordKind::Snapshot;
    ASSERT_TRUE(r.ingest(snapshot));
    ASSERT_TRUE(r.ingest(rec(7, 14, "live-after-snapshot")));
    EXPECT_TRUE(r.finalize());
    EXPECT_FALSE(r.failed());
}

TEST(JournalReplayTest, GapAfterTheLatestSnapshotStillFailsClosed) {
    JournalReplay r(2);
    JournalRecord snapshot = rec(7, 10, "snapshot");
    snapshot.kind = JournalRecordKind::Snapshot;
    ASSERT_TRUE(r.ingest(snapshot));
    ASSERT_TRUE(r.ingest(rec(7, 12, "missing-live-record-11")));
    EXPECT_FALSE(r.finalize());
    EXPECT_TRUE(r.failed());
    EXPECT_NE(r.failureDetail().find("discontinuity"), std::string::npos);
}

TEST(JournalReplayTest, OutOfRangeVShardFailsClosed) {
    JournalReplay r(2);
    JournalRecord bad;
    bad.vshard = VShardId{4096};  // constructible, but out of range
    bad.vshardSeq = 1;
    EXPECT_FALSE(r.ingest(bad));
    EXPECT_TRUE(r.failed());
    EXPECT_FALSE(r.finalize());  // stays failed
}

TEST(JournalReplayTest, IngestAfterFinalizeThrows) {
    JournalReplay r(2);
    ASSERT_TRUE(r.ingest(rec(1, 1)));
    EXPECT_TRUE(r.finalize());
    EXPECT_THROW(r.ingest(rec(1, 2)), std::logic_error);
}

TEST(JournalReplayTest, InterleavedVShardsEachKeepIndependentBaselines) {
    JournalReplay r(3);
    // Two VShards with different starting sequences (post-GC retained logs).
    ASSERT_TRUE(r.ingest(rec(1, 100)));
    ASSERT_TRUE(r.ingest(rec(2, 5)));
    ASSERT_TRUE(r.ingest(rec(1, 101)));
    ASSERT_TRUE(r.ingest(rec(2, 6)));
    ASSERT_TRUE(r.ingest(rec(1, 102)));
    EXPECT_TRUE(r.finalize());
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
        EXPECT_TRUE(r.finalize());
        EXPECT_FALSE(r.failed());

        std::map<uint16_t, std::vector<uint64_t>> seqs;
        std::map<uint16_t, unsigned> coreOf;
        for (unsigned core = 0; core < coreCount; ++core) {
            for (const auto& rc : r.recordsForCore(core)) {
                seqs[rc.vshard.value()].push_back(rc.vshardSeq);
                auto [it, inserted] = coreOf.emplace(rc.vshard.value(), core);
                EXPECT_EQ(it->second, core)
                    << "vshard " << rc.vshard.value() << " split across cores at cc=" << coreCount;
            }
        }
        for (const auto& [v, core] : coreOf)
            EXPECT_EQ(core, assignCore(VShardId{v}, coreCount));
        return seqs;
    };

    const auto under2 = perVShardSeqs(2);
    const auto under4 = perVShardSeqs(4);
    const auto under7 = perVShardSeqs(7);
    EXPECT_EQ(under2, under4);
    EXPECT_EQ(under4, under7);
    for (uint16_t v : vshards)
        EXPECT_EQ(under4.at(v), (std::vector<uint64_t>{1, 2, 3, 4}));
}

}  // namespace
