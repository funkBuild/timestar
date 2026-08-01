#include "../../../lib/storage/journal_gc.hpp"

#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"  // recoverRaftState: the PRODUCTION reader
#include "../../../lib/storage/journal_replay.hpp"
#include "../../../lib/storage/journal_retention.hpp"
#include "../../../lib/storage/journal_writer.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/file.hh>
#include <string>
#include <utility>
#include <vector>

// ASSERT_* cannot early-return from a coroutine; guard explicitly.
#define ASSERT_TRUE_OR_RETURN(cond) \
    do {                            \
        EXPECT_TRUE(cond);          \
        if (!(cond))                \
            co_return;              \
    } while (0)

namespace fs = std::filesystem;

namespace {

using timestar::JournalGc;
using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalRetention;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_jgc_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x33);
    h.coreNumber = 1;
    h.bootId.fill(0x44);
    return h;
}

// A Data record carrying a REAL Raft index, so recovery reconstructs a log from it.
// `raftIndex == seq` because each of these tests writes one entry per sequence for a
// VShard; that is what lets the assertions below run through `recoverRaftState`, which is
// what production runs, instead of through a replay layer nothing calls.
JournalRecord rec(uint16_t vshard, uint64_t seq, std::string payload) {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
    r.raftIndex = seq;
    r.kind = JournalRecordKind::Data;
    r.payload = std::move(payload);
    return r;
}

// Count how many segment files currently exist on disk.
size_t segmentFileCount(const fs::path& dir) {
    size_t n = 0;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (JournalWriter::parseSegmentFilename(entry.path().filename().string()))
            ++n;
    }
    return n;
}

// A tiny segment so a handful of records force several rotations (sealed segs).
constexpr size_t kSmallSegBytes = JournalSegmentHeader::kEncodedBytes + 100;

seastar::future<> testFullyReleasedSegmentsAreDeleted() {
    const auto dir = tmpDir("released");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 8; ++s)
        co_await w.append(rec(1, s, "payload"));  // one VShard, many sealed segments
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();
    EXPECT_GT(active, 0u) << "expected rotations to seal earlier segments";
    const size_t before = segmentFileCount(dir);

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 1000);  // everything released

    auto result = co_await JournalGc::collect(dir, active, w, ret);
    EXPECT_TRUE(result.copyForwardSegments.empty());
    EXPECT_EQ(result.copiedRecords, 0u);
    EXPECT_EQ(result.deletedSegments.size(), before - 1);  // all sealed segments, active kept

    // The active segment survives; every sealed segment is gone.
    EXPECT_EQ(segmentFileCount(dir), 1u);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalGcTest, FullyReleasedSegmentsAreDeleted) {
    testFullyReleasedSegmentsAreDeleted().get();
}

seastar::future<> testLaggardRecordsAreCopiedForwardAndSurvive() {
    const auto dir = tmpDir("laggard");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    // VShard 1 is caught up; VShard 2 is a laggard. Interleave so early sealed
    // segments carry a mix of released (vs1) and live (vs2) records.
    for (uint64_t s = 1; s <= 6; ++s) {
        co_await w.append(rec(1, s, "vs1"));
        co_await w.append(rec(2, s, "vs2-live"));
    }
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 1000);  // vshard 1 fully released
    ret.setReleased(VShardId{2}, 0);     // vshard 2 laggard: nothing released

    auto result = co_await JournalGc::collect(dir, active, w, ret);
    EXPECT_GT(result.copiedRecords, 0u) << "laggard live records must be copied forward";
    EXPECT_FALSE(result.copyForwardSegments.empty());
    co_await w.close();

    // Reopen and recover through THE PRODUCTION PATH -- `JournalWriter::open()` handing
    // its record set to `recoverRaftState`. Copy-forward relocates vs2's older records
    // behind its newer ones physically, so this is the assertion that matters: recovery
    // orders by vshard_seq, not by position, and reconstructs the laggard's log intact.
    // (Asserting through `JournalReplay` instead would prove nothing about a real restart:
    // that class has no production caller.)
    JournalWriter w2(dir, header(), kSmallSegBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();

    timestar::raft::RecoveredRaftState st = timestar::raft::recoverRaftState(recovered, VShardId{2});
    EXPECT_EQ(st.log.lastIndex(), 6u) << "no laggard record may be lost to copy-forward";
    for (uint64_t i = 1; i <= 6; ++i) {
        const timestar::raft::LogEntry* e = st.log.entryAt(i);
        EXPECT_NE(e, nullptr);
        if (e)
            EXPECT_EQ(e->data, "vs2-live") << "a relocated record must rebuild the same entry, not a shifted one";
    }
    fs::remove_all(dir);
}
TEST(JournalGcTest, LaggardRecordsAreCopiedForwardAndSurvive) {
    testLaggardRecordsAreCopiedForwardAndSurvive().get();
}

seastar::future<> testFullyLiveSegmentsAreLeftUntouched() {
    const auto dir = tmpDir("fullylive");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 8; ++s)
        co_await w.append(rec(3, s, "live"));
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();
    const size_t before = segmentFileCount(dir);

    JournalRetention ret;  // nothing released -> every record live

    auto result = co_await JournalGc::collect(dir, active, w, ret);
    EXPECT_TRUE(result.deletedSegments.empty()) << "fully-live segments must not be churned";
    EXPECT_TRUE(result.copyForwardSegments.empty());
    EXPECT_EQ(segmentFileCount(dir), before);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalGcTest, FullyLiveSegmentsAreLeftUntouched) {
    testFullyLiveSegmentsAreLeftUntouched().get();
}

// ---------------------------------------------------------------------------
// Wiring GC to a live journal (debt D-34). Three properties that are the difference
// between reclaiming disk and losing a replica's log.
// ---------------------------------------------------------------------------

// A SEGMENT HOLDING UN-SNAPSHOTTED STATE IS NOT DELETED. This is the one that matters:
// the laggard's records are the only copy, and with copy-forward OFF (the per-VShard
// layout, where GC must never touch the writer) the ONLY safe answer is to leave the
// segment alone.
seastar::future<> testDeleteOnlyPinsASegmentWithLiveRecords() {
    const auto dir = tmpDir("pinned");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 6; ++s) {
        co_await w.append(rec(1, s, "vs1"));
        co_await w.append(rec(2, s, "vs2-live"));
    }
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();
    const size_t before = segmentFileCount(dir);
    EXPECT_GT(before, 1u) << "expected rotations to seal earlier segments";

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 1000);  // caught up
    ret.setReleased(VShardId{2}, 0);     // laggard: nothing released

    auto result = co_await JournalGc::collect(dir, active, w, ret, JournalGc::Options{.copyForward = false});
    EXPECT_TRUE(result.deletedSegments.empty()) << "a segment holding un-snapshotted state must survive";
    EXPECT_EQ(result.copiedRecords, 0u) << "delete-only must never write to the journal";
    // GC STOPS at the first segment it cannot reclaim rather than skipping ahead, so what
    // survives is a physical suffix -- and `pinnedSegments` is the CENSUS of that suffix
    // (every sealed segment left behind), not just the one that halted the pass.
    EXPECT_EQ(result.pinnedSegments.size(), before - 1);
    EXPECT_EQ(result.pinnedSegments.front(), 0u) << "the halting segment is the oldest sealed one";
    EXPECT_EQ(segmentFileCount(dir), before);
    co_await w.close();

    // And the laggard's records are all still readable.
    JournalWriter w2(dir, header(), kSmallSegBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();
    size_t vs2 = 0;
    for (const auto& r : recovered)
        if (r.vshard.value() == 2)
            ++vs2;
    EXPECT_EQ(vs2, 6u);
    fs::remove_all(dir);
}
TEST(JournalGcTest, DeleteOnlyPinsASegmentWithLiveRecords) {
    testDeleteOnlyPinsASegmentWithLiveRecords().get();
}

// The complement: with every record released, delete-only still deletes. Without this the
// test above would pass on a GC that never deletes anything at all.
seastar::future<> testDeleteOnlyStillDeletesAFullyCoveredSegment() {
    const auto dir = tmpDir("deleteonly");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 8; ++s)
        co_await w.append(rec(1, s, "payload"));
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();
    const size_t before = segmentFileCount(dir);

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 1000);
    auto result = co_await JournalGc::collect(dir, active, w, ret, JournalGc::Options{.copyForward = false});
    EXPECT_EQ(result.deletedSegments.size(), before - 1);
    EXPECT_TRUE(result.pinnedSegments.empty());
    EXPECT_EQ(segmentFileCount(dir), 1u);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalGcTest, DeleteOnlyStillDeletesAFullyCoveredSegment) {
    testDeleteOnlyStillDeletesAFullyCoveredSegment().get();
}

// A MOSTLY-LIVE SEGMENT IS PINNED EVEN WITH COPY-FORWARD ON. Copy-forward is a WRITE:
// rewriting most of a segment to reclaim the rest costs more traffic than it saves, and
// the budget also bounds how long the shared journal's exclusive section holds the writer.
seastar::future<> testCopyForwardRespectsItsBudget() {
    const auto dir = tmpDir("budget");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 6; ++s) {
        co_await w.append(rec(1, s, "vs1"));
        co_await w.append(rec(2, s, "vs2-live"));
    }
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();
    const size_t before = segmentFileCount(dir);

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 1000);
    auto result = co_await JournalGc::collect(dir, active, w, ret,
                                              JournalGc::Options{.copyForward = true, .maxCopyForwardRecords = 0});
    EXPECT_EQ(result.copiedRecords, 0u);
    EXPECT_TRUE(result.deletedSegments.empty());
    EXPECT_EQ(result.pinnedSegments.size(), before - 1)
        << "GC stops at the first over-budget segment and reports every sealed segment it leaves behind";
    EXPECT_EQ(segmentFileCount(dir), before);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalGcTest, CopyForwardRespectsItsBudget) {
    testCopyForwardRespectsItsBudget().get();
}

// SHARED-JOURNAL HEAD-OF-LINE RECLAMATION (debt D-39). Segment 0 is pinned by
// VShard 2, but segment 1 contains only a released VShard-1 record. Shared mode must
// delete segment 1 instead of treating segment 0 as authority over the whole suffix.
// This deliberately leaves VShard 1 with seq {1, 3}; seq 3 is a retained Snapshot, so
// both the production recovery path and the stricter future replay path must accept the
// covered gap while preserving the boundary.
seastar::future<> testSharedGcContinuesPastAPinAndRecoveryAcceptsTheCoveredGap() {
    const auto dir = tmpDir("pastpin");
    constexpr size_t segmentBytes = JournalSegmentHeader::kEncodedBytes + 90;
    JournalWriter w(dir, header(), segmentBytes);
    co_await w.open();

    // These two small records share segment 0. VShard 1's is released; VShard 2's is
    // live, making the segment partially released and over the zero-record copy budget.
    co_await w.append(rec(1, 1, "old"));
    co_await w.append(rec(2, 1, "live"));
    // Large enough to rotate into segment 1, which is fully released.
    co_await w.append(rec(1, 2, std::string(30, 'r')));
    // Rotate once more so segment 1 is sealed and retain a snapshot in active segment 2.
    JournalRecord snapshot = rec(1, 3, std::string(60, 's'));
    snapshot.kind = JournalRecordKind::Snapshot;
    snapshot.raftIndex = 2;
    snapshot.raftTerm = 1;
    co_await w.append(snapshot);
    co_await w.barrier();
    ASSERT_TRUE_OR_RETURN(w.currentSegmentNumber() >= 2);

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 2);
    ret.setReleased(VShardId{2}, 0);
    auto result = co_await JournalGc::collect(
        dir, w.currentSegmentNumber(), w, ret,
        JournalGc::Options{.copyForward = true, .maxCopyForwardRecords = 0, .continuePastPinned = true});
    EXPECT_EQ(result.pinnedSegments, (std::vector<uint64_t>{0}));
    EXPECT_EQ(result.deletedSegments, (std::vector<uint64_t>{1}))
        << "a pin may retain itself, not an independently released physical suffix";
    co_await w.close();

    JournalWriter w2(dir, header(), segmentBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();
    auto state = timestar::raft::recoverRaftState(recovered, VShardId{1});
    ASSERT_TRUE_OR_RETURN(state.snapshot.has_value());
    EXPECT_EQ(state.snapshot->index, 2u);
    EXPECT_EQ(state.nextSeq, 4u);

    timestar::JournalReplay replay(1);
    for (const auto& record : recovered)
        ASSERT_TRUE_OR_RETURN(replay.ingest(record));
    EXPECT_TRUE(replay.finalize()) << replay.failureDetail();
    fs::remove_all(dir);
}
TEST(JournalGcTest, SharedGcContinuesPastAPinAndRecoveryAcceptsTheCoveredGap) {
    testSharedGcContinuesPastAPinAndRecoveryAcceptsTheCoveredGap().get();
}

// A PARTIALLY-DELETED SEQUENCE RECOVERS. GC deletes oldest-first, one segment at a time,
// and STOPS at the first segment it cannot reclaim, so a crash mid-run leaves a prefix
// removed and the rest present -- a physical SUFFIX of the segment sequence, and therefore
// a gap-free suffix per VShard. (The crash window between a copy-forward barrier and its
// unlink is asserted separately, against the production recovery path, in
// journal_reclaim_floor_test.cpp.)
seastar::future<> testAPartiallyDeletedSequenceRecovers() {
    const auto dir = tmpDir("partial");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 12; ++s)
        co_await w.append(rec(1, s, "payload"));
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();
    co_await w.close();

    // Remove the OLDEST sealed segment only -- "the run stopped after one unlink".
    std::vector<uint64_t> sealed;
    for (const auto& e : fs::directory_iterator(dir))
        if (auto n = JournalWriter::parseSegmentFilename(e.path().filename().string()); n && *n < active)
            sealed.push_back(*n);
    std::sort(sealed.begin(), sealed.end());
    ASSERT_TRUE_OR_RETURN(sealed.size() >= 2);
    fs::remove(dir / JournalWriter::segmentFilename(sealed.front()));

    JournalWriter w2(dir, header(), kSmallSegBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();
    // Asserted on the RECORD STREAM `JournalWriter::open()` returns -- the production
    // reader -- rather than on a reconstructed Raft log, and deliberately: a deleted prefix
    // means the surviving records are legitimately NOT a log starting at index 1, so the
    // property under test is the one GC actually guarantees (contiguity of what is left),
    // not what a log builder would make of it.
    std::vector<uint64_t> seqs;
    for (const auto& r : recovered)
        if (r.vshard.value() == 1)
            seqs.push_back(r.vshardSeq);
    std::sort(seqs.begin(), seqs.end());  // physical order is not authoritative after GC
    EXPECT_FALSE(seqs.empty());
    for (size_t i = 1; i < seqs.size(); ++i)
        EXPECT_EQ(seqs[i], seqs[i - 1] + 1) << "the surviving suffix must be gap-free";
    fs::remove_all(dir);
}
TEST(JournalGcTest, APartiallyDeletedSequenceRecovers) {
    testAPartiallyDeletedSequenceRecovers().get();
}

}  // namespace
