#include "../../../lib/storage/journal_gc.hpp"

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

JournalRecord rec(uint16_t vshard, uint64_t seq, std::string payload) {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
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

    // Reopen and recover, then route the recovered stream through the REAL replay
    // layer (not a manual sort): copy-forward relocates vs2's older records behind
    // its newer ones, so replay MUST sort by sequence and still validate gap-free.
    JournalWriter w2(dir, header(), kSmallSegBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();

    timestar::JournalReplay replay(1);
    for (const auto& r : recovered)
        EXPECT_TRUE(replay.ingest(r));
    EXPECT_TRUE(replay.finalize()) << "recovery must not fail closed: " << replay.failureDetail();

    std::vector<uint64_t> vs2Seqs;
    for (const auto& r : replay.recordsForCore(replay.ownerCore(VShardId{2}))) {
        if (r.vshard.value() == 2) {
            EXPECT_EQ(r.payload, "vs2-live");
            vs2Seqs.push_back(r.vshardSeq);
        }
    }
    // Already in sequence order out of replay; no laggard record may be lost.
    EXPECT_EQ(vs2Seqs, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6}));
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
    EXPECT_EQ(result.pinnedSegments.size(), before - 1);
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
    EXPECT_EQ(result.pinnedSegments.size(), before - 1);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalGcTest, CopyForwardRespectsItsBudget) {
    testCopyForwardRespectsItsBudget().get();
}

// CRASH BETWEEN THE COPY-FORWARD BARRIER AND THE UNLINK. That window is not avoidable --
// two files cannot be made durable and removed atomically -- so the design pays for it
// with a byte-identical DUPLICATE and requires recovery to absorb one. Modelled by taking
// the segment's bytes before the collect and putting the file back afterwards, which is
// exactly the on-disk state a kill -9 in that window leaves.
seastar::future<> testCrashBetweenTheCopyBarrierAndTheUnlinkRecovers() {
    const auto dir = tmpDir("crashgc");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    for (uint64_t s = 1; s <= 6; ++s) {
        co_await w.append(rec(1, s, "vs1"));
        co_await w.append(rec(2, s, "vs2-live"));
    }
    co_await w.barrier();
    const uint64_t active = w.currentSegmentNumber();

    // Snapshot every sealed segment's bytes: these are the files GC is about to remove.
    std::vector<std::pair<fs::path, std::string>> saved;
    for (const auto& e : fs::directory_iterator(dir)) {
        auto n = JournalWriter::parseSegmentFilename(e.path().filename().string());
        if (n && *n < active) {
            const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(e.path());
            saved.emplace_back(e.path(), std::string(bytes.data(), bytes.size()));
        }
    }
    ASSERT_TRUE_OR_RETURN(!saved.empty());

    JournalRetention ret;
    ret.setReleased(VShardId{1}, 1000);
    ret.setReleased(VShardId{2}, 0);
    auto result = co_await JournalGc::collect(dir, active, w, ret);
    EXPECT_GT(result.copiedRecords, 0u);
    co_await w.close();

    // "The crash": the unlink never became durable, so the old segments are still there
    // alongside the copies that were just made durable in the active segment.
    for (const auto& [path, bytes] : saved) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    JournalWriter w2(dir, header(), kSmallSegBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();

    timestar::JournalReplay replay(1);
    for (const auto& r : recovered)
        EXPECT_TRUE(replay.ingest(r));
    EXPECT_TRUE(replay.finalize()) << "a duplicated record must be absorbed, not fail recovery: "
                                   << replay.failureDetail();
    std::vector<uint64_t> vs2Seqs;
    for (const auto& r : replay.recordsForCore(replay.ownerCore(VShardId{2})))
        if (r.vshard.value() == 2)
            vs2Seqs.push_back(r.vshardSeq);
    EXPECT_EQ(vs2Seqs, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6})) << "exactly one copy of every laggard record";
    fs::remove_all(dir);
}
TEST(JournalGcTest, CrashBetweenTheCopyBarrierAndTheUnlinkRecovers) {
    testCrashBetweenTheCopyBarrierAndTheUnlinkRecovers().get();
}

// A PARTIALLY-DELETED SEQUENCE RECOVERS. GC deletes oldest-first, one segment at a time,
// so a crash mid-run leaves a prefix removed and the rest present. Each deletion removes
// only records at or below a released watermark, so what survives is still a gap-free
// suffix per VShard -- which is what recovery validates.
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
    timestar::JournalReplay replay(1);
    for (const auto& r : recovered)
        EXPECT_TRUE(replay.ingest(r));
    EXPECT_TRUE(replay.finalize()) << "a retained log may begin mid-sequence after GC: " << replay.failureDetail();
    const auto& out = replay.recordsForCore(replay.ownerCore(VShardId{1}));
    EXPECT_FALSE(out.empty());
    for (size_t i = 1; i < out.size(); ++i)
        EXPECT_EQ(out[i].vshardSeq, out[i - 1].vshardSeq + 1) << "the surviving suffix must be gap-free";
    fs::remove_all(dir);
}
TEST(JournalGcTest, APartiallyDeletedSequenceRecovers) {
    testAPartiallyDeletedSequenceRecovers().get();
}

}  // namespace
