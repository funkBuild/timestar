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
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

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

}  // namespace
