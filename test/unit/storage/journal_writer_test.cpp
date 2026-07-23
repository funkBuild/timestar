#include "../../../lib/storage/journal_writer.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_jw_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 2;
    h.bootId.fill(0x22);
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

// ---- pure filename helpers (no reactor needed) ----

TEST(JournalWriterTest, SegmentFilenameRoundTrips) {
    EXPECT_EQ(JournalWriter::segmentFilename(0), "seg_00000000000000000000.jnl");
    EXPECT_EQ(JournalWriter::segmentFilename(42), "seg_00000000000000000042.jnl");
    EXPECT_EQ(JournalWriter::parseSegmentFilename("seg_00000000000000000042.jnl"), 42u);
    EXPECT_FALSE(JournalWriter::parseSegmentFilename("seg_42.jnl").has_value());                // not padded
    EXPECT_FALSE(JournalWriter::parseSegmentFilename("seg_00000000000000000042").has_value());  // no suffix
    EXPECT_FALSE(JournalWriter::parseSegmentFilename("MANIFEST").has_value());
    EXPECT_FALSE(JournalWriter::parseSegmentFilename("seg_x.jnl").has_value());
}

// ---- I/O tests (run on the reactor via .get()) ----

seastar::future<> testWriteBarrierReopenRecover() {
    const auto dir = tmpDir("recover");
    const std::vector<JournalRecord> records = {rec(0, 1, "alpha"), rec(0, 2, "beta"), rec(5, 1, ""),
                                                rec(4095, 100, "gamma")};
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        EXPECT_TRUE(recovered.empty());
        for (const auto& r : records)
            co_await w.append(r);
        co_await w.barrier();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        EXPECT_EQ(recovered.size(), records.size());
        for (size_t i = 0; i < records.size() && i < recovered.size(); ++i)
            EXPECT_EQ(recovered[i], records[i]);
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(JournalWriterTest, WriteBarrierReopenRecover) {
    testWriteBarrierReopenRecover().get();
}

seastar::future<> testRotationAcrossSegments() {
    const auto dir = tmpDir("rotate");
    // A tiny segment so a handful of records force several rotations.
    const size_t segBytes = JournalSegmentHeader::kEncodedBytes + 120;
    std::vector<JournalRecord> records;
    for (uint64_t s = 1; s <= 12; ++s)
        records.push_back(rec(1, s, "payload"));  // ~40 bytes each -> ~2-3 per segment

    uint64_t lastSegment = 0;
    {
        JournalWriter w(dir, header(), segBytes);
        co_await w.open();
        for (const auto& r : records)
            co_await w.append(r);
        co_await w.barrier();
        lastSegment = w.currentSegmentNumber();
        co_await w.close();
    }
    EXPECT_GT(lastSegment, 0u) << "expected at least one rotation";

    {
        JournalWriter w(dir, header(), segBytes);
        auto recovered = co_await w.open();
        EXPECT_EQ(recovered.size(), records.size());
        for (size_t i = 0; i < records.size() && i < recovered.size(); ++i)
            EXPECT_EQ(recovered[i].vshardSeq, records[i].vshardSeq);
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(JournalWriterTest, RotationAcrossSegments) {
    testRotationAcrossSegments().get();
}

seastar::future<> testTornTailIsRecovered() {
    const auto dir = tmpDir("torn");
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        co_await w.append(rec(0, 1, "good-one"));
        co_await w.append(rec(0, 2, "good-two"));
        co_await w.barrier();
        co_await w.close();
    }
    // Simulate a crash mid-append: append a partial record's bytes to seg_0.
    {
        const auto seg0 = dir / JournalWriter::segmentFilename(0);
        std::string partial = rec(0, 3, "torn-tail-never-completed").encode();
        partial.resize(partial.size() / 2);
        std::ofstream ofs(seg0, std::ios::binary | std::ios::app);
        ofs << partial;
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        EXPECT_EQ(recovered.size(), 2u);  // torn tail dropped, good records intact
        if (recovered.size() >= 2) {
            EXPECT_EQ(recovered[0].payload, "good-one");
            EXPECT_EQ(recovered[1].payload, "good-two");
        }
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(JournalWriterTest, TornTailIsRecovered) {
    testTornTailIsRecovered().get();
}

// Regression for the recovery-idempotency bug: a legitimately torn final
// segment must stay recoverable across MORE than one restart, even once newer
// segments exist (recovery repairs the torn tail rather than leaving it).
seastar::future<> testSecondReopenAfterTornDoesNotFence() {
    const auto dir = tmpDir("torn2");
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        co_await w.append(rec(0, 1, "one"));
        co_await w.append(rec(0, 2, "two"));
        co_await w.barrier();
        co_await w.close();
    }
    {
        const auto seg0 = dir / JournalWriter::segmentFilename(0);
        std::string partial = rec(0, 3, "torn").encode();
        partial.resize(partial.size() / 2);
        std::ofstream(seg0, std::ios::binary | std::ios::app) << partial;
    }
    // Run B: recover (this must REPAIR the torn tail) and create seg_1.
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        EXPECT_EQ(recovered.size(), 2u);
        co_await w.close();
    }
    // Run C: seg_0 is now non-final (seg_1 exists). If Run B had left it torn,
    // this would fence and throw. It must recover cleanly.
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        EXPECT_EQ(recovered.size(), 2u);
        EXPECT_FALSE(w.fenced());
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(JournalWriterTest, SecondReopenAfterTornDoesNotFence) {
    testSecondReopenAfterTornDoesNotFence().get();
}

// Regression: a segment created but crashed before its header was durable
// (empty / un-headed) must be discarded on recovery, not treated as fatal
// corruption.
seastar::future<> testUnheadedFinalSegmentIsDiscarded() {
    const auto dir = tmpDir("unheaded");
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        co_await w.append(rec(0, 1, "one"));
        co_await w.append(rec(0, 2, "two"));
        co_await w.barrier();
        co_await w.close();
    }
    // A crash left an empty seg_1 (created, header not yet durable).
    {
        const auto seg1 = dir / JournalWriter::segmentFilename(1);
        std::ofstream(seg1, std::ios::binary);  // zero-length
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        EXPECT_EQ(recovered.size(), 2u);  // seg_0's records; empty seg_1 discarded
        EXPECT_FALSE(w.fenced());
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(JournalWriterTest, UnheadedFinalSegmentIsDiscarded) {
    testUnheadedFinalSegmentIsDiscarded().get();
}

seastar::future<> testFreshOpenIsEmpty() {
    const auto dir = tmpDir("fresh");
    JournalWriter w(dir, header(), 1u << 20);
    auto recovered = co_await w.open();
    EXPECT_TRUE(recovered.empty());
    EXPECT_EQ(w.currentSegmentNumber(), 0u);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalWriterTest, FreshOpenIsEmpty) {
    testFreshOpenIsEmpty().get();
}

}  // namespace
