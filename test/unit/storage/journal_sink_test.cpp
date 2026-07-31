// D-10: the shared per-shard Raft journal and its group-commit coalescer.
//
// The property under test is the ORDERING CONTRACT, not the throughput: a waiter's
// sync() must not resolve until an fdatasync covering ITS appends has completed.
// The tests below check that DIRECTLY -- by reading the segment file back off disk
// at the instant sync() resolves -- rather than by trusting the counters, because a
// coalescer that released waiters early would look identical in the counters and
// would lose acknowledged writes.
#include "../../../lib/storage/journal_sink.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/file.hh>
#include <seastar/util/later.hh>
#include <span>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ASSERT_* cannot early-return from a coroutine; guard explicitly.
#define ASSERT_TRUE_OR_RETURN(cond) \
    do {                            \
        EXPECT_TRUE(cond);          \
        if (!(cond))                \
            co_return;              \
    } while (0)

namespace {

using timestar::DirectJournalSink;
using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::SharedShardJournal;
using timestar::VShardId;

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_js_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 3;
    h.bootId.fill(0x33);
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

// Everything a fresh reader would find durable in the segment RIGHT NOW -- i.e.
// what a crash at this instant would leave behind. This is the only honest way to
// assert "durable before released": it reads the file, not the writer's own state.
seastar::future<std::vector<JournalRecord>> durableRecords(const fs::path& dir, uint64_t segment) {
    const auto path = dir / JournalWriter::segmentFilename(segment);
    const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(path);
    auto scan = timestar::scanJournalSegment(std::span<const char>(bytes.data(), bytes.size()));
    std::vector<JournalRecord> out;
    if (scan)
        out = std::move(scan->records);
    co_return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// The default sink: unchanged behaviour, one fsync per sync().
// ---------------------------------------------------------------------------

seastar::future<> testDirectSinkSyncsAlone() {
    const auto dir = tmpDir("direct");
    JournalWriter w(dir, header(), 1u << 20);
    co_await w.open();
    DirectJournalSink sink(w);
    const uint64_t before = sink.fsyncs();
    for (uint64_t s = 1; s <= 4; ++s) {
        auto r = rec(7, s, "payload");
        co_await sink.append(r);
        co_await sink.sync();
    }
    // One barrier per sync: the per-VShard layout coalesces NOTHING, which is the
    // baseline D-10 exists to improve on.
    EXPECT_EQ(sink.syncRequests(), 4u);
    EXPECT_EQ(sink.fsyncs() - before, 4u);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalSinkTest, DirectSinkSyncsAlonePerRequest) {
    testDirectSinkSyncsAlone().get();
}

// ---------------------------------------------------------------------------
// The shared journal: coalescing, and the ordering that makes it safe.
// ---------------------------------------------------------------------------

seastar::future<> testSharedJournalCoalesces() {
    const auto dir = tmpDir("coalesce");
    JournalWriter w(dir, header(), 1u << 26);
    co_await w.open();
    SharedShardJournal sink(w);

    // Four "groups" append and then sync. The FIRST sync starts a round
    // immediately (nothing is in flight), and that round's barrier is real I/O, so
    // the other three register while it is in flight and are served together by the
    // next one. That is the production shape: a group that arrives during a round
    // rides the following fsync rather than paying for its own.
    std::vector<JournalRecord> records;
    for (uint16_t g = 0; g < 4; ++g)
        records.push_back(rec(g, 1, "entry"));
    for (const auto& r : records)
        co_await sink.append(r);

    std::vector<seastar::future<>> waits;
    for (int i = 0; i < 4; ++i)
        waits.push_back(sink.sync());  // started, not awaited: they overlap
    co_await seastar::when_all_succeed(waits.begin(), waits.end());

    EXPECT_EQ(sink.syncRequests(), 4u);
    EXPECT_EQ(sink.rounds(), 2u) << "one round in flight, one for everyone who arrived during it";
    EXPECT_EQ(sink.maxRoundWaiters(), 3u) << "three sync()s served by ONE fdatasync";
    EXPECT_LT(sink.fsyncs(), sink.syncRequests()) << "coalescing must actually reduce the fsync count";

    co_await sink.stop();
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalSinkTest, SharedJournalCoalescesConcurrentSyncsIntoOneFsync) {
    testSharedJournalCoalesces().get();
}

seastar::future<> testWaiterIsNeverReleasedBeforeItsFsync() {
    const auto dir = tmpDir("ordering");
    JournalWriter w(dir, header(), 1u << 26);
    co_await w.open();
    const uint64_t seg = w.currentSegmentNumber();
    SharedShardJournal sink(w);

    // THE CONTRACT, checked against the disk. Ten groups append one record each and
    // sync concurrently. When a waiter's sync() resolves, every record appended
    // before it asked must already be readable from the segment FILE -- because
    // that is precisely the state a kill -9 at that instant would leave, and the
    // group is about to send/apply/ack on the strength of it.
    std::vector<JournalRecord> records;
    for (uint16_t g = 0; g < 10; ++g)
        records.push_back(rec(g, 1, "durable-before-observable"));
    for (const auto& r : records)
        co_await sink.append(r);

    std::vector<seastar::future<>> waits;
    for (int i = 0; i < 10; ++i)
        waits.push_back(sink.sync());
    co_await seastar::when_all_succeed(waits.begin(), waits.end());

    auto onDisk = co_await durableRecords(dir, seg);
    EXPECT_EQ(onDisk.size(), records.size()) << "a waiter was released over records that were not durable";
    for (size_t i = 0; i < records.size() && i < onDisk.size(); ++i)
        EXPECT_EQ(onDisk[i], records[i]) << i;

    co_await sink.stop();
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalSinkTest, NoWaiterIsReleasedBeforeItsCoveringFsync) {
    testWaiterIsNeverReleasedBeforeItsFsync().get();
}

seastar::future<> testLateJoinerGetsItsOwnRound() {
    const auto dir = tmpDir("latejoin");
    JournalWriter w(dir, header(), 1u << 26);
    co_await w.open();
    const uint64_t seg = w.currentSegmentNumber();
    SharedShardJournal sink(w);

    // Group A appends and syncs to completion. Group B then appends -- so B's bytes
    // did NOT exist when A's barrier ran -- and syncs. B must get a barrier of its
    // own; a coalescer that credited B to A's completed round would release it over
    // bytes that were never flushed.
    auto a = rec(1, 1, "first");
    co_await sink.append(a);
    co_await sink.sync();
    const uint64_t afterA = sink.fsyncs();
    {
        auto onDisk = co_await durableRecords(dir, seg);
        EXPECT_EQ(onDisk.size(), 1u);
        if (!onDisk.empty())
            EXPECT_EQ(onDisk[0], a);
    }

    auto b = rec(2, 1, "second");
    co_await sink.append(b);
    co_await sink.sync();
    EXPECT_GT(sink.fsyncs(), afterA) << "a later append must be covered by a LATER fsync";
    {
        auto onDisk = co_await durableRecords(dir, seg);
        EXPECT_EQ(onDisk.size(), 2u);
        if (onDisk.size() >= 2)
            EXPECT_EQ(onDisk[1], b);
    }

    co_await sink.stop();
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalSinkTest, ARecordAppendedAfterARoundGetsALaterFsync) {
    testLateJoinerGetsItsOwnRound().get();
}

seastar::future<> testSyncAfterStopFailsClosed() {
    const auto dir = tmpDir("stopped");
    JournalWriter w(dir, header(), 1u << 26);
    co_await w.open();
    SharedShardJournal sink(w);
    auto r = rec(1, 1, "x");
    co_await sink.append(r);
    co_await sink.sync();
    co_await sink.stop();

    // After stop() no round will ever run, so a waiter would wait forever. A hung
    // sync() is a group that neither acks nor fails, which is worse than an error.
    bool threw = false;
    try {
        co_await sink.sync();
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "sync() after stop() must fail, never hang";

    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalSinkTest, SyncAfterStopFailsRatherThanHangs) {
    testSyncAfterStopFailsClosed().get();
}

// ---------------------------------------------------------------------------
// runExclusive(): the seam segment GC copy-forward runs inside (debt D-34).
// ---------------------------------------------------------------------------
//
// Copy-forward APPENDS relocated records to the very writer every group on the shard is
// group-committing through, and then barriers. Interleaving that with a round is not a
// tidiness question: an append that lands inside a barrier is written at an offset
// computed before it existed and then erased from `tail_` as though it had been flushed.
// So the exclusion must hold for the WHOLE copy, not per call -- a round that barriered
// between two relocated records would report them durable while the source segment is
// about to be unlinked.
//
// ASSERTED WITH AN ORDERING TOKEN, not a clock. Both sides stamp a shared monotonic step
// counter; the exclusive section's stamps must come out CONTIGUOUS, i.e. no round stamp
// ever falls between them. A sleep-versus-fdatasync race would pass against broken code
// on slow storage, which is exactly how the first version of this test failed to detect
// an unlocked runExclusive.
//
// The detector still needs the competing round's barrier to actually COMPLETE while the
// section is open, so the wait below yields UNTIL THE ROUND STAMPS -- up to a generous
// turn cap -- rather than a fixed number of turns. That is the difference that makes its
// power independent of disk speed: a slow fdatasync costs more reactor turns, and the loop
// simply spends more of them. (Under the FIXED code the round can never stamp, so the loop
// always runs to the cap; a few thousand reactor turns is microseconds of scheduling.)
// Reactor turns the exclusive section will wait for the competing round to finish. Only an
// upper bound: the loop exits as soon as the round stamps, which is the failure it is
// looking for.
constexpr int kMaxWaitTurns = 20000;

seastar::future<> testRunExclusiveHoldsOffGroupCommitRounds() {
    const auto dir = tmpDir("exclusive");
    JournalWriter w(dir, header(), 1u << 20);
    co_await w.open();
    SharedShardJournal sink(w);

    unsigned step = 0;
    std::vector<unsigned> exclusiveSteps;
    std::vector<unsigned> roundSteps;
    uint64_t roundsAtEntry = 0;
    uint64_t roundsAtExit = 0;

    auto ex = sink.runExclusive([&]() -> seastar::future<> {
        roundsAtEntry = sink.rounds();
        exclusiveSteps.push_back(++step);
        co_await w.append(rec(1, 1, "relocated-a"));
        exclusiveSteps.push_back(++step);
        // `yield()` ALWAYS defers -- `coroutine::maybe_yield` does not (it only yields
        // under preemption pressure, so in a tight loop it is a no-op and the round never
        // gets a turn, which made an earlier version of this test pass against the
        // unlocked code).
        for (int i = 0; i < kMaxWaitTurns && roundSteps.empty(); ++i)
            co_await seastar::yield();
        exclusiveSteps.push_back(++step);
        co_await w.append(rec(1, 2, "relocated-b"));
        co_await w.barrier();
        exclusiveSteps.push_back(++step);
        roundsAtExit = sink.rounds();
    });

    // A group tries to group-commit while the copy is in flight.
    co_await sink.append(rec(2, 1, "group"));
    auto s = sink.sync().then([&] { roundSteps.push_back(++step); });

    co_await std::move(ex);
    co_await std::move(s);

    // 1. The exclusive section's stamps are CONTIGUOUS: nothing else stamped between them.
    ASSERT_TRUE_OR_RETURN(exclusiveSteps.size() == 4);
    for (size_t i = 1; i < exclusiveSteps.size(); ++i)
        EXPECT_EQ(exclusiveSteps[i], exclusiveSteps[i - 1] + 1)
            << "a group-commit round stamped between two steps of the copy-forward";
    // 2. The round's stamp is strictly after the whole section.
    ASSERT_TRUE_OR_RETURN(roundSteps.size() == 1);
    EXPECT_GT(roundSteps[0], exclusiveSteps.back());
    // 3. And no round's BARRIER completed while the section held the writer -- the round
    //    counter is bumped immediately after the barrier attempt, so this is the direct
    //    statement of "no other fdatasync ran against this buffer".
    EXPECT_EQ(roundsAtExit, roundsAtEntry) << "a group-commit barrier ran inside the exclusive section";

    co_await sink.stop();
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalSinkTest, RunExclusiveHoldsOffGroupCommitRounds) {
    testRunExclusiveHoldsOffGroupCommitRounds().get();
}
