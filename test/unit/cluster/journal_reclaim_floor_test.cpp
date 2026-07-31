// THE RECLAIM FLOOR (debt D-34): which of a group's journal records it no longer needs.
//
// D-6 gave the Raft log a snapshot boundary, so restart replay is bounded. Nothing
// DELETED the sealed segments holding the records below that boundary, so `cluster_raft/`
// grew without limit however often a node snapshotted. Segment GC needs one number per
// VShard -- the highest vshard_seq whose record is dead -- and getting that number wrong
// in the generous direction is data loss, not a space leak.
//
// The obvious answer ("everything below the Snapshot record") is wrong twice over, and
// these tests are built around exactly those two ways:
//
//   * the log entries the snapshot deliberately RETAINED were appended BEFORE it, so
//     their seqs are LOWER than the snapshot record's;
//   * the HardState record is written only when term/vote CHANGE, so a group with stable
//     leadership has one, from startup, below everything.
//
// Each test names the record that must survive and asserts the floor stays under it.
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_gc.hpp"
#include "../../../lib/storage/journal_retention.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/file.hh>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace timestar::raft;
using timestar::JournalRecord;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;

namespace fs = std::filesystem;

namespace {

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_d34floor_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x21);
    h.coreNumber = 1;
    h.bootId.fill(0x22);
    return h;
}

LogEntry entry(Term t, LogIndex i, std::string data) {
    LogEntry e;
    e.term = t;
    e.index = i;
    e.type = EntryType::Normal;
    e.data = std::move(data);
    return e;
}

// A tiny segment so a handful of records force several rotations (sealed segments).
constexpr size_t kSmallSegBytes = JournalSegmentHeader::kEncodedBytes + 120;

Snapshot snapAt(LogIndex index, Term term) {
    Snapshot s;
    s.index = index;
    s.term = term;
    s.data = "payload";
    return s;
}

// Nothing is releasable before a snapshot exists: without one the whole log is live and
// the very first record is the log's own beginning.
seastar::future<> testNoSnapshotReleasesNothing() {
    const auto dir = tmpDir("nosnap");
    JournalWriter w(dir, header(), 1u << 20);
    co_await w.open();
    JournalRaftPersistence p(w, VShardId{4});
    EXPECT_EQ(p.releasedSeq(), 0u);
    co_await p.persistHardState(HardState{1, 1});
    co_await p.persistEntries({entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")});
    co_await p.sync();
    EXPECT_EQ(p.releasedSeq(), 0u) << "an uncompacted log releases nothing, ever";
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, NoSnapshotReleasesNothing) {
    testNoSnapshotReleasesNothing().get();
}

// THE CENTRAL CASE. Entries 1..5, snapshot at 3. Entries 4 and 5 are RETAINED by
// compaction and their records were written BEFORE the snapshot record, so the floor must
// stop below entry 4's record -- releasing to the snapshot record's own seq would delete
// exactly the unflushed suffix D-6 exists to keep.
seastar::future<> testFloorStopsBelowTheRetainedSuffix() {
    const auto dir = tmpDir("suffix");
    JournalWriter w(dir, header(), 1u << 20);
    co_await w.open();
    JournalRaftPersistence p(w, VShardId{9});

    co_await p.persistHardState(HardState{2, 1});  // seq 1
    const uint64_t seqEntry1 = p.nextSeq();        // seq 2 .. 6 are entries 1..5
    co_await p.persistEntries(
        {entry(2, 1, "e1"), entry(2, 2, "e2"), entry(2, 3, "e3"), entry(2, 4, "e4"), entry(2, 5, "e5")});
    const uint64_t seqEntry4 = seqEntry1 + 3;
    co_await p.sync();

    co_await p.persistSnapshot(snapAt(3, 2), /*receivedFromPeer=*/false);
    const uint64_t snapSeq = p.nextSeq() - 1;
    // ...and the hard state right behind it, exactly as RaftGroup::compact does -- the
    // startup HardState record at seq 1 would otherwise pin the floor to nothing, which
    // is its own test below.
    co_await p.persistHardState(HardState{2, 1});
    co_await p.sync();  // only a SUCCESSFUL sync promotes the floor -- see releasedSeq()
    EXPECT_GT(snapSeq, seqEntry4) << "the snapshot record is NEWER than the entries it retains";

    // Floor is one below entry 4's record -- NOT one below the snapshot record.
    EXPECT_EQ(p.releasedSeq(), seqEntry4 - 1);
    EXPECT_LT(p.releasedSeq(), snapSeq - 1) << "releasing to the snapshot record would delete entries 4 and 5";
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, FloorStopsBelowTheRetainedSuffix) {
    testFloorStopsBelowTheRetainedSuffix().get();
}

// A group whose whole log is compacted away still needs its snapshot record AND its hard
// state. With no entries left, the floor is bounded by whichever of those two is older.
seastar::future<> testFloorNeverPassesTheNewestHardState() {
    const auto dir = tmpDir("hardstate");
    JournalWriter w(dir, header(), 1u << 20);
    co_await w.open();
    JournalRaftPersistence p(w, VShardId{11});

    co_await p.persistHardState(HardState{5, 3});  // seq 1 -- the ONLY hard state
    co_await p.persistEntries({entry(5, 1, "a"), entry(5, 2, "b")});
    co_await p.persistSnapshot(snapAt(2, 5), /*receivedFromPeer=*/false);
    co_await p.sync();

    // The whole log is compacted, so the entry term drops out -- but the hard state at
    // seq 1 is still the only record of this replica's term and vote. Nothing may go.
    EXPECT_EQ(p.releasedSeq(), 0u) << "the only HardState record pins the floor at the bottom";

    // Re-persisting the hard state (which RaftGroup::compact now does) moves that pin up
    // and is the ONLY reason a stable-leadership group ever reclaims anything.
    co_await p.persistHardState(HardState{5, 3});
    co_await p.sync();
    const uint64_t hsSeq = p.nextSeq() - 1;
    const uint64_t snapSeq = hsSeq - 1;
    EXPECT_EQ(p.releasedSeq(), snapSeq - 1) << "now bounded by the snapshot record, which is the older of the two";
    EXPECT_GT(p.releasedSeq(), 0u);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, FloorNeverPassesTheNewestHardState) {
    testFloorNeverPassesTheNewestHardState().get();
}

// A RESTART MUST NOT RESET THE FLOOR UPWARD. A fresh persistence object knows none of the
// on-disk records' seqs; if it is not seeded, its "oldest live entry" is the first entry
// appended AFTER the restart -- a much higher seq -- and the floor jumps straight over the
// recovered log suffix. recoverRaftState therefore reports the seed and this asserts the
// seeded floor equals the pre-restart one.
seastar::future<> testARecoveredJournalSeedsTheSameFloor() {
    const auto dir = tmpDir("seed");
    const VShardId vs{13};
    uint64_t floorBefore = 0;
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        co_await p.persistHardState(HardState{4, 2});
        co_await p.persistEntries({entry(4, 1, "a"), entry(4, 2, "b"), entry(4, 3, "c"), entry(4, 4, "d")});
        co_await p.persistSnapshot(snapAt(2, 4), /*receivedFromPeer=*/false);
        co_await p.persistHardState(HardState{4, 2});
        co_await p.sync();
        floorBefore = p.releasedSeq();
        EXPECT_GT(floorBefore, 0u);
        co_await w.close();
    }
    {
        JournalWriter w2(dir, header(), 1u << 20);
        auto recovered = co_await w2.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        // The seed must name the surviving entries (3 and 4) and both watermark records.
        EXPECT_EQ(st.retention.entrySeqs.size(), 2u);
        EXPECT_GT(st.retention.latestSnapshotSeq, 0u);
        EXPECT_GT(st.retention.latestHardStateSeq, st.retention.latestSnapshotSeq);

        JournalRaftPersistence p2(w2, vs, st.nextSeq);
        EXPECT_EQ(p2.releasedSeq(), 0u) << "unseeded, a fresh object must claim NOTHING is released";
        p2.seedRetention(std::move(st.retention));
        EXPECT_EQ(p2.releasedSeq(), floorBefore) << "the seeded floor must equal the pre-restart floor";
        co_await w2.close();
    }
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, ARecoveredJournalSeedsTheSameFloor) {
    testARecoveredJournalSeedsTheSameFloor().get();
}

// A re-append at a lower index supersedes the records above it (recoverRaftState drops
// that suffix), so those records stop pinning the floor -- and the re-appended entry,
// being newer, becomes the pin instead.
seastar::future<> testAReappendSupersedesTheRecordsAboveIt() {
    const auto dir = tmpDir("reappend");
    JournalWriter w(dir, header(), 1u << 20);
    co_await w.open();
    JournalRaftPersistence p(w, VShardId{17});
    co_await p.persistHardState(HardState{1, 1});
    co_await p.persistEntries({entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")});
    // A new leader overwrites index 2 onward.
    const uint64_t seqReappend = p.nextSeq();
    co_await p.persistEntries({entry(2, 2, "B"), entry(2, 3, "C")});
    co_await p.persistSnapshot(snapAt(1, 1), /*receivedFromPeer=*/false);
    co_await p.persistHardState(HardState{2, 1});
    co_await p.sync();
    // Index 1 is compacted away; the oldest live entry is the RE-APPENDED index 2, whose
    // record is at seqReappend, not the superseded original.
    EXPECT_EQ(p.releasedSeq(), seqReappend - 1);
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, AReappendSupersedesTheRecordsAboveIt) {
    testAReappendSupersedesTheRecordsAboveIt().get();
}

// ===========================================================================
// THE FLOOR IS A DURABILITY WATERMARK, NOT AN INTENT ONE.
// ===========================================================================
//
// The bookkeeping behind the floor advances SYNCHRONOUSLY inside persistSnapshot /
// persistHardState -- before the bytes reach the writer's buffer, let alone a barrier. If
// releasedSeq() reported that, this interleaving destroys a replica:
//
//   compact() appends Snapshot(S) + HardState(S+1) -> sync() throws EIO -> the snapshot
//   sweep logs and carries on -> the floor sits at S-1 while S and S+1 are on no disk ->
//   the next GC pass deletes every sealed segment below it, INCLUDING the group's only
//   HardState -> restart with no vote, no snapshot and no log.
//
// So the test asserts the floor after a failed sync AND that a GC pass driven by it
// deletes nothing.

// A sink that appends for real (so the records exist and the segments seal) but fails
// every sync -- the EIO barrier, without needing a failing disk.
class ThrowingSyncSink final : public timestar::JournalSink {
public:
    explicit ThrowingSyncSink(JournalWriter& w) : w_(w) {}
    seastar::future<> append(const timestar::JournalRecord& r) override { return w_.append(r); }
    seastar::future<> sync() override {
        ++syncRequests_;
        return seastar::make_exception_future<>(std::runtime_error("simulated barrier EIO"));
    }
    [[nodiscard]] uint64_t fsyncs() const override { return w_.fsyncs(); }
    [[nodiscard]] uint64_t syncRequests() const override { return syncRequests_; }

private:
    JournalWriter& w_;
    uint64_t syncRequests_ = 0;
};

seastar::future<> testAFailedSyncNeverAdvancesTheFloor() {
    const auto dir = tmpDir("eio");
    const VShardId vs{23};
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    ThrowingSyncSink sink(w);
    JournalRaftPersistence p(sink, vs);

    co_await p.persistHardState(HardState{7, 4});
    std::vector<LogEntry> es;
    for (uint64_t i = 1; i <= 10; ++i)
        es.push_back(entry(7, i, "entry-payload-" + std::to_string(i)));
    co_await p.persistEntries(std::move(es));

    // The compact() shape: snapshot, hard state, sync -- and the sync fails.
    co_await p.persistSnapshot(snapAt(10, 7), /*receivedFromPeer=*/false);
    co_await p.persistHardState(HardState{7, 4});
    bool threw = false;
    try {
        co_await p.sync();
    } catch (const std::exception&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "the barrier must surface its failure";

    // THE ASSERTION. Every record above is in the writer's buffer and some of it is in
    // sealed segments, but nothing was fsync'd, so nothing may be declared released.
    EXPECT_EQ(p.releasedSeq(), 0u) << "a floor derived from an un-synced snapshot would delete the only HardState";
    EXPECT_TRUE(p.floorFenced());
    // And it stays frozen: a later sync must not promote a sample that assumed those
    // records landed.
    bool threwAgain = false;
    try {
        co_await p.sync();
    } catch (const std::exception&) {
        threwAgain = true;
    }
    EXPECT_TRUE(threwAgain);
    EXPECT_EQ(p.releasedSeq(), 0u);

    // Driven end to end: a GC pass at this floor deletes nothing.
    timestar::JournalRetention ret;
    ret.setReleased(vs, p.releasedSeq());
    auto result = co_await timestar::JournalGc::collect(dir, w.currentSegmentNumber(), w, ret,
                                                        timestar::JournalGcOptions{.copyForward = false});
    EXPECT_TRUE(result.deletedSegments.empty()) << "a floor of 0 must reclaim nothing at all";

    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, AFailedSyncNeverAdvancesTheFloor) {
    testAFailedSyncNeverAdvancesTheFloor().get();
}

// The complement, so the test above cannot pass on a floor that never advances: the same
// sequence over a HEALTHY sink advances the floor and the same GC pass deletes segments.
seastar::future<> testASuccessfulSyncDoesAdvanceTheFloor() {
    const auto dir = tmpDir("okio");
    const VShardId vs{29};
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    JournalRaftPersistence p(w, vs);

    co_await p.persistHardState(HardState{7, 4});
    std::vector<LogEntry> es;
    for (uint64_t i = 1; i <= 10; ++i)
        es.push_back(entry(7, i, "entry-payload-" + std::to_string(i)));
    co_await p.persistEntries(std::move(es));
    co_await p.persistSnapshot(snapAt(10, 7), /*receivedFromPeer=*/false);
    co_await p.persistHardState(HardState{7, 4});
    co_await p.sync();

    EXPECT_GT(p.releasedSeq(), 0u);
    EXPECT_FALSE(p.floorFenced());
    timestar::JournalRetention ret;
    ret.setReleased(vs, p.releasedSeq());
    auto result = co_await timestar::JournalGc::collect(dir, w.currentSegmentNumber(), w, ret,
                                                        timestar::JournalGcOptions{.copyForward = false});
    EXPECT_FALSE(result.deletedSegments.empty()) << "a durable floor must actually reclaim";
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, ASuccessfulSyncDoesAdvanceTheFloor) {
    testASuccessfulSyncDoesAdvanceTheFloor().get();
}

// ===========================================================================
// The crash window between the copy-forward barrier and the unlink, asserted against
// the PRODUCTION recovery path (debt D-34).
// ===========================================================================
//
// That window is unavoidable -- two files cannot be made durable and removed atomically --
// so the design pays for it with a byte-identical DUPLICATE. What must absorb it is
// `JournalWriter::open()` -> `recoverRaftState`, which is what production runs.
// (`JournalReplay::finalize` dedupes too, but it has no production caller, so asserting
// against it would prove nothing about a real restart.)
seastar::future<> testCrashBetweenTheCopyBarrierAndTheUnlinkRecovers() {
    const auto dir = tmpDir("crashgc");
    const VShardId caught{31};   // fully released -> its records are reclaimable
    const VShardId laggard{37};  // nothing released -> its records must be copied forward
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();
    {
        JournalRaftPersistence pc(w, caught);
        JournalRaftPersistence pl(w, laggard);
        for (uint64_t i = 1; i <= 6; ++i) {
            co_await pc.persistEntries({entry(1, i, "caught-" + std::to_string(i))});
            co_await pl.persistEntries({entry(1, i, "laggard-" + std::to_string(i))});
        }
        co_await pc.sync();
    }
    const uint64_t active = w.currentSegmentNumber();

    // Snapshot the sealed segments' bytes: these are the files GC is about to remove.
    std::vector<std::pair<fs::path, std::string>> saved;
    for (const auto& e : fs::directory_iterator(dir)) {
        auto n = JournalWriter::parseSegmentFilename(e.path().filename().string());
        if (n && *n < active) {
            const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(e.path());
            saved.emplace_back(e.path(), std::string(bytes.data(), bytes.size()));
        }
    }
    EXPECT_FALSE(saved.empty());

    timestar::JournalRetention ret;
    ret.setReleased(caught, 1000);
    ret.setReleased(laggard, 0);
    auto result = co_await timestar::JournalGc::collect(dir, active, w, ret);
    EXPECT_GT(result.copiedRecords, 0u) << "the laggard's records must be relocated";
    co_await w.close();

    // "The crash": the unlink never became durable, so the old segments are back alongside
    // the copies just made durable in the active segment.
    for (const auto& [path, bytes] : saved) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }

    JournalWriter w2(dir, header(), kSmallSegBytes);
    auto recovered = co_await w2.open();
    co_await w2.close();

    // THE PRODUCTION PATH. Duplicates must reconstruct the identical log.
    RecoveredRaftState st = recoverRaftState(recovered, laggard);
    EXPECT_EQ(st.log.lastIndex(), 6u) << "no laggard entry may be lost across the crash window";
    for (uint64_t i = 1; i <= 6; ++i) {
        const LogEntry* e = st.log.entryAt(i);
        EXPECT_NE(e, nullptr);
        if (e)
            EXPECT_EQ(e->data, "laggard-" + std::to_string(i)) << "a duplicate must re-apply IDENTICALLY, not shift";
    }
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, CrashBetweenTheCopyBarrierAndTheUnlinkRecovers) {
    testCrashBetweenTheCopyBarrierAndTheUnlinkRecovers().get();
}

// seedRetention promotes its value STRAIGHT into the durable watermark, which is sound
// only because recovered records came off the disk. Called after an append it would launder
// merely-BUFFERED state into durability -- the exact bug class the two-watermark rewrite
// exists to remove -- so the precondition is enforced rather than documented.
seastar::future<> testSeedRetentionRefusesToLaunderBufferedState() {
    const auto dir = tmpDir("seedguard");
    JournalWriter w(dir, header(), kSmallSegBytes);
    co_await w.open();

    JournalRetentionSeed seed;
    seed.latestHardStateSeq = 1;
    seed.latestSnapshotSeq = 2;

    {
        JournalRaftPersistence p(w, VShardId{47});
        // An append happened first: the seed can no longer be assumed durable.
        co_await p.persistHardState(HardState{1, 1});
        bool threw = false;
        try {
            p.seedRetention(seed);
        } catch (const std::logic_error&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "seeding after an append would promote buffered state as durable";
        EXPECT_EQ(p.releasedSeq(), 0u);
    }
    {
        JournalRaftPersistence p(w, VShardId{47});
        p.seedRetention(seed);  // the legitimate call, before anything is appended
        const uint64_t seeded = p.releasedSeq();
        bool threwOnSecond = false;
        try {
            p.seedRetention(seed);
        } catch (const std::logic_error&) {
            threwOnSecond = true;
        }
        EXPECT_TRUE(threwOnSecond) << "a second seed must be refused, not silently re-promoted";
        EXPECT_EQ(p.releasedSeq(), seeded);
    }
    co_await w.close();
    fs::remove_all(dir);
}
TEST(JournalReclaimFloorTest, SeedRetentionRefusesToLaunderBufferedState) {
    testSeedRetentionRefusesToLaunderBufferedState().get();
}

// A retired VShard must be FORGOTTEN, not left at its last watermark: a re-add gets a
// fresh journal whose vshard_seq restarts at 1, and a stale watermark would mark the whole
// new journal released (debt D-40).
TEST(JournalReclaimFloorTest, ClearReleasedForgetsAVShardSoAReAddStartsFromZero) {
    timestar::JournalRetention ret;
    ret.setReleased(VShardId{41}, 500);
    ret.setReleased(VShardId{43}, 7);
    EXPECT_EQ(ret.released(VShardId{41}), 500u);
    EXPECT_EQ(ret.trackedVShards(), 2u);

    // The monotonic rule is right for a live group and lethal for a re-added one.
    ret.setReleased(VShardId{41}, 1);
    EXPECT_EQ(ret.released(VShardId{41}), 500u) << "setReleased must stay monotonic";

    ret.clearReleased(VShardId{41});
    EXPECT_EQ(ret.released(VShardId{41}), 0u) << "a retired VShard must start from zero again";
    EXPECT_EQ(ret.trackedVShards(), 1u);
    EXPECT_EQ(ret.released(VShardId{43}), 7u) << "and its neighbours are untouched";
}

}  // namespace
