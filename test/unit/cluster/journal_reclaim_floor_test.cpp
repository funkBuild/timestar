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
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
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

}  // namespace
