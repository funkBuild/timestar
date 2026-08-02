// Journal-backed Raft persistence: a real durability round-trip. Persist hard
// state + a log (incl. a config entry) through a JournalWriter, fsync, close,
// then reopen and reconstruct the RaftLog + HardState from the recovered
// records -- proving the log survives a process restart.
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/cluster/raft/raft_codec.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <unistd.h>
#include <string_view>
#include <vector>

using namespace timestar::raft;
using timestar::JournalRecord;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;

namespace fs = std::filesystem;

// EXPECT inside a coroutine cannot early-return with ASSERT_*, so guard explicitly.
#define ASSERT_TRUE_OR_RETURN(cond)   \
    do {                              \
        EXPECT_TRUE(cond);            \
        if (!(cond))                  \
            co_return;                \
    } while (0)

namespace {

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() / ("timestar_raftjnl_" + tag + "_" + std::to_string(::getpid()) +
                                            "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 2;
    h.bootId.fill(0x33);
    return h;
}

LogEntry entry(Term t, LogIndex i, std::string data, EntryType type = EntryType::Normal) {
    LogEntry e;
    e.term = t;
    e.index = i;
    e.type = type;
    e.data = std::move(data);
    return e;
}

uint64_t fnv(std::string_view bytes) {
    uint64_t hash = 1469598103934665603ull;
    for (const unsigned char byte : bytes) {
        hash ^= byte;
        hash *= 1099511628211ull;
    }
    return hash;
}

uint64_t fnvZeros(uint64_t size) {
    std::array<char, 1 << 20> zeros{};
    uint64_t hash = 1469598103934665603ull;
    while (size != 0) {
        const size_t count = static_cast<size_t>(std::min<uint64_t>(zeros.size(), size));
        for (size_t i = 0; i < count; ++i) {
            hash ^= 0;
            hash *= 1099511628211ull;
        }
        size -= count;
    }
    return hash;
}

SnapshotFilePtr snapshotFile(const fs::path& path, std::string_view bytes) {
    fs::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
    auto file = std::make_shared<SnapshotFile>();
    file->path = path;
    file->size = bytes.size();
    file->hash = fnv(bytes);
    file->removeOnDestroy = true;
    return file;
}

seastar::future<> testPersistThenRecover() {
    const auto dir = tmpDir("recover");
    const VShardId vs{7};

    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        // A leader's Ready: term/vote, then three entries (one a config change).
        co_await p.persistHardState(HardState{3, 2});
        std::vector<LogEntry> es = {entry(3, 1, "a"), entry(3, 2, "cfg", EntryType::ConfigChange),
                                    entry(3, 3, "b")};
        co_await p.persistEntries(es);
        co_await p.sync();
        co_await w.close();
    }

    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);

        EXPECT_EQ(st.hardState.currentTerm, 3u);
        EXPECT_EQ(st.hardState.votedFor, 2u);
        EXPECT_EQ(st.log.lastIndex(), 3u);
        EXPECT_EQ(st.log.term(1), std::optional<Term>(3));
        EXPECT_EQ(st.log.term(3), std::optional<Term>(3));
        auto tail = st.log.entriesFrom(1);
        EXPECT_EQ(tail.size(), 3u);
        if (tail.size() == 3u) {
            EXPECT_EQ(tail[0].data, "a");
            EXPECT_EQ(tail[1].type, EntryType::ConfigChange);
            EXPECT_EQ(tail[1].data, "cfg");
            EXPECT_EQ(tail[2].data, "b");
        }
        EXPECT_GT(st.nextSeq, 1u);
        co_await w.close();
    }
    fs::remove_all(dir);
}

seastar::future<> testTruncationAndReAppendRecover() {
    const auto dir = tmpDir("trunc");
    const VShardId vs{9};
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        // Append 1..3 at term 1, then a conflicting re-append of 2..3 at term 2
        // (a follower overwriting a suffix). The lower-index re-append supersedes.
        co_await p.persistEntries({entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")});
        co_await p.persistEntries({entry(2, 2, "B"), entry(2, 3, "C")});
        co_await p.persistHardState(HardState{2, 0});
        co_await p.sync();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        EXPECT_EQ(st.log.lastIndex(), 3u);
        EXPECT_EQ(st.log.term(1), std::optional<Term>(1));
        EXPECT_EQ(st.log.term(2), std::optional<Term>(2));  // overwritten
        EXPECT_EQ(st.log.term(3), std::optional<Term>(2));
        auto tail = st.log.entriesFrom(2);
        EXPECT_EQ(tail.size(), 2u);
        if (tail.size() == 2u) {
            EXPECT_EQ(tail[0].data, "B");
            EXPECT_EQ(tail[1].data, "C");
        }
        co_await w.close();
    }
    fs::remove_all(dir);
}

// REVIEW F2: PROVENANCE SURVIVES RECOVERY, and it is a durability contract rather than
// bookkeeping. `drainReady` fsyncs an incoming Snapshot record BEFORE `applySnapshot`
// writes the TSM files, so a crash in that window leaves the log truncated to the boundary
// and the Engine holding only whichever files landed. Recovery MUST re-install a RECEIVED
// snapshot and MUST NOT re-install a produced one -- and the two are byte-identical in
// shape, so this flag is the only thing that can tell them apart.
seastar::future<> testSnapshotProvenance() {
    const auto dir = tmpDir("snapprov");
    const VShardId vs{12};
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        Snapshot snap;
        snap.index = 7;
        snap.term = 3;
        snap.config.voters = {1, 2, 3};
        snap.data = "received-state@7";
        co_await p.persistSnapshot(snap, /*receivedFromPeer=*/true);
        co_await p.sync();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        ASSERT_TRUE_OR_RETURN(st.snapshot.has_value());
        EXPECT_TRUE(st.snapshotFromPeer) << "a RECEIVED snapshot must recover as received, or the replica comes back "
                                            "believing it is caught up over whatever files happened to land";
        EXPECT_EQ(st.snapshot->index, 7u);
        EXPECT_EQ(st.snapshot->data, "received-state@7");
        EXPECT_EQ(st.snapshot->config.voters, (std::vector<NodeId>{1, 2, 3}));
        co_await w.close();
    }
    fs::remove_all(dir);
}

seastar::future<> testSnapshotRecover() {
    const auto dir = tmpDir("snap");
    const VShardId vs{11};
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        co_await p.persistEntries({entry(1, 1, "a"), entry(1, 2, "b")});
        Snapshot snap;
        snap.index = 5;
        snap.term = 2;
        snap.config.voters = {1, 2, 3};
        snap.data = "state@5";
        co_await p.persistSnapshot(snap, /*receivedFromPeer=*/false);
        co_await p.persistEntries({entry(2, 6, "f")});  // entry above the snapshot
        co_await p.sync();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        EXPECT_EQ(st.log.snapshotIndex(), 5u);
        EXPECT_EQ(st.log.snapshotTerm(), 2u);
        EXPECT_EQ(st.log.firstIndex(), 6u);
        EXPECT_EQ(st.log.lastIndex(), 6u);
        // The snapshot's config AND state-machine data must survive recovery.
        EXPECT_TRUE(st.snapshot.has_value());
        if (st.snapshot) {
            EXPECT_EQ(st.snapshot->index, 5u);
            EXPECT_EQ(st.snapshot->config.voters, (std::vector<NodeId>{1, 2, 3}));
            EXPECT_EQ(st.snapshot->data, "state@5");
        }
        EXPECT_FALSE(st.snapshotFromPeer) << "a locally PRODUCED snapshot must recover as produced";
        auto tail = st.log.entriesFrom(6);
        EXPECT_EQ(tail.size(), 1u);
        if (tail.size() == 1u)
            EXPECT_EQ(tail[0].data, "f");
        co_await w.close();
    }
    fs::remove_all(dir);
}

seastar::future<> testFileSnapshotReplacementAndRecovery() {
    const auto dir = tmpDir("filesnap");
    const auto sidecars = dir / "sidecars";
    const VShardId vs{13};
    fs::path oldPath;
    fs::path latestPath;
    {
        JournalWriter w(dir / "journal", header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs, 1, sidecars);

        Snapshot first;
        first.index = 5;
        first.term = 2;
        first.config.voters = {1, 2, 3};
        first.file = snapshotFile(sidecars / "producer-first.tmp", "first-sidecar");
        co_await p.persistSnapshot(first, false);
        co_await p.sync();
        oldPath = first.file->path;
        first.file.reset();

        Snapshot second;
        second.index = 9;
        second.term = 3;
        second.config.voters = {1, 2, 3};
        second.file = snapshotFile(sidecars / "producer-second.tmp", "latest-sidecar");
        co_await p.persistSnapshot(second, false);
        co_await p.sync();
        latestPath = second.file->path;
        second.file.reset();

        EXPECT_FALSE(fs::exists(oldPath)) << "a durably superseded sidecar must not leak forever";
        EXPECT_TRUE(fs::exists(latestPath));
        co_await w.close();
    }

    // The append-only journal still contains the first descriptor, whose file
    // was correctly retired above. Recovery must require only the latest one.
    {
        JournalWriter w(dir / "journal", header(), 1u << 20);
        auto recovered = co_await w.open();
        auto state = recoverRaftState(recovered, vs, sidecars);
        ASSERT_TRUE_OR_RETURN(state.snapshot && state.snapshot->file);
        EXPECT_EQ(state.snapshot->index, 9u);
        EXPECT_EQ(state.snapshot->file->path, latestPath);
        co_await validateSnapshotFile(*state.snapshot->file);

        std::ofstream(sidecars / "snapshot_v1_stage_orphan.bin") << "partial";
        fs::create_directories(sidecars / "extract_tmp");
        std::ofstream(sidecars / "extract_tmp" / "orphan.tsm") << "partial";
        co_await cleanupSnapshotDirectory(sidecars, state.snapshot->file);
        EXPECT_TRUE(fs::exists(latestPath));
        EXPECT_FALSE(fs::exists(sidecars / "snapshot_v1_stage_orphan.bin"));
        EXPECT_FALSE(fs::exists(sidecars / "extract_tmp"));

        std::ofstream(sidecars / "unexpected") << "do not ignore me";
        bool unknownRejected = false;
        try {
            co_await cleanupSnapshotDirectory(sidecars, state.snapshot->file);
        } catch (const std::exception&) {
            unknownRejected = true;
        }
        EXPECT_TRUE(unknownRejected) << "the sidecar directory is an exact namespace";
        fs::remove(sidecars / "unexpected");

        std::fstream damaged(latestPath, std::ios::binary | std::ios::in | std::ios::out);
        char byte = 0;
        damaged.read(&byte, 1);
        damaged.seekp(0);
        damaged.put(static_cast<char>(byte ^ 1));
        damaged.close();
        bool corruptionRejected = false;
        try {
            co_await validateSnapshotFile(*state.snapshot->file);
        } catch (const std::exception&) {
            corruptionRejected = true;
        }
        EXPECT_TRUE(corruptionRejected) << "descriptor hash must cover the complete sidecar";
        co_await w.close();
    }
    fs::remove_all(dir);
}

seastar::future<> testFileSnapshotPreparationFailureFencesPersistence() {
    const auto dir = tmpDir("filesnap_fence");
    const auto sidecars = dir / "sidecars";
    JournalWriter writer(dir / "journal", header(), 1u << 20);
    co_await writer.open();
    JournalRaftPersistence persistence(writer, VShardId{15}, 1, sidecars);

    Snapshot snapshot;
    snapshot.index = 5;
    snapshot.term = 2;
    snapshot.config.voters = {1, 2, 3};
    // A producer outside the exact private namespace is rejected during
    // promotion, after the persistence sequence was reserved.
    snapshot.file = snapshotFile(dir / "outside" / "producer.tmp", "snapshot");

    bool preparationRejected = false;
    try {
        co_await persistence.persistSnapshot(snapshot, false);
    } catch (const std::exception&) {
        preparationRejected = true;
    }
    EXPECT_TRUE(preparationRejected);

    bool syncRejected = false;
    try {
        co_await persistence.sync();
    } catch (const std::exception&) {
        syncRejected = true;
    }
    EXPECT_TRUE(syncRejected) << "failed snapshot preparation must permanently fence its advanced sequence state";

    co_await writer.close();
    snapshot.file.reset();
    fs::remove_all(dir);
}

seastar::future<> testFileSnapshotBeyondInlineLimit() {
    const auto dir = tmpDir("largefilesnap");
    const auto sourcePath = dir / "source" / "large.bin";
    const auto receiverDir = dir / "receiver";
    const uint64_t total = kMaxVShardSnapshotBytes + 1;
    fs::create_directories(sourcePath.parent_path());
    {
        // Sparse source keeps the gate's workspace footprint close to the one
        // received sidecar while still forcing every byte through hydration,
        // transport framing and receiver staging.
        std::ofstream source(sourcePath, std::ios::binary | std::ios::trunc);
        source.seekp(static_cast<std::streamoff>(total - 1));
        source.put('\0');
    }
    auto source = std::make_shared<SnapshotFile>();
    source->path = sourcePath;
    source->size = total;
    source->hash = fnvZeros(total);

    JournalWriter leaderWriter(dir / "leader_journal", header(), 1u << 20);
    JournalWriter receiverWriter(dir / "receiver_journal", header(), 1u << 20);
    JournalRaftPersistence leader(leaderWriter, VShardId{14}, 1, dir / "leader_sidecars");
    JournalRaftPersistence receiver(receiverWriter, VShardId{14}, 1, receiverDir);

    SnapshotFilePtr received;
    size_t maxChunk = 0;
    for (uint64_t offset = 0; offset < total; offset += kMaxSnapshotChunkBytes) {
        InstallSnapshot chunk;
        chunk.term = 4;
        chunk.leaderId = 1;
        chunk.lastIncludedIndex = 50;
        chunk.lastIncludedTerm = 3;
        chunk.config.voters = {1, 2, 3};
        chunk.sourceFile = source;
        chunk.sourceLength = static_cast<size_t>(std::min<uint64_t>(kMaxSnapshotChunkBytes, total - offset));
        chunk.offset = offset;
        chunk.totalBytes = total;
        chunk.done = offset + chunk.sourceLength == total;
        co_await leader.hydrateSnapshotChunk(chunk);
        maxChunk = std::max(maxChunk, chunk.data.size());

        auto wire = decodeEnvelope(encodeEnvelope(Envelope{14, Message{.to = 2, .from = 1, .payload = chunk}}));
        ASSERT_TRUE_OR_RETURN(wire.has_value());
        auto* decoded = std::get_if<InstallSnapshot>(&wire->message.payload);
        ASSERT_TRUE_OR_RETURN(decoded != nullptr);
        co_await receiver.stageSnapshotChunk(*decoded);
        EXPECT_TRUE(decoded->data.empty()) << "the receiver must release each wire chunk after staging it";
        if (offset == 0) {
            // Model a failed append that reached the file but not accepted Raft
            // progress. The next chunk must repair back to the accepted prefix
            // before appending, rather than splice these bytes into the payload.
            const auto staged = *fs::directory_iterator(receiverDir);
            std::ofstream extra(staged.path(), std::ios::binary | std::ios::app);
            extra << "partial-unaccepted-append";
        }
        if (decoded->completedFile)
            received = decoded->completedFile;
    }
    ASSERT_TRUE_OR_RETURN(received != nullptr);
    EXPECT_EQ(received->size, total);
    EXPECT_EQ(received->hash, source->hash);
    EXPECT_LE(maxChunk, kMaxSnapshotChunkBytes);
    co_await validateSnapshotFile(*received);
    received.reset();
    fs::remove_all(dir);
}

}  // namespace

TEST(RaftJournalPersistenceTest, PersistThenRecover) {
    testPersistThenRecover().get();
}
TEST(RaftJournalPersistenceTest, TruncationAndReAppendRecover) {
    testTruncationAndReAppendRecover().get();
}
TEST(RaftJournalPersistenceTest, SnapshotRecover) {
    testSnapshotRecover().get();
}
TEST(RaftJournalPersistenceTest, SnapshotProvenanceSurvivesRecovery) {
    testSnapshotProvenance().get();
}
TEST(RaftJournalPersistenceTest, FileSnapshotReplacementRecoversLatestAndCleansOrphans) {
    testFileSnapshotReplacementAndRecovery().get();
}
TEST(RaftJournalPersistenceTest, FileSnapshotPreparationFailureFencesPersistence) {
    testFileSnapshotPreparationFailureFencesPersistence().get();
}
TEST(RaftJournalPersistenceTest, DISABLED_FileSnapshotStreamsBeyond128MiBInlineLimit) {
    testFileSnapshotBeyondInlineLimit().get();
}
