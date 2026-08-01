#include "replicated_vshard_host.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"
#include "../../utils/logger.hpp"  // timestar::http_log
#include "../raft/raft_node.hpp"

#include <algorithm>
#include <cstdlib>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <stdexcept>
#include <utility>

namespace timestar::cluster {

namespace fs = std::filesystem;

namespace {
uint8_t hexNibble(char c) {
    if (c >= '0' && c <= '9')
        return static_cast<uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f')
        return static_cast<uint8_t>(c - 'a' + 10);
    if (c >= 'A' && c <= 'F')
        return static_cast<uint8_t>(c - 'A' + 10);
    throw std::invalid_argument("journal identity must contain only hexadecimal characters");
}

std::array<uint8_t, 16> parseUuidBytes(std::string_view value) {
    if (value.size() != 32)
        throw std::invalid_argument("journal identity UUID must contain exactly 32 hexadecimal characters");
    std::array<uint8_t, 16> out{};
    for (size_t i = 0; i < out.size(); ++i)
        out[i] = static_cast<uint8_t>((hexNibble(value[2 * i]) << 4) | hexNibble(value[2 * i + 1]));
    return out;
}
}  // namespace

JournalIdentity JournalIdentity::fromHex(std::string_view clusterUuid, std::string_view bootId) {
    return JournalIdentity{parseUuidBytes(clusterUuid), parseUuidBytes(bootId)};
}

JournalIdentity JournalIdentity::testing() {
    JournalIdentity out;
    out.clusterUuid.fill(0x11);
    out.bootId.fill(0x44);
    return out;
}

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, std::chrono::milliseconds tick)
    : ReplicatedVShardHost(store, transport, self, std::move(journalRoot), JournalIdentity::testing(), tick) {}

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, JournalIdentity identity,
                                           std::chrono::milliseconds tick)
    : store_(store),
      self_(self),
      journalRoot_(std::move(journalRoot)),
      journalIdentity_(identity),
      registry_(transport, tick) {}

ReplicatedVShardHost::~ReplicatedVShardHost() {
    // stop() (async: drains ticks, closes journal writers) MUST be called before
    // destruction. Destruction alone tears down the registry (groups) before
    // vshards_ (per the declaration order), which is UAF-safe, but skips writer
    // close() -> fd leak. Warn loudly rather than fail silently.
    if (!stopped_ && !vshards_.empty())
        timestar::http_log.warn(
            "ReplicatedVShardHost destroyed without stop(): {} VShard journal(s) not closed (fd leak)",
            vshards_.size());
}

bool ReplicatedVShardHost::sharedJournalEnabled() {
    // DEFAULT OFF, and the default is the decision, not an oversight (debt D-10).
    // The ordering contract on SharedShardJournal is airtight as reasoned and as
    // unit-tested, but a shared journal changes the FENCE BLAST RADIUS (one bad
    // fdatasync stops every group on the reactor, not one), it makes segment
    // retention cluster-wide (D-34's bricks were written for exactly this layout
    // and still have no caller), and its win is DISK-only -- invisible on the tmpfs
    // box every gate in this tree runs on. Shipping it on by default would trade a
    // measured-at-zero benefit here for a real change to the durability path, which
    // is the wrong side of that bet. Opt in with TIMESTAR_CLUSTER_SHARED_JOURNAL=1.
    static const bool on = [] {
        const char* e = std::getenv("TIMESTAR_CLUSTER_SHARED_JOURNAL");
        return e && e[0] == '1';
    }();
    return on;
}

uint64_t ReplicatedVShardHost::journalFsyncs() const {
    if (sharedWriter_)
        return sharedWriter_->fsyncs();
    uint64_t n = 0;
    for (const auto& [vs, state] : vshards_)
        if (state.writer)
            n += state.writer->fsyncs();
    return n;
}

uint64_t ReplicatedVShardHost::journalSyncRequests() const {
    uint64_t n = 0;
    if (sharedSink_)
        return sharedSink_->syncRequests();
    for (const auto& [vs, state] : vshards_)
        if (state.persistence)
            n += state.persistence->syncRequests();
    return n;
}

seastar::future<> ReplicatedVShardHost::addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts) {
    // AN IDENTITY-ASSUMING SITE, deliberately left so (debt D-11, ADR 0004). Three
    // things below are keyed by the VShard id AS a group id:
    //   * `registry_.addGroup(vshard, ...)` -- the group id the transport peeks and
    //     the registry maps;
    //   * the journal directory `vshard_<id>/`;
    //   * `EngineDataStateMachine(store_, VShardId{vshard})` -- one state machine per
    //     VShard, where a consolidated group needs one fanning out over its K VShards.
    // Converting these is not indirection, it is the consolidation itself (a group
    // gains a member list, its snapshot becomes a manifest of K VShard snapshots, and
    // its compaction boundary becomes the MIN over them). D-11 converts the ROUTING so
    // that work is local to this file and the snapshot path; see the ADR's "What
    // remains before consolidation is possible".
    //
    // A VShard is hosted at most once: re-adding would open a second JournalWriter on
    // the same dir (two recoverers over one journal) and leak the old writer's fd.
    if (registry_.group(vshard))
        throw std::runtime_error("ReplicatedVShardHost::addVShard: VShard already hosted");
    // EVERY group on this shard shares ONE snapshot transfer budget (debt D-37), stamped
    // in here rather than at the ClusterDataPlane call site that builds `opts`: the budget
    // is a per-SHARD object and that site builds one options struct for the whole node.
    // The host outlives the registry (declaration order), so the pointer outlives every
    // RaftNode that holds it.
    opts.snapshotBudget = &snapshotBudget_;
    VShardState vs;
    JournalSegmentHeader hdr;
    hdr.clusterUuid = journalIdentity_.clusterUuid;
    hdr.coreNumber = static_cast<uint16_t>(seastar::this_shard_id());
    hdr.bootId = journalIdentity_.bootId;

    // ONE journal per shard (debt D-10, opt-in) or one per VShard (default).
    //
    // Recovery is the SAME code either way: recoverRaftState(records, vshard)
    // filters a record set by VShard and replays it in vshard_seq order, so an
    // interleaved shared stream reconstructs each group's log exactly as a
    // dedicated one does. Physical order never mattered -- ADR 0001 made
    // vshard_seq the ordering key precisely because segment-GC copy-forward can
    // relocate a laggard's records behind its own newer ones.
    if (sharedJournalEnabled()) {
        if (!sharedWriter_) {
            fs::path dir = journalRoot_ / ("shard_" + std::to_string(seastar::this_shard_id()));
            fs::create_directories(dir);
            // A larger rotation target than the per-VShard 1 MiB: this file carries
            // every group on the reactor (~1365 at 4096/3 shards), so the per-VShard
            // size would rotate -- and seal, and fsync, and sync_directory --
            // constantly.
            sharedWriter_ = std::make_unique<JournalWriter>(dir, hdr, sharedJournalSegmentBytes_);
            sharedRecovered_ = co_await sharedWriter_->open();
            sharedSink_ = std::make_unique<SharedShardJournal>(*sharedWriter_);
            timestar::http_log.info(
                "cluster: shard {} opened a SHARED Raft journal at {} ({} record(s) recovered); every group on this "
                "reactor now group-commits through one fdatasync (debt D-10)",
                seastar::this_shard_id(), dir.string(), sharedRecovered_.size());
        }
    }
    std::vector<JournalRecord> ownRecovered;
    if (!sharedSink_) {
        fs::path dir = journalRoot_ / ("vshard_" + std::to_string(vshard));
        fs::create_directories(dir);
        vs.writer = std::make_unique<JournalWriter>(dir, hdr, journalSegmentBytes_);
        ownRecovered = co_await vs.writer->open();
    }
    // Shared mode reads the ONE recovered set (never copied -- it is ~1365 groups'
    // records and every addVShard on this shard needs it); per-VShard mode reads
    // this VShard's own, which is dropped at the end of this frame.
    const std::vector<JournalRecord>& recovered = sharedSink_ ? sharedRecovered_ : ownRecovered;
    raft::RecoveredRaftState st = raft::recoverRaftState(recovered, VShardId{vshard});

    std::vector<NodeId> baseVoters = voters;
    std::vector<NodeId> baseLearners;
    if (st.snapshot && !st.snapshot->config.voters.empty()) {
        // The membership as of the boundary lives ONLY in the snapshot once its
        // ConfigChange entries are compacted away, so it -- not the configured voter list
        // -- is the config floor for a recovered group.
        baseVoters = st.snapshot->config.voters;
        baseLearners = st.snapshot->config.learners;
    }

    vs.persistence = sharedSink_
                         ? std::make_unique<raft::JournalRaftPersistence>(static_cast<JournalSink&>(*sharedSink_),
                                                                          VShardId{vshard}, st.nextSeq)
                         : std::make_unique<raft::JournalRaftPersistence>(*vs.writer, VShardId{vshard}, st.nextSeq);
    // SEED THE RECLAIM FLOOR FROM WHAT WAS ACTUALLY RECOVERED (debt D-34). Mandatory,
    // not an optimisation: a fresh persistence object knows none of the on-disk records'
    // seqs, so its "oldest live entry" would be the first entry appended AFTER this
    // restart -- a much higher seq -- and the floor would jump straight over the
    // recovered log suffix and release the records it is made of.
    vs.persistence->seedRetention(std::move(st.retention));
    // The recovered floor is the starting point for reclamation too, so a node that
    // restarts over an already-compacted journal can collect on its FIRST pass instead
    // of waiting for the next compaction.
    if (const uint64_t floor = vs.persistence->releasedSeq(); floor > 0)
        retention_.setReleased(VShardId{vshard}, floor);
    vs.sm = std::make_unique<EngineDataStateMachine>(store_, VShardId{vshard});
    raft::RaftNode node(self_, baseVoters, std::move(st.log), st.hardState, opts, baseLearners);
    if (st.snapshot && st.snapshotFromPeer) {
        // A RECEIVED SNAPSHOT MUST BE RE-INSTALLED (review F2), and this is the case that
        // turned a fail-closed refusal into silent loss before it was caught.
        //
        // `RaftGroup::drainReady` persists AND FSYNCS the incoming Snapshot record BEFORE
        // `sm_.applySnapshot` writes the TSM files -- correct for Raft (the Ready contract
        // requires durability before anything observable) and it means the payload is
        // durable in the JOURNAL and nowhere else until the install finishes. A kill -9 in
        // that window leaves a replica whose log has been truncated to the boundary and
        // whose Engine holds only whichever files landed. Skipping the re-install there --
        // as the produced-snapshot reasoning below would have us do -- makes that replica
        // report itself caught up, serve replica reads out of a hole, and, if elected,
        // build and serve a SNAPSHOT out of that hole.
        //
        // Re-installing is idempotent: `TSMFileManager::addTSMFile` is keyed by the file's
        // rank (tier+seq, parsed from the name), so a file `Engine::init` already
        // registered is not registered twice, and `restoreVShardSnapshot` verifies before
        // it installs. So this is safe whether the crash landed before, during or after
        // the original install.
        timestar::http_log.info(
            "cluster: VShard {} recovered a RECEIVED snapshot at boundary index {} (term {}); re-installing its "
            "payload, because a crash between persisting the record and installing the files would otherwise leave "
            "this replica reporting itself caught up over a hole",
            vshard, st.snapshot->index, st.snapshot->term);
        co_await vs.sm->applySnapshot(*st.snapshot);
        // MOVE on the LAST use (debt D-32): `seedRecoveredSnapshot` takes its Snapshot by
        // value, so an lvalue duplicated the whole payload once per recovered group -- on
        // the startup path, where a shard opens ~1365 of them.
        node.seedRecoveredSnapshot(std::move(*st.snapshot));
    } else if (st.snapshot) {
        // RECOVERING A LOCALLY-PRODUCED SNAPSHOT (debt D-6). This used to throw outright --
        // which was correct while nothing ever compacted, and would have turned the FIRST
        // restart after D-6 into a fail-closed refusal to start.
        //
        // THE PAYLOAD IS NOT RE-INSTALLED, and that is deliberate rather than lazy. A
        // VShard snapshot is a manifest plus the bytes of the TSM files it references, and
        // `snapshotVShard` builds it FROM THIS NODE'S OWN on-disk files at a boundary that
        // by construction covers only FLUSHED data. Those files are still on this disk
        // after a restart, and `Engine::init` has already registered them -- so the local
        // state machine is already at or above the boundary and there is nothing to
        // restore. Re-installing would be actively WRONG, not merely wasteful:
        // `installVShardSnapshotFiles` calls `TSMFileManager::addTSMFile` for every file it
        // writes, so a file already registered by init would be registered TWICE and its
        // points counted twice by every query.
        //
        // What the core does still need is the snapshot as something it can SERVE: without
        // it a restarted leader has no payload for a follower below its boundary. That is
        // what seedRecoveredSnapshot does, and it deliberately does NOT surface the
        // snapshot to this node's own state machine.
        //
        // (The log suffix ABOVE the boundary replays as usual, which is what covers the
        // unflushed writes the boundary was chosen to exclude.)
        // Read what the log line needs BEFORE the move (debt D-32) -- see the received
        // branch above for why the move is worth having.
        const uint64_t snapIndex = st.snapshot->index;
        const uint64_t snapTerm = st.snapshot->term;
        auto receipts = data::decodeSnapshotDeleteReceipts(st.snapshot->data);
        if (!receipts)
            throw std::runtime_error(
                "ReplicatedVShardHost: locally produced snapshot has invalid delete-receipt framing");
        vs.sm->restoreDeleteReceipts(std::move(*receipts), snapIndex);
        node.seedRecoveredSnapshot(std::move(*st.snapshot));
        timestar::http_log.info(
            "cluster: VShard {} recovered from a compacted journal at boundary index {} (term {}); the local Engine "
            "already holds the flushed data the snapshot describes, so only the log suffix above it is replayed",
            vshard, snapIndex, snapTerm);
    }
    registry_.addGroup(vshard, std::move(node), *vs.persistence, *vs.sm);
    vshards_[vshard] = std::move(vs);
    co_return;
}

seastar::future<bool> ReplicatedVShardHost::propose(uint16_t vshard, data::ReplicatedCommand cmd) {
    return propose(vshard, std::move(cmd), std::nullopt);
}

seastar::future<bool> ReplicatedVShardHost::propose(uint16_t vshard, data::ReplicatedCommand cmd,
                                                    data::OptDeadline deadline) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::propose: VShard not hosted here");
    if (const auto* writes = std::get_if<data::WriteBatch>(&cmd))
        co_await store_.checkWriteAdmission(*writes);
    co_return co_await g->proposeAndAwaitApplied(data::encodeReplicatedCommand(cmd), deadline);
}

seastar::future<data::ProposeOutcome> ReplicatedVShardHost::proposeCommandHinted(uint16_t vshard,
                                                                                 data::ReplicatedCommand cmd,
                                                                                 data::OptDeadline deadline) {
    data::ProposeOutcome out;
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g) {
        out.rejects.push_back(data::SliceReject{vshard, raft::kNoNode, data::WriteFailure::NotLeader});
        co_return out;
    }
    try {
        if (co_await propose(vshard, std::move(cmd), deadline)) {
            out.committed = true;
            out.committedVShards.push_back(vshard);
        } else {
            out.rejects.push_back(classifyRefusal(vshard));
        }
    } catch (...) {
        const auto kind = data::classifyLocalWriteFailure(std::current_exception());
        if (!data::isRetryableWriteFailure(kind))
            throw;
        NodeId hint = g->leader();
        if (hint == self_)
            hint = raft::kNoNode;
        out.rejects.push_back(data::SliceReject{vshard, hint, kind});
    }
    co_return out;
}

seastar::future<uint64_t> ReplicatedVShardHost::snapshotVShard(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        co_return 0;  // not hosted here
    // REFUSE WHILE ANY ROLLED-OVER STORE IS STILL AWAITING CONVERSION TO TSM.
    //
    // This is the FIRST of two conditions that make the boundary below safe, and it is the
    // one the snapshot-durability gate caught: WAL->TSM conversions run 6 at a time and so
    // complete OUT OF ORDER, which means "the highest revision in TSM" can sit ABOVE an
    // unconverted store's revisions. Truncating there discards log entries whose data lives
    // only in RAM -- measured as 7 of 200 acknowledged points lost across a kill -9, with
    // the three nodes disagreeing about how many (they had converted different stores).
    // Full reasoning on EngineLocalStore::hasUnconvertedStores.
    //
    // Skipping is free: the sweep comes back every few seconds and conversions finish in
    // seconds. A larger log is a cost; a truncated log is data loss.
    if (co_await store_.hasUnconvertedStores(VShardId{vshard})) {
        ++snapshotsSkippedPendingConversion_;
        co_return 0;
    }

    // Capture only FLUSHED (TSM) data. manifest.snapshotRevision is the highest
    // revision the snapshot reproduces; since revisions are stamped from the log index
    // (ADR 0003), it is a safe log-truncation boundary. Entries after it may hold data
    // that lives only in the memory store, so they MUST stay in the log -- compacting to
    // appliedIndex would truncate them and lose unflushed data on restart.
    auto payload = co_await store_.buildVShardSnapshot(VShardId{vshard});
    const uint64_t maxFlushedRevision = payload.manifest.snapshotRevision;
    if (maxFlushedRevision == 0) {
        ++snapshotsSkippedUnflushed_;
        co_return 0;  // no flushed data yet -> nothing to compact
    }

    // TRUNCATE ONE ENTRY BELOW THE HIGHEST FLUSHED REVISION, not at it. The SECOND of the
    // two conditions that make this boundary safe (the first is the pending-conversion
    // refusal above). Found while wiring the trigger (D-6), and a real hole rather than
    // belt-and-braces.
    //
    // `snapshotRevision` is the MAXIMUM revision appearing in the flushed extents, and a
    // revision is one whole log ENTRY -- but an entry's points do not all flush together.
    // `EngineLocalStore::applyWrites` buckets a batch's series by core and issues several
    // `insertBatch` calls, any of which can trigger a memory-store rollover: so entry N can
    // legitimately have SOME of its points in TSM and the rest still in the memory store,
    // and the manifest then reports `snapshotRevision == N` for a partially-flushed entry.
    // Truncating AT N discards the log entry holding the unflushed remainder, and those
    // points are gone on the next restart -- a silent data loss on the one invariant this
    // whole path exists to protect.
    //
    // Backing off by one entry closes it for the cost of retaining one entry: entry N stays
    // in the log and replays, and re-applying a fully-flushed entry is idempotent under LWW
    // (identical revision re-stamp, ADR 0003).
    uint64_t upto = maxFlushedRevision - 1;
    // And never above what this replica has APPLIED. RaftNode::compact clamps to
    // lastApplied_ itself, but clamping here too means the value we log and RETURN is the
    // one that was really used -- a caller that trusts the return value to mean "the log
    // below this is gone" would otherwise be wrong on a lagging replica.
    upto = std::min<uint64_t>(upto, g->appliedIndex());
    if (upto == 0) {
        ++snapshotsSkippedUnflushed_;
        co_return 0;
    }
    // NOTHING TO GAIN, AND SOMETHING TO LOSE, FROM RE-SNAPSHOTTING THE SAME BOUNDARY
    // (review F3). A group whose flush watermark has not moved since its last snapshot
    // computes the SAME `upto` on every sweep. Compacting again truncates nothing, but it
    // does REPLACE `snapshot_.data` in place -- which invalidates any in-flight
    // InstallSnapshot transfer, and at an unchanged (index, term) it does so without
    // tripping the reply path's "our snapshot moved on" guard. `RaftNode::compact` now
    // drops live transfers defensively, but the transfer should not be disturbed at all
    // for a boundary that is not advancing.
    if (upto <= static_cast<uint64_t>(g->node().log().snapshotIndex())) {
        ++snapshotsSkippedNoAdvance_;
        co_return 0;
    }
    // Delete idempotency is replicated state just like the data. Include only
    // receipts covered by this exact boundary; suffix receipts must be rebuilt
    // by suffix replay or their deletes could be skipped after restore.
    if (auto state = vshards_.find(vshard); state != vshards_.end() && state->second.sm)
        payload.deleteReceipts = state->second.sm->deleteReceiptsThrough(upto);
    // CONSUMING encode (debt D-32). This `std::move` used to be dead -- the only overload
    // took a const&, so the rvalue bound to it and every file was copied, leaving the
    // producer holding the payload twice with `payload` still alive for the whole
    // compaction below it. It now really does consume: `payload`'s file bodies are gone
    // when this returns.
    std::string encoded = data::encodeSnapshotPayload(std::move(payload));
    // REFUSE TO COMPACT INTO A SNAPSHOT NOBODY CAN RECEIVE (write-scaleout 5 review, F3c).
    // Compaction is the point of no return: it DISCARDS the log prefix this snapshot
    // replaces, so a follower that later needs catching up has only the snapshot to be
    // caught up WITH. If that snapshot cannot be delivered, the follower can never be
    // caught up at all -- and the log entries that would have done it are gone. Declining
    // to compact leaves the group exactly as it was: larger log, working replication.
    //
    // RECONCILED WITH CHUNKING (D-5/D-6). The threshold was `kMaxRaftPayloadBytes`, i.e.
    // "no single message could carry it". Chunking removed that limit, and the retuned size
    // chain would have DROPPED that number to 28 MiB -- so leaving the check as it stood
    // would have made compaction refuse MORE often after the fix meant to enable it.
    //
    // It is now `kMaxVShardSnapshotBytes` (128 MiB), which is a MEMORY bound rather than a
    // message bound: `buildVShardSnapshot` materializes the whole payload in RAM here, the
    // leader holds it for as long as it is servable, and the receiver stages it in RAM
    // before installing. Copies of an unbounded payload on a reactor with a fixed memory
    // pool are an OOM. That is a ~4.5x RISE over the old effective ceiling, so snapshots
    // that used to block compaction now ship -- as a pipeline of 4 MiB chunks.
    //
    // D-32 removed four of the concurrent copies and left the bound where it is; the
    // corrected census (and why the number stays) is at kMaxVShardSnapshotBytes.
    if (encoded.size() > raft::kMaxVShardSnapshotBytes) {
        timestar::http_log.error(
            "cluster: NOT compacting VShard {}: its snapshot is {} bytes, over the {} byte total-snapshot bound, so it "
            "cannot be held in memory to serve (chunking bounds the MESSAGE, not the payload). The log is kept instead "
            "(it will keep growing). Streaming the payload via disk rather than staging it in RAM is the fix (D-32).",
            vshard, encoded.size(), raft::kMaxVShardSnapshotBytes);
        ++snapshotsRefusedTooLarge_;
        co_return 0;
    }
    co_await g->compact(upto, std::move(encoded));
    ++snapshotsTaken_;
    if (auto it = vshards_.find(vshard); it != vshards_.end()) {
        it->second.lastSnapshot = seastar::lowres_clock::now();
        if (it->second.sm)
            it->second.sm->noteSnapshotTaken();  // the bytes below the new boundary are gone
    }
    co_return upto;
}

// ---------------------------------------------------------------------------
// Journal segment reclamation (debt D-34)
// ---------------------------------------------------------------------------

size_t ReplicatedVShardHost::publishReclaimFloors() {
    size_t advanced = 0;
    for (const auto& [vs, state] : vshards_) {
        if (!state.persistence)
            continue;
        const uint64_t floor = state.persistence->releasedSeq();
        // JournalRetention::setReleased is monotonic on its own, but comparing here is
        // what tells the caller whether anything CHANGED -- which is what decides
        // whether a collect pass has any work to do at all.
        if (floor > retention_.released(VShardId{vs})) {
            retention_.setReleased(VShardId{vs}, floor);
            ++advanced;
        }
    }
    return advanced;
}

seastar::future<size_t> ReplicatedVShardHost::reclaimJournalSegments() {
    // One pass at a time: a pass suspends over file I/O, and two concurrent passes over
    // the same directory would both plan against the same segment and race the unlink.
    if (stopped_ || journalGcRunning_ || snapshotGate_.is_closed())
        co_return 0;
    // UNDER THE SAME GATE AS THE SWEEP. This is a public entry (tests, and an operator
    // action later), so it can be called from outside snapshotSweep(); without the hold, a
    // pass in flight when stop() runs would keep unlinking against writers that stop() is
    // closing. stop() closes this gate before it touches the writers.
    auto held = snapshotGate_.hold();
    journalGcRunning_ = true;
    size_t deleted = 0;
    size_t pinned = 0;  // census for THIS pass; see journalSegmentsPinnedLastPass()
    std::exception_ptr err;
    try {
        publishReclaimFloors();
        ++journalGcPasses_;
        if (sharedSink_ && sharedWriter_) {
            // SHARED LAYOUT (debt D-10). ONE directory holding every group on this
            // reactor, so a segment is reclaimable only once EVERY group's floor has
            // passed every record in it -- planSegment's rule verbatim -- and a single
            // laggard would otherwise pin the whole 64 MiB segment for ~1365 groups.
            // Copy-forward is therefore ON, and it runs inside runExclusive() because
            // it APPENDS to the same writer every group is group-committing through.
            const fs::path dir = journalRoot_ / ("shard_" + std::to_string(seastar::this_shard_id()));
            const uint64_t active = sharedWriter_->currentSegmentNumber();
            // The sink is handed IN rather than wrapped around the whole collect: reading
            // and scanning each sealed segment is the expensive part and needs no lock (a
            // sealed segment is immutable), so only the bounded copy-forward write runs
            // inside runExclusive. Wrapping the whole call would hold every group on the
            // reactor off its group-commit for the length of a directory walk.
            auto result =
                co_await JournalGc::collect(dir, active, *sharedWriter_, retention_,
                                            JournalGc::Options{.continuePastPinned = true}, sharedSink_.get());
            deleted += result.deletedSegments.size();
            pinned += result.pinnedSegments.size();
            journalRecordsCopiedForward_ += result.copiedRecords;
        } else {
            // PER-VSHARD LAYOUT (the default). Each directory holds exactly one group's
            // records, so its own floor decides everything and DELETE-ONLY is both
            // sufficient and the reason no exclusion is needed: nothing here touches the
            // writer, and a sealed segment strictly below the active one is not
            // something the writer will ever touch either.
            //
            // Visit only the groups whose floor MOVED. Re-scanning ~1365 directories a
            // minute to re-derive an unchanged answer is the difference between a
            // background task and a background problem.
            for (auto& [vs, state] : vshards_) {
                if (stopped_)
                    break;
                if (!state.writer || !state.persistence)
                    continue;
                const uint64_t floor = retention_.released(VShardId{vs});
                if (floor == 0 || floor <= state.lastGcFloor)
                    continue;
                const fs::path dir = journalRoot_ / ("vshard_" + std::to_string(vs));
                auto result = co_await JournalGc::collect(dir, state.writer->currentSegmentNumber(), *state.writer,
                                                          retention_, JournalGc::Options{.copyForward = false});
                // Recorded AFTER the pass: a throw leaves it unchanged so the next pass
                // retries rather than skipping a directory it never finished.
                state.lastGcFloor = floor;
                deleted += result.deletedSegments.size();
                pinned += result.pinnedSegments.size();
            }
        }
    } catch (...) {
        err = std::current_exception();
    }
    journalGcRunning_ = false;
    journalSegmentsDeleted_ += deleted;
    journalSegmentsPinnedLastPass_ = pinned;  // a gauge: replaced, never accumulated
    if (err)
        std::rethrow_exception(err);
    co_return deleted;
}

// ---------------------------------------------------------------------------
// The snapshot producer trigger (debt D-6)
// ---------------------------------------------------------------------------

void ReplicatedVShardHost::startSnapshotTrigger() {
    if (snapshotTriggerEnabled_ || stopped_)
        return;
    // (a) THE COHESION GATE, checked ONCE here rather than 1365 times in the sweep.
    // `buildVShardSnapshot` throws on a non-cohesive core count -- a single-core snapshot
    // would omit the series that scatter across cores, i.e. ship a PARTIAL snapshot -- and
    // that is a property of the node, not of a group, so it can never become true later.
    // Disabling the whole trigger with ONE warning is the difference between a legible
    // startup line and a permanent error every five seconds.
    if (!timestar::vshardsCohesiveOnCores(seastar::smp::count)) {
        timestar::http_log.warn(
            "cluster: the Raft snapshot trigger is DISABLED on this node: {} cores is not VShard-cohesive, so a "
            "single-core snapshot would omit series that scatter across cores. Raft logs will grow without bound and "
            "restart replay will lengthen accordingly (debt D-6).",
            seastar::smp::count);
        return;
    }
    snapshotTriggerEnabled_ = true;
    // JITTER the first fire so shards do not sweep in lockstep (they share the disk).
    snapshotCursor_ = seastar::this_shard_id() * 397;
    snapshotTimer_.set_callback([this] {
        if (stopped_ || snapshotSweepRunning_ || snapshotGate_.is_closed())
            return;
        snapshotSweepRunning_ = true;
        // The sweep body is a NAMED MEMBER COROUTINE. A coroutine lambda here would have
        // its frame owned by the with_gate temporary and outlive what it captured -- the
        // standing rule in this tree, and the bug that segfaulted shard 0 in raft_group.cpp.
        (void)seastar::with_gate(snapshotGate_,
                                 [this] { return snapshotSweep().finally([this] { snapshotSweepRunning_ = false; }); });
    });
    const auto jitter = std::chrono::milliseconds((seastar::this_shard_id() * 731) % 5000);  // NOLINT
    snapshotTimer_.arm(seastar::lowres_clock::now() + kSnapshotSweepInterval + jitter, {kSnapshotSweepInterval});
}

seastar::future<> ReplicatedVShardHost::snapshotSweep() {
    try {
        co_await maybeSnapshotOnce();
        // RECLAIM ON THE SNAPSHOT SWEEP'S TIMER, at a slower cadence (debt D-34).
        //
        // THREE things move a floor, and compaction is only the dominant one. The floor is
        // `min(newest HardState, newest Snapshot, oldest live entry) - 1`, so it also
        // advances when (b) a group whose HARD STATE was the binding term re-persists it --
        // an election, or compaction's own re-persist -- and when (c) a CONFLICTING
        // RE-APPEND in persistEntries pops the superseded suffix off the back of
        // `entrySeqs_` and raises its front, with no snapshot and no hard state involved at
        // all. None of the three is frequent enough to want a 5 s cadence, and a collect
        // reads whole sealed segments, so it gets its own longer interval and piggy-backs
        // on this timer rather than owning one. Failures are caught here with the snapshot
        // failure: nothing was deleted (the unlink is the last step of each segment) and
        // the next pass retries.
        const auto now = seastar::lowres_clock::now();
        if (lastJournalGc_.time_since_epoch().count() == 0 || now - lastJournalGc_ >= kJournalGcInterval) {
            lastJournalGc_ = now;
            const size_t deleted = co_await reclaimJournalSegments();
            if (deleted > 0)
                timestar::http_log.info(
                    "cluster: shard {} reclaimed {} sealed Raft journal segment(s) below the snapshot boundary "
                    "({} total, {} record(s) copied forward)",
                    seastar::this_shard_id(), deleted, journalSegmentsDeleted_, journalRecordsCopiedForward_);
        }
    } catch (const std::exception& e) {
        // (e) A FAILED SNAPSHOT MUST NEVER TAKE THE WRITE PATH WITH IT. Nothing was
        // compacted (compact() is the last step and it either ran or did not), so the group
        // is exactly as it was: a larger log and working replication. Log and move on.
        timestar::http_log.warn("cluster: Raft snapshot sweep failed: {} (the log is kept; will retry)", e.what());
    }
    co_return;
}

seastar::future<size_t> ReplicatedVShardHost::maybeSnapshotOnce() {
    size_t taken = 0;
    if (stopped_ || vshards_.empty())
        co_return 0;
    ++snapshotSweeps_;
    const auto now = seastar::lowres_clock::now();

    // Collect candidates first, THEN snapshot: `snapshotVShard` suspends, and mutating
    // `vshards_` under a suspended iterator over it is how this would become a UAF.
    std::vector<uint16_t> candidates;
    const size_t productionLimit = snapshotProductionLimit();
    // (c) STAGGER: start the scan at a rotating offset so the same low-numbered VShards do
    // not always win the shard's production budget. Two passes over a map is cheaper than
    // any ordering trick and this runs once every 5 s.
    const size_t n = vshards_.size();
    const size_t start = n == 0 ? 0 : snapshotCursor_ % n;
    size_t nextCursor = n == 0 ? 0 : (start + 1) % n;
    size_t idx = 0;
    for (int lap = 0; lap < 2 && candidates.size() < productionLimit; ++lap) {
        idx = 0;
        for (const auto& [vs, state] : vshards_) {
            const size_t currentIdx = idx++;
            const bool inRange = (lap == 0) ? (currentIdx >= start) : (currentIdx < start);
            if (!inRange)
                continue;
            if (candidates.size() >= productionLimit)
                break;
            raft::RaftGroup* g = registry_.group(vs);
            if (!g || !state.sm)
                continue;
            // Not again inside kMinSnapshotInterval, so a hot VShard cannot monopolize the
            // production budget while every other group's log grows.
            if (state.lastSnapshot.time_since_epoch().count() != 0 && now - state.lastSnapshot < snapshotMinInterval_)
                continue;
            const raft::LogIndex boundary = g->node().log().snapshotIndex();
            const uint64_t applied = g->appliedIndex();
            const uint64_t entriesSince = applied > boundary ? applied - boundary : 0;
            snapshotMaxEntriesSinceSeen_ = std::max(snapshotMaxEntriesSinceSeen_, entriesSince);
            // EITHER threshold suffices; a zero threshold disables that half.
            const bool byEntries = snapshotEntryThreshold_ != 0 && entriesSince >= snapshotEntryThreshold_;
            const bool byBytes =
                snapshotByteThreshold_ != 0 && state.sm->appliedBytesSinceSnapshot() >= snapshotByteThreshold_;
            const bool always = snapshotEntryThreshold_ == 0 && snapshotByteThreshold_ == 0;
            if (!byEntries && !byBytes && !always)
                continue;
            candidates.push_back(vs);
            // Resume immediately AFTER the last group selected, not by an arithmetic
            // cursor jump. A `limit + 1` jump creates permanent-looking stripes when its
            // stride shares a divisor with the hosted-group count; with a min interval
            // shorter than a full scan, already-snapshotted groups can become eligible
            // again before those stripes ever get a turn.
            nextCursor = (currentIdx + 1) % n;
        }
    }
    snapshotCursor_ = nextCursor;

    // Sequential, deliberately: shared mode raises the number attempted per pass so a
    // fair scan completes within its target cycle, but it must not mean "read and encode
    // N VShards' TSM files at once on one reactor".
    for (uint16_t vs : candidates) {
        if (stopped_)
            break;
        const uint64_t upto = co_await snapshotVShard(vs);
        if (upto > 0) {
            ++taken;
            timestar::http_log.debug("cluster: snapshotted VShard {} and compacted its Raft log up to index {}", vs,
                                     upto);
        }
    }
    co_return taken;
}

seastar::future<bool> ReplicatedVShardHost::proposeBatch(data::WriteBatch batch) {
    // Group the batch's series by VShard (a series routes to its VShard by hash, same
    // authority every replica uses -- see WriteSeries::vshard). schemaVersion is
    // carried per group.
    return proposeVShardBatches(data::splitByVShard(std::move(batch)));
}

// WHY A BARE `false` NEEDS DECODING (write-scaleout 5 review, F1).
//
// `RaftGroup::proposeAndAwaitApplied` returns false for TWO different situations:
// `role_ != Leader` (ask someone else) and `leadTransferee_ != kNoNode` (I am the leader,
// I am handing leadership away, ask me again shortly). This site used to label both
// `NotLeader` and attach `g->leader()` as the hint -- which, when WE are the refusing
// leader, is OURSELVES. The router accepts the hint, re-buckets the slice into its LOCAL
// view, and asks the same refusing group again; six attempts later the client gets a 503
// saying "not-leader" and NO RPC EVER LEFT THE NODE. That is what made the one-node-down
// symptom look like a routing bug rather than a refusal.
//
// So: report the two apart, and NEVER hand back a hint naming this node. With no hint the
// router re-resolves from its live leader view on the next attempt instead of looping.
data::SliceReject ReplicatedVShardHost::classifyRefusal(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    NodeId hint = g ? g->leader() : raft::kNoNode;
    data::WriteFailure kind = data::WriteFailure::NotLeader;
    if (g && hint == self_) {
        // We ARE the leader and we refused: a transfer is in flight (or leadership flapped
        // between the propose and here). There is nowhere else to send this slice.
        kind = data::WriteFailure::LeaderRefused;
        hint = raft::kNoNode;
        ++proposeRefusedWhileLeader_;
        const auto now = seastar::lowres_clock::now();
        if (now - lastRefusalLog_ >= std::chrono::seconds(5)) {
            lastRefusalLog_ = now;
            timestar::http_log.warn(
                "cluster: this node LEADS VShard {} and refused a proposal ({} such refusals so far); a leadership "
                "transfer is {}in flight. Writes to it fail retryably until the transfer completes or is abandoned.",
                vshard, proposeRefusedWhileLeader_, g->transferInFlight() ? "" : "no longer ");
        }
    }
    return data::SliceReject{vshard, hint, kind};
}

seastar::future<bool> ReplicatedVShardHost::proposeVShardBatches(data::VShardBatches byVShard) {
    // Membership check for the WHOLE batch BEFORE any replication, so a routing error
    // fails atomically (nothing proposed) rather than after some groups committed.
    for (const auto& [vs, b] : byVShard)
        if (!registry_.group(vs))
            throw std::runtime_error("ReplicatedVShardHost::proposeBatch: VShard not led here");
    // One admission decision for the whole request, before ANY group can append.
    // Apply is unconditional after this point, including on followers and replay.
    co_await store_.checkWriteAdmission(data::viewOf(byVShard));
    // Replicate every VShard group CONCURRENTLY; every group must commit for the
    // batch to ack.
    //
    // These are independent Raft groups with no ordering between them (this was never
    // an atomic multi-group commit -- see the header contract), but they used to be
    // proposed ONE AT A TIME, so a batch spanning N VShards serialised N full quorum
    // round trips (durable append + replicate + commit + apply) end to end. That was
    // the dominant write latency: measured at ~187ms average per 10k-point batch,
    // matching ~20 VShards x ~9ms. Issuing them together overlaps the network and
    // fsync waits instead of stacking them.
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byVShard.size());
    for (auto& [vs, b] : byVShard) {
        auto* g = registry_.group(vs);
        pending.push_back(g->proposeAndAwaitApplied(data::encodeWriteCommand(b)));
    }

    // Await EVERY proposal even after one fails -- abandoning an in-flight future
    // would let its group's commit land on a destroyed continuation.
    bool allOk = true;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            const bool ok = co_await std::move(f);
            allOk = allOk && ok;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return allOk;
}

seastar::future<data::ProposeOutcome> ReplicatedVShardHost::proposeVShardBatchesHinted(data::VShardBatchView view,
                                                                                       data::OptDeadline deadline) {
    data::ProposeOutcome out;
    // Membership check for the WHOLE view BEFORE any replication (same contract as the
    // bool overload): a routing error rejects atomically rather than after some groups
    // committed. Reported as rejects rather than thrown so the coordinator can re-resolve
    // and retry -- a group that is briefly not hosted here IS a routing miss, not a bug.
    //
    // When it fires NOTHING is proposed, so EVERY group in the view is uncommitted and
    // every one of them is named. Naming only the not-hosted groups (as this first did)
    // made the reject list a strict subset of the truth, and a caller that derived
    // "uncommitted == rejects" then acked slices this node never replicated. The
    // committed-set contract in node_store.hpp is the caller-side half of the same fix;
    // both halves are needed, because either alone leaves the other's mistake fatal.
    bool anyMissing = false;
    for (const auto* g : view)
        if (!registry_.group(g->first))
            anyMissing = true;
    if (anyMissing) {
        for (const auto* g : view)
            out.rejects.push_back(data::SliceReject{
                g->first, registry_.group(g->first) ? registry_.group(g->first)->leader() : raft::kNoNode,
                data::WriteFailure::NotLeader});
        co_return out;  // committedVShards stays empty: nothing was proposed
    }

    // Reject overloaded work before the first Raft append. The caller classifies
    // WriteOverloadedError as retryable and, critically, unambiguous.
    co_await store_.checkWriteAdmission(view);

    // Replicate every group CONCURRENTLY (see proposeVShardBatches for why serialising
    // them cost a full quorum round trip per VShard).
    std::vector<seastar::future<bool>> pending;
    pending.reserve(view.size());
    for (const auto* g : view) {
        raft::RaftGroup* grp = registry_.group(g->first);
        // encodeWriteCommand rather than propose(vshard, ReplicatedCommand{...}): filling
        // the variant would COPY the slice, which the pre-3b path avoided only by moving
        // it (and so losing it). The retry needs the groups kept, so the copy is removed
        // instead of paid.
        pending.push_back(grp->proposeAndAwaitApplied(data::encodeWriteCommand(g->second), deadline));
    }

    // Await EVERY proposal even after one fails -- abandoning an in-flight future would
    // let its group's commit land on a destroyed continuation.
    std::exception_ptr fatalErr;
    for (size_t i = 0; i < pending.size(); ++i) {
        const uint16_t vs = view[i]->first;
        try {
            if (co_await std::move(pending[i])) {
                out.committedVShards.push_back(vs);  // durable quorum commit -- the ONLY way in
            } else {
                out.rejects.push_back(classifyRefusal(vs));
            }
        } catch (...) {
            const auto kind = data::classifyLocalWriteFailure(std::current_exception());
            if (!data::isRetryableWriteFailure(kind)) {
                if (!fatalErr)
                    fatalErr = std::current_exception();
                continue;
            }
            raft::RaftGroup* g = registry_.group(vs);
            NodeId hint = g ? g->leader() : raft::kNoNode;
            if (hint == self_)
                hint = raft::kNoNode;  // never point a retry back at the node that just failed it
            out.rejects.push_back(data::SliceReject{vs, hint, kind});
        }
    }
    if (fatalErr)
        std::rethrow_exception(fatalErr);
    // Derived from what actually committed, never from what was rejected.
    out.committed = out.committedVShards.size() == view.size();
    co_return out;
}

raft::RaftGroup* ReplicatedVShardHost::group(uint16_t vshard) {
    return registry_.group(vshard);
}

seastar::future<bool> ReplicatedVShardHost::awaitApplyCatchUp(std::chrono::milliseconds budget) {
    // FAIL CLOSED DURING SHUTDOWN. A node tearing down cannot prove it has applied what it
    // committed, and "we are stopping" is not evidence that the answer would be complete --
    // it is the reason it might not be. The caller turns this into QUERY_INCOMPLETE, which
    // is what a client should see from a node on its way out.
    if (stopped_ || readFenceGate_.is_closed())
        co_return false;
    const auto holder = readFenceGate_.hold();

    // Enroll every hosted group ONCE, before any suspension. The policy (apply_fence.hpp)
    // owns the two reasons a group is pending and when each clears; this function owns
    // only the sampling, the sleep and the budget.
    ApplyFencePolicy fence;
    for (auto& [vs, state] : vshards_)
        fence.enroll(vs, fenceStateOf(vs));
    if (fence.clear())
        co_return true;  // caught up: the fast path, and it never suspends

    // POLL rather than register a waiter per group. `waitApplied` would be the obvious
    // tool, but it takes the group's lock to register -- and a shard hosting ~1000 groups
    // would take ~1000 locks on a path that is almost always a no-op, on a reactor whose
    // Ready drains want those same locks. Polling costs a handful of integer reads per
    // still-pending group per pass and, unlike a registered waiter, needs nothing unwound
    // when the budget runs out. It also re-reads `hasCurrentTermCommit`, which a waiter
    // registered against a stale commit index could not.
    const auto deadline = seastar::lowres_clock::now() + budget;
    while (seastar::lowres_clock::now() < deadline) {
        co_await seastar::sleep(std::chrono::milliseconds(2));
        if (stopped_)
            co_return false;  // same reasoning as the entry check
        if (fence.refresh([this](uint16_t vs) { return fenceStateOf(vs); }))
            co_return true;
    }
    // FAIL CLOSED. The caller turns this into a QUERY_INCOMPLETE, which is the honest
    // answer: this node holds acknowledged data it cannot yet read out.
    const auto stuck = fence.pendingGroups();
    timestar::http_log.warn(
        "cluster: shard {} still has {} group(s) that cannot prove they have applied what they committed after {} ms "
        "(first: VShard {}); failing the read closed rather than answering out of state that is behind its own log "
        "(debt D-36)",
        seastar::this_shard_id(), stuck.size(), budget.count(), stuck.empty() ? 0 : stuck.front());
    co_return false;
}

FenceGroupState ReplicatedVShardHost::fenceStateOf(uint16_t vshard) {
    FenceGroupState s;
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        return s;  // not hosted: the policy drops it
    s.hosted = true;
    s.hasCurrentTermCommit = g->hasCurrentTermCommit();
    s.commitIndex = g->commitIndex();
    s.appliedIndex = g->appliedIndex();
    return s;
}

NodeId ReplicatedVShardHost::leaderOf(uint16_t vshard) const {
    raft::RaftGroup* g = const_cast<raft::RaftGroupRegistry&>(registry_).group(vshard);
    return g ? g->leader() : raft::kNoNode;
}

seastar::future<raft::LogIndex> ReplicatedVShardHost::leaderReadIndex(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::leaderReadIndex: VShard not hosted here");
    // readBarrier() runs a quorum-confirmed ReadIndex round and REJECTS (throws) if this
    // node is not the current-term leader -- exactly the partition/redirect signal the
    // reaching replica needs, so no forwarding of stale barriers.
    return g->readBarrier();
}

seastar::future<raft::LogIndex> ReplicatedVShardHost::leaderCommitIndex(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::leaderCommitIndex: VShard not hosted here");
    if (!g->isLeader())
        // Bounded-staleness freshness must come from the leader's commit index; a
        // follower's is itself possibly stale, so reject rather than mislead.
        throw std::runtime_error("ReplicatedVShardHost::leaderCommitIndex: not the leader for this VShard");
    return seastar::make_ready_future<raft::LogIndex>(g->commitIndex());
}

seastar::future<> ReplicatedVShardHost::stop() {
    if (stopped_)
        co_return;
    stopped_ = true;
    // Stop the snapshot trigger FIRST and wait for any in-flight sweep: it reads Engine
    // files and calls RaftGroup::compact, both of which need the registry and the store
    // still standing. (e) honours stop() -- an unclosed gate here is a compaction landing
    // on a torn-down group.
    snapshotTimer_.cancel();
    // The read fence FIRST: a waiting fence resumes into `registry_`/`vshards_`, so it has
    // to be drained while both still stand (debt D-36). It waits at most its own budget.
    if (!readFenceGate_.is_closed())
        co_await readFenceGate_.close();
    if (!snapshotGate_.is_closed())
        co_await snapshotGate_.close();
    co_await registry_.stop();  // stops the tick loop + drains
    // The shared sink first: a round in flight holds a reference to the writer, and
    // a waiter it has not yet resolved must be failed rather than left hanging.
    if (sharedSink_)
        co_await sharedSink_->stop();
    for (auto& [vs, state] : vshards_)
        if (state.writer)
            co_await state.writer->close();
    vshards_.clear();
    if (sharedWriter_)
        co_await sharedWriter_->close();
    sharedSink_.reset();
    sharedWriter_.reset();
    sharedRecovered_.clear();
}

}  // namespace timestar::cluster
