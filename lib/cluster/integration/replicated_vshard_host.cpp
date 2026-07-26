#include "replicated_vshard_host.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"
#include "../../utils/logger.hpp"  // timestar::http_log
#include "../raft/raft_node.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <stdexcept>

namespace timestar::cluster {

namespace fs = std::filesystem;

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, std::chrono::milliseconds tick)
    : store_(store), self_(self), journalRoot_(std::move(journalRoot)), registry_(transport, tick) {}

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

seastar::future<> ReplicatedVShardHost::addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts) {
    // A VShard is hosted at most once: re-adding would open a second JournalWriter on
    // the same dir (two recoverers over one journal) and leak the old writer's fd.
    if (registry_.group(vshard))
        throw std::runtime_error("ReplicatedVShardHost::addVShard: VShard already hosted");
    VShardState vs;
    fs::path dir = journalRoot_ / ("vshard_" + std::to_string(vshard));
    fs::create_directories(dir);

    JournalSegmentHeader hdr;
    hdr.clusterUuid.fill(0x11);  // TODO(M3 group-0): the real cluster UUID from node.json
    hdr.coreNumber = static_cast<uint16_t>(seastar::this_shard_id());
    hdr.bootId.fill(0x44);
    vs.writer = std::make_unique<JournalWriter>(dir, hdr, 1u << 20);
    auto recovered = co_await vs.writer->open();
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

    vs.persistence = std::make_unique<raft::JournalRaftPersistence>(*vs.writer, VShardId{vshard}, st.nextSeq);
    vs.sm = std::make_unique<EngineDataStateMachine>(store_, VShardId{vshard});
    raft::RaftNode node(self_, baseVoters, std::move(st.log), st.hardState, opts, baseLearners);
    if (st.snapshot) {
        // RECOVERING A COMPACTED JOURNAL (debt D-6). This used to throw outright -- which
        // was correct while nothing ever compacted, and would have turned the FIRST restart
        // after D-6 into a fail-closed refusal to start.
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
        node.seedRecoveredSnapshot(*st.snapshot);
        timestar::http_log.info(
            "cluster: VShard {} recovered from a compacted journal at boundary index {} (term {}); the local Engine "
            "already holds the flushed data the snapshot describes, so only the log suffix above it is replayed",
            vshard, st.snapshot->index, st.snapshot->term);
    }
    registry_.addGroup(vshard, std::move(node), *vs.persistence, *vs.sm);
    vshards_[vshard] = std::move(vs);
    co_return;
}

seastar::future<bool> ReplicatedVShardHost::propose(uint16_t vshard, const data::ReplicatedCommand& cmd) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::propose: VShard not hosted here");
    return g->proposeAndAwaitApplied(data::encodeReplicatedCommand(cmd));
}

seastar::future<uint64_t> ReplicatedVShardHost::snapshotVShard(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        co_return 0;  // not hosted here
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

    // TRUNCATE ONE ENTRY BELOW THE HIGHEST FLUSHED REVISION, not at it. Found while wiring
    // the trigger (D-6), and it is a real hole rather than belt-and-braces.
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
    // before installing. Three copies of an unbounded payload on a reactor with a fixed
    // memory pool is an OOM. That is a ~4.5x RISE over the old effective ceiling, so
    // snapshots that used to block compaction now ship -- as a pipeline of 4 MiB chunks.
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
    // (c) STAGGER: start the scan at a rotating offset so the same low-numbered VShards do
    // not always win the shard's one slot. Two passes over a map is cheaper than any
    // ordering trick and this runs once every 5 s.
    const size_t n = vshards_.size();
    const size_t start = n == 0 ? 0 : snapshotCursor_ % n;
    size_t idx = 0;
    for (int lap = 0; lap < 2 && candidates.size() < kMaxConcurrentSnapshots; ++lap) {
        idx = 0;
        for (const auto& [vs, state] : vshards_) {
            const bool inRange = (lap == 0) ? (idx >= start) : (idx < start);
            ++idx;
            if (!inRange)
                continue;
            if (candidates.size() >= kMaxConcurrentSnapshots)
                break;
            raft::RaftGroup* g = registry_.group(vs);
            if (!g || !state.sm)
                continue;
            // Not again inside kMinSnapshotInterval, so a hot VShard cannot monopolize the
            // slot while every other group's log grows.
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
        }
    }
    snapshotCursor_ += kMaxConcurrentSnapshots + 1;

    // kMaxConcurrentSnapshots is 1, so this is a loop for the sake of the constant rather
    // than for parallelism -- deliberately: raising the constant must not silently mean
    // "read and encode N VShards' TSM files at once on one reactor".
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
    for (auto& [vs, b] : byVShard)
        pending.push_back(propose(vs, data::ReplicatedCommand{std::move(b)}));

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
    if (!snapshotGate_.is_closed())
        co_await snapshotGate_.close();
    co_await registry_.stop();  // stops the tick loop + drains
    for (auto& [vs, state] : vshards_)
        if (state.writer)
            co_await state.writer->close();
    vshards_.clear();
}

}  // namespace timestar::cluster
