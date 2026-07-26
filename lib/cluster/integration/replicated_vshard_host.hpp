#pragma once

#include "../../storage/journal_segment.hpp"
#include "../data/node_store.hpp"  // ProposeSink
#include "../data/replicated_command.hpp"
#include "../raft/raft_group_registry.hpp"
#include "../raft/raft_journal_persistence.hpp"
#include "engine_data_state_machine.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

namespace timestar::cluster {

using timestar::raft::NodeId;

// Hosts the per-VShard Raft groups this node replicates, over the REAL Engine
// (integration plan M3, the "per-core RaftGroupRegistry hosting an
// EngineDataStateMachine per locally-replicated vshard" piece). One instance per
// core; all its groups share one transport and one tick timer (RaftGroupRegistry).
//
// Given a VShard's voter set (from placement -- static config in v1, group-0 in the
// full M3), addVShard() creates the group's journal-backed persistence, an
// EngineDataStateMachine over the shared EngineLocalStore, and adds the RaftGroup to
// the registry. propose() replicates a ReplicatedCommand to a VShard's group and
// resolves only on durable-quorum commit + apply.
class ReplicatedVShardHost : public data::ProposeSink, public data::LeaderResolver, public data::ReadIndexSink {
public:
    ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                         std::filesystem::path journalRoot,
                         std::chrono::milliseconds tick = std::chrono::milliseconds(20));
    ~ReplicatedVShardHost();
    ReplicatedVShardHost(const ReplicatedVShardHost&) = delete;
    ReplicatedVShardHost& operator=(const ReplicatedVShardHost&) = delete;

    // Instantiate the Raft group for `vshard` with `voters`. `opts` tunes election
    // timing (production: uniform; tests: preferred leader). Journal lives under
    // journalRoot/vshard_<id>.
    seastar::future<> addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts = {});

    // Replicate a command to a VShard's group; resolves true on durable quorum commit
    // + apply, false if this node is not the leader (caller redirects to the leader).
    seastar::future<bool> propose(uint16_t vshard, const data::ReplicatedCommand& cmd);

    // The write-path entry point: split a WriteBatch by VShard and replicate each
    // group through its Raft group. This node must LEAD every VShard in the batch
    // (the write router groups by leader node before forwarding here). Series whose
    // VShard this node does not host are a routing error (throws, atomically -- the
    // membership check runs before any replication).
    //
    // NOT an atomic multi-group commit: groups are proposed sequentially, so a slice
    // that commits is durably applied and VISIBLE before the whole batch acks. Returns
    // true only if EVERY group committed; a false/throw from a later group leaves
    // earlier slices applied and makes the caller retry the WHOLE batch. Re-proposing
    // an already-applied slice commits at a NEW log index -> a HIGHER revision; for
    // identical data that is idempotent under LWW (same value). CAVEAT: if a
    // concurrent client wrote a NEWER value to the same (series, timestamp) into an
    // already-committed group between the first commit and the retry, the retry
    // re-applies the OLD batch value at the higher revision and clobbers it -- a
    // narrow LWW inversion inherent to per-group (not cross-group-atomic) commit.
    // Cross-group atomicity is out of scope for v1.
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override;

    // The same, for a batch already split by VShard (write-scaleout 2b). This is what
    // proposeBatch reduces to, and what the write router calls directly for the groups
    // this node leads -- the split happened once at ingress. Identical semantics,
    // including the atomic membership check before any replication.
    seastar::future<bool> proposeVShardBatches(data::VShardBatches groups) override;

    // The production entry (write-scaleout 3a/3b): propose a BORROWED selection of
    // groups and report per-VShard which did not commit, with THIS node's current view
    // of who leads them. Two things differ from the bool overloads and both matter:
    //
    //  * the groups are not consumed, so the coordinator can re-dispatch exactly the
    //    slices that failed without having copied the batch on the happy path;
    //  * a rejection carries `g->leader()`, which is what turns "your guess was wrong"
    //    into "node N leads it now" -- the alive-but-deposed primary case that had no
    //    covered failover path in v1.
    //
    // A group this node does not HOST is reported as a hintless reject and NOTHING in
    // the view is proposed, preserving the atomic membership check of the bool overload.
    seastar::future<data::ProposeOutcome> proposeVShardBatchesHinted(data::VShardBatchView view,
                                                                     data::OptDeadline deadline) override;

    // Compact this node's Raft log for `vshard` by snapshotting its FLUSHED data and
    // handing the payload to RaftGroup::compact (the M3 snapshot PRODUCER). Truncates
    // the log only up to the snapshot's covered revision (== the log index, ADR 0003):
    // entries whose data is still in the memory store (revision > that) are RETAINED,
    // so no unflushed data is lost -- compacting to appliedIndex instead would truncate
    // them. Returns the revision compacted to (0 if the VShard is not hosted here or
    // has no flushed data yet). Any replica may compact its own log (not leader-only).
    // Requires a VShard-cohesive core count (buildVShardSnapshot throws otherwise).
    seastar::future<uint64_t> snapshotVShard(uint16_t vshard);

    // ProposeSink (debt D-14): un-hibernate the groups on THIS shard that still believe
    // `node` leads them, so a killed leader costs an election rather than a
    // hibernation-stretched one. Same remedy the read path applies to an unreachable
    // leader; idempotent and self-limiting (the wake window expires on its own).
    size_t wakeGroupsLedBy(NodeId node) override { return registry_.wakeFollowersOf(node); }

    raft::RaftGroup* group(uint16_t vshard);
    // LeaderResolver: the current Raft leader of `vshard` per this node's local group
    // (kNoNode if not hosted here or no leader elected yet).
    NodeId leaderOf(uint16_t vshard) const override;
    // Does this node hold a REPLICA of `vshard` at all? leaderOf() alone cannot answer
    // that -- it returns kNoNode both for "hosted, no election yet" and for "not mine"
    // -- and conflating the two is what made every read fail on an RF < N cluster
    // (debt D-13). Every accounting of leaderlessness must gate on this first.
    bool hosts(uint16_t vshard) const { return vshards_.count(vshard) != 0; }

    // ReadIndexSink (M4 replica-read leader-reach): confirm a linearizable ReadIndex /
    // report the commit index for `vshard`. Both throw if this VShard is not hosted here
    // or this node is not its current leader (readBarrier() rejects non-leaders), so a
    // reaching replica gets a clean partition/redirect signal rather than a stale value.
    seastar::future<raft::LogIndex> leaderReadIndex(uint16_t vshard) override;
    seastar::future<raft::LogIndex> leaderCommitIndex(uint16_t vshard) override;
    raft::RaftGroupRegistry& registry() { return registry_; }
    // How many proposals this node refused WHILE BEING THE LEADER of the VShard (a
    // leadership transfer in flight). Non-zero means writes are failing for a reason no
    // amount of re-routing can fix -- see classifyRefusal (write-scaleout 5 review, F1).
    uint64_t proposeRefusedWhileLeader() const { return proposeRefusedWhileLeader_; }
    size_t vshardCount() const { return vshards_.size(); }

    void startTicking() { registry_.startTicking(); }
    seastar::future<> stop();

    // ---- the snapshot PRODUCER trigger (debt D-6) ----
    //
    // Before this, `snapshotVShard` had NO production caller: nothing ever compacted, so
    // every group's Raft log grew without bound until a restart replayed the whole thing.
    // F3c and the Phase-6 headroom fix both protected a path nothing called.
    //
    // POLICY. A group is a candidate when EITHER of two things has grown enough past its
    // last snapshot: entries (bounds restart REPLAY time) or applied entry bytes (bounds
    // journal DISK). See EngineDataStateMachine::appliedBytesSinceSnapshot for why one
    // alone is not enough.
    static constexpr uint64_t kSnapshotEntryThreshold = 8192;
    static constexpr uint64_t kSnapshotBytesThreshold = uint64_t{64} << 20;
    // How often the sweep looks. A snapshot is expensive (it reads every TSM file the
    // VShard touches), so this is deliberately slow relative to the 20 ms Raft tick: the
    // thresholds decide WHETHER, this only decides how promptly.
    static constexpr std::chrono::seconds kSnapshotSweepInterval{5};
    // RATE LIMIT. At most this many snapshots in flight per SHARD, over ~1365 groups on a
    // 3-core 4096-VShard node. Snapshotting reads whole TSM files and encodes them, so a
    // stampede would compete with the write path for the same reactor and the same disk --
    // and the point of a background trigger is that nobody notices it.
    static constexpr size_t kMaxConcurrentSnapshots = 1;
    // A group is not snapshotted again inside this window even if it re-crosses a
    // threshold, so a hot VShard cannot monopolize the shard's one slot.
    static constexpr std::chrono::seconds kMinSnapshotInterval{60};

    // Override the policy above. Exists for two reasons: a test cannot practically write
    // 8192 entries or 64 MiB, and an operator with an unusual workload (very large batches,
    // or a very slow disk) needs the knob without a rebuild. Zero on either threshold
    // disables that half; both zero makes every group eligible on every sweep.
    void setSnapshotPolicy(uint64_t entryThreshold, uint64_t byteThreshold, std::chrono::seconds minInterval) {
        snapshotEntryThreshold_ = entryThreshold;
        snapshotByteThreshold_ = byteThreshold;
        snapshotMinInterval_ = minInterval;
    }

    // Start the periodic sweep. Separate from startTicking() so a test can drive
    // maybeSnapshotOnce() by hand instead.
    void startSnapshotTrigger();

    // ONE sweep pass: pick at most `kMaxConcurrentSnapshots` eligible groups and snapshot
    // them. Returns how many were snapshotted. Exposed for tests (and for an operator
    // action later); the periodic timer just calls it.
    seastar::future<size_t> maybeSnapshotOnce();

    // Observability for the gate and for `/cluster/status`.
    uint64_t snapshotsTaken() const { return snapshotsTaken_; }
    uint64_t snapshotsRefusedTooLarge() const { return snapshotsRefusedTooLarge_; }
    uint64_t snapshotsSkippedUnflushed() const { return snapshotsSkippedUnflushed_; }
    // Sweeps that declined because a rolled-over store had not yet reached TSM. Steady
    // non-zero means this shard is converting continuously and its logs will stay long --
    // the conservative half of the trade in EngineLocalStore::hasUnconvertedStores.
    uint64_t snapshotsSkippedPendingConversion() const { return snapshotsSkippedPendingConversion_; }
    // Sweeps that declined because the boundary would not have ADVANCED (the flush
    // watermark has not moved since the last snapshot). Expected to dominate on a
    // low-traffic group; it is the cheap no-op arm, not a problem.
    uint64_t snapshotsSkippedNoAdvance() const { return snapshotsSkippedNoAdvance_; }
    // Sweep passes run, and the highest entries-since-snapshot any group showed on the last
    // pass. Together they answer the FIRST question when logs are not being compacted --
    // "is the sweep running at all, and is anything close to the threshold?" -- which
    // `snapshotsTaken() == 0` on its own cannot.
    uint64_t snapshotSweeps() const { return snapshotSweeps_; }
    uint64_t snapshotMaxEntriesSinceSeen() const { return snapshotMaxEntriesSinceSeen_; }
    bool snapshotTriggerEnabled() const { return snapshotTriggerEnabled_; }

private:
    struct VShardState {
        std::unique_ptr<JournalWriter> writer;
        std::unique_ptr<raft::JournalRaftPersistence> persistence;
        std::unique_ptr<EngineDataStateMachine> sm;
        // When this group was last snapshotted (default-constructed == never), for
        // kMinSnapshotInterval.
        seastar::lowres_clock::time_point lastSnapshot{};
    };

    EngineLocalStore& store_;
    NodeId self_;
    std::filesystem::path journalRoot_;
    // DECLARATION ORDER IS LOAD-BEARING: registry_ holds RaftGroups that BORROW
    // (RaftPersistence&/RaftStateMachine&) into vshards_' unique_ptrs, so registry_
    // (and its groups) MUST tear down before vshards_. Members destruct in reverse
    // declaration order, so vshards_ is declared FIRST and registry_ LAST.
    std::map<uint16_t, VShardState> vshards_;
    raft::RaftGroupRegistry registry_;
    bool stopped_ = false;
    // See classifyRefusal / proposeRefusedWhileLeader.
    data::SliceReject classifyRefusal(uint16_t vshard);
    uint64_t proposeRefusedWhileLeader_ = 0;
    seastar::lowres_clock::time_point lastRefusalLog_{};

    // ---- snapshot trigger state (debt D-6) ----
    //
    // The sweep body is a NAMED MEMBER COROUTINE, not a lambda inside with_gate: a
    // coroutine frame captured by a with_gate temporary outlives the frame it referenced
    // (the standing rule in this tree, learned the hard way in raft_group.cpp).
    seastar::future<> snapshotSweep();
    seastar::timer<seastar::lowres_clock> snapshotTimer_;
    seastar::gate snapshotGate_;
    bool snapshotSweepRunning_ = false;  // one sweep at a time; the timer skips if busy
    bool snapshotTriggerEnabled_ = false;
    // STAGGER. The sweep starts scanning at a rotating offset, so the same low-numbered
    // VShards are not always the ones that reach the shard's one snapshot slot. Seeded
    // from the shard id so two shards do not walk in lockstep either.
    size_t snapshotCursor_ = 0;
    uint64_t snapshotEntryThreshold_ = kSnapshotEntryThreshold;
    uint64_t snapshotByteThreshold_ = kSnapshotBytesThreshold;
    std::chrono::seconds snapshotMinInterval_ = kMinSnapshotInterval;
    uint64_t snapshotsTaken_ = 0;
    uint64_t snapshotsRefusedTooLarge_ = 0;
    uint64_t snapshotsSkippedUnflushed_ = 0;
    uint64_t snapshotsSkippedPendingConversion_ = 0;
    uint64_t snapshotsSkippedNoAdvance_ = 0;
    uint64_t snapshotSweeps_ = 0;
    uint64_t snapshotMaxEntriesSinceSeen_ = 0;
};

}  // namespace timestar::cluster
