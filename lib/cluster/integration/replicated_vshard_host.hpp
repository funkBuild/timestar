#pragma once

#include "../../storage/journal_gc.hpp"
#include "../../storage/journal_retention.hpp"
#include "../../storage/journal_segment.hpp"
#include "../../storage/journal_sink.hpp"
#include "../data/node_store.hpp"  // ProposeSink
#include "../data/replicated_command.hpp"
#include "../raft/raft_group_registry.hpp"
#include "../raft/raft_journal_persistence.hpp"
#include "apply_fence.hpp"
#include "engine_data_state_machine.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>
#include <string_view>

namespace timestar::cluster {

using timestar::raft::NodeId;

// Durable identity stamped into every Raft journal segment. Production constructs
// this from node.json's cluster UUID and a fresh process boot UUID. The testing
// value exists only to keep isolated host tests independent of server bootstrap.
struct JournalIdentity {
    std::array<uint8_t, 16> clusterUuid{};
    std::array<uint8_t, 16> bootId{};

    static JournalIdentity fromHex(std::string_view clusterUuid, std::string_view bootId);
    static JournalIdentity testing();
};

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
    // The coordinator uses the same 600 ms per-attempt bound. Receiver-side
    // enforcement is independently required: an RPC timeout/disconnect does
    // not cancel the server coroutine or its Raft apply waiter.
    static constexpr std::chrono::milliseconds kProposalTimeout{600};

    ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                         std::filesystem::path journalRoot,
                         std::chrono::milliseconds tick = std::chrono::milliseconds(20));
    ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                         std::filesystem::path journalRoot, JournalIdentity identity,
                         std::chrono::milliseconds tick = std::chrono::milliseconds(20));
    ~ReplicatedVShardHost();
    ReplicatedVShardHost(const ReplicatedVShardHost&) = delete;
    ReplicatedVShardHost& operator=(const ReplicatedVShardHost&) = delete;

    // Is this host appending into ONE shared per-shard journal (debt D-10) rather
    // than a journal per VShard? Opt-in via TIMESTAR_CLUSTER_SHARED_JOURNAL=1;
    // DEFAULT OFF. Resolved once per process.
    static bool sharedJournalEnabled();

    // fdatasyncs issued and sync() requests served across this shard's journals.
    // Their RATIO is the coalescing factor, and it is the only measurable evidence
    // of D-10 on a tmpfs box (where the elapsed-time win is invisible by
    // construction -- plan 5.3). ~1.0 per-VShard; > 1 shared.
    uint64_t journalFsyncs() const;
    uint64_t journalSyncRequests() const;

    // Aggregate uncommitted Raft-log admission (CR-FIX-080). One budget is shared
    // by every data group hosted on this reactor shard. The per-group ceiling
    // preserves room for at least four independently partitioned hot groups, while
    // still admitting the largest legal single proposal.
    static constexpr size_t kMaxUncommittedProposalBytes = raft::kMaxInboundRaftMemory;
    static constexpr size_t kMaxUncommittedProposalBytesPerGroup =
        raft::estimatedLogEntryBytes(raft::RaftGroup::kMaxProposalBytes);
    static_assert(kMaxUncommittedProposalBytesPerGroup * 4 <= kMaxUncommittedProposalBytes,
                  "the uncommitted Raft budget must preserve admission fairness across at least four groups");
    size_t uncommittedProposalBytes() const { return uncommittedProposalBudget_.current(); }
    size_t uncommittedProposalPeakBytes() const { return uncommittedProposalBudget_.peak(); }
    uint64_t uncommittedProposalRefusals() const { return uncommittedProposalBudget_.refusals(); }
    static constexpr size_t uncommittedProposalLimitBytes() { return kMaxUncommittedProposalBytes; }
    static constexpr size_t uncommittedProposalPerGroupLimitBytes() { return kMaxUncommittedProposalBytesPerGroup; }

    // Journal segment ROTATION TARGET, per layout. Must be set before addVShard.
    //
    // A TEST SEAM, and nothing more -- there is no config key behind it and no operator
    // can reach it. Segment reclamation (debt D-34) only ever acts on SEALED segments, and
    // at the production 1 MiB per-VShard target proving a reclaim means pushing a megabyte
    // of Raft entries through a quorum, which is not a unit test. If an operator ever does
    // need this, wire it to `timestar.toml` rather than promoting the setter.
    void setJournalSegmentBytes(size_t perVShard, size_t shared) {
        journalSegmentBytes_ = perVShard;
        sharedJournalSegmentBytes_ = shared;
    }

    // Instantiate the Raft group for `vshard` with `voters`. `opts` tunes election
    // timing (production: uniform; tests: preferred leader). The journal lives
    // under journalRoot/vshard_<id> (default) or, with the shared journal enabled,
    // journalRoot/shard_<core> shared by every group on this reactor.
    using RecoveredConfigValidator = std::function<bool(const raft::Config&)>;
    seastar::future<> addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts = {},
                                RecoveredConfigValidator recoveredConfigValidator = {});

    // Replicate a command to a VShard's group; resolves true on durable quorum commit
    // + apply, false if this node is not the leader (caller redirects to the leader).
    // Own the command because admission introduces a suspension before encoding.
    // A const-reference parameter would dangle when callers pass a temporary.
    seastar::future<bool> propose(uint16_t vshard, data::ReplicatedCommand cmd);
    seastar::future<bool> propose(uint16_t vshard, data::ReplicatedCommand cmd, data::OptDeadline deadline);
    seastar::future<data::ProposeOutcome> proposeCommandHinted(uint16_t vshard, data::ReplicatedCommand cmd,
                                                               data::OptDeadline deadline) override;

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

    // Compact this node's Raft log for `vshard` by snapshotting its resolved TSM view
    // and handing the payload to RaftGroup::compact (the M3 snapshot PRODUCER). The
    // active store's oldest surviving revision fences the retained suffix; an applied
    // prefix represented entirely by durable deletes may therefore advance beyond the
    // highest point revision without crossing unflushed data. Returns the Raft index
    // compacted to (0 if the VShard is not hosted here or no safe boundary advances).
    // Any replica may compact its own log (not leader-only).
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

    // WAIT UNTIL THIS SHARD'S GROUPS HAVE APPLIED WHAT THEY HAD ALREADY COMMITTED
    // (debt D-36). Returns true if they caught up within `budget`, false on timeout.
    //
    // THE READ FENCE. An acknowledged write is durable at COMMIT and readable only at
    // APPLY, and the node-local query reads the Engine -- i.e. applied state -- with no
    // regard for the log above it. After a whole-cluster restart every replica sits with
    // a recovered, committed, unapplied suffix, and a query issued in that window
    // answered HTTP 200 while silently omitting acknowledged points. That is exactly the
    // "incomplete results are failures, never short answers" rule, applied to a
    // completeness condition the single-node code cannot have.
    //
    // THE BAR IS SAMPLED AT ENTRY, deliberately: a query must see every write
    // acknowledged BEFORE it started, and owes nothing to writes still in flight
    // alongside it. Sampling the commit index once and waiting for THAT is a bounded
    // wait; waiting for "no lag at all" on a node under continuous ingest is not a
    // bound, it is a livelock.
    //
    // FREE WHEN CAUGHT UP, which is the common case: a handful of integer reads per
    // hosted group, and the fast path never suspends.
    //
    // WHAT "CAUGHT UP" MEANS IS NOT "commit == applied" -- see ApplyFencePolicy. A group
    // with no CURRENT-TERM commit has a commit index that proves nothing, and reporting
    // it as caught up is how the first version of this fence let a freshly elected
    // leader answer out of an entire unapplied recovered log.
    seastar::future<bool> awaitApplyCatchUp(std::chrono::milliseconds budget);
    // How many proposals this node refused WHILE BEING THE LEADER of the VShard (a
    // leadership transfer in flight). Non-zero means writes are failing for a reason no
    // amount of re-routing can fix -- see classifyRefusal (write-scaleout 5 review, F1).
    uint64_t proposeRefusedWhileLeader() const { return proposeRefusedWhileLeader_; }
    size_t vshardCount() const { return vshards_.size(); }
    std::optional<EngineDataStateMachine::DeleteReceiptCounts> deleteReceiptCounts(uint16_t vshard);

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
    // RATE LIMIT. The default per-VShard journal produces ONE snapshot per pass.
    // Shared journals need a different cadence: one lagging floor participates in every
    // segment on the reactor, so one-per-pass takes 85 minutes even on the supported
    // four-core topology (and 5.7 hours at one core). Derive a SEQUENTIAL per-pass batch
    // that targets one fair scan every 15 minutes. The snapshots are never concurrent --
    // maybeSnapshotOnce awaits each before starting the next -- so this does not multiply
    // the 128 MiB per-snapshot memory ceiling. A slow disk naturally stretches the cycle
    // because the one sweep remains in flight and later timer callbacks skip it.
    static constexpr size_t kPrivateJournalSnapshotsPerSweep = 1;
    static constexpr std::chrono::minutes kSharedJournalSnapshotTargetCycle{15};
    static constexpr size_t sharedJournalSnapshotsPerSweep(size_t hostedVShards) {
        constexpr auto sweepsPerTarget =
            std::chrono::duration_cast<std::chrono::seconds>(kSharedJournalSnapshotTargetCycle).count() /
            kSnapshotSweepInterval.count();
        static_assert(sweepsPerTarget > 0);
        return std::max<size_t>(
            1, (hostedVShards + static_cast<size_t>(sweepsPerTarget) - 1) / static_cast<size_t>(sweepsPerTarget));
    }
    // A group is not snapshotted again inside this window even if it re-crosses a
    // threshold, so a hot VShard cannot monopolize the shard's production budget.
    static constexpr std::chrono::seconds kMinSnapshotInterval{60};

    // THE OTHER RATE LIMIT, and it bounds the other direction (debt D-37).
    // The per-sweep budget above caps snapshot PRODUCTION on this shard; this caps
    // concurrent snapshot TRANSFERS out of it. Production was capped from the start
    // because it costs local CPU and disk; transfers were not, because each one is
    // individually paced at one unacked chunk -- but a shard hosts ~1365 groups and
    // nothing summed them, so a node returning from a long outage could be sent up to that
    // many 4 MiB chunks at once.
    //
    // FOUR, because 4 x kMaxSnapshotChunkBytes = 16 MiB is a quarter of the peer's
    // per-shard Raft inbound admission budget (kMaxInboundRaftMemory, 64 MiB) -- PER
    // SENDER, and that qualifier is load-bearing (review F2). The budget is the RECEIVER's
    // and every leader shipping to it spends from the same one, so this bounds one
    // sender's share and N-1 senders compose: at N=3 two catching-up leaders can claim
    // half of it, at N=5 four can claim all of it. The cap is still the right shape -- a
    // sender can only cap what it sends -- but it is not a receive-side guarantee, and the
    // register names that as a residual rather than pretending otherwise.
    //
    // The other three quarters have to stay available for appends and heartbeats: while a
    // transfer is in flight its chunks ARE that follower's heartbeat, so snapshot traffic
    // that crowds out ordinary replication makes followers campaign -- the failure this cap
    // exists to prevent, not one to trade for. Four also keeps the pipeline full: a
    // transfer costs one round trip per chunk, so four in flight saturate a link that one
    // would leave idle between acks.
    static constexpr size_t kMaxConcurrentSnapshotTransfers = 4;
    static_assert(kMaxConcurrentSnapshotTransfers * raft::kMaxSnapshotChunkBytes <= raft::kMaxInboundRaftMemory / 4,
                  "ONE SENDER's concurrent snapshot chunks must not be able to claim more than a quarter of a "
                  "peer's Raft inbound budget, or a catching-up node starves ordinary replication. N-1 senders "
                  "still compose -- that is a named residual, not something this assert covers [debt D-37]");

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

    // ONE sweep pass: pick at most the active journal layout's sequential batch of
    // eligible groups and snapshot them. Returns how many were snapshotted. Exposed for
    // tests (and for an operator action later); the periodic timer just calls it.
    seastar::future<size_t> maybeSnapshotOnce();
    size_t snapshotProductionLimit() const {
        return sharedSink_ ? sharedJournalSnapshotsPerSweep(vshards_.size()) : kPrivateJournalSnapshotsPerSweep;
    }

    // ---- journal SEGMENT reclamation (debt D-34) ----
    //
    // D-6 made the snapshot boundary real (the log is compacted and the boundary
    // survives a restart), so replay is bounded. It did NOT reclaim a byte of disk: the
    // sealed segments holding the compacted-away records were still there, and
    // `cluster_raft/` grew without limit however often a node snapshotted.
    //
    // This turns the boundary into deleted files, in three steps:
    //
    //  1. PUBLISH THE FLOOR. Every group's JournalRaftPersistence knows the highest
    //     vshard_seq it no longer needs (releasedSeq(), which is NOT simply the snapshot
    //     record's seq -- read that comment). publishReclaimFloors() copies those into
    //     `retention_`, whose watermarks advance monotonically.
    //  2. PLAN. `JournalRetention::planSegment` decides per SEALED segment: fully
    //     released -> delete; otherwise it is pinned by whoever still needs it.
    //  3. RECLAIM. `JournalGc` does the I/O, oldest segment first, never touching the
    //     active one.
    //
    // BOTH LAYOUTS, DIFFERENT SHAPES, same rule:
    //   * per-VShard journals (the DEFAULT): a segment holds exactly one group's
    //     records, so "fully released" is decided by that group alone and copy-forward
    //     could gain at most the one partially-released 1 MiB segment. It runs
    //     DELETE-ONLY, which means it never touches the writer and so needs no exclusion
    //     against the group's own appends. Only VShards whose floor ADVANCED since their
    //     last pass are visited, so an idle node does not scan ~1365 directories.
    //   * shared per-shard journal (`TIMESTAR_CLUSTER_SHARED_JOURNAL=1`, D-10): a
    //     segment holds every group on the reactor, so one laggard pins 64 MiB for
    //     everyone. Copy-forward is what the bricks were written for and it is enabled,
    //     under `SharedShardJournal::runExclusive` -- appending relocated records
    //     concurrently with a group-commit barrier would write them at an offset
    //     computed before they existed.
    //
    // CRASH DURING GC IS SAFE BY ORDERING, not by luck: copy the live records forward,
    // BARRIER, then unlink, then sync the directory. Every crash window leaves at least
    // one complete generation, and the overlap window leaves a BYTE-IDENTICAL duplicate.
    // What absorbs that duplicate is the PRODUCTION recovery path and nothing else:
    // `JournalWriter::open()` hands its record set to `recoverRaftState`, which sorts one
    // VShard's records by vshard_seq and replays them, so a repeat re-applies the same
    // HardState / re-places the same entry at the same index / re-decodes the same
    // Snapshot and the reconstructed state is identical. (`JournalReplay::finalize`
    // dedupes too and rejects any sequence gap not covered by a later retained Snapshot,
    // but it has NO production caller, so it is not what makes this safe.) A
    // partially-deleted sequence is fine for the same reason: each segment is an
    // independent step. Private journals preserve a physical suffix; shared journals may
    // leave holes only in records their retention floors prove obsolete.
    static constexpr std::chrono::seconds kJournalGcInterval{60};
    // Run one reclamation pass now (publish floors, then collect). Returns the number of
    // segment files deleted. Exposed for tests and for an operator action; the sweep
    // calls it on its own cadence.
    seastar::future<size_t> reclaimJournalSegments();
    // Copy every group's current reclaim floor into `retention_`. Returns how many
    // advanced. Separated from the collect so a test can assert the floor itself.
    size_t publishReclaimFloors();
    const JournalRetention& journalRetention() const { return retention_; }

    uint64_t journalSegmentsDeleted() const { return journalSegmentsDeleted_; }
    // Sealed segments the LAST pass left behind, across every journal it inspected -- a
    // GAUGE, not a cumulative count, and deliberately so. Shared mode scans past pins, so
    // these are individually unreclaimable segments rather than an uninspected physical
    // suffix. `pinned` + the pass's deletion delta answers "is retention keeping up?". A
    // running pin total would be meaningless: the same retained segment would be counted
    // again on every pass until its group compacts.
    uint64_t journalSegmentsPinnedLastPass() const { return journalSegmentsPinnedLastPass_; }
    uint64_t journalRecordsCopiedForward() const { return journalRecordsCopiedForward_; }
    uint64_t journalGcPasses() const { return journalGcPasses_; }

    // Observability for the gate and for `/cluster/status`.
    uint64_t snapshotsTaken() const { return snapshotsTaken_; }
    uint64_t snapshotsRefusedTooLarge() const { return snapshotsRefusedTooLarge_; }
    uint64_t snapshotsSkippedUnflushed() const { return snapshotsSkippedUnflushed_; }
    // Sweeps that declined because a rolled-over store had not yet reached TSM. Steady
    // non-zero means this shard is converting continuously and its logs will stay long --
    // the conservative half of the trade in EngineLocalStore::hasUnconvertedStores.
    uint64_t snapshotsSkippedPendingConversion() const { return snapshotsSkippedPendingConversion_; }
    // The flushed data boundary still precedes a receipt-retirement entry, so
    // compacting there could make a suffix retry execute twice after recovery.
    uint64_t snapshotsSkippedDeleteState() const { return snapshotsSkippedDeleteState_; }
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
    // The shard-level transfer budget (debt D-37): how many transfers are shipping chunks
    // right now, and how many groups are queued behind them. `waiting` steadily non-zero
    // means this shard is catching a peer up on more groups than the cap allows at once,
    // which is the burst being shaped rather than a fault.
    size_t snapshotTransfersActive() const { return snapshotBudget_.active(); }
    size_t snapshotTransfersWaiting() const { return snapshotBudget_.waiting(); }
    static constexpr size_t snapshotTransferCap() { return kMaxConcurrentSnapshotTransfers; }

private:
    struct VShardState {
        std::unique_ptr<JournalWriter> writer;
        std::unique_ptr<raft::JournalRaftPersistence> persistence;
        std::unique_ptr<EngineDataStateMachine> sm;
        // Admission + proposal + post-apply inspection must be one ordered
        // operation per group; otherwise a concurrent newer delete could retire
        // this request between its floor check and log append.
        std::unique_ptr<seastar::semaphore> deleteProposalLock = std::make_unique<seastar::semaphore>(1);
        // When this group was last snapshotted (default-constructed == never), for
        // kMinSnapshotInterval.
        seastar::lowres_clock::time_point lastSnapshot{};
        // The reclaim floor this VShard's journal directory was last collected at
        // (debt D-34, per-VShard layout only). A pass whose floor has not moved would
        // re-scan the directory and re-read its sealed segments to reach the same
        // decision, so it is skipped -- that is what keeps a ~1365-group node from
        // doing 1365 directory walks every minute.
        uint64_t lastGcFloor = 0;
    };

    // One group's state for the read fence (debt D-36). Non-const because
    // RaftGroupRegistry::group() is; it mutates nothing.
    FenceGroupState fenceStateOf(uint16_t vshard);

    EngineLocalStore& store_;
    NodeId self_;
    std::filesystem::path journalRoot_;
    JournalIdentity journalIdentity_;
    // ---- shared per-shard journal (debt D-10), null unless opted in ----
    //
    // Opened lazily on the first addVShard and shared by every group on this
    // reactor. `sharedRecovered_` is the ONE recovered record set; each group
    // filters it with recoverRaftState(records, vshard), which already does exactly
    // that (it sorts by vshard_seq, so physical interleaving is irrelevant).
    //
    // DECLARATION ORDER: sharedSink_ borrows sharedWriter_, and vshards_' persistence
    // objects borrow sharedSink_; members destruct in reverse declaration order, so
    // the writer is declared FIRST and the sink after it, both BEFORE vshards_.
    // Segment rotation targets; see setJournalSegmentBytes(). The shared journal
    // carries every group on the reactor (~1365 at 4096 VShards / 3 shards), so the
    // per-VShard target would rotate -- and seal, and fsync, and sync_directory --
    // constantly.
    size_t journalSegmentBytes_ = 1u << 20;
    size_t sharedJournalSegmentBytes_ = 1u << 26;
    std::unique_ptr<JournalWriter> sharedWriter_;
    std::unique_ptr<SharedShardJournal> sharedSink_;
    std::vector<JournalRecord> sharedRecovered_;
    // DECLARATION ORDER IS LOAD-BEARING: registry_ holds RaftGroups that BORROW
    // (RaftPersistence&/RaftStateMachine&) into vshards_' unique_ptrs, so registry_
    // (and its groups) MUST tear down before vshards_. Members destruct in reverse
    // declaration order, so vshards_ is declared FIRST and registry_ LAST.
    std::map<uint16_t, VShardState> vshards_;
    // DECLARED BEFORE registry_ for the same reason vshards_ is: every RaftNode in the
    // registry holds a POINTER to this budget, so it must outlive them (debt D-37).
    raft::SnapshotTransferBudget snapshotBudget_{kMaxConcurrentSnapshotTransfers};
    // Every RaftGroup in registry_ publishes its current tail here, so declaration
    // order is load-bearing just like snapshotBudget_: this must outlive registry_.
    raft::UncommittedProposalBudget uncommittedProposalBudget_{kMaxUncommittedProposalBytes,
                                                               kMaxUncommittedProposalBytesPerGroup};
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
    // The read fence's own gate (debt D-36). A fence that is WAITING is suspended in a
    // sleep with nothing but `this` keeping it honest, so it must not outlive the host --
    // it resumes to read `registry_` and `vshards_`. Closed FIRST in stop(), before the
    // registry, so an in-flight fence finishes against a live host and a new one is
    // refused rather than started. Its own gate rather than snapshotGate_'s: a read must
    // not be held up by a snapshot sweep's lifetime, nor a shutdown by a read's.
    seastar::gate readFenceGate_;
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
    uint64_t snapshotsSkippedDeleteState_ = 0;
    uint64_t snapshotsSkippedNoAdvance_ = 0;
    uint64_t snapshotSweeps_ = 0;
    uint64_t snapshotMaxEntriesSinceSeen_ = 0;

    // ---- journal segment reclamation state (debt D-34) ----
    JournalRetention retention_;
    seastar::lowres_clock::time_point lastJournalGc_{};
    bool journalGcRunning_ = false;  // one pass at a time (it suspends over file I/O)
    uint64_t journalSegmentsDeleted_ = 0;
    uint64_t journalSegmentsPinnedLastPass_ = 0;
    uint64_t journalRecordsCopiedForward_ = 0;
    uint64_t journalGcPasses_ = 0;
};

}  // namespace timestar::cluster
