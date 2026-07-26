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
#include <seastar/core/lowres_clock.hh>

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

private:
    struct VShardState {
        std::unique_ptr<JournalWriter> writer;
        std::unique_ptr<raft::JournalRaftPersistence> persistence;
        std::unique_ptr<EngineDataStateMachine> sm;
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
};

}  // namespace timestar::cluster
