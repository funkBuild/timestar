#pragma once

#include "../../storage/journal_segment.hpp"
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
class ReplicatedVShardHost {
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
    seastar::future<bool> proposeBatch(data::WriteBatch batch);

    raft::RaftGroup* group(uint16_t vshard);
    raft::RaftGroupRegistry& registry() { return registry_; }
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
};

}  // namespace timestar::cluster
