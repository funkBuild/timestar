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
    raft::RaftGroupRegistry registry_;
    std::map<uint16_t, VShardState> vshards_;
    bool stopped_ = false;
};

}  // namespace timestar::cluster
