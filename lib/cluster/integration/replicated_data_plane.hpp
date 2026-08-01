#pragma once

#include "../data/replicated_command_router.hpp"
#include "../data/replicated_write_router.hpp"
#include "../data/vshard_directory.hpp"
#include "../raft/raft_driver.hpp"  // RaftTransport
#include "engine_local_store.hpp"
#include "replicated_vshard_host.hpp"

#include <chrono>
#include <filesystem>
#include <seastar/core/future.hh>

namespace timestar::cluster {

// The RF=3 node composition (integration plan M3): the replicated analogue of the M2
// ClusterDataPlane. Wires the per-VShard Raft host over the real Engine and the
// leader-aware write router into one service. The Raft transport (replica<->replica)
// and the data-plane NodeTransport (coordinator->leader proposeWrite) are INJECTED,
// so production supplies RaftRpcTransport + DataPlaneRpc while a test supplies an
// in-memory router + a direct double -- the assembly logic is identical.
class ReplicatedDataPlane {
public:
    ReplicatedDataPlane(EngineLocalStore& store, raft::RaftTransport& raftTransport, data::NodeTransport& client,
                        const data::VShardDirectory& dir, data::NodeId self, std::filesystem::path journalRoot,
                        std::chrono::milliseconds tick = std::chrono::milliseconds(20))
        : host_(store, raftTransport, self, std::move(journalRoot), tick),
          router_(dir, host_, client, host_),
          commandRouter_(dir, host_, client, host_) {}
    ReplicatedDataPlane(EngineLocalStore& store, raft::RaftTransport& raftTransport, data::NodeTransport& client,
                        const data::VShardDirectory& dir, data::NodeId self, std::filesystem::path journalRoot,
                        JournalIdentity identity, std::chrono::milliseconds tick = std::chrono::milliseconds(20))
        : host_(store, raftTransport, self, std::move(journalRoot), identity, tick),
          router_(dir, host_, client, host_),
          commandRouter_(dir, host_, client, host_) {}

    // Instantiate the local Raft groups this node replicates (one per entry of
    // ClusterRuntime::localReplicaGroups()).
    seastar::future<> addVShard(uint16_t vshard, std::vector<data::NodeId> voters, raft::RaftOptions opts = {}) {
        return host_.addVShard(vshard, std::move(voters), opts);
    }

    // Route + replicate a write to each VShard's leader (durable quorum commit). The
    // VShardBatches overload takes a batch the caller already split by VShard
    // (write-scaleout 2b) -- the production path, since the request shard splits once
    // and buckets the groups by owning shard.
    seastar::future<> write(data::WriteBatch batch) { return router_.write(std::move(batch)); }
    seastar::future<> write(data::VShardBatches groups) { return router_.write(std::move(groups)); }
    seastar::future<> command(uint16_t vshard, data::ReplicatedCommand cmd) {
        return commandRouter_.propose(vshard, std::move(cmd));
    }

    // The Raft propose target incoming proposeWrite RPCs dispatch into (host_ is the
    // ProposeSink). The server wires DataPlaneRpc::setProposeSink to this.
    data::ProposeSink& proposeSink() { return host_; }
    ReplicatedVShardHost& host() { return host_; }

    void startTicking() { host_.startTicking(); }
    // Start the background Raft-log snapshot/compaction trigger (debt D-6). Separate from
    // startTicking() because it is a POLICY loop rather than consensus: a test drives
    // `host().maybeSnapshotOnce()` directly instead, and the composition tests that only
    // exercise replication leave it off.
    void startSnapshotTrigger() { host_.startSnapshotTrigger(); }
    seastar::future<> stop() { return host_.stop(); }

private:
    // host_ before router_: router_ borrows host_ as its local ProposeSink.
    ReplicatedVShardHost host_;
    data::ReplicatedBatchWriteRouter router_;
    data::ReplicatedCommandRouter commandRouter_;
};

}  // namespace timestar::cluster
