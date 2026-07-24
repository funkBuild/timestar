#pragma once

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
        : host_(store, raftTransport, self, std::move(journalRoot), tick), router_(dir, host_, client) {}

    // Instantiate the local Raft groups this node replicates (one per entry of
    // ClusterRuntime::localReplicaGroups()).
    seastar::future<> addVShard(uint16_t vshard, std::vector<data::NodeId> voters, raft::RaftOptions opts = {}) {
        return host_.addVShard(vshard, std::move(voters), opts);
    }

    // Route + replicate a write to each VShard's leader (durable quorum commit).
    seastar::future<> write(data::WriteBatch batch) { return router_.write(std::move(batch)); }

    // The Raft propose target incoming proposeWrite RPCs dispatch into (host_ is the
    // ProposeSink). The server wires DataPlaneRpc::setProposeSink to this.
    data::ProposeSink& proposeSink() { return host_; }
    ReplicatedVShardHost& host() { return host_; }

    void startTicking() { host_.startTicking(); }
    seastar::future<> stop() { return host_.stop(); }

private:
    // host_ before router_: router_ borrows host_ as its local ProposeSink.
    ReplicatedVShardHost host_;
    data::ReplicatedBatchWriteRouter router_;
};

}  // namespace timestar::cluster
