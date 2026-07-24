#pragma once

#include "node_store.hpp"  // NodeTransport, ProposeSink
#include "vshard_directory.hpp"
#include "write_record.hpp"

#include <seastar/core/future.hh>

namespace timestar::data {

// The RF=3 write router (integration plan M3): routes a WriteBatch to each VShard's
// Raft LEADER to be replicated -- the local ProposeSink (this node's
// ReplicatedVShardHost) when this node leads the VShard, else proposeWrite RPC to the
// leader node. The leader hint is the placement primary (reps.front()); after a
// leadership move the primary may not be the current leader, in which case propose
// returns false (not-leader) and the whole write fails so the caller retries against
// the advanced map -- no silent partial commit.
//
// Contrast with the M2 NodeWriteRouter (RF=1): that applies directly on the owner;
// this REPLICATES through Raft on the leader, acking only on durable quorum commit.
class ReplicatedWriteRouter {
public:
    ReplicatedWriteRouter(const VShardDirectory& dir, ProposeSink& local, NodeTransport& client)
        : dir_(dir), local_(local), client_(client) {}

    // Route + replicate. Resolves when every VShard-leader group has durably committed
    // on quorum. Throws if any series' VShard is unassigned (before dispatch), if a
    // leader is stale (a group returned not-leader), or on a transport error.
    seastar::future<> write(WriteBatch batch);

private:
    const VShardDirectory& dir_;
    ProposeSink& local_;
    NodeTransport& client_;
};

}  // namespace timestar::data
