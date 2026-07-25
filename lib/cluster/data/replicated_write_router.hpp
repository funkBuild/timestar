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
// v1 LIMITATION (documented, not a bug): the hint is the placement PRIMARY, and a
// not-leader `false` carries NO hint of the actual leader back to the caller. This is
// safe only under the model invariant "placement primary == Raft leader" (see
// vshard_directory.hpp): a primary that FAILS is replaced by a placement/epoch change
// the caller re-reads, and proposing to a dead primary throws (transport error, also
// retried). The narrow uncovered case is a primary that is alive but no longer leader
// with NO map change -- the caller would re-route to the same stale primary and spin.
// Propagating the real leader on `false` (a leader-hint field) is a later refinement.
//
// Contrast with the M2 NodeWriteRouter (RF=1): that applies directly on the owner;
// this REPLICATES through Raft on the leader, acking only on durable quorum commit.
class ReplicatedBatchWriteRouter {
public:
    ReplicatedBatchWriteRouter(const VShardDirectory& dir, ProposeSink& local, NodeTransport& client,
                               const LeaderResolver& leaders)
        : dir_(dir), local_(local), client_(client), leaders_(leaders) {}

    // Route + replicate. Resolves when every VShard-leader group has durably committed
    // on quorum. Throws if any series' VShard is unassigned (before dispatch), if a
    // leader is stale (a group returned not-leader), or on a transport error.
    seastar::future<> write(WriteBatch batch);
    // Same, for a batch the caller already split by VShard (write-scaleout 2b): the
    // production path splits ONCE at ingress and every layer below only re-buckets
    // those groups. Identical semantics; the WriteBatch overload just splits first.
    seastar::future<> write(VShardBatches groups);

private:
    const VShardDirectory& dir_;
    ProposeSink& local_;
    NodeTransport& client_;
    const LeaderResolver& leaders_;
};

}  // namespace timestar::data
