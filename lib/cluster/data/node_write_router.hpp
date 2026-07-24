#pragma once

#include "node_store.hpp"
#include "vshard_directory.hpp"
#include "write_record.hpp"

#include <seastar/core/future.hh>

namespace timestar::data {

// The enriched (lossless WriteBatch) analogue of WriteRouter (integration plan
// F.5). Routes any-node writes to each series' VShard owner: groups the batch's
// WriteSeries by owner, applies the local group directly through the NodeStore,
// and forwards each remote group as one batched WriteBatch RPC via NodeTransport.
//
// The routing INVARIANTS are identical to WriteRouter (that is the point of F.5 --
// one contract, two command types):
//   - resolve every series' owner BEFORE any dispatch; a series whose VShard is
//     unassigned rejects the whole batch atomically (nothing written), never a
//     silent drop or a local fallback (which would break RF=1 == single-node).
//   - once dispatch starts, all groups run concurrently and are all awaited; on a
//     mid-fan-out failure the first error propagates but already-succeeded groups
//     stay applied (no cross-node atomic commit). Safe under global last-write-wins
//     for identical points; a retry with DIFFERENT values for the same
//     (series, timestamp) must know the earlier groups were applied.
class NodeWriteRouter {
public:
    NodeWriteRouter(const VShardDirectory& dir, NodeStore& local, NodeTransport& client)
        : dir_(dir), local_(local), client_(client) {}

    // Route a WriteBatch. Resolves when every group (local + forwarded) has been
    // durably accepted by its owner.
    seastar::future<> write(WriteBatch batch);

private:
    const VShardDirectory& dir_;
    NodeStore& local_;
    NodeTransport& client_;
};

}  // namespace timestar::data
