#pragma once

#include "data_plane.hpp"
#include "vshard_directory.hpp"

#include <seastar/core/future.hh>
#include <vector>

namespace timestar::data {

// Routes any-node writes to the VShard owner (the plan's "route any-node writes
// to VShard leaders"): groups points by owner, applies the local group directly,
// and forwards each remote group to its owner in one batched RPC. A point whose
// VShard is unassigned is a routing error -- surfaced, never silently dropped or
// applied locally (which would violate the RF=1==single-node contract).
class WriteRouter {
public:
    WriteRouter(const VShardDirectory& dir, LocalStore& local, DataPlaneClient& client)
        : dir_(dir), local_(local), client_(client) {}

    // Route a batch. Resolves when every group (local + forwarded) has been
    // durably accepted by its owner. Rejection contract:
    //   - If ANY point is unassigned, the batch is rejected BEFORE any dispatch
    //     (throws, atomically nothing written) -- retry after the map advances.
    //   - Once dispatch starts, all groups run concurrently and are all awaited;
    //     if one owner fails, the first error is propagated but groups that
    //     already succeeded stay applied (no cross-node atomic commit). Retries
    //     are safe under global last-write-wins for identical points.
    seastar::future<> write(std::vector<DataPoint> points);

private:
    const VShardDirectory& dir_;
    LocalStore& local_;
    DataPlaneClient& client_;
};

}  // namespace timestar::data
