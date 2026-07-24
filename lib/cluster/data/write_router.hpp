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
    // durably accepted by its owner. Throws std::runtime_error if any point is
    // unassigned (the whole batch is rejected -- the caller retries after the map
    // advances rather than getting a partial write).
    seastar::future<> write(std::vector<DataPoint> points);

private:
    const VShardDirectory& dir_;
    LocalStore& local_;
    DataPlaneClient& client_;
};

}  // namespace timestar::data
