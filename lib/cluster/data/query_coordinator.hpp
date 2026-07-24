#pragma once

#include "data_plane.hpp"
#include "vshard_directory.hpp"

#include <seastar/core/future.hh>

namespace timestar::data {

// Scatter-gather query coordinator (the plan's "remote storage-side query
// discovery and partial aggregation"). Fans a query out to every node that owns
// a VShard, gathers each node's partial (raw points or per-series AggStates), and
// merges them. Because RF=1 partitions series disjointly across owner nodes, the
// merged result is exactly what a single node holding all the data would return
// -- the Phase-4 gate property. The merge reuses the associative AggState so the
// answer is placement-independent.
class QueryCoordinator {
public:
    QueryCoordinator(const VShardDirectory& dir, LocalStore& local, DataPlaneClient& client)
        : dir_(dir), local_(local), client_(client) {}

    // Execute a query across the cluster and return the merged partial (raw:
    // time-sorted union; aggregated: per-series merged AggStates).
    seastar::future<QueryPartial> query(QuerySpec spec);

private:
    const VShardDirectory& dir_;
    LocalStore& local_;
    DataPlaneClient& client_;
};

}  // namespace timestar::data
