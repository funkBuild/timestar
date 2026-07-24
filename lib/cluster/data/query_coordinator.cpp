#include "query_coordinator.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <vector>

namespace timestar::data {

seastar::future<QueryPartial> QueryCoordinator::query(QuerySpec spec) {
    // Discover the fan-out set: every node that owns a VShard. (A later phase can
    // narrow this using the query's series/tag filter against the directory; the
    // full fan-out is always correct, just less selective.)
    const std::set<NodeId> targets = dir_.ownerNodes();
    const NodeId self = dir_.self();

    // Start every partial concurrently, remembering which target each belongs to
    // only for ordering-independence (the merge is commutative).
    std::vector<seastar::future<QueryPartial>> pending;
    pending.reserve(targets.size());
    for (NodeId t : targets) {
        if (t == self)
            pending.push_back(local_.queryLocal(spec));
        else
            pending.push_back(client_.queryRemote(t, spec));
    }

    QueryPartial merged;
    for (auto& f : pending) {
        QueryPartial part = co_await std::move(f);
        // Raw: concatenate; final sort below. Aggregated: merge per-series.
        merged.raw.insert(merged.raw.end(), std::make_move_iterator(part.raw.begin()),
                          std::make_move_iterator(part.raw.end()));
        for (auto& [series, st] : part.perSeries)
            merged.perSeries[series].merge(st);
    }

    // Deterministic order for raw results: by (timestamp, series) so the merged
    // answer is placement-independent and matches the single-node ordering.
    std::sort(merged.raw.begin(), merged.raw.end(), [](const DataPoint& a, const DataPoint& b) {
        if (a.timestamp != b.timestamp)
            return a.timestamp < b.timestamp;
        return a.series < b.series;
    });
    co_return merged;
}

}  // namespace timestar::data
