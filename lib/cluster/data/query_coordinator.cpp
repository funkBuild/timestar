#include "query_coordinator.hpp"

#include "../../core/vshard.hpp"  // VIRTUAL_SHARD_COUNT

#include <algorithm>
#include <exception>
#include <seastar/core/coroutine.hh>
#include <vector>

namespace timestar::data {

seastar::future<QueryPartial> QueryCoordinator::query(QuerySpec spec) {
    // Completeness FIRST (the plan's failure contract): every VShard that could
    // hold a matching series must be assigned, else the answer would be a silent
    // partial. Unfiltered scans require ALL VShards assigned; a series filter only
    // requires those series' VShards. Throwing QueryIncomplete keeps
    // "could-not-read" distinct from "genuinely empty".
    if (spec.series.empty()) {
        for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v)
            if (dir_.ownerOf(v) == kNoNode)
                throw QueryIncomplete("query spans an unassigned VShard");
    } else {
        for (const auto& s : spec.series)
            if (dir_.ownerOfSeries(s) == kNoNode)
                throw QueryIncomplete("query references a series in an unassigned VShard");
    }

    // Fan out to every owner node. (A later phase can narrow this using the series
    // filter; full fan-out is always correct, just less selective.)
    const std::set<NodeId> targets = dir_.ownerNodes();
    const NodeId self = dir_.self();
    std::vector<seastar::future<QueryPartial>> pending;
    pending.reserve(targets.size());
    for (NodeId t : targets) {
        if (t == self)
            pending.push_back(local_.queryLocal(spec));
        else
            pending.push_back(client_.queryRemote(t, spec));
    }

    // Await ALL partials (never abandon an in-flight future), then propagate the
    // first failure -- a node that could not answer fails the WHOLE query rather
    // than yielding a silent partial merge.
    QueryPartial merged;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            QueryPartial part = co_await std::move(f);
            merged.raw.insert(merged.raw.end(), std::make_move_iterator(part.raw.begin()),
                              std::make_move_iterator(part.raw.end()));
            for (auto& [series, st] : part.perSeries)
                merged.perSeries[series].merge(st);
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);

    // Deterministic TOTAL order for raw results: (timestamp, series, value), so
    // the merged answer is placement-independent even if a store held duplicate
    // (series, timestamp) keys (it should not, under LWW -- but the ordering must
    // not depend on that).
    std::sort(merged.raw.begin(), merged.raw.end(), [](const DataPoint& a, const DataPoint& b) {
        if (a.timestamp != b.timestamp)
            return a.timestamp < b.timestamp;
        if (!(a.series == b.series))
            return a.series < b.series;
        return a.value < b.value;
    });
    co_return merged;
}

}  // namespace timestar::data
