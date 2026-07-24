#include "node_query_coordinator.hpp"

#include "../../core/vshard.hpp"  // VIRTUAL_SHARD_COUNT

#include <exception>
#include <seastar/core/coroutine.hh>
#include <vector>

namespace timestar::data {

namespace {
QueryResponse incompleteResponse(const std::string& reason) {
    QueryResponse r;
    r.success = false;
    r.errorCode = "QUERY_INCOMPLETE";
    r.errorMessage = reason;
    return r;
}
}  // namespace

seastar::future<QueryResponse> NodeQueryCoordinator::query(QueryRequest request) {
    // Completeness FIRST: an unfiltered scan spans every VShard, so any unassigned
    // VShard means the answer would be a silent partial. (A scope/series filter
    // could narrow this, like QueryCoordinator; full-scan completeness is the
    // conservative correct baseline.)
    for (uint16_t v = 0; v < timestar::VIRTUAL_SHARD_COUNT; ++v)
        if (dir_.ownerOf(v) == kNoNode)
            co_return incompleteResponse("query spans an unassigned VShard");

    // Snapshot the fan-out targets and epoch BEFORE the first await (the directory's
    // map may be move-reassigned by an epoch advance across a suspension).
    const std::set<NodeId> targets = dir_.ownerNodes();
    const NodeId self = dir_.self();
    const uint64_t epoch = dir_.epoch();

    // DEFERRED to M3 (RF>1 / migration), correct under static RF=1 today:
    //   - nq.vshards is left empty: each node answers from ALL its local series.
    //     Under RF=1 a node's Engine holds ONLY its owned VShards (disjoint
    //     placement), so the union is exactly the single-node set with no overlap.
    //     Once RF>1 or an in-flight move leaves replica/stale data on a node that is
    //     ALSO primary for other VShards, that node must be restricted to req.vshards
    //     or its extra data double-counts in the merge. queryLocal must honour
    //     req.vshards (vshard-restricted discovery) before that lands.
    //   - nq.mapEpoch is pinned but not yet fenced by the node; harmless while the
    //     completeness pre-check + disjoint placement hold, an epoch-fence is an M3
    //     reconfiguration concern.
    // Cross-node grouping also assumes a homogeneous cluster binary (groupKeyHash is
    // std::hash<string>, unseeded but stdlib/version-dependent).
    std::vector<seastar::future<NodeQueryPartial>> pending;
    pending.reserve(targets.size());
    for (NodeId t : targets) {
        NodeQueryRequest nq;
        nq.request = request;  // copy per target; `request` is moved into finalize below
        nq.mapEpoch = epoch;
        if (t == self)
            pending.push_back(local_.queryLocal(std::move(nq)));
        else
            pending.push_back(client_.queryNode(t, std::move(nq)));
    }

    // Await ALL partials (never abandon an in-flight future), unioning as we go.
    std::vector<PartialAggregationResult> allPartials;
    std::vector<timestar::http::SeriesResult> allNonNumeric;
    std::vector<std::string> incompleteReasons;
    uint64_t totalSeriesFound = 0;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            NodeQueryPartial part = co_await std::move(f);
            if (!part.incompleteReasons.empty()) {
                for (auto& r : part.incompleteReasons)
                    incompleteReasons.push_back(std::move(r));
                continue;  // fail-closed below; keep draining the rest
            }
            totalSeriesFound += part.seriesFound;
            allPartials.insert(allPartials.end(), std::make_move_iterator(part.partials.begin()),
                               std::make_move_iterator(part.partials.end()));
            allNonNumeric.insert(allNonNumeric.end(), std::make_move_iterator(part.nonNumeric.begin()),
                                 std::make_move_iterator(part.nonNumeric.end()));
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    if (!incompleteReasons.empty()) {
        std::string joined;
        for (const auto& r : incompleteReasons) {
            if (!joined.empty())
                joined += "; ";
            joined += r;
        }
        co_return incompleteResponse("a node could not answer: " + joined);
    }

    // Enforce maxSeriesCount over the UNION: each node passes its own cardinality
    // check in queryLocalPartials, but the cluster's total could still exceed the
    // limit that single-node executeQuery enforces. Same error shape/code.
    if (totalSeriesFound > http::HttpQueryHandler::maxSeriesCount()) {
        QueryResponse r;
        r.success = false;
        r.errorCode = "TOO_MANY_SERIES";
        r.errorMessage = "Too many series: " + std::to_string(totalSeriesFound) + " exceeds limit of " +
                         std::to_string(http::HttpQueryHandler::maxSeriesCount());
        co_return r;
    }

    // One merge+finalize over the union -- identical to the single-node multi-shard
    // path, so the cluster answer is the single-node answer.
    co_return co_await finalizer_.finalizeClusterPartials(std::move(request), std::move(allPartials),
                                                          std::move(allNonNumeric));
}

}  // namespace timestar::data
