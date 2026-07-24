#include "node_write_router.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
#include <stdexcept>
#include <utility>

namespace timestar::data {

seastar::future<> NodeWriteRouter::write(WriteBatch batch) {
    // Group by owner node first, so each destination gets ONE batched WriteBatch.
    // Resolve every owner and validate the whole batch BEFORE dispatching anything:
    // a single unassigned VShard rejects the batch (no partial write). Each series'
    // seriesKey is hashed once here (the plan's noted hot-path cost); the owner is
    // hash -> VShard -> node.
    std::map<NodeId, WriteBatch> byOwner;
    for (auto& s : batch.series) {
        const SeriesId128 id = SeriesId128::fromSeriesKey(s.seriesKey);
        const NodeId owner = dir_.ownerOfSeries(id);
        if (owner == kNoNode)
            throw std::runtime_error("NodeWriteRouter: VShard unassigned for series");
        WriteBatch& dest = byOwner[owner];
        dest.schemaVersion = batch.schemaVersion;  // carried per destination
        dest.series.push_back(std::move(s));
    }

    // Dispatch: the local group directly through the NodeStore, remote groups via
    // one WriteBatch RPC each. Start all groups concurrently.
    const NodeId self = dir_.self();
    std::vector<seastar::future<>> pending;
    pending.reserve(byOwner.size());
    for (auto& [owner, group] : byOwner) {
        if (owner == self)
            pending.push_back(local_.applyWrites(std::move(group)));
        else
            pending.push_back(client_.forwardWriteBatch(owner, std::move(group)));
    }

    // Await ALL dispatches (never abandon an in-flight forward), then propagate the
    // first failure. Already-succeeded groups stay applied -- no cross-node atomic
    // commit (see the header's retry note).
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            co_await std::move(f);
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
}

}  // namespace timestar::data
