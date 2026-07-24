#include "write_router.hpp"

#include <map>
#include <seastar/core/coroutine.hh>
#include <stdexcept>

namespace timestar::data {

seastar::future<> WriteRouter::write(std::vector<DataPoint> points) {
    // Group by owner node first, so each destination gets ONE batched dispatch.
    // Validate the whole batch before dispatching anything: a single unassigned
    // point rejects the batch (no partial write).
    std::map<NodeId, std::vector<DataPoint>> byOwner;
    for (auto& p : points) {
        const NodeId owner = dir_.ownerOfSeries(p.series);
        if (owner == kNoNode)
            throw std::runtime_error("WriteRouter: VShard unassigned for series");
        byOwner[owner].push_back(std::move(p));
    }

    // Dispatch: the local group directly, remote groups via one RPC each. Start
    // all groups concurrently.
    const NodeId self = dir_.self();
    std::vector<seastar::future<>> pending;
    for (auto& [owner, group] : byOwner) {
        if (owner == self)
            pending.push_back(local_.applyWrites(std::move(group)));
        else
            pending.push_back(client_.forwardWrites(owner, std::move(group)));
    }
    // Await ALL dispatches (never abandon an in-flight forward), then propagate
    // the first failure. NOTE: on a mid-fan-out failure, groups that already
    // succeeded stay applied -- there is no cross-node atomic commit here. That is
    // safe under the global last-write-wins invariant (an identical retried point
    // overwrites), but a caller retrying a batch with DIFFERENT values for the
    // same (series, timestamp) must be aware the earlier groups were applied.
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
