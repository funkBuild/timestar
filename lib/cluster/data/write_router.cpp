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
    // all remote forwards, then await them (fan-out concurrency).
    const NodeId self = dir_.self();
    std::vector<seastar::future<>> pending;
    for (auto& [owner, group] : byOwner) {
        if (owner == self)
            pending.push_back(local_.applyWrites(std::move(group)));
        else
            pending.push_back(client_.forwardWrites(owner, std::move(group)));
    }
    for (auto& f : pending)
        co_await std::move(f);
}

}  // namespace timestar::data
