#include "replicated_write_router.hpp"

#include "../../core/placement_table.hpp"  // virtualShard

#include <map>
#include <seastar/core/coroutine.hh>
#include <stdexcept>
#include <utility>

namespace timestar::data {

seastar::future<> ReplicatedWriteRouter::write(WriteBatch batch) {
    // Group by VShard-leader node. Resolve every leader BEFORE dispatch: a single
    // unassigned VShard rejects the whole batch (no partial replication).
    std::map<NodeId, WriteBatch> byLeader;
    for (auto& s : batch.series) {
        const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(s.seriesKey));
        const NodeId leader = dir_.ownerOf(vs);  // placement primary = leader hint
        if (leader == kNoNode)
            throw std::runtime_error("ReplicatedWriteRouter: VShard unassigned for series");
        WriteBatch& dest = byLeader[leader];
        dest.schemaVersion = batch.schemaVersion;
        dest.series.push_back(std::move(s));
    }

    // Dispatch: local groups through this node's Raft propose sink, remote groups via
    // proposeWrite to the leader. Start all concurrently.
    const NodeId self = dir_.self();
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byLeader.size());
    for (auto& [leader, group] : byLeader) {
        if (leader == self)
            pending.push_back(local_.proposeBatch(std::move(group)));
        else
            pending.push_back(client_.proposeWrite(leader, std::move(group)));
    }

    // Await ALL (never abandon an in-flight replication), collect the first error and
    // whether every group committed. A not-leader (false) fails the whole write so the
    // caller retries against the current leader/map -- never a silent partial commit.
    bool allCommitted = true;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            const bool committed = co_await std::move(f);
            allCommitted = allCommitted && committed;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    if (!allCommitted)
        throw std::runtime_error("ReplicatedWriteRouter: a VShard leader was stale (retry)");
}

}  // namespace timestar::data
