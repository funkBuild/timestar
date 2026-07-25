#include "replicated_write_router.hpp"

#include "../../core/placement_table.hpp"  // virtualShard

#include <map>
#include <seastar/core/coroutine.hh>
#include <stdexcept>
#include <utility>

namespace timestar::data {

seastar::future<> ReplicatedBatchWriteRouter::write(WriteBatch batch) {
    // Entry for callers that still hold an unsplit batch (RF=3 tests, the RF=1-shaped
    // seams). The production path arrives already split -- see the overload.
    return write(splitByVShard(std::move(batch)));
}

seastar::future<> ReplicatedBatchWriteRouter::write(VShardBatches groups) {
    // Bucket the (already per-VShard) groups by their leader NODE. Resolve every
    // leader BEFORE dispatch: a single unassigned VShard rejects the whole batch (no
    // partial replication).
    //
    // The groups are the same objects the caller split at ingress, so this is a bucket
    // of group handles -- the series inside them are not re-hashed and, for the local
    // leader, not moved at all (write-scaleout 2a/2b). Only REMOTE leaders need their
    // groups merged, because one forwarded propose is one WriteBatch on the wire.
    const NodeId self = dir_.self();
    VShardBatches localGroups;
    std::map<NodeId, VShardBatches> byLeader;
    for (auto& g : groups) {
        const uint16_t vs = g.first;
        if (dir_.ownerOf(vs) == kNoNode)
            throw std::runtime_error("ReplicatedBatchWriteRouter: VShard unassigned for series");
        // Route to the CURRENT Raft leader (follows failover); fall back to the
        // placement primary when the leader is not locally known (a stale primary
        // then returns not-leader -> retry).
        NodeId leader = leaders_.leaderOf(vs);
        if (leader == kNoNode)
            leader = dir_.ownerOf(vs);
        if (leader == self)
            localGroups.push_back(std::move(g));
        else
            byLeader[leader].push_back(std::move(g));
    }

    // Dispatch: local groups through this node's Raft propose sink, remote groups via
    // proposeWrite to the leader. Start all concurrently.
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byLeader.size() + 1);
    if (!localGroups.empty())
        pending.push_back(local_.proposeVShardBatches(std::move(localGroups)));
    for (auto& [leader, lgroups] : byLeader)
        pending.push_back(client_.proposeWrite(leader, mergeVShardBatches(std::move(lgroups))));

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
        throw std::runtime_error("ReplicatedBatchWriteRouter: a VShard leader was stale (retry)");
}

}  // namespace timestar::data
