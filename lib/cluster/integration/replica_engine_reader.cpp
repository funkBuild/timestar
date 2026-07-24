#include "replica_engine_reader.hpp"

#include <seastar/core/coroutine.hh>

namespace timestar::cluster {

seastar::future<ReplicaEngineReader::Result> ReplicaEngineReader::read(data::NodeQueryRequest req,
                                                                       data::ReadConsistency mode,
                                                                       data::ReadEnvelope token,
                                                                       uint64_t maxLagIndex) {
    // Bind everything reached through `this` to frame-locals BEFORE any co_await, so an
    // in-flight read survives the facade being destroyed (the coroutine-lifetime rule).
    auto& group = group_;
    auto& store = store_;
    const uint16_t vshard = vshard_;
    auto leaderReadIndex = leaderReadIndex_;
    auto leaderCommit = leaderCommit_;

    switch (mode) {
        case data::ReadConsistency::Linearizable: {
            raft::LogIndex ri = co_await leaderReadIndex();  // throws -> reject on partition
            co_await group.waitApplied(ri);                  // never serve below the barrier
            break;
        }
        case data::ReadConsistency::Session: {
            // Read-your-writes / monotonic: never serve below the token index.
            co_await group.waitApplied(token.appliedIndex);
            break;
        }
        case data::ReadConsistency::BoundedStaleness: {
            raft::LogIndex leaderCommitIdx = co_await leaderCommit();  // throws -> reject
            raft::LogIndex applied = group.appliedIndex();
            // Overflow-safe lag check (a huge maxLagIndex sentinel must not wrap).
            if (leaderCommitIdx > applied && (leaderCommitIdx - applied) > maxLagIndex)
                throw data::ReplicaReadUnavailable("replica read: staleness exceeds bound");
            break;  // within bound: serve local applied state
        }
    }

    // Restrict the answer to THIS reader's VShard: a replica hosts specific VShards and
    // must not answer for series outside them (the coordinator sums completeness per
    // VShard, and an unrestricted read would double-count under RF>1).
    req.vshards = {vshard};
    data::NodeQueryPartial partial = co_await store.queryLocal(std::move(req));
    co_return Result{std::move(partial),
                     data::ReadEnvelope{group.groupId(), group.currentTerm(), group.appliedIndex()}};
}

}  // namespace timestar::cluster
