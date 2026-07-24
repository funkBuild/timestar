#pragma once

#include "../data/node_query.hpp"
#include "../data/replica_read.hpp"  // ReadConsistency, ReadEnvelope, ReplicaReadUnavailable
#include "../raft/raft_group.hpp"
#include "engine_local_store.hpp"

#include <cstdint>
#include <functional>
#include <seastar/core/future.hh>

namespace timestar::cluster {

// Serves reads from ONE replica of a VShard over the REAL Engine (integration plan
// M4). This is the production analogue of the Phase-6 data::ReplicaVShard, which
// serves from the toy DataStateMachine's in-memory query; here the freshness gate is
// identical but the answer comes from EngineLocalStore::queryLocal, restricted to this
// reader's VShard (a replica must only answer for the VShards it hosts; the coordinator
// accounts completeness per VShard).
//
// A linearizable replica read needs NO new Raft protocol: confirm a quorum ReadIndex
// at the current leader (injected -- the local group's readBarrier when this replica
// leads, else an RPC to the leader), wait until THIS replica has applied through it,
// then serve locally. The leader-reach fn THROWS if no leader can confirm, so a
// partitioned replica rejects rather than serving stale.
class ReplicaEngineReader {
public:
    // Confirm a linearizable ReadIndex at the current leader (throws on partition).
    using LeaderReadIndexFn = std::function<seastar::future<raft::LogIndex>()>;
    // Cheaply fetch the leader's commit index for bounded-staleness (throws on partition).
    using LeaderCommitFn = std::function<seastar::future<raft::LogIndex>()>;

    ReplicaEngineReader(raft::RaftGroup& group, EngineLocalStore& store, uint16_t vshard,
                        LeaderReadIndexFn leaderReadIndex, LeaderCommitFn leaderCommit)
        : group_(group),
          store_(store),
          vshard_(vshard),
          leaderReadIndex_(std::move(leaderReadIndex)),
          leaderCommit_(std::move(leaderCommit)) {}

    struct Result {
        data::NodeQueryPartial partial;
        data::ReadEnvelope envelope;  // freshness served at (== next Session token)
    };

    // Serve `req` at `mode` freshness. `token` is used for Session; `maxLagIndex` for
    // BoundedStaleness. Throws (leader-reach failure or ReplicaReadUnavailable) rather
    // than ever serving below the requested freshness.
    seastar::future<Result> read(data::NodeQueryRequest req, data::ReadConsistency mode,
                                 data::ReadEnvelope token, uint64_t maxLagIndex);

    // Current freshness without serving (selection / eligibility).
    data::ReadEnvelope envelope() const {
        return {group_.groupId(), group_.currentTerm(), group_.appliedIndex()};
    }

private:
    raft::RaftGroup& group_;
    EngineLocalStore& store_;
    uint16_t vshard_;
    LeaderReadIndexFn leaderReadIndex_;
    LeaderCommitFn leaderCommit_;
};

}  // namespace timestar::cluster
