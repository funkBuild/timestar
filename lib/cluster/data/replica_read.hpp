#pragma once

#include "../raft/raft_types.hpp"

#include <cstdint>
#include <stdexcept>

namespace timestar::data {

// A replica read that could not be served at the requested consistency -- the
// serving replica could not confirm freshness (partitioned from the leader), or
// staleness exceeded the bound. Fail-closed: the coordinator retries elsewhere or
// fails the query; it must NEVER be downgraded to a silent stale answer.
class ReplicaReadUnavailable : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// How fresh a replica read must be before it may be served locally.
enum class ReadConsistency : uint8_t {
    // Freshest: obtain a quorum-confirmed ReadIndex from the leader, then serve
    // once THIS replica has applied through it. Linearizable from any replica.
    Linearizable = 0,
    // Read-your-writes / monotonic: serve once this replica has applied through
    // the session token's index (a prior write/read the client observed). No
    // leader round -- cheap once caught up.
    Session = 1,
    // Serve local applied state if it is within `maxLagIndex` of the leader's
    // current commit; reject if the leader is unreachable or the bound is
    // exceeded. Explicitly NON-linearizable and it must never be substituted where
    // Linearizable is required. The bound is relative to the REACHABLE leader's
    // commit: under a minority-leader partition (an old leader that has not stepped
    // down, sharing the minority with this replica) that reference is frozen, so
    // the replica can serve data arbitrarily stale w.r.t. the majority. Enabling
    // Raft CheckQuorum (so a leader without quorum steps down) tightens this;
    // strict TIME-bounded staleness needs the deferred clock-error decision.
    BoundedStaleness = 2,
};

// The barrier metadata every replica-read result carries (§"Read consistency":
// "every query result envelope carries the VShard, term, and applied index it was
// served at"). It IS the session token: feed it back on the next Session read for
// read-your-writes / monotonic reads.
struct ReadEnvelope {
    uint16_t vshard = 0;
    raft::Term term = 0;
    uint64_t appliedIndex = 0;
    friend bool operator==(const ReadEnvelope&, const ReadEnvelope&) = default;
};

}  // namespace timestar::data
