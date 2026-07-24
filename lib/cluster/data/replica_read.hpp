#pragma once

#include "../raft/raft_group.hpp"
#include "data_plane.hpp"  // QuerySpec, QueryPartial
#include "data_state_machine.hpp"

#include <cstdint>
#include <functional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
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

struct ReplicaReadRequest {
    QuerySpec spec;
    ReadConsistency mode = ReadConsistency::Linearizable;
    ReadEnvelope token{};      // Session: wait until applied >= token.appliedIndex
    uint64_t maxLagIndex = 0;  // BoundedStaleness: max (leaderCommit - localApplied)
};

struct ReplicaReadResult {
    QueryPartial partial;
    ReadEnvelope envelope;  // freshness this result was served at (== next session token)
};

// Serves reads from ONE replica of a VShard. ReplicaVShard is the production impl;
// the interface lets the selector/coordinator be tested with mocks.
class ReplicaReader {
public:
    virtual ~ReplicaReader() = default;
    virtual seastar::future<ReplicaReadResult> read(ReplicaReadRequest req) = 0;
    // Current freshness of this replica without serving (for selection/eligibility).
    virtual ReadEnvelope envelope() const = 0;
};

// A replica of a VShard (leader, follower, or non-voting read replica) that serves
// reads from its locally-applied state at the requested consistency. The replica
// applies the same committed log, so its state is correct as of its applied index;
// the mode decides how fresh that must be before serving.
//
// A linearizable replica read needs NO new Raft protocol: obtain a quorum-confirmed
// ReadIndex from the current leader (the leader already has readBarrier()), then
// wait until THIS replica has applied through it, then serve locally. The
// "reach the leader" step is injected -- a local leader group in tests, an RPC to
// the leader node in production -- and THROWS if no leader can confirm, so a
// partitioned replica rejects rather than serving stale (the gate's partition
// guard).
class ReplicaVShard : public ReplicaReader {
public:
    // Confirm a linearizable ReadIndex at the current leader. Throws if no leader
    // is reachable/confirmable (partition) -> the read is rejected.
    using LeaderReadIndexFn = std::function<seastar::future<raft::LogIndex>()>;
    // Cheaply fetch the leader's current commit index (no quorum round) for
    // bounded-staleness freshness. Throws if the leader is unreachable.
    using LeaderCommitFn = std::function<seastar::future<raft::LogIndex>()>;

    ReplicaVShard(raft::RaftGroup& group, DataStateMachine& sm, LeaderReadIndexFn leaderReadIndex,
                  LeaderCommitFn leaderCommit)
        : group_(group),
          sm_(sm),
          leaderReadIndex_(std::move(leaderReadIndex)),
          leaderCommit_(std::move(leaderCommit)) {}

    seastar::future<ReplicaReadResult> read(ReplicaReadRequest req) override {
        // Bind everything reached through `this` to frame-locals BEFORE any
        // co_await, so an in-flight read survives the facade being destroyed
        // (the coroutine-lifetime rule; see replicated_vshard.hpp).
        auto& group = group_;
        auto& sm = sm_;
        auto leaderReadIndex = leaderReadIndex_;
        auto leaderCommit = leaderCommit_;

        switch (req.mode) {
            case ReadConsistency::Linearizable: {
                raft::LogIndex ri = co_await leaderReadIndex();  // throws -> reject on partition
                co_await group.waitApplied(ri);                  // never serve below the barrier
                break;
            }
            case ReadConsistency::Session: {
                // Read-your-writes / monotonic: never serve below the token index.
                co_await group.waitApplied(req.token.appliedIndex);
                break;
            }
            case ReadConsistency::BoundedStaleness: {
                raft::LogIndex leaderCommitIdx = co_await leaderCommit();  // throws -> reject
                raft::LogIndex applied = group.appliedIndex();
                // Overflow-safe lag check (a huge maxLagIndex sentinel must not wrap).
                if (leaderCommitIdx > applied && (leaderCommitIdx - applied) > req.maxLagIndex)
                    throw ReplicaReadUnavailable("replica read: staleness exceeds bound");
                break;  // within bound: serve local applied state
            }
        }
        co_return ReplicaReadResult{sm.query(req.spec),
                                    ReadEnvelope{group.groupId(), group.currentTerm(), sm.appliedIndex()}};
    }

    ReadEnvelope envelope() const override { return {group_.groupId(), group_.currentTerm(), sm_.appliedIndex()}; }

private:
    raft::RaftGroup& group_;
    DataStateMachine& sm_;
    LeaderReadIndexFn leaderReadIndex_;
    LeaderCommitFn leaderCommit_;
};

}  // namespace timestar::data
