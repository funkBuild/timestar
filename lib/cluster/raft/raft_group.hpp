#pragma once

#include "raft_driver.hpp"
#include "raft_node.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <string>

namespace timestar::raft {

// One replicated group (one VShard) running on one core: a deterministic
// RaftNode wrapped by the async Seastar driver loop. It owns the Ready contract
// end to end -- persist (snapshot, hardState, entries) durably, THEN send
// messages, THEN apply committed output, THEN advance -- and serializes every
// node mutation so no input interleaves a ready()/advance() cycle.
class RaftGroup {
public:
    RaftGroup(uint16_t groupId, RaftNode node, RaftPersistence& persistence, RaftTransport& transport,
              RaftStateMachine& sm)
        : groupId_(groupId), node_(std::move(node)), persistence_(persistence), transport_(transport), sm_(sm) {}

    // Inputs. Each mutates the node then drains its Ready under the group lock.
    seastar::future<> step(Message m);
    // Forwarded to RaftNode::tick -- `passes` > 1 credits hibernation-skipped intervals.
    seastar::future<> tick(unsigned passes = 1);
    // An entry larger than this can be appended and COMMITTED and then never delivered
    // to a follower -- the transport refuses it, the follower can never catch up, and the
    // group is permanently one replica short with the offending entry already durable.
    // Failing the write CLOSED at propose is strictly cheaper than discovering that
    // afterwards, and it is where the caller can still be told (write-scaleout 5 review,
    // F3b). Sized under the transport's send mirror to leave room for the envelope header
    // and the AppendEntries framing around the entry -- the one payload bound every
    // producer shares (raft_types.hpp).
    static constexpr size_t kMaxProposalBytes = kMaxRaftPayloadBytes;

    seastar::future<bool> propose(std::string data);
    // Propose `data` and resolve only once THIS entry is committed AND applied on
    // this node -- the commit/apply acknowledgement `propose()` does not give.
    // Returns true when applied while continuously leader (a durable, quorum-
    // committed write). Returns false if this node is not the leader at propose
    // time (the caller redirects to the leader). Throws "propose: leadership lost
    // before commit" if leadership is lost before the entry applies -- the entry
    // may or may not have committed, so the caller retries the whole idempotent
    // batch (LWW re-application is harmless; an un-acked write is never lost).
    seastar::future<bool> proposeAndAwaitApplied(std::string data);
    // ... bounded by `deadline` (std::nullopt == unbounded, the overload above).
    //
    // WHY A WAITER NEEDS A DEADLINE AT ALL: the waiter is resolved by drainReady, and
    // drainReady only runs when something drives this group. A leader that has LOST
    // QUORUM -- two of three replicas down or partitioned -- keeps ticking but can never
    // commit, and with checkQuorum off it never steps down either, so nothing ever
    // resolves or fails the waiter. It suspends FOREVER. (The data plane leaves checkQuorum
    // OFF -- debt D-9/D-29 -- so this deadline is the ONLY escape there today. Even with the
    // flag on it would be a belt rather than a replacement: it bounds the CONDITION, in
    // units of one election timeout, while this bounds each WAITER, and a caller passing
    // nullopt still gets an unbounded wait.) Every resource the caller holds
    // for the duration (the coordinator's in-flight-byte charge, an inbound RPC slot)
    // is held forever with it, and a between-attempts deadline one layer up cannot help:
    // the attempt never returns to be timed out. RF=3 with two nodes down must FAIL the
    // write, not hang -- that is the fail-closed contract.
    //
    // On expiry this throws seastar::timed_out_error. The outcome is genuinely AMBIGUOUS
    // (the entry is appended here and a later quorum may still commit it), so callers
    // must classify it as retryable-ambiguous, exactly like a transport failure; LWW
    // re-application makes the retry harmless (see cluster/data/write_errors.hpp).
    //
    // The entry's waiter is deliberately LEFT in applyWaiters_ rather than erased:
    // removing it would race drainReady, which walks and resolves that list under the
    // group lock while this coroutine is suspended outside it. Instead the underlying
    // future is kept alive by with_timeout and its result discarded, so a later apply
    // resolves a promise that is still valid. Waiters that will never resolve are
    // reclaimed when leadership is lost (releaseApplyWaiters fails them all), which is
    // why the data plane also enables checkQuorum -- a partitioned leader steps down
    // within an election timeout and drains them.
    seastar::future<bool> proposeAndAwaitApplied(std::string data,
                                                 std::optional<seastar::lowres_clock::time_point> deadline);
    seastar::future<> campaign();
    seastar::future<bool> proposeConfChange(std::vector<NodeId> voters, std::vector<NodeId> learners);
    // true iff a transfer was actually ARMED by this call (debt D-24); see
    // RaftNode::transferLeadership for the early returns that answer false.
    //
    // `armed`, if given, is written SYNCHRONOUSLY at the moment the core answers --
    // before the Ready drain that follows it, which persists and sends and can therefore
    // THROW. A returned future carries either a value or an exception and never both, so
    // a drain failure would otherwise lose the fact that the transfer IS armed: the group
    // refuses proposals for the whole abandon window while the caller records nothing,
    // which is D-24's inflation with the sign flipped (an undercount, and a target whose
    // deficit was never charged). A caller that keeps a counter and catches exceptions
    // must account from THIS, not from the return value.
    seastar::future<bool> transferLeadership(NodeId target, bool* armed = nullptr);
    // Trigger a state-machine snapshot compaction up to `upto` with the given
    // opaque payload (the caller produced it from its state machine).
    seastar::future<> compact(LogIndex upto, std::string snapshotData);

    // Linearizable read barrier: resolves with a ReadIndex once a quorum has
    // confirmed current-term leadership AND this node has applied through that
    // index. The caller may then read committed state (e.g. the group-0 map)
    // linearizably. Fails if this node is not the leader (redirect to the leader).
    seastar::future<LogIndex> readBarrier();

    // Resolve once THIS node has applied through `index`, regardless of role.
    // Unlike proposeAndAwaitApplied it neither requires nor tracks leadership: a
    // follower or non-voting read replica uses it to wait for replication to reach
    // a leader-confirmed ReadIndex before serving a linearizable replica read, and
    // for read-your-writes it waits for a session token's index. It never fails on
    // a role change -- a replica that never catches up is a failed read the caller
    // bounds with a timeout, not a hang. Resolves immediately if already applied.
    seastar::future<> waitApplied(LogIndex index);

    // Highest log index this node has applied to its state machine (any role) --
    // the freshness signal a bounded-staleness replica read reports and compares.
    LogIndex appliedIndex() const { return appliedIndex_; }

    // Observers (safe to read between async operations on the same core).
    uint16_t groupId() const { return groupId_; }
    Role role() const { return node_.role(); }
    bool isLeader() const { return node_.isLeader(); }
    Term currentTerm() const { return node_.currentTerm(); }
    NodeId leader() const { return node_.leader(); }
    bool transferInFlight() const { return node_.transferInFlight(); }
    LogIndex commitIndex() const { return node_.commitIndex(); }
    // Highest index this leader knows replicated on `peer` (M5 move catchUp signal).
    LogIndex matchIndexOf(NodeId peer) const { return node_.matchIndexOf(peer); }
    // Ticks since `peer` last replied to us in this term (RaftNode::kNeverAcked if
    // never). The balancer's liveness gate; see ShardRaftPlane::rebalance.
    uint64_t ticksSinceAck(NodeId peer) const { return node_.ticksSinceAck(peer); }
    unsigned heartbeatTimeout() const { return node_.heartbeatTimeout(); }
    const RaftNode& node() const { return node_; }

private:
    // The ready/persist/send/apply/advance loop. MUST run under the group lock so
    // no step()/tick()/propose() interleaves a ready()..advance() pair.
    seastar::future<> drainReady();

    // Resolve any read barriers whose ReadIndex this node has now applied through.
    void releaseReadBarriers();
    // Resolve any proposeAndAwaitApplied waiters whose entry this node has now
    // applied; fail all of them on leadership loss (same contract as read barriers).
    void releaseApplyWaiters();
    // Resolve any waitApplied waiters this node has now applied through (any role;
    // never failed on role change).
    void releaseAppliedWaiters();

    uint16_t groupId_;
    RaftNode node_;
    RaftPersistence& persistence_;
    RaftTransport& transport_;
    RaftStateMachine& sm_;
    // Serializes all node mutation on this core. ALWAYS taken with
    // `seastar::get_units` inside the method's OWN coroutine body -- never with
    // `with_semaphore(lambda)`, whose closure dies at the lambda's first suspension
    // while the coroutine frame is still pointing into it (rule at the top of
    // raft_group.cpp; debt D-33).
    seastar::semaphore lock_{1};

    // Read-barrier tracking.
    uint64_t appliedIndex_ = 0;  // highest entry index applied to the SM
    uint64_t nextReadCtx_ = 1;
    std::map<uint64_t, LogIndex> confirmedReads_;                 // ctx -> ReadIndex (awaiting apply)
    std::map<uint64_t, seastar::promise<LogIndex>> readWaiters_;  // ctx -> caller promise

    // proposeAndAwaitApplied tracking: (entry index -> caller promise). Resolved
    // true once appliedIndex_ >= index while leader; failed on leadership loss.
    std::vector<std::pair<LogIndex, seastar::promise<bool>>> applyWaiters_;

    // waitApplied tracking: (index -> caller promise). Resolved (value) once
    // appliedIndex_ >= index, regardless of role; never failed on role change.
    std::vector<std::pair<LogIndex, seastar::promise<>>> appliedWaiters_;
};

}  // namespace timestar::raft
