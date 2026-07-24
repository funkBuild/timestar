#pragma once

#include "../raft/raft_group.hpp"
#include "data_command.hpp"
#include "data_state_machine.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace timestar::data {

// The leader-facing operations on one replicated VShard, as an interface so the
// routing layer (ReplicatedWriteRouter/ReplicatedQueryCoordinator) can be tested
// without a live Raft stack. ReplicatedVShard is the production implementation; a
// forwarder-to-remote-leader is the other (Phase-4 RPC style).
class VShardLeader {
public:
    virtual ~VShardLeader() = default;
    // true = committed by durable quorum + applied here; false = not leader
    // (redirect); throws on leadership loss (retry the whole idempotent batch).
    // Takes the command BY VALUE so an implementation may be a coroutine that
    // holds it across a suspension without the caller's temporary dangling.
    virtual seastar::future<bool> write(DataCommand cmd) = 0;
    // Linearizable leader read; throws "not leader" (redirect).
    virtual seastar::future<QueryPartial> linearizableRead(QuerySpec spec) = 0;
};

// The production data path for ONE replicated VShard: a Raft group whose applied
// state IS a DataStateMachine. This is the RF>=1 replacement for the Phase-4
// direct local apply -- every write goes through consensus (so all replicas
// converge) and every read is linearizable behind a ReadIndex barrier.
//
// It holds no state of its own; it just binds the group to its state machine and
// enforces the leader-only contract. Cross-node routing (finding which node leads
// a VShard) lives above this, in the write/query coordinator, exactly as in
// Phase 4 -- this brick is what a request does once it reaches the owning node.
class ReplicatedVShard : public VShardLeader {
public:
    ReplicatedVShard(raft::RaftGroup& group, DataStateMachine& sm) : group_(group), sm_(sm) {}

    // Replicate a data command and resolve only once it is committed by a durable
    // quorum AND applied here. Returns false if this node is not the leader (the
    // caller redirects to the leader). Throws on leadership loss before commit --
    // the caller retries the whole batch, which is idempotent (LWW), so an
    // un-acknowledged write is never lost and a re-applied one never duplicates.
    seastar::future<bool> write(DataCommand cmd) override {
        return group_.proposeAndAwaitApplied(encodeDataCommand(cmd));
    }

    // Linearizable leader read: take a ReadIndex barrier (confirms current-term
    // leadership by quorum and waits until this node has applied through the
    // barrier), then read the applied state. Throws "readBarrier: not leader" if
    // this node is not the leader -- the caller redirects. The result reflects
    // every write acknowledged before the read began.
    seastar::future<QueryPartial> linearizableRead(QuerySpec spec) override {
        // Bind the group/SM to frame-local references BEFORE suspending: this
        // coroutine must not dereference `this` after the readBarrier co_await, so
        // that an in-flight read survives the facade being destroyed mid-read
        // (e.g. VShard decommission). The referenced RaftGroup/DataStateMachine
        // outlive the facade and the read regardless -- the same coroutine
        // lifetime rule the HTTP query path follows.
        auto& group = group_;
        auto& sm = sm_;
        co_await group.readBarrier();
        co_return sm.query(spec);
    }

    // Read-envelope metadata every query result carries (the barrier future modes
    // need; §"Read consistency"): the VShard's group, the serving term, and the
    // applied index the read was served at.
    uint16_t groupId() const { return group_.groupId(); }
    raft::Term term() const { return group_.currentTerm(); }
    uint64_t appliedIndex() const { return sm_.appliedIndex(); }

    bool isLeader() const { return group_.isLeader(); }
    raft::NodeId leader() const { return group_.leader(); }

private:
    raft::RaftGroup& group_;
    DataStateMachine& sm_;
};

}  // namespace timestar::data
