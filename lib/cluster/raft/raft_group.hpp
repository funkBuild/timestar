#pragma once

#include "raft_driver.hpp"
#include "raft_node.hpp"

#include <cstdint>
#include <map>
#include <seastar/core/future.hh>
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
    seastar::future<> tick();
    seastar::future<bool> propose(std::string data);
    seastar::future<> campaign();
    seastar::future<bool> proposeConfChange(std::vector<NodeId> voters, std::vector<NodeId> learners);
    seastar::future<> transferLeadership(NodeId target);
    // Trigger a state-machine snapshot compaction up to `upto` with the given
    // opaque payload (the caller produced it from its state machine).
    seastar::future<> compact(LogIndex upto, std::string snapshotData);

    // Linearizable read barrier: resolves with a ReadIndex once a quorum has
    // confirmed current-term leadership AND this node has applied through that
    // index. The caller may then read committed state (e.g. the group-0 map)
    // linearizably. Fails if this node is not the leader (redirect to the leader).
    seastar::future<LogIndex> readBarrier();

    // Observers (safe to read between async operations on the same core).
    uint16_t groupId() const { return groupId_; }
    Role role() const { return node_.role(); }
    bool isLeader() const { return node_.isLeader(); }
    Term currentTerm() const { return node_.currentTerm(); }
    NodeId leader() const { return node_.leader(); }
    LogIndex commitIndex() const { return node_.commitIndex(); }
    const RaftNode& node() const { return node_; }

private:
    // The ready/persist/send/apply/advance loop. MUST run under the group lock so
    // no step()/tick()/propose() interleaves a ready()..advance() pair.
    seastar::future<> drainReady();

    // Resolve any read barriers whose ReadIndex this node has now applied through.
    void releaseReadBarriers();

    uint16_t groupId_;
    RaftNode node_;
    RaftPersistence& persistence_;
    RaftTransport& transport_;
    RaftStateMachine& sm_;
    seastar::semaphore lock_{1};  // serializes all node mutation on this core

    // Read-barrier tracking.
    uint64_t appliedIndex_ = 0;              // highest entry index applied to the SM
    uint64_t nextReadCtx_ = 1;
    std::map<uint64_t, LogIndex> confirmedReads_;              // ctx -> ReadIndex (awaiting apply)
    std::map<uint64_t, seastar::promise<LogIndex>> readWaiters_;  // ctx -> caller promise
};

}  // namespace timestar::raft
