#pragma once

#include "raft_driver.hpp"
#include "raft_group.hpp"
#include "raft_node.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

namespace timestar::raft {

// All Raft groups (VShards) hosted on ONE core, sharing ONE transport and ONE
// periodic timer -- no per-group sockets, threads, or timer services. This is
// what lets a single core run thousands of lightweight groups. Incoming
// envelopes are routed to the addressed group by id; the timer ticks every
// group each interval.
class RaftGroupRegistry {
public:
    RaftGroupRegistry(RaftTransport& transport, std::chrono::milliseconds tickInterval);

    // Add a group. Its RaftGroup shares this registry's transport (so its sends
    // multiplex over the same per-host connections). persistence/sm are borrowed
    // and must outlive the registry.
    RaftGroup& addGroup(uint16_t groupId, RaftNode node, RaftPersistence& persistence, RaftStateMachine& sm);
    // Remove one live group, refusing new lookups first and then draining all
    // operations that already entered it. Idempotent for an absent group.
    seastar::future<bool> removeGroup(uint16_t groupId);
    RaftGroup* group(uint16_t groupId);
    // Keep a removed group object alive while an owning layer drains wrappers
    // that may still hold a raw pointer between group operations.
    std::shared_ptr<RaftGroup> groupHandle(uint16_t groupId);
    size_t size() const { return groups_.size(); }

    // Route an incoming envelope to its group (wire this as the transport's
    // onDeliver). Unknown groups are dropped.
    seastar::future<> deliver(Envelope env);

    // Start / stop the shared periodic tick loop.
    void startTicking();
    seastar::future<> stop();

    // Hibernation: a quiescent FOLLOWER group (has a leader, no pending Ready) is
    // skipped for up to `followerSkip` consecutive passes before a check-tick,
    //
    // The skip changes how often a group is RUN and must not change what time it thinks it
    // is: `tickAll` credits the skipped passes to the next tick (`RaftNode::tick(passes)`),
    // so the election timeout -- and, with CheckQuorum on, the disruption-guard LEASE that
    // shares its counter -- still expire in REAL time. Advancing by one after skipping nine
    // stretched both tenfold and cost a measured availability regression; see tickAll.
    // since a live leader's heartbeats keep it a follower regardless of its own
    // ticking and a dead leader is still detected by the periodic check-tick.
    // Leaders and candidates always tick. This is what keeps the per-tick cost of
    // thousands of idle groups on one core low.
    // followerSkip == 0 disables hibernation. Default keeps it on.
    void setHibernation(unsigned followerSkip) { followerSkip_ = followerSkip; }
    uint64_t skippedTicks() const { return skippedTicks_; }

    // Ticks that threw (debt D-36). A group's tick drives its whole Ready drain --
    // persist, send, APPLY -- so an apply that throws surfaces here.
    uint64_t tickErrors() const { return tickErrors_; }

    // Un-hibernate every group that still believes `leader` leads it, so it ticks at
    // full rate. Returns how many woke.
    //
    // WHAT THIS IS STILL FOR, now that skipped passes are credited (D-29(b)): granularity,
    // not magnitude. A hibernating follower's election timeout expires on schedule in real
    // time, but it can only NOTICE at its next check-tick, so detection lands up to
    // followerSkip passes (~200 ms in production) late and the group's own Ready work is
    // deferred with it. Waking removes that lag for the groups behind a peer we already
    // know is unreachable, which is what the read and write paths do before answering
    // QUERY_INCOMPLETE or giving up on a batch.
    //
    // It used to be load-bearing for a much bigger number -- hibernation stretched the
    // timeout ITSELF tenfold (2.5-5 s -> 25-50 s), so a few hundred idle groups pointing at
    // a dead node failed every query cluster-wide for the whole stretched window. That is
    // fixed at the source in tickAll; this remains as the latency trim.
    //
    // It only resets the skip counter -- no forced election, so it cannot disturb a healthy
    // group.
    size_t wakeFollowersOf(NodeId leader);

    // TEST SEAM: run ONE tick pass synchronously. Not a knob -- `startTicking()` is the
    // production driver, and this exists so a test of the pass's own behaviour (its
    // failure isolation, debt D-36) never has to depend on a timer firing or on wall
    // time. The pass it runs is the production pass, byte for byte.
    seastar::future<> tickAllForTest() { return tickAll(); }

private:
    seastar::future<> tickAll();

    RaftTransport& transport_;
    std::chrono::milliseconds tickInterval_;
    // shared_ptr is intentional: a tick pass snapshots the live group set before
    // its first suspension. Concurrent removal can erase the registry entry while
    // that pass still owns a safe, already-retired group object.
    std::map<uint16_t, std::shared_ptr<RaftGroup>> groups_;
    std::map<uint16_t, unsigned> skips_;     // consecutive passes a group has been skipped
    std::map<uint16_t, unsigned> awakeFor_;  // passes left of forced full-rate ticking
    // Must exceed the election timeout in passes (125-250) so a woken group campaigns.
    static constexpr unsigned kWakePasses = 400;
    seastar::timer<> timer_;
    seastar::gate gate_;
    unsigned followerSkip_ = 9;  // idle followers tick every 10th pass by default
    uint64_t skippedTicks_ = 0;  // metric: total idle-follower ticks skipped
    uint64_t tickErrors_ = 0;    // metric: ticks that threw (debt D-36)
    bool ticking_ = false;       // guards against overlapping tick passes
    bool stopping_ = false;
};

}  // namespace timestar::raft
