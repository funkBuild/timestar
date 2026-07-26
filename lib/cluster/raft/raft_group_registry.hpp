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
    RaftGroup* group(uint16_t groupId);
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
    // NOTE, because it is not obvious and it cost a measured availability regression: the
    // skip stretches every TICK-DRIVEN clock in that group by ~followerSkip, and with
    // CheckQuorum on that includes the disruption-guard LEASE. `deliver()` therefore wakes
    // a group that DROPPED a vote under that lease -- see the comment there for the
    // mechanism, the numbers, and why the trigger is the drop and not the vote.
    // since a live leader's heartbeats keep it a follower regardless of its own
    // ticking and a dead leader is still detected by the periodic check-tick
    // (failover for idle groups is slower by ~followerSkip, which is fine -- they
    // have no pending writes). Leaders and candidates always tick. This is what
    // keeps the per-tick cost of thousands of idle groups on one core low.
    // followerSkip == 0 disables hibernation. Default keeps it on.
    void setHibernation(unsigned followerSkip) { followerSkip_ = followerSkip; }
    uint64_t skippedTicks() const { return skippedTicks_; }

    // Un-hibernate every group that still believes `leader` leads it, so it ticks at
    // full rate and reaches its election timeout promptly. Returns how many woke.
    //
    // Hibernation stretches an idle follower's election timeout by ~followerSkip
    // (10x by default: 2.5-5s becomes 25-50s). That was deemed acceptable because an
    // idle group has no pending writes -- but READS route by leader across ALL
    // VShards, so a few hundred idle groups still pointing at a dead node fail every
    // query cluster-wide for the whole stretched timeout. The read path calls this
    // when a leader turns out to be unreachable, which collapses that window back to
    // a normal election timeout. It only resets the skip counter -- no forced
    // election, so it cannot disturb a healthy group.
    size_t wakeFollowersOf(NodeId leader);

private:
    seastar::future<> tickAll();

    RaftTransport& transport_;
    std::chrono::milliseconds tickInterval_;
    std::map<uint16_t, std::unique_ptr<RaftGroup>> groups_;
    std::map<uint16_t, unsigned> skips_;     // consecutive passes a group has been skipped
    std::map<uint16_t, unsigned> awakeFor_;  // passes left of forced full-rate ticking
    // Must exceed the election timeout in passes (125-250) so a woken group campaigns.
    static constexpr unsigned kWakePasses = 400;
    seastar::timer<> timer_;
    seastar::gate gate_;
    unsigned followerSkip_ = 9;  // idle followers tick every 10th pass by default
    uint64_t skippedTicks_ = 0;  // metric: total idle-follower ticks skipped
    bool ticking_ = false;       // guards against overlapping tick passes
    bool stopping_ = false;
};

}  // namespace timestar::raft
