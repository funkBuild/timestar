#include "raft_group_registry.hpp"

#include <seastar/core/coroutine.hh>
#include <variant>

namespace timestar::raft {

RaftGroupRegistry::RaftGroupRegistry(RaftTransport& transport, std::chrono::milliseconds tickInterval)
    : transport_(transport), tickInterval_(tickInterval) {}

RaftGroup& RaftGroupRegistry::addGroup(uint16_t groupId, RaftNode node, RaftPersistence& persistence,
                                       RaftStateMachine& sm) {
    auto g = std::make_unique<RaftGroup>(groupId, std::move(node), persistence, transport_, sm);
    RaftGroup& ref = *g;
    groups_[groupId] = std::move(g);
    return ref;
}

RaftGroup* RaftGroupRegistry::group(uint16_t groupId) {
    auto it = groups_.find(groupId);
    return it == groups_.end() ? nullptr : it->second.get();
}

seastar::future<> RaftGroupRegistry::deliver(Envelope env) {
    if (stopping_ || gate_.is_closed())
        return seastar::make_ready_future<>();
    auto it = groups_.find(env.groupId);
    if (it == groups_.end())
        return seastar::make_ready_future<>();  // no such group here; drop
    // Run the step under the gate so stop()'s gate.close() waits for it -- the
    // group must not be destroyed while a delivery is mid-flight in its step().
    RaftGroup* g = it->second.get();
    // A VOTE THE LEASE DROPPED UN-HIBERNATES THE VOTER, and with CheckQuorum on this is
    // load bearing rather than an optimisation (debt D-9).
    //
    // Hibernation ticks a quiescent follower 1-in-`followerSkip_`, which stretches every
    // tick-driven clock in the group by that factor -- including, once CheckQuorum is on,
    // the disruption-guard lease: 2.5-5 s becomes 25-50 s. A vote arriving inside that
    // stretched lease is dropped SILENTLY (no reply, no term bump), so a group whose leader
    // has DIED cannot be voted into a new one until the VOTER's stretched lease expires,
    // while the candidate -- which the write path has usually already woken via
    // wakeFollowersOf -- campaigns and is refused every time.
    //
    // MEASURED on node_kill_round.sh (3-node RF=3, kill -9 mid-bench, same session, only
    // CheckQuorum differing): without this, enabling CheckQuorum moved the band from
    // 49/400 failed batches and an 8 s recovery to 153/400 and 43 s, every failure
    // "(last: transport)" -- with the election stalled the coordinator had no leader for
    // those groups and fell back to the placement primary, which was the dead node.
    //
    // THE TRIGGER IS THE DROP, NOT THE VOTE, and the difference is the whole design. Waking
    // on any RequestVote also wakes for every ordinary election and every leadership
    // TRANSFER -- and the balancer transfers thousands of groups every few seconds, so that
    // version kept most of the map awake and gave back the CPU hibernation exists to save
    // (it failed HibernationSkipsIdleFollowersButStillReplicates outright). A vote the lease
    // DROPPED is exactly the pathological case: a transfer vote bypasses the lease and is
    // never counted, and a vote arriving after the lease has expired is never counted
    // either. Waking changes no Raft state -- the group only gets its own clock back.
    const uint64_t droppedBefore = g->node().leaseDroppedVotes();
    const uint16_t gid = env.groupId;
    return seastar::with_gate(gate_, [this, g, gid, droppedBefore, m = std::move(env.message)]() mutable {
        return g->step(std::move(m)).then([this, g, gid, droppedBefore] {
            if (g->node().leaseDroppedVotes() != droppedBefore)
                awakeFor_[gid] = kWakePasses;
        });
    });
}

void RaftGroupRegistry::startTicking() {
    timer_.set_callback([this] {
        // Skip if a previous tick pass is still running (thousands of groups can
        // take longer than one interval) or we are shutting down.
        if (ticking_ || stopping_ || gate_.is_closed())
            return;
        ticking_ = true;
        (void)seastar::with_gate(
            gate_, [this]() -> seastar::future<> { return tickAll().finally([this] { ticking_ = false; }); });
    });
    timer_.arm_periodic(tickInterval_);
}

size_t RaftGroupRegistry::wakeFollowersOf(NodeId leader) {
    if (leader == kNoNode)
        return 0;
    size_t woken = 0;
    for (auto& [gid, g] : groups_) {
        if (g->leader() != leader)
            continue;
        // Tick at FULL rate for a bounded window. Note that zeroing skips_ would do
        // the opposite -- it restarts the skip countdown, delaying the next
        // check-tick. The window must comfortably exceed the election timeout
        // (125-250 passes) so the group actually times out and campaigns; it then
        // stops being a quiescent follower and hibernation resumes naturally.
        awakeFor_[gid] = kWakePasses;
        ++woken;
    }
    return woken;
}

seastar::future<> RaftGroupRegistry::tickAll() {
    for (auto& [gid, g] : groups_) {
        if (stopping_)
            co_return;
        // Hibernate a quiescent follower: skip its tick for up to followerSkip_
        // passes. A live leader's heartbeats (delivered via step, independent of
        // this group's own ticking) keep it a follower; a dead leader stops
        // heartbeating and the periodic check-tick still eventually times it out.
        const bool quiescentFollower = g->role() == Role::Follower && g->leader() != kNoNode && !g->node().hasReady();
        // A group woken by wakeFollowersOf() bypasses hibernation until its window
        // expires (self-limiting, so a wake can never pin a group awake forever).
        bool forcedAwake = false;
        if (auto w = awakeFor_.find(gid); w != awakeFor_.end()) {
            if (w->second > 0) {
                --w->second;
                forcedAwake = true;
            } else {
                awakeFor_.erase(w);
            }
        }
        if (followerSkip_ != 0 && quiescentFollower && !forcedAwake) {
            unsigned& s = skips_[gid];
            if (s < followerSkip_) {
                ++s;
                ++skippedTicks_;
                continue;
            }
            s = 0;  // time for a check-tick
        } else {
            skips_[gid] = 0;
        }
        co_await g->tick();
    }
}

seastar::future<> RaftGroupRegistry::stop() {
    stopping_ = true;
    timer_.cancel();
    co_await gate_.close();
}

}  // namespace timestar::raft
