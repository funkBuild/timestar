#include "raft_group_registry.hpp"

#include <seastar/core/coroutine.hh>

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
    return seastar::with_gate(gate_, [g, m = std::move(env.message)]() mutable {
        return g->step(std::move(m));
    });
}

void RaftGroupRegistry::startTicking() {
    timer_.set_callback([this] {
        // Skip if a previous tick pass is still running (thousands of groups can
        // take longer than one interval) or we are shutting down.
        if (ticking_ || stopping_ || gate_.is_closed())
            return;
        ticking_ = true;
        (void)seastar::with_gate(gate_, [this]() -> seastar::future<> {
            return tickAll().finally([this] { ticking_ = false; });
        });
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
        const bool quiescentFollower = g->role() == Role::Follower && g->leader() != kNoNode &&
                                       !g->node().hasReady();
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
