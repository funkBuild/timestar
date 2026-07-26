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
    return seastar::with_gate(gate_, [g, m = std::move(env.message)]() mutable { return g->step(std::move(m)); });
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
        }
        // TICK FOR THE PASSES WE SKIPPED, NOT JUST FOR THIS ONE (debt D-29(b)). Hibernation
        // is about how often we RUN a group, and it must not change what time that group
        // thinks it is: every clock in RaftNode::tick is tick-driven, and with CheckQuorum
        // on one of them is the disruption-guard LEASE. Skipping nine passes and then
        // advancing by one stretched that lease tenfold (2.5-5 s -> 25-50 s), so a group
        // whose leader had DIED refused the very votes that would have replaced it --
        // measured as 153/400 failed batches and a 43 s recovery on node_kill_round.sh
        // against 49/400 and 8 s with CheckQuorum off. Crediting the skipped passes makes
        // the lease, and the election timeout behind it, expire in REAL time; hibernation
        // still saves exactly the same work, because the saving is the calls we did not
        // make, not the time we pretended had not passed.
        //
        // The credit is also paid to a group that has STOPPED being quiescent (the counter
        // is only cleared here), so a follower that becomes a candidate or takes a proposal
        // does not silently forget the interval it spent hibernating.
        //
        // ELECTION-STORM NOTE: a crediting tick can cross the election timeout by up to
        // followerSkip_ passes rather than landing exactly on it, so crossings quantise to
        // every followerSkip_+1-th pass. That coarsens the randomized timeout
        // (electionTimeoutMin..Max, 125-250 passes in production) but does not defeat it --
        // 126 distinct timeouts still spread over ~13 distinct check-tick passes, and the
        // gates that would show a storm (rolling_rebalance under sustained writes,
        // node_kill_round) are green. If followerSkip_ ever approaches the timeout SPREAD,
        // that stops being true and the crossings need explicit jitter.
        unsigned& skipped = skips_[gid];
        const unsigned passes = 1 + skipped;
        skipped = 0;
        co_await g->tick(passes);
    }
}

seastar::future<> RaftGroupRegistry::stop() {
    stopping_ = true;
    timer_.cancel();
    co_await gate_.close();
}

}  // namespace timestar::raft
