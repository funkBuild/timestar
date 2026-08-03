#include "raft_group_registry.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>

namespace timestar::raft {

RaftGroupRegistry::RaftGroupRegistry(RaftTransport& transport, std::chrono::milliseconds tickInterval)
    : transport_(transport), tickInterval_(tickInterval) {}

RaftGroup& RaftGroupRegistry::addGroup(uint16_t groupId, RaftNode node, RaftPersistence& persistence,
                                       RaftStateMachine& sm) {
    auto g = std::make_shared<RaftGroup>(groupId, std::move(node), persistence, transport_, sm);
    RaftGroup& ref = *g;
    groups_[groupId] = std::move(g);
    return ref;
}

seastar::future<bool> RaftGroupRegistry::removeGroup(uint16_t groupId) {
    auto it = groups_.find(groupId);
    if (it == groups_.end())
        co_return false;
    auto group = std::move(it->second);
    groups_.erase(it);
    skips_.erase(groupId);
    wakeNext_.erase(groupId);
    co_await group->retire();
    co_return true;
}

RaftGroup* RaftGroupRegistry::group(uint16_t groupId) {
    auto it = groups_.find(groupId);
    return it == groups_.end() ? nullptr : it->second.get();
}

std::shared_ptr<RaftGroup> RaftGroupRegistry::groupHandle(uint16_t groupId) {
    auto it = groups_.find(groupId);
    return it == groups_.end() ? nullptr : it->second;
}

seastar::future<> RaftGroupRegistry::deliver(Envelope env) {
    if (stopping_ || gate_.is_closed())
        return seastar::make_ready_future<>();
    auto it = groups_.find(env.groupId);
    if (it == groups_.end())
        return seastar::make_ready_future<>();  // no such group here; drop
    // Run the step under the gate so stop()'s gate.close() waits for it -- the
    // group must not be destroyed while a delivery is mid-flight in its step().
    auto g = it->second;
    return seastar::with_gate(
        gate_, [g = std::move(g), m = std::move(env.message)]() mutable { return g->step(std::move(m)); });
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
        // Bypass hibernation on the next pass without discarding the number of passes
        // already skipped. tickAll() pays that exact credit to the group, so a dead
        // leader's elapsed election time is preserved and a healthy leader costs only
        // this one extra tick.
        wakeNext_.insert(gid);
        ++woken;
    }
    return woken;
}

seastar::future<> RaftGroupRegistry::tickAll() {
    // Never suspend an iterator into groups_: removeGroup() may erase from the
    // map while this pass is awaiting another group. The shared references also
    // keep already-selected groups alive until this pass observes retirement.
    std::vector<std::pair<uint16_t, std::shared_ptr<RaftGroup>>> groups;
    groups.reserve(groups_.size());
    for (const auto& item : groups_)
        groups.push_back(item);
    // Do not let ordered-map position become transport priority. Outbound Raft
    // admission is intentionally bounded, so starting every pass at the lowest
    // group id would let the same early groups fill it and starve a high-id
    // catch-up forever. Rotate by about a quarter (plus one to avoid permanent
    // quadrants) so every region gets early service within a handful of passes.
    if (!groups.empty()) {
        tickStart_ %= groups.size();
        std::rotate(groups.begin(), groups.begin() + tickStart_, groups.end());
        tickStart_ = (tickStart_ + groups.size() / 4 + 1) % groups.size();
    }
    for (auto& [gid, g] : groups) {
        if (stopping_)
            co_return;
        if (g->retiring())
            continue;
        // Hibernate a quiescent follower: skip its tick for up to followerSkip_
        // passes. A live leader's heartbeats (delivered via step, independent of
        // this group's own ticking) keep it a follower; a dead leader stops
        // heartbeating and the periodic check-tick still eventually times it out.
        const bool quiescentFollower = g->role() == Role::Follower && g->leader() != kNoNode && !g->node().hasReady();
        // A targeted wake bypasses hibernation ONCE. If the group becomes a candidate it
        // is no longer quiescent and will tick normally; if its leader is healthy it can
        // immediately resume hibernating instead of being pinned awake for seconds.
        const bool forcedAwake = wakeNext_.erase(gid) != 0;
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
        // ONE GROUP'S FAILURE MUST NOT COST EVERY OTHER GROUP ITS TICK (debt D-36).
        //
        // A tick drives the group's whole Ready drain -- persist, send, APPLY -- and
        // apply is allowed to throw (`EngineDataStateMachine::apply` routes through
        // `Engine::insertBatch`, which refuses while the shard's ingest is backlogged).
        // That throw used to propagate straight out of this loop, and `groups_` is an
        // ORDERED map, so the same low id aborted the pass at the same place every time
        // and every higher id was never ticked at all -- no election timer, no
        // heartbeat, no drain. Measured across one RF=3 restart: 23 aborted passes cost
        // 16,511 group-ticks, while acknowledged points sat committed and unapplied.
        //
        // The failure itself is NOT swallowed in the sense that matters: the group's
        // own Ready is not advanced (RaftGroup::drainReady propagates before
        // `node_.advance`), so the entry is retried on this group's next tick and
        // re-apply is idempotent. What is dropped is only the propagation OUT of the
        // pass, which nothing above could act on anyway -- the timer callback discards
        // the future. Counted, and the reason is logged at the apply site.
        try {
            co_await g->tick(passes);
        } catch (...) {
            ++tickErrors_;
        }
    }
}

seastar::future<> RaftGroupRegistry::stop() {
    stopping_ = true;
    timer_.cancel();
    co_await gate_.close();
}

}  // namespace timestar::raft
