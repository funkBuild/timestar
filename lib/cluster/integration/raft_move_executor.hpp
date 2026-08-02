#pragma once

#include "../movement/mover.hpp"
#include "../raft/raft_group.hpp"

#include <algorithm>
#include <chrono>
#include <functional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <vector>

namespace timestar::cluster {

using movement::ConfigTarget;
using movement::MoveExecutor;
using movement::MoveJob;
using timestar::raft::NodeId;

// Production MoveExecutor over a real per-VShard RaftGroup (integration plan M5
// task 1): the Mover drives one bounded membership change to completion through this,
// committing each config via joint consensus and catching the destination up via the
// leader's match-index before promoting it. Every method is idempotent -- a
// crash-resumed re-issue of an already-applied step is a no-op -- matching the
// Mover's forward-only, crash-resumable contract.
//
// commitConfig/catchUp AWAIT the transition actually taking effect, so the group MUST
// be ticking while they run (the RaftGroupRegistry's timer in production; the driver
// loop in tests). They poll the committed config / match-index and bound the wait,
// throwing rather than hanging if it never lands.
class RaftGroupMoveExecutor : public MoveExecutor {
public:
    RaftGroupMoveExecutor(raft::RaftGroup& group, std::function<seastar::future<>(const MoveJob&)> persist,
                          unsigned maxWaitMillis = 4000)
        : group_(group), persist_(std::move(persist)), maxWait_(maxWaitMillis) {}

    std::vector<NodeId> voters() const override { return group_.node().config().voters; }
    std::vector<NodeId> learners() const override { return group_.node().config().learners; }

    seastar::future<bool> commitConfig(ConfigTarget target) override {
        if (configApplied(target))
            co_return true;  // idempotent: the exact stable config is durable and applied

        // A controller can restart after the joint entry committed and appended
        // final Cnew but before that final entry committed. Raft's active config
        // already looks stable in this window, yet proposing the next transition
        // is forbidden. Wait for the existing target to cross the apply boundary
        // instead of mistaking "appended" for "finished".
        if (configActive(target)) {
            for (unsigned i = 0; i < maxWait_; ++i) {
                if (configApplied(target))
                    co_return true;
                if (!group_.isLeader())
                    co_return false;
                if (!configActive(target))
                    throw movement::UnsafeMove("commitConfig: in-flight target configuration was replaced");
                co_await seastar::sleep(std::chrono::milliseconds(1));
            }
            throw movement::UnsafeMove("commitConfig: existing target configuration was not applied in time");
        }

        if (!group_.isLeader())
            co_return false;  // not the leader -> the current leader redrives the job
        try {
            // The group-level waiter covers BOTH the joint entry and the
            // automatically appended final Cnew entry. Returning before final
            // apply is what used to strand every replace at Promoted: the
            // immediate remove-old proposal saw an outstanding config and was
            // rejected.
            co_return co_await group_.proposeConfChangeAndAwaitApplied(
                std::move(target.voters), std::move(target.learners),
                seastar::lowres_clock::now() + std::chrono::milliseconds(maxWait_));
        } catch (const raft::LeadershipLostError&) {
            co_return false;  // the current data-group leader will redrive the durable job
        } catch (const seastar::timed_out_error&) {
            throw movement::UnsafeMove("commitConfig: target configuration did not apply in time");
        }
    }

    seastar::future<uint64_t> catchUp(NodeId dest) override {
        // Caught up once the leader knows `dest` has replicated through the current
        // commit index -- then promoting it cannot stall commits.
        const uint64_t target = group_.commitIndex();
        for (unsigned i = 0; i < maxWait_; ++i) {
            if (group_.matchIndexOf(dest) >= target)
                co_return target;
            if (!group_.isLeader())
                throw movement::UnsafeMove("catchUp: lost leadership");
            co_await seastar::sleep(std::chrono::milliseconds(1));
        }
        throw movement::UnsafeMove("catchUp: destination did not catch up in time");
    }

    seastar::future<> persist(const MoveJob& job) override { return persist_(job); }

private:
    bool configActive(const ConfigTarget& t) const {
        const auto& c = group_.node().config();
        return !c.joint() && sameSet(c.voters, t.voters) && sameSet(c.learners, t.learners);
    }
    bool configApplied(const ConfigTarget& t) const {
        if (!configActive(t))
            return false;
        const raft::LogIndex index = group_.node().latestConfigIndex();
        // kNoIndex means this is the snapshot/base configuration, which was
        // necessarily installed before the group became available.
        return index == raft::kNoIndex || (group_.commitIndex() >= index && group_.appliedIndex() >= index);
    }
    static bool sameSet(std::vector<NodeId> a, std::vector<NodeId> b) {
        std::sort(a.begin(), a.end());
        std::sort(b.begin(), b.end());
        return a == b;
    }

    raft::RaftGroup& group_;
    std::function<seastar::future<>(const MoveJob&)> persist_;
    unsigned maxWait_;
};

}  // namespace timestar::cluster
