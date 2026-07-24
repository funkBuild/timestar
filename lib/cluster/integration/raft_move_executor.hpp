#pragma once

#include "../movement/mover.hpp"
#include "../raft/raft_group.hpp"

#include <algorithm>
#include <chrono>
#include <functional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
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
        if (configActive(target))
            co_return true;  // idempotent: already committed and active
        if (!group_.isLeader())
            co_return false;  // not the leader -> the current leader redrives the job
        const bool proposed = co_await group_.proposeConfChange(target.voters, target.learners);
        if (!proposed)
            co_return false;
        // Await the joint transition committing and leaving joint (Cnew active).
        for (unsigned i = 0; i < maxWait_; ++i) {
            if (configActive(target))
                co_return true;
            if (!group_.isLeader())
                co_return false;  // lost leadership mid-change; a new leader redrives
            co_await seastar::sleep(std::chrono::milliseconds(1));
        }
        throw movement::UnsafeMove("commitConfig: target config did not become active in time");
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
