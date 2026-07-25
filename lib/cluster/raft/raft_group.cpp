#include "raft_group.hpp"

#include <algorithm>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <stdexcept>

namespace timestar::raft {

seastar::future<> RaftGroup::drainReady() {
    // Precondition: caller holds lock_. Drain every pending Ready in order.
    while (node_.hasReady()) {
        RaftNode::Ready rd = node_.ready();

        // 1. Make durable BEFORE anything observable. Append snapshot first (it
        //    may supersede the log), then hard state, then the new log entries,
        //    then one sync() -- a single fsync makes the whole Ready durable.
        bool persisted = false;
        if (rd.snapshot) {
            co_await persistence_.persistSnapshot(*rd.snapshot);
            persisted = true;
        }
        if (rd.hardState) {
            co_await persistence_.persistHardState(*rd.hardState);
            persisted = true;
        }
        if (!rd.entries.empty()) {
            co_await persistence_.persistEntries(rd.entries);
            persisted = true;
        }
        if (persisted)
            co_await persistence_.sync();

        // 2. Only now may we tell peers what we have committed to durably.
        for (auto& m : rd.messages)
            co_await transport_.send(Envelope{groupId_, m});

        // 3. Apply committed output to the state machine (snapshot install first).
        if (rd.snapshot) {
            co_await sm_.applySnapshot(*rd.snapshot);
            appliedIndex_ = std::max<uint64_t>(appliedIndex_, rd.snapshot->index);
        }
        for (auto& e : rd.committed) {
            if (e.type == EntryType::Normal && !e.data.empty())
                co_await sm_.apply(e);
            appliedIndex_ = std::max<uint64_t>(appliedIndex_, e.index);
        }

        // Record newly-confirmed read barriers, then release any whose ReadIndex
        // we have now applied through.
        for (const auto& rs : rd.readStates)
            confirmedReads_[rs.context] = rs.readIndex;
        releaseReadBarriers();
        // Resolve write waiters whose entry we have now applied (or fail them all
        // if we just lost leadership).
        releaseApplyWaiters();
        // Resolve role-agnostic apply waiters (replica reads) we have caught up to.
        releaseAppliedWaiters();

        // 4. Acknowledge: advance persistence/apply watermarks and drain messages.
        node_.advance(rd);
    }
}

void RaftGroup::releaseReadBarriers() {
    if (!node_.isLeader()) {
        // Leadership lost: no barrier can be confirmed here. Fail every waiter so
        // the caller redirects to the new leader (never hang).
        for (auto& [ctx, p] : readWaiters_)
            p.set_exception(std::make_exception_ptr(std::runtime_error("readBarrier: leadership lost")));
        readWaiters_.clear();
        confirmedReads_.clear();
        return;
    }
    for (auto it = confirmedReads_.begin(); it != confirmedReads_.end();) {
        if (appliedIndex_ >= it->second) {
            if (auto w = readWaiters_.find(it->first); w != readWaiters_.end()) {
                w->second.set_value(it->second);
                readWaiters_.erase(w);
            }
            it = confirmedReads_.erase(it);
        } else {
            ++it;
        }
    }
}

void RaftGroup::releaseApplyWaiters() {
    if (!node_.isLeader()) {
        // Leadership lost: the entry may or may not have committed. Fail every
        // waiter so the caller retries the whole (idempotent) batch against the
        // new leader -- an un-acknowledged write is never lost, and LWW makes a
        // re-applied batch harmless. Never hang.
        for (auto& [idx, p] : applyWaiters_)
            p.set_exception(std::make_exception_ptr(std::runtime_error("propose: leadership lost before commit")));
        applyWaiters_.clear();
        return;
    }
    for (auto it = applyWaiters_.begin(); it != applyWaiters_.end();) {
        if (appliedIndex_ >= it->first) {
            it->second.set_value(true);
            it = applyWaiters_.erase(it);
        } else {
            ++it;
        }
    }
}

void RaftGroup::releaseAppliedWaiters() {
    for (auto it = appliedWaiters_.begin(); it != appliedWaiters_.end();) {
        if (appliedIndex_ >= it->first) {
            it->second.set_value();
            it = appliedWaiters_.erase(it);
        } else {
            ++it;
        }
    }
}

seastar::future<> RaftGroup::waitApplied(LogIndex index) {
    // Register the waiter under the lock so no concurrent drainReady observes a
    // half-created waiter. Resolve immediately if we have already applied through
    // `index` (common for a caught-up replica). No leadership requirement.
    std::optional<seastar::future<>> fut;
    co_await seastar::with_semaphore(lock_, 1, [this, index, &fut]() -> seastar::future<> {
        if (appliedIndex_ >= index) {
            fut = seastar::make_ready_future<>();
            co_return;
        }
        seastar::promise<> promise;
        fut = promise.get_future();
        appliedWaiters_.emplace_back(index, std::move(promise));
        co_return;
    });
    co_await std::move(*fut);
}

seastar::future<bool> RaftGroup::proposeAndAwaitApplied(std::string data) {
    // Register the waiter INSIDE the lock (mirroring readBarrier): capture the
    // proposed entry's index and register its promise before any drainReady can
    // observe it, so a leader flap cannot resolve or fail a half-created waiter.
    std::optional<seastar::future<bool>> fut;
    bool notLeader = false;
    co_await seastar::with_semaphore(lock_, 1,
                                     [this, data = std::move(data), &fut, &notLeader]() mutable -> seastar::future<> {
                                         if (!node_.propose(std::move(data))) {
                                             notLeader = true;
                                             co_return;  // not the leader: fut stays empty
                                         }
                                         const LogIndex idx = node_.log().lastIndex();  // the entry we just appended
                                         seastar::promise<bool> promise;
                                         fut = promise.get_future();
                                         applyWaiters_.emplace_back(idx, std::move(promise));
                                         // GROUP COMMIT. drainReady() makes the whole pending Ready
                                         // durable with ONE fsync, so when several writes are queued on
                                         // this group it is far cheaper to let them all append first and
                                         // flush once than to fsync per proposal. If callers are already
                                         // waiting on lock_, skip our drain: each of them appends in turn
                                         // and the LAST one (which sees no waiters) flushes everyone's
                                         // entries together.
                                         //
                                         // Safety: skipping leaves the entry in the IN-MEMORY log only --
                                         // nothing is persisted, sent to peers, or applied -- so the
                                         // "durable before observable" ordering is untouched. Progress is
                                         // guaranteed because tick() and step() also drain under this same
                                         // lock, so a deferred entry is flushed within one tick at worst.
                                         // Latency is unaffected in practice: the proposal has to await a
                                         // quorum round trip regardless, and the deferral only lasts until
                                         // the already-queued callers finish appending.
                                         if (lock_.waiters() == 0)
                                             co_await drainReady();  // may commit+apply (single voter) and resolve
                                     });
    if (notLeader)
        co_return false;
    co_return co_await std::move(*fut);
}

seastar::future<LogIndex> RaftGroup::readBarrier() {
    // Register the waiter INSIDE the lock (with requestReadIndex), so no
    // concurrent drainReady can observe/fail a half-created waiter and so a
    // leader->follower->leader flap between here and acquiring the lock cannot
    // spuriously fail a barrier we could still satisfy.
    std::optional<seastar::future<LogIndex>> fut;
    co_await seastar::with_semaphore(lock_, 1, [this, &fut]() -> seastar::future<> {
        if (!node_.isLeader())
            co_return;  // fut stays empty -> not leader
        const uint64_t ctx = nextReadCtx_++;
        seastar::promise<LogIndex> promise;
        fut = promise.get_future();
        readWaiters_.emplace(ctx, std::move(promise));
        node_.requestReadIndex(ctx);
        co_await drainReady();  // heartbeats out; confirmation arrives on later steps
    });
    if (!fut)
        throw std::runtime_error("readBarrier: not leader");  // caller redirects to the leader
    co_return co_await std::move(*fut);
}

seastar::future<> RaftGroup::step(Message m) {
    return seastar::with_semaphore(lock_, 1, [this, m = std::move(m)]() mutable -> seastar::future<> {
        node_.step(std::move(m));
        co_await drainReady();
    });
}

seastar::future<> RaftGroup::tick() {
    return seastar::with_semaphore(lock_, 1, [this]() -> seastar::future<> {
        node_.tick();
        co_await drainReady();
    });
}

seastar::future<bool> RaftGroup::propose(std::string data) {
    return seastar::with_semaphore(lock_, 1, [this, data = std::move(data)]() mutable -> seastar::future<bool> {
        const bool ok = node_.propose(std::move(data));
        co_await drainReady();
        co_return ok;
    });
}

seastar::future<> RaftGroup::campaign() {
    return seastar::with_semaphore(lock_, 1, [this]() -> seastar::future<> {
        node_.campaign();
        co_await drainReady();
    });
}

seastar::future<bool> RaftGroup::proposeConfChange(std::vector<NodeId> voters, std::vector<NodeId> learners) {
    return seastar::with_semaphore(
        lock_, 1,
        [this, voters = std::move(voters), learners = std::move(learners)]() mutable -> seastar::future<bool> {
            const bool ok = node_.proposeConfChange(std::move(voters), std::move(learners));
            co_await drainReady();
            co_return ok;
        });
}

seastar::future<> RaftGroup::transferLeadership(NodeId target) {
    return seastar::with_semaphore(lock_, 1, [this, target]() -> seastar::future<> {
        node_.transferLeadership(target);
        co_await drainReady();
    });
}

seastar::future<> RaftGroup::compact(LogIndex upto, std::string snapshotData) {
    return seastar::with_semaphore(lock_, 1,
                                   [this, upto, snapshotData = std::move(snapshotData)]() mutable -> seastar::future<> {
                                       node_.compact(upto, std::move(snapshotData));
                                       co_await drainReady();
                                   });
}

}  // namespace timestar::raft
