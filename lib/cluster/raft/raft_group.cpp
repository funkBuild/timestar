#include "raft_group.hpp"

#include <algorithm>
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

seastar::future<LogIndex> RaftGroup::readBarrier() {
    const uint64_t ctx = nextReadCtx_++;
    seastar::promise<LogIndex> promise;
    auto fut = promise.get_future();
    readWaiters_.emplace(ctx, std::move(promise));

    co_await seastar::with_semaphore(lock_, 1, [this, ctx]() -> seastar::future<> {
        if (!node_.isLeader()) {
            // Not the leader: fail the barrier so the caller redirects.
            if (auto w = readWaiters_.find(ctx); w != readWaiters_.end()) {
                w->second.set_exception(
                    std::make_exception_ptr(std::runtime_error("readBarrier: not leader")));
                readWaiters_.erase(w);
            }
            co_return;
        }
        node_.requestReadIndex(ctx);
        co_await drainReady();  // heartbeats out; confirmation arrives on later steps
    });
    co_return co_await std::move(fut);
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
    return seastar::with_semaphore(
        lock_, 1, [this, upto, snapshotData = std::move(snapshotData)]() mutable -> seastar::future<> {
            node_.compact(upto, std::move(snapshotData));
            co_await drainReady();
        });
}

}  // namespace timestar::raft
