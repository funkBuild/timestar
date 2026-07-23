#include "raft_group.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>

namespace timestar::raft {

seastar::future<> RaftGroup::drainReady() {
    // Precondition: caller holds lock_. Drain every pending Ready in order.
    while (node_.hasReady()) {
        RaftNode::Ready rd = node_.ready();

        // 1. Make durable BEFORE anything observable. Snapshot first (it may
        //    supersede the log), then hard state, then the new log entries.
        if (rd.snapshot)
            co_await persistence_.persistSnapshot(*rd.snapshot);
        if (rd.hardState)
            co_await persistence_.persistHardState(*rd.hardState);
        if (!rd.entries.empty())
            co_await persistence_.persistEntries(rd.entries);

        // 2. Only now may we tell peers what we have committed to durably.
        for (auto& m : rd.messages)
            co_await transport_.send(Envelope{groupId_, m});

        // 3. Apply committed output to the state machine (snapshot install first).
        if (rd.snapshot)
            co_await sm_.applySnapshot(*rd.snapshot);
        for (auto& e : rd.committed) {
            if (e.type == EntryType::Normal && !e.data.empty())
                co_await sm_.apply(e);
        }

        // 4. Acknowledge: advance persistence/apply watermarks and drain messages.
        node_.advance(rd);
    }
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
