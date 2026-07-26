#include "journal_sink.hpp"

#include <seastar/core/coroutine.hh>
#include <utility>

namespace timestar {

seastar::future<> SharedShardJournal::append(const JournalRecord& record) {
    // Exclusive with every other append AND with the barrier -- see the ordering
    // contract. get_units rather than with_semaphore(lambda) so nothing captures a
    // coroutine frame.
    auto units = co_await seastar::get_units(ioLock_, 1);
    co_await writer_.append(record);
}

seastar::future<> SharedShardJournal::sync() {
    // Fail rather than hang: after stop() no round will ever run, so a waiter
    // registered here would wait forever -- and a hung sync() is a group that never
    // acks and never fails, the worst of both.
    if (gate_.is_closed())
        throw std::runtime_error("SharedShardJournal: sync() after stop()");
    ++syncRequests_;
    // JOIN THE NEXT ROUND. Everything this caller appended is already in the
    // writer's buffer (its appends were awaited), so any barrier that runs from
    // here on covers it. Registering BEFORE starting the loop is what makes that
    // true -- a round can only take a waiter that is already in `pending_`.
    seastar::promise<> p;
    auto f = p.get_future();
    pending_.push_back(std::move(p));
    if (!roundLoopRunning_) {
        roundLoopRunning_ = true;
        // Fire-and-forget under the gate; stop() waits for it. The failure path is
        // inside runRounds(), which fails its waiters rather than escaping.
        (void)seastar::with_gate(gate_, [this] { return runRounds(); }).handle_exception([](std::exception_ptr) {
            // runRounds() fails its waiters itself and never escapes; this only
            // absorbs a gate_closed race so no exceptional future is dropped.
        });
    }
    co_await std::move(f);
}

seastar::future<> SharedShardJournal::runRounds() {
    while (!pending_.empty()) {
        // 1. Take the waiter set. Everything these waiters appended is in the buffer
        //    already; the barrier below therefore covers all of them. Waiters that
        //    arrive from here on land in a FRESH `pending_` and are served by the
        //    next iteration -- never by this barrier, which may already have
        //    computed its flush extent.
        std::vector<seastar::promise<>> round;
        round.swap(pending_);
        std::exception_ptr err;
        try {
            // 2. Barrier under the io lock: flush + fdatasync, exclusive with appends.
            auto units = co_await seastar::get_units(ioLock_, 1);
            co_await writer_.barrier();
        } catch (...) {
            err = std::current_exception();
        }
        ++rounds_;
        if (round.size() > maxRoundWaiters_)
            maxRoundWaiters_ = round.size();
        // 3. Release. This is the ONLY place a waiter is resolved, and it is
        //    strictly after the barrier returned. A failed barrier fenced the
        //    writer, so every waiter fails and no group in this round can ack.
        for (auto& w : round) {
            if (err)
                w.set_exception(err);
            else
                w.set_value();
        }
    }
    roundLoopRunning_ = false;
}

seastar::future<> SharedShardJournal::stop() {
    if (!gate_.is_closed())
        co_await gate_.close();
    // A waiter registered after the gate closed would never be served; fail it
    // rather than hang. (with_gate throws gate_closed_exception at the caller, so
    // this is belt and braces for a race between sync()'s push_back and the close.)
    auto leftover = std::move(pending_);
    pending_.clear();
    for (auto& w : leftover)
        w.set_exception(std::make_exception_ptr(std::runtime_error("SharedShardJournal stopped before this sync")));
}

}  // namespace timestar
