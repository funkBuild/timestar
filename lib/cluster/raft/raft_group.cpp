#include "raft_group.hpp"

#include "../../utils/logger.hpp"

#include <algorithm>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/with_timeout.hh>
#include <stdexcept>

namespace timestar::raft {

// TEMPORARY write-path profiling, enabled with TIMESTAR_RAFT_PROFILE=1.
// NOTE: accumulate ONLY into these shard-local counters. An earlier version passed
// references to the caller's coroutine frame into the with_semaphore lambda; the
// lambda outlives the frame when the caller's future is abandoned, so it wrote
// through dangling pointers and segfaulted shard 0 under load.
namespace {
struct ProfileCounters {
    uint64_t proposals = 0, drains = 0;
    uint64_t appliedNs = 0;  // propose entry -> apply-waiter resolved
    uint64_t inLockNs = 0;   // lock acquisition + append + (maybe) drain
    uint64_t persistNs = 0;  // persistEntries + sync
    uint64_t applyNs = 0;    // state-machine apply
    uint64_t sendNs = 0;     // transport sends
};
thread_local ProfileCounters g_prof;
bool profileEnabled() {
    static const bool on = [] {
        const char* e = std::getenv("TIMESTAR_RAFT_PROFILE");
        return e && e[0] == '1';
    }();
    return on;
}
inline uint64_t nowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}
void maybeReport() {
    auto& p = g_prof;
    if (p.proposals < 25)
        return;
    const double n = static_cast<double>(p.proposals);
    timestar::timestar_log.info(
        "[RAFT_PROFILE] n={} drains={} commit_latency={:.2f}ms in_lock={:.2f}ms "
        "persist={:.2f}ms apply={:.2f}ms send={:.2f}ms (per proposal)",
        p.proposals, p.drains, p.appliedNs / n / 1e6, p.inLockNs / n / 1e6, p.persistNs / n / 1e6, p.applyNs / n / 1e6,
        p.sendNs / n / 1e6);
    p = ProfileCounters{};
}
}  // namespace

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
        const uint64_t tP0 = profileEnabled() ? nowNs() : 0;
        if (persisted)
            co_await persistence_.sync();
        if (profileEnabled()) {
            g_prof.persistNs += nowNs() - tP0;
            ++g_prof.drains;
        }

        // 2. Only now may we tell peers what we have committed to durably.
        const uint64_t tS0 = profileEnabled() ? nowNs() : 0;
        for (auto& m : rd.messages)
            co_await transport_.send(Envelope{groupId_, m});
        if (profileEnabled())
            g_prof.sendNs += nowNs() - tS0;

        // 3. Apply committed output to the state machine (snapshot install first).
        if (rd.snapshot) {
            co_await sm_.applySnapshot(*rd.snapshot);
            appliedIndex_ = std::max<uint64_t>(appliedIndex_, rd.snapshot->index);
        }
        const uint64_t tA0 = profileEnabled() ? nowNs() : 0;
        for (auto& e : rd.committed) {
            if (e.type == EntryType::Normal && !e.data.empty())
                co_await sm_.apply(e);
            appliedIndex_ = std::max<uint64_t>(appliedIndex_, e.index);
        }
        if (profileEnabled())
            g_prof.applyNs += nowNs() - tA0;

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
            p.set_exception(std::make_exception_ptr(LeadershipLostError("propose: leadership lost before commit")));
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
    return proposeAndAwaitApplied(std::move(data), std::nullopt);
}

seastar::future<bool> RaftGroup::proposeAndAwaitApplied(std::string data,
                                                        std::optional<seastar::lowres_clock::time_point> deadline) {
    // FAIL CLOSED before the entry is durable (write-scaleout 5 review, F3b). An entry
    // over the transport's send bound can commit here and then never reach a follower,
    // leaving the group permanently one replica short with the offending entry already in
    // every surviving log -- unfixable without surgery. A throw is a bad answer; a
    // committed undeliverable entry is a worse one, and only the throw reaches a caller
    // who can still do something about it.
    if (data.size() > kMaxProposalBytes)
        throw ProposalTooLargeError("raft: proposal of " + std::to_string(data.size()) +
                                    " bytes exceeds the deliverable maximum of " + std::to_string(kMaxProposalBytes) +
                                    " bytes for group " + std::to_string(groupId_));
    const uint64_t tEnter = profileEnabled() ? nowNs() : 0;
    // Register the waiter INSIDE the lock (mirroring readBarrier): capture the
    // proposed entry's index and register its promise before any drainReady can
    // observe it, so a leader flap cannot resolve or fail a half-created waiter.
    std::optional<seastar::future<bool>> fut;
    bool notLeader = false;
    co_await seastar::with_semaphore(lock_, 1,
                                     [this, data = std::move(data), &fut, &notLeader]() mutable -> seastar::future<> {
                                         const uint64_t tL0 = profileEnabled() ? nowNs() : 0;
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
                                         if (profileEnabled())
                                             g_prof.inLockNs += nowNs() - tL0;
                                     });
    if (notLeader)
        co_return false;
    // with_timeout does NOT cancel or abandon the inner future: it keeps it alive under a
    // then_wrapped and discards the late result. That is exactly what is needed here --
    // the waiter's promise stays registered in applyWaiters_ and a later apply resolves a
    // promise that is still valid, instead of one destroyed out from under drainReady.
    const bool ok = deadline ? co_await seastar::with_timeout(*deadline, std::move(*fut)) : co_await std::move(*fut);
    if (profileEnabled()) {
        g_prof.appliedNs += nowNs() - tEnter;
        ++g_prof.proposals;
        maybeReport();
    }
    co_return ok;
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

seastar::future<> RaftGroup::tick(unsigned passes) {
    return seastar::with_semaphore(lock_, 1, [this, passes]() -> seastar::future<> {
        node_.tick(passes);
        co_await drainReady();
    });
}

seastar::future<bool> RaftGroup::propose(std::string data) {
    if (data.size() > kMaxProposalBytes)
        return seastar::make_exception_future<bool>(ProposalTooLargeError(
            "raft: proposal of " + std::to_string(data.size()) + " bytes exceeds the deliverable maximum of " +
            std::to_string(kMaxProposalBytes) + " bytes for group " + std::to_string(groupId_)));
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
    // THE LAMBDA HERE IS DELIBERATELY NOT A COROUTINE, and this is not a style choice.
    //
    // `seastar::with_semaphore` moves `func` into a `.then` continuation and invokes it
    // there; that continuation -- and with it the CLOSURE -- is destroyed as soon as the
    // invocation returns a future, i.e. AT THE FIRST SUSPENSION. A coroutine lambda's frame
    // does not copy its captures; it holds a pointer to the closure. So any capture
    // (`this` included) read AFTER a `co_await` is read out of freed memory.
    //
    // Every other with_semaphore body in this file happens to be safe only because it
    // touches its captures EXCLUSIVELY BEFORE its single `co_await drainReady()` -- a
    // fragile property nothing enforces (filed as debt D-33). This body cannot be: it
    // suspends on persistSnapshot and then needs `persistence_` and `this` again. Written
    // as a coroutine lambda it read a destroyed closure and `RaftNode::ready()` copied a
    // garbage `Snapshot` out of it -- a std::bad_alloc on a vector with a nonsense length,
    // caught by the recovery test rather than by reasoning.
    return seastar::with_semaphore(lock_, 1, [this, upto, data = std::move(snapshotData)]() mutable {
        return compactLocked(upto, std::move(data));
    });
}

// Precondition: the caller holds lock_. A NAMED MEMBER COROUTINE, so `upto`,
// `snapshotData` and `this` live in this coroutine's own frame (see compact()).
seastar::future<> RaftGroup::compactLocked(LogIndex upto, std::string snapshotData) {
    const LogIndex before = node_.log().snapshotIndex();
    node_.compact(upto, std::move(snapshotData));
    // PERSIST THE PRODUCED SNAPSHOT (debt D-6). Found by the recovery test the moment the
    // producer had a caller at all.
    //
    // `RaftNode::compact` trims only the IN-MEMORY log. The journal is append-only, so with
    // no Snapshot record the boundary does not survive a restart: `recoverRaftState` sees
    // every Data record ever written and replays the ENTIRE history. Compaction would then
    // bound nothing and reclaim nothing -- exactly the unbounded replay D-6 exists to fix.
    //
    // PERSISTED BEFORE anything depends on the prefix being gone, and both crash windows
    // are safe either way: a crash after the record and before the in-memory trim recovers
    // AT the boundary (the payload is durable), and a crash before the record recovers with
    // the full log (no compaction, no loss).
    if (node_.log().snapshotIndex() > before && node_.servableSnapshot().index != kNoIndex) {
        co_await persistence_.persistSnapshot(node_.servableSnapshot());
        co_await persistence_.sync();
    }
    co_await drainReady();
}

}  // namespace timestar::raft
