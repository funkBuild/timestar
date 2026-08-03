#include "raft_group.hpp"

#include "../../utils/logger.hpp"

#include <algorithm>
#include <exception>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <stdexcept>

namespace timestar::raft {

// THE GROUP LOCK IS TAKEN WITH `get_units`, NEVER WITH `with_semaphore` -- and that is a
// correctness rule, not a style one (debt D-33).
//
// `seastar::with_semaphore` moves `func` into a `.then` continuation and invokes it
// there; that continuation -- and with it the CLOSURE -- is destroyed as soon as the
// invocation returns a future, i.e. AT THE FIRST SUSPENSION. A coroutine lambda's frame
// does not copy its captures, it holds a POINTER to the closure, so every capture
// (`this` included) read after a `co_await` is read out of freed memory.
//
// Of the ten bodies this file used to hold, EIGHT suspended under the lock --
// `co_await drainReady()` is the whole point of them -- and each survived only because
// it touched its captures EXCLUSIVELY BEFORE that single suspension: a property nothing
// stated, nothing tested, and any edit could break. (The remaining two were exempt for
// reasons of their own, both accidental: `compact()` had already been rewritten as a
// plain lambda delegating to a named coroutine, and `waitApplied`'s body never awaited
// at all -- it was a coroutine lambda only by virtue of `co_return`, so its closure
// could not die under it.) `compact()` is what proved the hazard real: a second
// suspension made `RaftNode::ready()` copy a garbage `Snapshot` out of the dead closure
// (std::bad_alloc on a vector with a nonsense length, D-6).
//
// `auto units = co_await seastar::get_units(lock_, 1);` has no closure at all. The
// method IS the coroutine, so its parameters, its locals and the units themselves live
// in the frame that awaits, and the frame outlives every suspension by construction.
// `units` releases the lock when it leaves scope -- including on an exception and on an
// early `co_return` -- so the three methods that must wait OUTSIDE the lock
// (`proposeAndAwaitApplied`, `readBarrier`, `waitApplied`) scope it in an explicit block
// and await their waiter after it. Adding a suspension anywhere here is now safe; the
// only rule left is not to hold `units` across a wait for another group's work.
//
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

RaftGroup::~RaftGroup() {
    if (uncommittedProposalBudget_)
        uncommittedProposalBudget_->update(groupId_, 0);
}

seastar::gate::holder RaftGroup::holdOperation() {
    ensureActive();
    return operationGate_.hold();
}

void RaftGroup::ensureActive() const {
    if (retiring_)
        throw std::runtime_error("raft: group " + std::to_string(groupId_) + " is retired");
}

seastar::future<> RaftGroup::retire() {
    if (retiring_)
        co_return;
    retiring_ = true;
    {
        auto units = co_await seastar::get_units(lock_, 1);
        auto failure = std::make_exception_ptr(std::runtime_error("raft: group retired"));
        for (auto& [ctx, promise] : readWaiters_)
            promise.set_exception(failure);
        readWaiters_.clear();
        confirmedReads_.clear();
        for (auto& [index, promise] : applyWaiters_)
            promise.set_exception(failure);
        applyWaiters_.clear();
        for (auto& waiter : configWaiters_)
            waiter.promise.set_exception(failure);
        configWaiters_.clear();
        for (auto& [index, promise] : appliedWaiters_)
            promise.set_exception(failure);
        appliedWaiters_.clear();
        if (uncommittedProposalBudget_)
            uncommittedProposalBudget_->update(groupId_, 0);
    }
    co_await operationGate_.close();
}

void RaftGroup::syncUncommittedBudget() {
    if (uncommittedProposalBudget_)
        uncommittedProposalBudget_->update(groupId_, node_.uncommittedLogBytes());
}

void RaftGroup::requireUncommittedBudget(size_t payloadBytes) {
    if (!uncommittedProposalBudget_ ||
        uncommittedProposalBudget_->allows(groupId_, estimatedLogEntryBytes(payloadBytes)))
        return;
    throw ProposalBudgetExceededError("raft: uncommitted proposal budget is full for group " +
                                      std::to_string(groupId_) + " (group " +
                                      std::to_string(uncommittedProposalBudget_->groupCurrent(groupId_)) + "/" +
                                      std::to_string(uncommittedProposalBudget_->perGroupLimit()) + " bytes, shard " +
                                      std::to_string(uncommittedProposalBudget_->current()) + "/" +
                                      std::to_string(uncommittedProposalBudget_->limit()) + " bytes)");
}

seastar::future<> RaftGroup::drainReady() {
    // Precondition: caller holds lock_. Drain every pending Ready in order.
    while (node_.hasReady()) {
        RaftNode::Ready rd = node_.ready();

        // 1. Make durable BEFORE anything observable. Append snapshot first (it
        //    may supersede the log), then hard state, then the new log entries,
        //    then one sync() -- a single fsync makes the whole Ready durable.
        bool persisted = false;
        if (rd.snapshot) {
            co_await persistence_.persistSnapshot(*rd.snapshot, /*receivedFromPeer=*/true);
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
        for (auto& m : rd.messages) {
            if (auto* snapshot = std::get_if<InstallSnapshot>(&m.payload))
                co_await persistence_.hydrateSnapshotChunk(*snapshot);
            co_await transport_.send(Envelope{groupId_, m});
        }
        if (profileEnabled())
            g_prof.sendNs += nowNs() - tS0;

        // 3. Apply committed output to the state machine (snapshot install first).
        if (rd.snapshot) {
            // MOVE, not copy (debt D-32). `applySnapshot` takes its Snapshot by value, so
            // an lvalue here duplicated the whole payload -- on the receiver, where two
            // copies (`snapshot_` and `pendingSnapshotApply_`) are already resident. This
            // is the payload's LAST use in this iteration: the index is read first, and
            // `advance` only tests whether rd.snapshot is engaged, which a moved-from
            // optional still is. It must NOT be moved into `persistSnapshot` above --
            // that one runs first and this one needs the bytes.
            const uint64_t snapIndex = rd.snapshot->index;
            co_await sm_.applySnapshot(std::move(*rd.snapshot));
            appliedIndex_ = std::max<uint64_t>(appliedIndex_, snapIndex);
        }
        const uint64_t tA0 = profileEnabled() ? nowNs() : 0;
        for (auto& e : rd.committed) {
            if (e.type == EntryType::Normal && !e.data.empty()) {
                // COUNT THE THROW, then let it out unchanged (debt D-36). Apply is
                // fail-stop by contract -- the entry must not be marked applied and
                // `node_.advance(rd)` below must not run -- but a stall that nothing
                // counts is indistinguishable from data loss from outside the process,
                // which is exactly the ambiguity D-36 was filed on.
                try {
                    co_await sm_.apply(e);
                } catch (const std::exception& ex) {
                    // RATE-LIMITED, and it has to be: the failure that motivated this
                    // counter fired 20,851 times in one restart. First occurrence, then
                    // every 1024th, so the REASON is always in the log and the log is
                    // never the reason the node is slow.
                    if (applyFailures_ % 1024 == 0)
                        timestar::http_log.warn(
                            "raft: group {} could not apply committed entry {} ({}); the entry is DURABLE and will be "
                            "retried, but every point in it is unreadable until it applies (debt D-36, occurrence {})",
                            groupId_, e.index, ex.what(), applyFailures_ + 1);
                    ++applyFailures_;
                    throw;
                } catch (...) {
                    ++applyFailures_;
                    throw;
                }
            }
            appliedIndex_ = std::max<uint64_t>(appliedIndex_, e.index);
        }
        if (profileEnabled())
            g_prof.applyNs += nowNs() - tA0;

        // Record newly-confirmed read barriers, then release any whose ReadIndex
        // we have now applied through.
        for (const auto& rs : rd.readStates)
            if (readWaiters_.contains(rs.context))
                confirmedReads_[rs.context] = rs.readIndex;
        releaseReadBarriers();
        // Resolve write waiters whose entry we have now applied (or fail them all
        // if we just lost leadership).
        releaseApplyWaiters();
        releaseConfigWaiters();
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

void RaftGroup::releaseConfigWaiters() {
    for (auto it = configWaiters_.begin(); it != configWaiters_.end();) {
        if (appliedIndex_ >= it->index) {
            if (node_.config() == it->expected) {
                it->promise.set_value(true);
            } else {
                it->promise.set_exception(std::make_exception_ptr(
                    LeadershipLostError("membership proposal was superseded before its final config applied")));
            }
            it = configWaiters_.erase(it);
        } else if (!node_.isLeader()) {
            it->promise.set_exception(
                std::make_exception_ptr(LeadershipLostError("membership leadership lost before final config")));
            it = configWaiters_.erase(it);
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
    auto operation = holdOperation();
    // Register the waiter under the lock so no concurrent drainReady observes a
    // half-created waiter. Resolve immediately if we have already applied through
    // `index` (common for a caught-up replica). No leadership requirement.
    std::optional<seastar::future<>> fut;
    {
        auto units = co_await seastar::get_units(lock_, 1);
        ensureActive();
        if (appliedIndex_ >= index) {
            fut = seastar::make_ready_future<>();
        } else {
            seastar::promise<> promise;
            fut = promise.get_future();
            appliedWaiters_.emplace_back(index, std::move(promise));
        }
    }  // lock released here -- the wait below must NOT hold it
    co_await std::move(*fut);
}

seastar::future<bool> RaftGroup::proposeAndAwaitApplied(std::string data) {
    return proposeAndAwaitApplied(std::move(data), std::nullopt);
}

seastar::future<bool> RaftGroup::proposeAndAwaitApplied(std::string data,
                                                        std::optional<seastar::lowres_clock::time_point> deadline) {
    auto operation = holdOperation();
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
    LogIndex waiterIndex = kNoIndex;
    {
        auto units = co_await seastar::get_units(lock_, 1);
        ensureActive();
        const uint64_t tL0 = profileEnabled() ? nowNs() : 0;
        // The absolute deadline also covers time queued for this lock. Refuse
        // before append if it has already expired; appending and only then
        // timing out would consume the uncommitted-tail budget for work whose
        // caller can no longer be waiting for it.
        if (deadline && seastar::lowres_clock::now() >= *deadline)
            throw seastar::timed_out_error{};
        if (node_.isLeader() && !node_.transferInFlight())
            requireUncommittedBudget(data.size());
        if (!node_.propose(std::move(data)))
            co_return false;  // not the leader (units released by the frame's unwind)
        syncUncommittedBudget();
        const LogIndex idx = node_.log().lastIndex();  // the entry we just appended
        waiterIndex = idx;
        seastar::promise<bool> promise;
        fut = promise.get_future();
        applyWaiters_.emplace_back(idx, std::move(promise));
        // GROUP COMMIT. drainReady() makes the whole pending Ready durable with ONE
        // fsync, so when several writes are queued on this group it is far cheaper to
        // let them all append first and flush once than to fsync per proposal. If
        // callers are already waiting on lock_, skip our drain: each of them appends in
        // turn and the LAST one (which sees no waiters) flushes everyone's entries
        // together.
        //
        // Safety: skipping leaves the entry in the IN-MEMORY log only -- nothing is
        // persisted, sent to peers, or applied -- so the "durable before observable"
        // ordering is untouched. Progress is guaranteed because tick() and step() also
        // drain under this same lock, so a deferred entry is flushed within one tick at
        // worst. Latency is unaffected in practice: the proposal has to await a quorum
        // round trip regardless, and the deferral only lasts until the already-queued
        // callers finish appending.
        if (lock_.waiters() == 0) {
            try {
                co_await drainReady();  // may commit+apply (single voter) and resolve
            } catch (...) {
                // The caller receives this ambiguous drain failure directly;
                // retaining its now-unreachable waiter would leak until a
                // later apply or leadership change (which may never arrive).
                auto waiter = std::find_if(applyWaiters_.begin(), applyWaiters_.end(),
                                           [waiterIndex](const auto& item) { return item.first == waiterIndex; });
                if (waiter != applyWaiters_.end())
                    applyWaiters_.erase(waiter);
                // Erasure breaks the promise. This coroutine still owns the
                // otherwise unreachable future, so consume that expected
                // broken-promise result before propagating the real drain error.
                if (fut->available())
                    fut->ignore_ready_future();
                throw;
            }
        }
        if (profileEnabled())
            g_prof.inLockNs += nowNs() - tL0;
    }  // lock released here -- the apply wait below must NOT hold it
    bool ok;
    if (!deadline) {
        ok = co_await std::move(*fut);
    } else {
        std::exception_ptr failure;
        try {
            ok = co_await seastar::with_timeout(*deadline, std::move(*fut));
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            // The waiter list and drainReady are both serialized by lock_. A
            // timeout therefore removes exactly this entry without racing a
            // concurrent apply. with_timeout still owns/observes the inner
            // future, so destroying the promise here produces a consumed broken
            // promise rather than an abandoned exceptional future.
            auto units = co_await seastar::get_units(lock_, 1);
            auto waiter = std::find_if(applyWaiters_.begin(), applyWaiters_.end(),
                                       [waiterIndex](const auto& item) { return item.first == waiterIndex; });
            if (waiter != applyWaiters_.end())
                applyWaiters_.erase(waiter);
            units.return_all();
            std::rethrow_exception(failure);
        }
    }
    if (profileEnabled()) {
        g_prof.appliedNs += nowNs() - tEnter;
        ++g_prof.proposals;
        maybeReport();
    }
    co_return ok;
}

seastar::future<LogIndex> RaftGroup::readBarrier(std::optional<seastar::lowres_clock::time_point> deadline) {
    auto operation = holdOperation();
    // Register the waiter INSIDE the lock (with requestReadIndex), so no
    // concurrent drainReady can observe/fail a half-created waiter and so a
    // leader->follower->leader flap between here and acquiring the lock cannot
    // spuriously fail a barrier we could still satisfy.
    std::optional<seastar::future<LogIndex>> fut;
    uint64_t waiterContext = 0;
    {
        auto units = co_await seastar::get_units(lock_, 1);
        ensureActive();
        if (deadline && seastar::lowres_clock::now() >= *deadline)
            throw seastar::timed_out_error{};
        if (node_.isLeader()) {  // otherwise fut stays empty -> not leader
            const uint64_t ctx = nextReadCtx_++;
            waiterContext = ctx;
            seastar::promise<LogIndex> promise;
            fut = promise.get_future();
            readWaiters_.emplace(ctx, std::move(promise));
            node_.requestReadIndex(ctx);
            try {
                co_await drainReady();  // heartbeats out; confirmation arrives on later steps
            } catch (...) {
                node_.cancelReadIndex(ctx);
                readWaiters_.erase(ctx);
                confirmedReads_.erase(ctx);
                if (fut->available())
                    fut->ignore_ready_future();
                throw;
            }
        }
    }  // lock released here -- the barrier wait below must NOT hold it
    if (!fut)
        throw std::runtime_error("readBarrier: not leader");  // caller redirects to the leader
    if (!deadline)
        co_return co_await std::move(*fut);

    std::exception_ptr failure;
    try {
        co_return co_await seastar::with_timeout(*deadline, std::move(*fut));
    } catch (...) {
        failure = std::current_exception();
    }
    // ReadIndex completions and waiter removal are serialized by lock_. The
    // timed wrapper still observes the inner broken-promise result, so erasing
    // an unresolved promise here cannot create an abandoned exceptional future.
    auto units = co_await seastar::get_units(lock_, 1);
    node_.cancelReadIndex(waiterContext);
    readWaiters_.erase(waiterContext);
    confirmedReads_.erase(waiterContext);
    units.return_all();
    std::rethrow_exception(failure);
}

seastar::future<> RaftGroup::step(Message m) {
    if (retiring_)
        co_return;
    auto operation = operationGate_.hold();
    auto units = co_await seastar::get_units(lock_, 1);
    if (retiring_)
        co_return;
    if (auto* snapshot = std::get_if<InstallSnapshot>(&m.payload)) {
        // Do no disk work for a message the deterministic core will reject
        // before snapshot handling. Without these two cheap fences, a stale or
        // already-committed peer could fill the staging directory even though
        // Raft would immediately answer without installing its bytes.
        if (snapshot->term >= node_.currentTerm() && snapshot->lastIncludedIndex > node_.commitIndex())
            co_await persistence_.stageSnapshotChunk(*snapshot);
    }
    try {
        node_.step(std::move(m));
    } catch (...) {
        syncUncommittedBudget();
        throw;
    }
    syncUncommittedBudget();
    co_await drainReady();
}

seastar::future<> RaftGroup::tick(unsigned passes) {
    if (retiring_)
        co_return;
    auto operation = operationGate_.hold();
    auto units = co_await seastar::get_units(lock_, 1);
    if (retiring_)
        co_return;
    node_.tick(passes);
    co_await drainReady();
}

seastar::future<bool> RaftGroup::propose(std::string data) {
    auto operation = holdOperation();
    // Refused BEFORE the lock, as it always was: the throw becomes this coroutine's
    // exceptional future, which is what `make_exception_future` produced here before.
    if (data.size() > kMaxProposalBytes)
        throw ProposalTooLargeError("raft: proposal of " + std::to_string(data.size()) +
                                    " bytes exceeds the deliverable maximum of " + std::to_string(kMaxProposalBytes) +
                                    " bytes for group " + std::to_string(groupId_));
    auto units = co_await seastar::get_units(lock_, 1);
    ensureActive();
    if (node_.isLeader() && !node_.transferInFlight())
        requireUncommittedBudget(data.size());
    const bool ok = node_.propose(std::move(data));
    if (ok)
        syncUncommittedBudget();
    co_await drainReady();
    co_return ok;
}

seastar::future<> RaftGroup::campaign() {
    auto operation = holdOperation();
    auto units = co_await seastar::get_units(lock_, 1);
    ensureActive();
    node_.campaign();
    syncUncommittedBudget();
    co_await drainReady();
}

seastar::future<bool> RaftGroup::proposeConfChange(std::vector<NodeId> voters, std::vector<NodeId> learners) {
    auto operation = holdOperation();
    auto units = co_await seastar::get_units(lock_, 1);
    ensureActive();
    const bool ok = node_.proposeConfChange(std::move(voters), std::move(learners));
    if (ok)
        syncUncommittedBudget();
    co_await drainReady();
    co_return ok;
}

seastar::future<bool> RaftGroup::proposeConfChangeAndAwaitApplied(
    std::vector<NodeId> voters, std::vector<NodeId> learners,
    std::optional<seastar::lowres_clock::time_point> deadline) {
    auto operation = holdOperation();
    Config expected{voters, /*votersOutgoing=*/{}, learners};
    std::optional<seastar::future<bool>> jointApplied;
    LogIndex jointIndex = kNoIndex;
    {
        auto units = co_await seastar::get_units(lock_, 1);
        ensureActive();
        const LogIndex before = node_.log().lastIndex();
        if (!node_.proposeConfChange(std::move(voters), std::move(learners)))
            co_return false;
        syncUncommittedBudget();
        // The one-voter fast path may commit the joint entry and synchronously
        // append final Cnew inside proposeConfChange(). Sampling lastIndex AFTER
        // the call therefore sometimes names the final entry, making the waiter
        // later reject a successful change because finalIndex <= jointIndex.
        // The proposed joint entry is exactly the next index from the pre-call
        // tail, regardless of whether the core also appended Cnew.
        jointIndex = before + 1;
        seastar::promise<bool> promise;
        jointApplied = promise.get_future();
        applyWaiters_.emplace_back(jointIndex, std::move(promise));
        try {
            co_await drainReady();
        } catch (...) {
            // The proposal outcome is ambiguous, but the caller is already
            // receiving the drain failure. Do not retain a second, unreachable
            // waiter when persistence, transport, or apply throws here.
            auto waiter = std::find_if(applyWaiters_.begin(), applyWaiters_.end(),
                                       [jointIndex](const auto& item) { return item.first == jointIndex; });
            if (waiter != applyWaiters_.end())
                applyWaiters_.erase(waiter);
            if (jointApplied->available())
                jointApplied->ignore_ready_future();
            throw;
        }
    }

    // The core appends final Cnew synchronously when the joint entry commits.
    // Waiting for the joint entry first gives this wrapper the final entry's
    // stable index without polling or guessing.
    if (!deadline) {
        co_await std::move(*jointApplied);
    } else {
        std::exception_ptr failure;
        try {
            co_await seastar::with_timeout(*deadline, std::move(*jointApplied));
        } catch (...) {
            failure = std::current_exception();
        }
        if (failure) {
            auto units = co_await seastar::get_units(lock_, 1);
            auto waiter = std::find_if(applyWaiters_.begin(), applyWaiters_.end(),
                                       [jointIndex](const auto& item) { return item.first == jointIndex; });
            if (waiter != applyWaiters_.end())
                applyWaiters_.erase(waiter);
            units.return_all();
            std::rethrow_exception(failure);
        }
    }

    std::optional<seastar::future<bool>> finalApplied;
    LogIndex finalIndex = kNoIndex;
    {
        auto units = co_await seastar::get_units(lock_, 1);
        ensureActive();
        finalIndex = node_.latestConfigIndex();
        if (node_.config().joint() || node_.config() != expected || finalIndex <= jointIndex)
            throw LeadershipLostError("membership proposal lost leadership before final config was appended");
        if (appliedIndex_ >= finalIndex)
            co_return true;
        if (!node_.isLeader())
            throw LeadershipLostError("membership leadership lost before final config applied");
        seastar::promise<bool> promise;
        finalApplied = promise.get_future();
        configWaiters_.push_back(ConfigApplyWaiter{finalIndex, std::move(expected), std::move(promise)});
    }
    if (!deadline)
        co_return co_await std::move(*finalApplied);

    std::exception_ptr failure;
    bool ok = false;
    try {
        ok = co_await seastar::with_timeout(*deadline, std::move(*finalApplied));
    } catch (...) {
        failure = std::current_exception();
    }
    if (failure) {
        auto units = co_await seastar::get_units(lock_, 1);
        auto waiter = std::find_if(configWaiters_.begin(), configWaiters_.end(),
                                   [finalIndex](const ConfigApplyWaiter& item) { return item.index == finalIndex; });
        if (waiter != configWaiters_.end())
            configWaiters_.erase(waiter);
        units.return_all();
        std::rethrow_exception(failure);
    }
    co_return ok;
}

seastar::future<bool> RaftGroup::transferLeadership(NodeId target, bool* armed) {
    auto operation = holdOperation();
    auto units = co_await seastar::get_units(lock_, 1);
    ensureActive();
    // Propagated, not discarded (debt D-24): the balancer's transfers_initiated counter is
    // only honest if it counts transfers that were actually ARMED, and this is the only
    // layer that knows. Ready is drained either way -- a call that started nothing may
    // still have output pending from something else.
    const bool started = node_.transferLeadership(target);
    // Published BEFORE the drain, which persists and sends and can throw: the arming
    // already happened inside the core and survives the failure, so the caller's record of
    // it must survive too. See the header.
    if (armed)
        *armed = started;
    co_await drainReady();
    co_return started;
}

seastar::future<> RaftGroup::compact(LogIndex upto, std::string snapshotData) {
    return compactImpl(upto, std::move(snapshotData), {});
}

seastar::future<> RaftGroup::compact(LogIndex upto, SnapshotFilePtr snapshotFile) {
    return compactImpl(upto, {}, std::move(snapshotFile));
}

seastar::future<> RaftGroup::compactImpl(LogIndex upto, std::string snapshotData, SnapshotFilePtr snapshotFile) {
    auto operation = holdOperation();
    auto units = co_await seastar::get_units(lock_, 1);
    ensureActive();
    const LogIndex before = node_.log().snapshotIndex();
    if (snapshotFile)
        node_.compact(upto, std::move(snapshotFile));
    else
        node_.compact(upto, std::move(snapshotData));
    syncUncommittedBudget();
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
        co_await persistence_.persistSnapshot(node_.servableSnapshot(), /*receivedFromPeer=*/false);
        // RE-PERSIST THE CURRENT HARD STATE (debt D-34). Not redundant, and not for this
        // group's benefit: it is what lets the journal's segment GC actually reclaim
        // anything. A HardState record is written only when the term or vote CHANGES, so
        // a group with stable leadership has exactly ONE, from startup -- and that record
        // is needed forever, which pins every segment at or after it and makes the
        // snapshot boundary reclaim nothing at all. One ~40-byte record moves the pin up
        // to the new boundary. Replaying it is idempotent: same term, same vote, and
        // recoverRaftState simply overwrites what an earlier identical record set.
        co_await persistence_.persistHardState(node_.hardState());
        co_await persistence_.sync();
    }
    co_await drainReady();
}

}  // namespace timestar::raft
