#pragma once

#include "journal_record.hpp"
#include "journal_writer.hpp"

#include <cstdint>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <vector>

namespace timestar {

// What a Raft group's persistence layer appends into, and makes durable through.
//
// Two implementations, and which one is live is a DEPLOYMENT choice (debt D-10):
//
//   * DirectJournalSink -- one JournalWriter per VShard, `sync()` is that writer's
//     own barrier. This is what every cluster runs today and the DEFAULT. Each
//     group syncs alone, to its own fd, so the fsync count is one per drained
//     Ready per group.
//   * SharedShardJournal -- ONE JournalWriter per reactor shard, shared by every
//     group on it, with group-commit coalescing: many groups' pending appends are
//     made durable by ONE fdatasync. This is write-scaleout 5b, whose premise
//     ("coalesce the per-shard fsyncs") could not be implemented before because
//     there was no shared writer to coalesce onto (plan 5.3).
//
// The interface is deliberately the two calls the Raft durability path already
// makes, in the order it already makes them: append everything the Ready produced,
// then ONE sync() that must not resolve until those appends are durable.
class JournalSink {
public:
    virtual ~JournalSink() = default;

    // Buffer a record. The caller must keep `record` alive until the returned
    // future resolves.
    virtual seastar::future<> append(const JournalRecord& record) = 0;

    // Make every record THIS caller has already appended durable. Must not resolve
    // before an fdatasync covering them has COMPLETED -- see the ordering contract
    // on SharedShardJournal.
    virtual seastar::future<> sync() = 0;

    // fdatasync calls actually issued underneath this sink, and sync() requests
    // served. Their ratio is the coalescing factor: 1:1 for DirectJournalSink, and
    // >1 requests per fsync is the whole point of the shared journal. It is the
    // ONLY honest evidence available on a tmpfs box, where the elapsed-time win is
    // invisible by construction (plan 5.3, and its risks section).
    [[nodiscard]] virtual uint64_t fsyncs() const = 0;
    [[nodiscard]] virtual uint64_t syncRequests() const = 0;
};

// Today's behaviour, unchanged: this group's own writer, this group's own fsync.
class DirectJournalSink final : public JournalSink {
public:
    explicit DirectJournalSink(JournalWriter& writer) : writer_(writer) {}

    seastar::future<> append(const JournalRecord& record) override { return writer_.append(record); }
    seastar::future<> sync() override {
        ++syncRequests_;
        return writer_.barrier();
    }
    [[nodiscard]] uint64_t fsyncs() const override { return writer_.fsyncs(); }
    [[nodiscard]] uint64_t syncRequests() const override { return syncRequests_; }

private:
    JournalWriter& writer_;
    uint64_t syncRequests_ = 0;
};

// One shard's shared journal: a single JournalWriter that every group on the
// reactor appends into, with the engine WAL's group-commit shape over the top.
//
// ================== THE ORDERING CONTRACT (read this before editing) =========
//
// The invariant that must hold, from RaftGroup::drainReady: a group may send
// messages, apply to its state machine, or resolve an apply waiter (i.e. ack a
// client) ONLY AFTER an fdatasync covering ITS OWN entries has completed.
//
// WHAT IS APPENDED. A group appends the whole of one Ready -- snapshot, hard
// state, entries -- and only then calls sync() once. Every append is AWAITED, so
// when a group reaches sync() its bytes are already in the writer's buffer. They
// cannot be anywhere else: `append` returns only after `tail_.append(bytes)` has
// run.
//
// WHEN THE SYNC FIRES. sync() enqueues a promise in `pending_` and, if no round
// is running, starts the round loop. A round:
//    1. TAKES the whole `pending_` set (swap) -- these are the waiters it will
//       satisfy;
//    2. acquires `ioLock_` and calls writer_.barrier(), which flushes everything
//       buffered and fdatasyncs;
//    3. resolves every promise in the set, and only then looks for more.
//
// HOW A GROUP LEARNS ITS ENTRIES ARE COVERED. It does not need to: coverage is
// structural. barrier() is a whole-buffer flush, not a ranged one, so it makes
// durable EVERYTHING appended before it ran. A waiter is in the round's set only
// if it called sync() -- which is only after its appends completed -- so its bytes
// were in the buffer strictly before the barrier began. The barrier therefore
// covers every waiter it will resolve. It also covers bytes belonging to groups
// that have not asked yet; over-covering is free and is the coalescing.
//
// WHY NO PATH CAN SEND OR ACK BEFORE ITS COVERING SYNC.
//   * drainReady awaits sync() before its send / apply / ack steps, and that is
//     unchanged by this class. So the question reduces to "can sync() resolve
//     early?".
//   * A promise is resolved in exactly one place: after `co_await
//     writer_.barrier()` has RETURNED inside the round that took it. There is no
//     other set_value.
//   * Between taking the set and entering the barrier we may suspend (acquiring
//     ioLock_). That is safe in the only direction that matters: a suspension can
//     only let MORE bytes into the buffer, never remove a waiter's bytes from it.
//     Nothing ever un-appends.
//   * Appends and barriers are mutually exclusive under `ioLock_`. This is
//     REQUIRED, not hygiene: JournalWriter::barrier() computes its aligned/tail
//     split, does several dma_writes, and only afterwards mutates alignedLen_ and
//     erases the flushed prefix from tail_. An append landing inside that window
//     would be written at an offset computed before it existed, and then erased as
//     though it had been. Likewise append() can ROTATE (seal + open a new
//     segment), which a concurrent append or barrier would be operating across.
//   * A barrier that throws FENCES the writer. The round fails every waiter in it
//     with that exception, and every later append/sync throws too -- so a group
//     whose sync failed can never ack. drainReady propagates, which stops that
//     group. The blast radius is wider than the per-VShard sink (all groups on the
//     shard, not one), and that is the correct direction: a shard that cannot
//     fdatasync must not ack anything.
//
// WHAT THIS CLASS DOES NOT SOLVE. Segment RETENTION. With one journal per shard a
// sealed segment holds records from many groups and can only be reclaimed once
// EVERY group's boundary has passed it (or its stragglers are copied forward).
// `JournalRetention`/`JournalGc` implement exactly that rule and have no caller
// (debt D-34) -- in either layout, so nothing regresses here, but the shared
// layout is the one those bricks were written for. See D-10's row in
// docs/write-scaleout-plan.md.
// ============================================================================
class SharedShardJournal final : public JournalSink {
public:
    explicit SharedShardJournal(JournalWriter& writer) : writer_(writer) {}

    seastar::future<> append(const JournalRecord& record) override;
    seastar::future<> sync() override;

    // Drain any in-flight round. MUST be awaited before the writer is closed: a
    // round holds a reference to it.
    seastar::future<> stop();

    [[nodiscard]] uint64_t fsyncs() const override { return writer_.fsyncs(); }
    [[nodiscard]] uint64_t syncRequests() const override { return syncRequests_; }
    // Waiters served by the largest single round so far -- the coalescing factor at
    // its peak, which is what "one fsync served N groups" means concretely.
    [[nodiscard]] uint64_t maxRoundWaiters() const { return maxRoundWaiters_; }
    [[nodiscard]] uint64_t rounds() const { return rounds_; }

private:
    // The round loop. A NAMED MEMBER COROUTINE, never a coroutine lambda handed to
    // with_gate/with_semaphore: such a lambda's frame points at a closure those
    // helpers destroy at the first suspension, which is how this tree has already
    // lost two afternoons (raft_group.cpp, replicated_vshard_host.cpp).
    seastar::future<> runRounds();

    JournalWriter& writer_;
    seastar::semaphore ioLock_{1};  // append XOR barrier; see the contract above
    std::vector<seastar::promise<>> pending_;
    seastar::gate gate_;
    bool roundLoopRunning_ = false;
    uint64_t syncRequests_ = 0;
    uint64_t rounds_ = 0;
    uint64_t maxRoundWaiters_ = 0;
};

}  // namespace timestar
