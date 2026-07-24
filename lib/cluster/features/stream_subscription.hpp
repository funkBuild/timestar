#pragma once

#include "../raft/raft_types.hpp"  // Term

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::features {

using timestar::raft::Term;

// One replicated event delivered to a cluster-aware subscription (docs/
// clustering.md §"Streaming subscriptions"). It carries its origin so the client
// can DEDUP and RESUME: (vshard, term, commit index) is the resume token, and the
// commit index is the exactly-once dedup key on an at-least-once channel.
struct StreamEvent {
    uint16_t vshard = 0;
    Term term = 0;
    uint64_t index = 0;  // Raft commit index -- the dedup / resume key
    std::string opId;    // operation id (trace/correlation only)
    std::string payload;
    friend bool operator==(const StreamEvent&, const StreamEvent&) = default;
};

// A stream saw a commit position it cannot reconcile -- an event from a strictly
// OLDER term than one already delivered (a stale/rogue source). Fail closed.
class StreamRegression : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Per-(subscription, VShard) delivery cursor. It turns the at-least-once channel
// into exactly-once by deduping on commit position, and it IS the resume token.
// Raft commit indices are monotonic across terms, so a term advance (new leader
// after a placement change) keeps deduping by index and simply re-registers from
// the last delivered position; a term REGRESSION is rejected as stale.
class SubscriptionCursor {
public:
    SubscriptionCursor() = default;
    // Resume from a client-supplied token (§"resumable event IDs"): the last
    // (term, index) the client durably observed. Any re-sent event at or below
    // this position is deduped, so a resumed subscription never re-delivers and a
    // rogue/stale first event below the resume point cannot poison the cursor.
    SubscriptionCursor(Term resumeTerm, uint64_t resumeIndex)
        : term_(resumeTerm), lastIndex_(resumeIndex), started_(true) {}

    // True if `e` should be DELIVERED (first sight), false if it is a DUPLICATE
    // (already delivered, drop it). Throws StreamRegression on an older-term event.
    //
    // CALLER CONTRACT for the no-loss guarantee: events must arrive in commit-index
    // order (the authoritative leader delivers them so). Dedup is by MAX delivered
    // index, so an out-of-order event below the max would be dropped as a
    // "duplicate" -- correct under in-order delivery, a loss under a reordering
    // source. In-order delivery makes the resume position exactly the last
    // contiguous position, so nothing in the covered range is skipped.
    bool accept(const StreamEvent& e) {
        if (started_ && e.term < term_)
            throw StreamRegression("stream: event from an older term than already delivered");
        if (e.term > term_)
            term_ = e.term;  // new leader; commit indices remain monotonic
        if (started_ && e.index <= lastIndex_)
            return false;  // duplicate by commit position
        started_ = true;
        lastIndex_ = e.index;
        return true;
    }

    bool started() const { return started_; }
    uint64_t lastIndex() const { return lastIndex_; }
    Term term() const { return term_; }

private:
    Term term_ = 0;
    uint64_t lastIndex_ = 0;
    bool started_ = false;
};

// The backfill -> live transition at ONE barrier (the gate: "events are neither
// lost nor silently duplicated"). The subscription starts BUFFERING live events
// BEFORE the barrier index is taken, so nothing between the barrier and "go live"
// is missed; then it delivers the backfill (committed state through the barrier)
// followed by the buffered+ongoing live events, with the cursor dropping the
// overlap. Because backfill covers (resumeFrom, barrier] and live covers
// everything after the buffer start (<= barrier and beyond), their union is a
// contiguous index range with no gap, and dedup removes the overlap.
class BackfillLiveStream {
public:
    // Deliver a backfill batch (events with index in (cursor.lastIndex(), barrier],
    // in commit order) then a live batch (events buffered since before the barrier,
    // indices may straddle the barrier, in commit order). Returns the events
    // actually delivered (deduped, in order). No event with index <= barrier is
    // ever delivered twice, and no index in the covered range is skipped.
    static std::vector<StreamEvent> deliver(SubscriptionCursor& cursor, uint64_t barrier,
                                            const std::vector<StreamEvent>& backfill,
                                            const std::vector<StreamEvent>& live) {
        std::vector<StreamEvent> out;
        // Backfill first: the committed state through the barrier.
        for (const auto& e : backfill) {
            if (e.index > barrier)
                continue;  // never deliver a post-barrier event on the backfill path
            if (cursor.accept(e))
                out.push_back(e);
        }
        // Then the live tail; the cursor drops anything the backfill already covered.
        for (const auto& e : live) {
            if (cursor.accept(e))
                out.push_back(e);
        }
        return out;
    }
};

}  // namespace timestar::features
