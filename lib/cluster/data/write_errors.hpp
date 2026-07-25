#pragma once

#include "../raft/raft_types.hpp"  // NodeId, kNoNode, LeadershipLostError

#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>

namespace timestar::data {

using timestar::raft::kNoNode;
using timestar::raft::NodeId;

// ---------------------------------------------------------------------------
// The replicated write path's failure TAXONOMY (write-scaleout 3b).
//
// It is deliberately an enum and not a boolean "retryable?" flag. Phase 4 (reconnect
// jitter, backoff-window retry, fault injection, connection health) has to give a
// dead-connection blip a different policy from a stale leader -- a longer window, a
// different delay -- and that is a change to the POLICY table below, not to the retry
// loop. A boolean would have to be widened at every call site instead.
//
// AMBIGUITY is the property that matters for correctness, and it is recorded per class:
//
//   * `NotLeader`     -- the target refused the proposal outright (`propose()` returned
//                        false). Nothing was appended to any log. UNAMBIGUOUS.
//   * `ShardStopping` -- the slice was turned away by a shard tearing itself down before
//                        it reached Raft (kShardStoppingError). UNAMBIGUOUS.
//   * `Overloaded`    -- refused by an admission bound (this node's in-flight byte gate,
//                        or a peer's rpc::resource_limits) BEFORE any handler ran.
//                        UNAMBIGUOUS.
//   * `LeadershipLost`-- the entry WAS appended here and then this node stopped leading.
//                        The successor may or may not commit it. AMBIGUOUS.
//   * `Transport`     -- an RPC failed. If the frame was already on the wire the peer may
//                        have committed it. AMBIGUOUS.
//
// Re-proposing an AMBIGUOUS slice is safe, and the audit is worth stating in full
// because the plan (§6) calls it out as a risk:
//
//   A point's revision is the Raft LOG INDEX of the entry that carried it, stamped at
//   APPLY (ADR 0003, EngineDataStateMachine::apply). A retried slice that in fact
//   committed the first time therefore applies a SECOND time under a HIGHER index, so
//   it IS "double-stamped" -- but with byte-identical values, so the LWW winner's VALUE
//   is unchanged and no reader can observe the difference.
//
//   The one observable consequence is ordering: if a third party wrote the SAME
//   (series, timestamp) in between, the retry's higher revision wins and that write is
//   overwritten. That is a legal linearization, not a lost ack: the retried write had
//   not returned to its client when the interleaving write committed, so the two
//   operations OVERLAP in real time and either order is correct. An acked-then-reverted
//   sequence is impossible, because a slice that returned success is never retried.
//
//   Deletes and retention cutoffs are NOT re-proposed by this machinery (the retry lives
//   in the WriteBatch router only), so the non-idempotent commands never see it.
//
//   FOR WHOEVER REPLICATES DELETES (M6): that immunity is incidental, not designed. A
//   delete is not idempotent against a concurrent write to the same point -- re-applying
//   one at a HIGHER revision would erase a write that legitimately landed after the
//   original delete committed, resurrecting nothing and losing real data. Today
//   DeleteRangeKey never reaches this retry loop because deletes are not routed through
//   Raft on the write path at all. The moment they are, this audit must be redone: either
//   deletes get revision-bounded tombstones (so a re-apply cannot outrank a later write),
//   or they must be excluded from the ambiguous-retry classes explicitly.
//
// `Unassigned` and `Fatal` are terminal: no amount of retrying fixes an unowned VShard
// or a journal I/O error, and hiding either behind a retry budget only delays the
// report.
enum class WriteFailure : uint8_t {
    None = 0,
    NotLeader,
    LeadershipLost,
    Transport,
    ShardStopping,
    Overloaded,
    Unassigned,
    Fatal,
};

// The retry POLICY table. Phase 4a extends this (and only this) to give a
// connection-backoff blip its own window.
constexpr bool isRetryableWriteFailure(WriteFailure f) {
    switch (f) {
        case WriteFailure::NotLeader:
        case WriteFailure::LeadershipLost:
        case WriteFailure::Transport:
        case WriteFailure::ShardStopping:
        case WriteFailure::Overloaded:
            return true;
        case WriteFailure::None:
        case WriteFailure::Unassigned:
        case WriteFailure::Fatal:
            return false;
    }
    return false;
}

// Whether the slice's outcome is UNKNOWN (the proposal may have committed). Recorded
// for the audit above and for reporting; it does not change the policy, because a
// re-applied WriteBatch is value-idempotent under LWW.
constexpr bool isAmbiguousWriteFailure(WriteFailure f) {
    return f == WriteFailure::LeadershipLost || f == WriteFailure::Transport;
}

constexpr const char* writeFailureName(WriteFailure f) {
    switch (f) {
        case WriteFailure::None:
            return "none";
        case WriteFailure::NotLeader:
            return "not-leader";
        case WriteFailure::LeadershipLost:
            return "leadership-lost";
        case WriteFailure::Transport:
            return "transport";
        case WriteFailure::ShardStopping:
            return "shard-stopping";
        case WriteFailure::Overloaded:
            return "overloaded";
        case WriteFailure::Unassigned:
            return "unassigned";
        case WriteFailure::Fatal:
            return "fatal";
    }
    return "?";
}

// ---------------------------------------------------------------------------
// Typed failures the HTTP layer maps to a status code. Before these existed every
// cluster write failure fell into http_write_handler's catch-all and became an opaque
// "Internal server error" 500 -- indistinguishable from a real bug, and wrong: a stale
// leader or an overload is the server asking the client to come back.

// A VShard in the batch has no owner in the current placement map. FAIL-CLOSED before
// any slice is dispatched, so nothing is durably committed for a batch that cannot be
// routed in full.
class UnassignedVShardError : public std::runtime_error {
public:
    explicit UnassignedVShardError(const std::string& what) : std::runtime_error(what) {}
};

// The coordinator exhausted its bounded retries (or its deadline) with slices still
// uncommitted. The whole batch failed -- never a silent partial ack -- and the write is
// safe to re-send (LWW).
class RetryableWriteError : public std::runtime_error {
public:
    explicit RetryableWriteError(const std::string& what) : std::runtime_error(what) {}
};

// A slice was routed to a shard whose data plane is tearing down (shutdown in progress).
// The write never reached Raft, so nothing was committed and it is safe to re-route.
//
// It has a real type because two things depended on recognising it and both were doing so
// by MESSAGE: the failure classifier, and http_write_handler's batch path -- which did NOT
// match it at all, so a shutting-down shard turned into HTTP 200 {"status":"partial"}, a
// silent partial batch of exactly the kind the ack contract forbids.
class ShardStoppingError : public std::runtime_error {
public:
    explicit ShardStoppingError(const std::string& what) : std::runtime_error(what) {}
};

// This node's own in-flight write budget is full (write-scaleout 3d). Explicit
// pushback, so overload degrades to a 503 the client can pace against rather than to
// unbounded queueing and timeout storms.
class WriteOverloadedError : public std::runtime_error {
public:
    explicit WriteOverloadedError(const std::string& what) : std::runtime_error(what) {}
};

// A single encoded slice exceeds what a peer's inbound RPC admission will ever accept.
// Terminal: retrying cannot shrink it, and it is the CALLER's frame that is wrong, so
// it surfaces as a 413 rather than a 503 or a 500.
class WriteFrameTooLargeError : public std::runtime_error {
public:
    explicit WriteFrameTooLargeError(const std::string& what) : std::runtime_error(what) {}
};

// ---------------------------------------------------------------------------
// Classify an exception raised by a LOCAL propose (same process, so the cause is
// knowable). Anything not explicitly recognised is Fatal: a retry budget must never be
// spent hiding a bug, and an unrecognised local failure is far more likely to be one
// than to be a transient.
inline WriteFailure classifyLocalWriteFailure(const std::exception_ptr& e) {
    try {
        std::rethrow_exception(e);
    } catch (const raft::LeadershipLostError&) {
        return WriteFailure::LeadershipLost;
    } catch (const ShardStoppingError&) {
        return WriteFailure::ShardStopping;
    } catch (const WriteOverloadedError&) {
        return WriteFailure::Overloaded;
    } catch (const WriteFrameTooLargeError&) {
        return WriteFailure::Fatal;
    } catch (const UnassignedVShardError&) {
        return WriteFailure::Unassigned;
    } catch (...) {
        return WriteFailure::Fatal;
    }
}

// Classify an exception raised by a REMOTE propose. The mirror-image judgement: a peer
// that errors is an AVAILABILITY problem (it is down, partitioned, overloaded, or
// mid-restart), which is exactly how the query path already treats an unreachable
// leader (`unreachableLeaders` -> QUERY_INCOMPLETE rather than a 500). We cannot tell
// its causes apart from here, so everything transport-shaped is Transport (ambiguous,
// retryable) and only the two failures we raise CLIENT-SIDE, before the frame is sent,
// are terminal.
inline WriteFailure classifyRemoteWriteFailure(const std::exception_ptr& e) {
    try {
        std::rethrow_exception(e);
    } catch (const WriteFrameTooLargeError&) {
        return WriteFailure::Fatal;  // our own frame is too big; a different peer will refuse it too
    } catch (const UnassignedVShardError&) {
        return WriteFailure::Unassigned;
    } catch (...) {
        return WriteFailure::Transport;
    }
}

}  // namespace timestar::data
