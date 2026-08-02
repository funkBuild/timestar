#pragma once

#include "../raft/raft_types.hpp"   // NodeId, kNoNode, LeadershipLostError
#include "../reconnect_policy.hpp"  // kReconnectBackoff -- the window the pacing must span

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <seastar/core/timed_out_error.hh>
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
//                        its uncommitted Raft-tail budget, or a peer's
//                        rpc::resource_limits) BEFORE the mutation was appended.
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
//   ReplicatedCommandRouter also uses this taxonomy for deletes. Legacy commands without
//   an operation ID still refuse the two ambiguous classes below: re-applying a physical
//   delete after a concurrent write would erase data ordered after the first attempt.
//   Modern per-VShard batches carry snapshot-durable operation receipts, so their exact
//   retries are replicated no-ops and may use the ordinary retry policy safely.
//
// `Unassigned` and `Fatal` are terminal: no amount of retrying fixes an unowned VShard
// or a journal I/O error, and hiding either behind a retry budget only delays the
// report.
enum class WriteFailure : uint8_t {
    None = 0,
    NotLeader,
    // THE LEADER ITSELF REFUSED (write-scaleout 5 review, F1). Distinct from NotLeader,
    // and the distinction is diagnostic rather than cosmetic: `RaftGroup::propose*`
    // returns a bare `false` for BOTH "I am not the leader" and "I AM the leader but I am
    // handing leadership away" (`leadTransferee_ != kNoNode`), and the sink used to label
    // both NotLeader. When the refuser is the leader, `g->leader()` is ITSELF, so the
    // reject carried a leader hint pointing back at the node that just refused, the router
    // re-bucketed the slice into its LOCAL view, and all six attempts asked the same
    // refusing group again -- no RPC ever left the node. The symptom read as "the
    // coordinator is proposing to the wrong place"; it was proposing to exactly the right
    // place, which was refusing.
    //
    // Retryable (the refusal is bounded by the transfer-abandon window), but it must never
    // be confused with NotLeader: a NotLeader retry goes somewhere ELSE, and this one has
    // nowhere else to go.
    LeaderRefused,
    LeadershipLost,
    Transport,
    ShardStopping,
    Overloaded,
    Unassigned,
    Fatal,
    Expired,
};

// The retry POLICY table. Phase 4a extends this (and only this) to give a
// connection-backoff blip its own window.
constexpr bool isRetryableWriteFailure(WriteFailure f) {
    switch (f) {
        case WriteFailure::NotLeader:
        case WriteFailure::LeaderRefused:
        case WriteFailure::LeadershipLost:
        case WriteFailure::Transport:
        case WriteFailure::ShardStopping:
        case WriteFailure::Overloaded:
            return true;
        case WriteFailure::None:
        case WriteFailure::Unassigned:
        case WriteFailure::Fatal:
        case WriteFailure::Expired:
            return false;
    }
    return false;
}

// ---------------------------------------------------------------------------
// The retry PACING table (write-scaleout 4a). This is the extension the enum was built
// for: not every retryable class wants the same delay, and one of them is coupled to a
// constant in the TRANSPORT.
//
// THE BUG THIS FIXES. `DataPlaneRpc::clientFor` hands back the DEAD client for
// `cluster::kReconnectBackoff` (200 ms) after a connection dies -- a deliberate fast-fail
// so a burst of writes to a down peer costs one dial, not one per write. The Phase-3b
// retry loop paused a flat 20 ms between attempts, so its whole budget (5 pauses = 100 ms)
// fit INSIDE that one window: all six attempts fast-failed against the same dead
// connection, the transport was never asked to re-dial, and a 200 ms TCP blip became a
// client-visible 5xx even though the peer was healthy the whole time. That is [D6]: the
// retry existed but could not reach the thing it was retrying.
//
// THE FIX. `Transport` (and `Overloaded`, whose cure is also "wait for something to
// drain") back off geometrically from the same 20 ms base, capped, so the five pauses span
// ~620 ms -- crossing the reconnect window about three times inside the 1.5 s write
// deadline, and giving the peer's own listener time to come back. The static_assert below
// pins that relationship, so shortening the schedule or lengthening the backoff cannot
// silently re-create the bug.
//
// The leader-shaped classes START flat at 20 ms for the same reason they always did -- a
// `NotLeader` retry goes to a DIFFERENT node (the hint from 3a) and a leadership transfer
// completes in single-digit milliseconds, so backing off immediately would turn every
// routine rebalance into hundreds of milliseconds of added p99 for no availability gain.
// What changed in D-14 is what happens AFTER those fast retries fail: see
// `isElectionWaitFailure` / `kElectionRetryDelay` below. `ShardStopping` keeps the flat
// 20 ms unconditionally: the slice is re-routed, not re-dialed and not waited on.
inline constexpr std::chrono::milliseconds kWriteRetryDelayBase{20};
inline constexpr std::chrono::milliseconds kWriteRetryDelayMax{320};

// ELECTION-SHAPED failures: the slice has no leader to go to until a Raft election (or a
// leadership transfer) completes. They share a POLICY, and it is not the transport one
// (debt D-14).
//
// A leadership transfer completes in single-digit milliseconds, so the first few retries
// stay fast -- that is what keeps routine background rebalancing a latency bump rather
// than an error. But if those fast retries did not find a leader, what the slice is
// waiting for is an ELECTION, and an election at the production timing takes 2.5-5 s
// (electionTimeoutMin/Max, 125-250 ticks at 20 ms). Continuing to poll every 20 ms only
// burns the attempt budget ~120 ms into a multi-second wait and then reports failure --
// which is most of the one-node-down 503 band: a ~1000-VShard batch fails if ANY slice is
// still leaderless, and every slice whose leader died is leaderless for seconds.
//
// So: fast while a transfer is plausible, then paced to span the election.
inline constexpr unsigned kFastLeaderRetries = 3;
inline constexpr std::chrono::milliseconds kElectionRetryDelay{400};

// Is this failure "waiting for a leader to be elected/settled"? Waiting is the CURE for
// these three and for no others -- `Transport` means the peer is gone (re-dial, do not
// wait longer than the reconnect window), `Overloaded` means something must drain, and
// the terminal classes never retry at all.
//
// LeadershipLost is included and is the subtle one: the entry was appended here and then
// this node stopped leading, so a successor election is exactly what the slice is waiting
// on. It is AMBIGUOUS (the successor may commit the original entry), which is already
// safe to retry for the reason audited above -- re-application is value-idempotent under
// LWW -- and waiting does not change that.
constexpr bool isElectionWaitFailure(WriteFailure f) {
    return f == WriteFailure::NotLeader || f == WriteFailure::LeaderRefused || f == WriteFailure::LeadershipLost;
}

// Delay before retry number `attempt`+1, given what the previous attempt failed with.
// `attempt` is 1-based (the attempt that just failed).
constexpr std::chrono::milliseconds writeFailureRetryDelay(WriteFailure f, unsigned attempt) {
    switch (f) {
        case WriteFailure::Transport:
        case WriteFailure::Overloaded: {
            // Geometric from the base: 20, 40, 80, 160, 320, 320...
            int64_t ms = kWriteRetryDelayBase.count();
            for (unsigned i = 1; i < attempt && ms < kWriteRetryDelayMax.count(); ++i)
                ms *= 2;
            return std::chrono::milliseconds(ms < kWriteRetryDelayMax.count() ? ms : kWriteRetryDelayMax.count());
        }
        case WriteFailure::NotLeader:
        case WriteFailure::LeaderRefused:
        case WriteFailure::LeadershipLost:
            // Fast while a leadership TRANSFER is the plausible cause, then paced to span
            // an ELECTION (debt D-14). The step is deliberate rather than geometric: the
            // two causes have wildly different timescales (milliseconds vs seconds) and
            // there is nothing in between worth interpolating.
            return attempt <= kFastLeaderRetries ? kWriteRetryDelayBase : kElectionRetryDelay;
        case WriteFailure::None:
        case WriteFailure::ShardStopping:
        case WriteFailure::Unassigned:
        case WriteFailure::Fatal:
        case WriteFailure::Expired:
            break;
    }
    return kWriteRetryDelayBase;
}

// Total time the pauses of a full `attempts`-attempt budget cover for class `f`, ignoring
// the attempts themselves (which fast-fail on a dead connection -- that is the whole
// problem). Used by the static_assert below and by the test that pins it.
constexpr std::chrono::milliseconds writeRetryScheduleSpan(WriteFailure f, unsigned attempts) {
    int64_t total = 0;
    for (unsigned a = 1; a < attempts; ++a)
        total += writeFailureRetryDelay(f, a).count();
    return std::chrono::milliseconds(total);
}

// Jitter applied to each retry pause, so N concurrent batches do not re-dial the same peer
// on the same 20/40/80 ms grid. It SHORTENS a pause as often as it lengthens it, which is
// why the coupling assertion below has to reason about the pessimal case.
inline constexpr unsigned kWriteRetryJitterPercent = 25;

// The SHORTEST the pauses of an `attempts`-attempt budget can be once jitter has had its
// worst say. This -- not the nominal span -- is what has to outlast the reconnect window.
constexpr std::chrono::milliseconds worstCaseWriteRetrySpan(WriteFailure f, unsigned attempts) {
    return writeRetryScheduleSpan(f, attempts) * (100 - kWriteRetryJitterPercent) / 100;
}

// The coupling is asserted where BOTH numbers are visible -- in replicated_write_router.hpp,
// against `ReplicatedBatchWriteRouter::kMaxAttempts` rather than a literal. Asserting a
// hardcoded attempt count here would have let someone drop kMaxAttempts to 4, restore
// [D6], and still compile.

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
        case WriteFailure::LeaderRefused:
            return "leader-refused-mid-transfer";
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
        case WriteFailure::Expired:
            return "expired";
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

// A non-idempotent mutation may already have committed, so repeating it could change
// the result relative to concurrent operations. The HTTP layer reports an UNKNOWN
// outcome without Retry-After; callers must verify state rather than blindly retry.
class AmbiguousMutationError : public std::runtime_error {
public:
    explicit AmbiguousMutationError(const std::string& what) : std::runtime_error(what) {}
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

// The request's durable delete identity has fallen behind the replicated
// retention/capacity floor. Re-proposing it would be unsafe; the state machine
// treats a committed late copy as a no-op and the HTTP layer reports conflict.
class DeleteReceiptExpiredError : public std::runtime_error {
public:
    explicit DeleteReceiptExpiredError(const std::string& what) : std::runtime_error(what) {}
};

// The same HTTP Idempotency-Key was reused with a different original request
// body or issuance timestamp while its frozen pattern plan is still retained.
// Executing either plan would make the key ambiguous, so this is a terminal 409.
class DeletePlanConflictError : public std::runtime_error {
public:
    explicit DeletePlanConflictError(const std::string& what) : std::runtime_error(what) {}
};

// A command/snapshot feature is newer than either the cluster-wide committed
// format or the destination peer's negotiated protocol. This is a terminal,
// pre-emission refusal: retrying another leader cannot make mixed-version bytes
// safe. The HTTP layer reports an explicit upgrade/activation conflict.
class ClusterFormatUnsupportedError : public std::runtime_error {
public:
    explicit ClusterFormatUnsupportedError(const std::string& what) : std::runtime_error(what) {}
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
    } catch (const seastar::timed_out_error&) {
        // A Raft waiter that hit its deadline. AMBIGUOUS in the strongest sense: the entry
        // is already appended locally and a later quorum may still commit it, so this is
        // classified exactly like a transport failure and is safe to retry only because
        // re-application is LWW-idempotent (see the audit above).
        return WriteFailure::Transport;
    } catch (const raft::ProposalBudgetExceededError&) {
        // Refused before append by the shard/group uncommitted-tail budget. The
        // outcome is known and admission may recover as soon as quorum replication
        // commits or truncates the retained tail.
        return WriteFailure::Overloaded;
    } catch (const WriteOverloadedError&) {
        return WriteFailure::Overloaded;
    } catch (const WriteFrameTooLargeError&) {
        return WriteFailure::Fatal;
    } catch (const DeleteReceiptExpiredError&) {
        return WriteFailure::Expired;
    } catch (const ClusterFormatUnsupportedError&) {
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
    } catch (const ClusterFormatUnsupportedError&) {
        return WriteFailure::Fatal;  // client-side negotiation refusal; no frame was sent
    } catch (...) {
        return WriteFailure::Transport;
    }
}

}  // namespace timestar::data
