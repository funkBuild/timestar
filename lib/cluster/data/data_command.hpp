#pragma once

#include "data_plane.hpp"  // DataPoint, SeriesId128

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace timestar::data {

// A replicated data-plane log record (docs/clustering.md §"Log record"). Each
// committed Raft entry in a VShard's data group carries exactly one of these
// (serialized); the DataStateMachine applies it deterministically on every
// replica, so replicas cannot diverge. Every command is idempotent by
// construction, which is what makes the client contract "retry the whole batch".
//
// Point revisions are NOT carried here: they are assigned deterministically at
// APPLY time from a per-VShard monotonic counter (identical apply order on every
// replica -> identical revisions), so a leader need not pre-read its counter and
// two proposals can never collide on a revision. Replicated writes start at
// revision 1; migrated pre-cluster data reserves revision 0.

// Write (or last-write-wins overwrite) a batch of points. Series creation is
// implicit get-or-create in the store's catalog on first sight of a series.
struct WritePoints {
    std::vector<DataPoint> points;
    uint64_t schemaVersion = 0;  // schema version this write was validated against
};

// Delete every point of `series` whose timestamp is in [startTime, endTime]
// (inclusive) and whose revision precedes the tombstone's apply-assigned
// revision.
//
// Log-ordered contract (NOT a logical no-op across intervening ops): a delete
// removes writes ordered BEFORE it in the log and is superseded by writes ordered
// AFTER it. A retried delete (the client re-sends the batch after a lost ack)
// commits at a NEW, later log position, so it is ordered against whatever
// committed in the meantime -- exactly as a delete issued once at that position
// would be. Both are valid linearizations of concurrent operations: because the
// client never received the first ack, its delete is concurrent with any write
// that committed in the gap, and either order is legal. The mirror also holds --
// a retried WRITE after an acked delete legitimately reappears (resurrection),
// which is intentional. Pure re-application (no intervening opposite op) is a
// true no-op. This is what the plan means by "range deletes are idempotent... a
// re-applied batch is applied identically on every replica, so replicas cannot
// diverge" -- convergence, not retry-invisibility. See docs/clustering.md
// §"Write path". "No acknowledged data loss" (the RF gate) is failure-induced
// loss, never a write superseded by a later-ordered delete.
struct DeleteRange {
    SeriesId128 series{};
    uint64_t startTime = 0;
    uint64_t endTime = 0;  // inclusive
};

// Drop every point older than `cutoffTime` across the VShard. Monotonic: the
// effective cutoff only ever advances, so re-applying an older cutoff is a no-op.
struct RetentionCutoff {
    uint64_t cutoffTime = 0;
};

using DataCommand = std::variant<WritePoints, DeleteRange, RetentionCutoff>;

// Wire serialization for one command (the Raft entry payload). Self-delimiting
// and trailer-checksummed; decode returns nullopt on ANY malformed, truncated,
// or checksum-mismatched input so a corrupt frame can never fabricate a command.
std::string encodeDataCommand(const DataCommand& cmd);
std::optional<DataCommand> decodeDataCommand(const std::string& bytes);

}  // namespace timestar::data
