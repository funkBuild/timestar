#pragma once

#include "write_record.hpp"  // WriteBatch

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace timestar::data {

// The ENRICHED replicated data-plane log record (integration plan M3, replacing the
// lossy DataPoint-based DataCommand). Each committed Raft entry in a VShard's data
// group carries exactly one of these; EngineDataStateMachine applies it
// deterministically through EngineLocalStore on every replica, so replicas cannot
// diverge. Point revisions are NOT carried in the write: they are assigned at APPLY
// time from the log position (ADR 0003), so a leader need not pre-read a counter and
// two proposals can never collide on a revision.

// Delete every point of the series identified by `seriesKey` (the lossless string
// whose hash routes it -- not a one-way DataPoint SeriesId128) in [startTime,
// endTime] inclusive. Applied as a physical range delete on every replica (NOT
// revision-bounded in v1 -- EngineDataStateMachine passes no revision floor). It
// still converges: strict log-order apply means a write ordered AFTER the delete is
// re-applied after it and survives, a write BEFORE is removed -- the log order is the
// linearization. A revision-bounded tombstone is a later refinement.
struct DeleteRangeKey {
    std::string seriesKey;
    uint64_t startTime = 0;
    uint64_t endTime = 0;  // inclusive
};

// Drop every point older than `cutoffTime` across the VShard. Monotonic. NOTE: in v1
// EngineDataStateMachine's apply of this command is a NO-OP (EngineLocalStore::
// applyRetention is a stub, wired in M1.x/M6) -- it is a uniform no-op on every
// replica (not divergent), but replicated retention is not yet functional.
struct RetentionCutoffCmd {
    uint64_t cutoffTime = 0;
};

using ReplicatedCommand = std::variant<WriteBatch, DeleteRangeKey, RetentionCutoffCmd>;

// Self-delimiting, trailer-checksummed wire form (a 1-byte kind tag + the payload,
// the WriteBatch arm reusing the tested encodeWriteBatch). decode returns nullopt on
// ANY malformed/truncated/checksum-mismatched input so a corrupt frame can never
// fabricate a command.
std::string encodeReplicatedCommand(const ReplicatedCommand& cmd);
// The WriteBatch arm WITHOUT materialising a ReplicatedCommand (write-scaleout 3b).
// Byte-identical to encodeReplicatedCommand(ReplicatedCommand{batch}); it exists so the
// replicated write path can propose a slice it does not own -- the retry loop keeps the
// groups so it can re-dispatch the failed ones -- without paying a WriteBatch copy per
// proposal just to fill the variant.
std::string encodeWriteCommand(const WriteBatch& batch);
std::optional<ReplicatedCommand> decodeReplicatedCommand(const std::string& bytes);

}  // namespace timestar::data
