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
// endTime] inclusive whose revision precedes the tombstone's apply-assigned revision.
struct DeleteRangeKey {
    std::string seriesKey;
    uint64_t startTime = 0;
    uint64_t endTime = 0;  // inclusive
};

// Drop every point older than `cutoffTime` across the VShard. Monotonic.
struct RetentionCutoffCmd {
    uint64_t cutoffTime = 0;
};

using ReplicatedCommand = std::variant<WriteBatch, DeleteRangeKey, RetentionCutoffCmd>;

// Self-delimiting, trailer-checksummed wire form (a 1-byte kind tag + the payload,
// the WriteBatch arm reusing the tested encodeWriteBatch). decode returns nullopt on
// ANY malformed/truncated/checksum-mismatched input so a corrupt frame can never
// fabricate a command.
std::string encodeReplicatedCommand(const ReplicatedCommand& cmd);
std::optional<ReplicatedCommand> decodeReplicatedCommand(const std::string& bytes);

}  // namespace timestar::data
