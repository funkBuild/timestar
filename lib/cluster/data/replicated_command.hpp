#pragma once

#include "write_record.hpp"  // WriteBatch

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace timestar::data {

// The typed replicated data-plane log record. Each committed Raft entry in a VShard's data
// group carries exactly one of these; EngineDataStateMachine applies it
// deterministically through EngineLocalStore on every replica, so replicas cannot
// diverge. Point revisions are NOT carried in the write: they are assigned at APPLY
// time from the log position (ADR 0003), so a leader need not pre-read a counter and
// two proposals can never collide on a revision.

// One canonical exact target inside an idempotent per-VShard delete operation.
// Targets are encoded in strict tuple order and may not repeat, making the batch
// bytes (and therefore its receipt hash) independent of HTTP request ordering.
struct DeleteRangeTarget {
    std::string seriesKey;
    uint64_t startTime = 0;
    uint64_t endTime = 0;

    auto operator<=>(const DeleteRangeTarget&) const = default;
};

// A whole HTTP operation's exact targets for ONE VShard. Keeping one receipt for
// this batch rather than one per series makes receipt growth proportional to
// requests x VShards, not to a selector/request's expanded series cardinality.
// The batch is still applied in deterministic target order. A crash before the
// final receipt replays the entry before any later log entry, so repeating an
// already-finished prefix cannot erase a write ordered after this command.
struct DeleteRangeBatch {
    std::vector<DeleteRangeTarget> targets;
    SeriesId128 operationId{};
    // Client-stable Unix epoch milliseconds. Required and non-zero.
    uint64_t issuedAtMs = 0;
};

inline constexpr size_t kMaxDeleteRangeBatchTargets = 10'000;
inline constexpr size_t kMaxDeleteReceiptsPerVShard = 1'024;
inline constexpr uint64_t kDeleteReceiptRetentionMs = 60 * 60 * 1'000;
inline constexpr uint64_t kDeleteReceiptFutureSkewMs = 5 * 60 * 1'000;

inline size_t encodedDeleteRangeBatchBytes(const std::vector<DeleteRangeTarget>& targets, uint64_t) {
    // magic/version + tag + operation ID + issuance time + target count + checksum.
    size_t bytes = 4 + 1 + 16 + 8 + 4 + 8;
    for (const auto& target : targets) {
        constexpr size_t framing = 4 + 8 + 8;  // key length + inclusive range
        if (target.seriesKey.size() > SIZE_MAX - framing || bytes > SIZE_MAX - framing - target.seriesKey.size())
            return SIZE_MAX;
        bytes += framing + target.seriesKey.size();
    }
    return bytes;
}

inline size_t encodedDeleteRangeBatchBytes(const DeleteRangeBatch& command) {
    return encodedDeleteRangeBatchBytes(command.targets, command.issuedAtMs);
}

// Stable digest of the exact delete target. Snapshot-persistent operation
// receipts retain this alongside the ID so accidental ID reuse for different
// command bytes fail-stops instead of silently acknowledging the wrong delete.
uint64_t deleteRangeCommandHash(const DeleteRangeBatch& command);

inline constexpr size_t kMaxRetentionMeasurementBytes = 1024;

inline bool validRetentionMeasurement(std::string_view measurement) {
    if (measurement.empty() || measurement.size() > kMaxRetentionMeasurementBytes)
        return false;
    for (const unsigned char c : measurement)
        if (c < 0x20 || c == 0x7f)
            return false;
    return true;
}

// Drop every point for `measurement` older than `cutoffTime` across this
// command's VShard. Group 0 assigns a globally contiguous sweepId and permits
// only one all-VShard fan-out at a time, allowing constant-space retry fencing.
// The controller computes the cutoff once; apply never consults a local clock.
struct RetentionCutoffCmd {
    uint64_t sweepId = 0;
    std::string measurement;
    uint64_t policyVersion = 0;
    uint64_t cutoffTime = 0;
};

using ReplicatedCommand = std::variant<WriteBatch, DeleteRangeBatch, RetentionCutoffCmd>;

// Self-delimiting v1, trailer-checksummed wire form (magic + kind tag + payload,
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

// What the command wrapper costs on top of the batch it carries: a 1-byte kind tag, the
// 4-byte length prefix of the sub-blob, and the 8-byte FNV trailer.
inline constexpr size_t kWriteCommandFramingBytes = 4 + 1 + 4 + 8;

// Exact command size without allocating the frame.
inline size_t encodedWriteCommandBytes(const WriteBatch& batch) {
    return encodedWriteBatchBytes(batch) + kWriteCommandFramingBytes;
}

// The first group of `view` whose command encoding could exceed `bound`, and by how much
// (debt D-31). `bound` is `RaftGroup::kMaxProposalBytes`; this is checked on the SEND side
// of a forwarded write so the refusal is a local, terminal 413 naming the VShard, rather
// than the receiving leader's `ProposalTooLargeError` arriving as an opaque remote error
// that the router retries against every other leader before reporting a 500.
//
// A frame carries a whole VIEW but a Raft entry carries ONE group, so the frame bound the
// send path already checks does not imply this one and cannot replace it: a frame within
// `kMaxOutboundFrameBytes` can still hold a single slice that re-encodes larger than a
// proposal may be. Cheap: it walks series, not bytes, on a path that
// is about to encode all of them anyway.
struct OversizeSlice {
    uint16_t vshard = 0;
    size_t bytes = 0;  // the BOUND-side estimate, i.e. what was compared
};
std::optional<OversizeSlice> firstUnproposableSlice(const VShardBatchView& view, size_t bound);

}  // namespace timestar::data
