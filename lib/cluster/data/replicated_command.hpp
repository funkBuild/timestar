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
    // A non-zero, request-stable operation identity makes an ambiguous retry a
    // replicated no-op after the first successful apply. Zero identifies the
    // legacy command encoding and deliberately retains its retry-unsafe
    // semantics for journal compatibility.
    SeriesId128 operationId{};
};

// Stable digest of the exact delete target. Snapshot-persistent operation
// receipts retain this alongside the ID so accidental ID reuse for different
// command bytes fail-stops instead of silently acknowledging the wrong delete.
uint64_t deleteRangeCommandHash(const DeleteRangeKey& command);

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

// What the command wrapper costs on top of the batch it carries: a 1-byte kind tag, the
// 4-byte length prefix of the sub-blob, and the 8-byte FNV trailer.
inline constexpr size_t kWriteCommandFramingBytes = 1 + 4 + 8;

// An upper bound, over every format version this codec can emit, on the bytes
// `encodeWriteCommand(batch)` produces -- i.e. on the size of the Raft ENTRY that batch
// becomes (debt D-31). See `maxEncodedBytes` for why the bound has to be
// version-independent.
inline size_t maxEncodedWriteCommandBytes(const WriteBatch& batch) {
    return maxEncodedBytes(batch) + kWriteCommandFramingBytes;
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
// proposal may be (see maxEncodedBytes). Cheap: it walks series, not bytes, on a path that
// is about to encode all of them anyway.
struct OversizeSlice {
    uint16_t vshard = 0;
    size_t bytes = 0;  // the BOUND-side estimate, i.e. what was compared
};
std::optional<OversizeSlice> firstUnproposableSlice(const VShardBatchView& view, size_t bound);

}  // namespace timestar::data
