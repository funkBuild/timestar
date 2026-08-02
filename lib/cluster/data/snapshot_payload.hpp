#pragma once

#include "../../core/series_id.hpp"
#include "../../storage/vshard_snapshot_manifest.hpp"

#include <optional>
#include <string>
#include <vector>

namespace timestar::data {

using timestar::VShardSnapshotManifest;

// One data file carried in a snapshot: its target name (as it will be installed into
// the receiver's tsm dir) and its raw bytes.
struct SnapshotFile {
    std::string name;
    std::string bytes;
};

// A replicated delete operation already applied at `appliedIndex`. These are
// state-machine data, not storage-engine tombstones: retaining them across Raft
// log compaction is what makes a client retry harmless after restart or
// InstallSnapshot. `commandHash` detects operation-ID reuse for another target.
struct DeleteOperationReceipt {
    SeriesId128 operationId{};
    uint64_t appliedIndex = 0;
    uint64_t commandHash = 0;
    // Client-stable issuance time carried by the replicated command. Non-zero.
    uint64_t issuedAtMs = 0;

    auto operator<=>(const DeleteOperationReceipt&) const = default;
};

struct DeleteReceiptSnapshotState {
    // Modern retries at or before this floor are known-retired no-ops. The
    // index identifies the log entry whose deterministic apply advanced it.
    uint64_t retiredBeforeMs = 0;
    uint64_t retiredAtIndex = 0;
    std::vector<DeleteOperationReceipt> receipts;
};

// The monolithic Raft InstallSnapshot payload for a VShard (integration plan M3): the
// VShardSnapshotManifest, deterministic series catalog, and every data file it
// references, self-contained so a
// lagging replica (or a joining node) can install the whole VShard state without the
// leader's live files. This is the "manifest + object stream" M3 wires as ONE payload;
// on-the-wire chunking is M5. EngineDataStateMachine::snapshot() builds it (from
// Engine::buildVShardSnapshotFiles) and applySnapshot() installs it (write files
// to temp -> Engine::restoreVShardSnapshot -> rebuild NativeIndex).
struct SnapshotPayload {
    VShardSnapshotManifest manifest;
    // Deterministic SeriesCatalog::snapshot() bytes, authenticated by catalogHash.
    std::string catalog;
    // Canonically sorted bounded receipts at or below the Raft snapshot boundary.
    uint64_t deleteReceiptsRetiredBeforeMs = 0;
    uint64_t deleteReceiptsRetiredAtIndex = 0;
    std::vector<DeleteOperationReceipt> deleteReceipts;
    std::vector<SnapshotFile> files;  // one per manifest.dataExtents entry, same order
};

// Explicitly-versioned v1, FNV-trailer-checksummed, bounds-checked codec.
// its encode()); decode returns nullopt on ANY malformed/truncated/checksum-mismatch,
// so a corrupt snapshot can never be installed as valid state.
std::string encodeSnapshotPayload(const SnapshotPayload& payload);
std::optional<SnapshotPayload> decodeSnapshotPayload(const std::string& bytes);

// Recovery of a locally-produced snapshot must restore state-machine receipt
// state without decoding/copying its potentially 128 MiB data objects. Both
// helpers verify the outer checksum and complete framing.
std::optional<std::vector<DeleteOperationReceipt>> decodeSnapshotDeleteReceipts(const std::string& bytes);
std::optional<DeleteReceiptSnapshotState> decodeSnapshotDeleteReceiptState(const std::string& bytes);

// CONSUMING overload (debt D-32). Byte-for-byte identical output; the difference is
// what is resident while it runs.
//
// The const& overload copies every file into the output, so the producer holds the whole
// payload TWICE at its peak -- and it grows the output geometrically from empty, so the
// realloc that crosses the last power of two briefly holds a third partial copy. It is
// the biggest single term in the memory multiple that `kMaxVShardSnapshotBytes` exists to
// bound, and it is pure waste on the one caller that matters: `snapshotVShard` builds a
// payload, encodes it, and never looks at it again.
//
// This overload reserves the exact size up front (no growth spikes) and RELEASES each
// file's bytes as it appends them, so peak residency is the output plus one file rather
// than the output plus the whole input. `payload` is left valid but unspecified -- its
// catalog and file bodies are gone. The const& overload stays for the callers that keep
// their payload (tests, and anything that encodes to compare).
std::string encodeSnapshotPayload(SnapshotPayload&& payload);

}  // namespace timestar::data
