#pragma once

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
    // Deterministic SeriesCatalog::snapshot() bytes. Non-empty payloads use
    // snapshot format v2 and manifest.catalogHash authenticates these bytes.
    // Empty means a decoded legacy v1 payload; production install rejects it
    // because it cannot restore discovery metadata.
    std::string catalog;
    std::vector<SnapshotFile> files;  // one per manifest.dataExtents entry, same order
};

// FNV-trailer-checksummed, bounds-checked codec (the manifest carries its own CRC in
// its encode()); decode returns nullopt on ANY malformed/truncated/checksum-mismatch,
// so a corrupt snapshot can never be installed as valid state.
std::string encodeSnapshotPayload(const SnapshotPayload& payload);
std::optional<SnapshotPayload> decodeSnapshotPayload(const std::string& bytes);

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
