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
// VShardSnapshotManifest plus every data file it references, self-contained so a
// lagging replica (or a joining node) can install the whole VShard state without the
// leader's live files. This is the "manifest + object stream" M3 wires as ONE payload;
// on-the-wire chunking is M5. EngineDataStateMachine::snapshot() builds it (from
// Engine::createVShardSnapshot + the referenced files) and applySnapshot() installs it
// (write files to temp -> Engine::restoreVShardSnapshot, verify-then-install).
struct SnapshotPayload {
    VShardSnapshotManifest manifest;
    std::vector<SnapshotFile> files;  // one per manifest.dataExtents entry, same order
};

// FNV-trailer-checksummed, bounds-checked codec (the manifest carries its own CRC in
// its encode()); decode returns nullopt on ANY malformed/truncated/checksum-mismatch,
// so a corrupt snapshot can never be installed as valid state.
std::string encodeSnapshotPayload(const SnapshotPayload& payload);
std::optional<SnapshotPayload> decodeSnapshotPayload(const std::string& bytes);

}  // namespace timestar::data
