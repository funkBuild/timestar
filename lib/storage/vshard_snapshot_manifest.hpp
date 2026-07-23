#pragma once

#include "../core/vshard.hpp"
#include "vshard_extent_map.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace timestar {

// The manifest describing ONE VShard snapshot (Task 4d). A snapshot includes the
// catalog, data extents, index extract, and tombstone objects -- and nothing
// else -- captured at a named revision. Restore verifies the whole-snapshot
// verification hash (parent plan, anti-entropy). This struct is that descriptor:
// it references the constituent objects and carries the two content hashes
// (verification hash over the resolved logical view; catalog hash over the
// catalog snapshot), so a restore can validate integrity before installing.
struct VShardSnapshotManifest {
    VShardId vshard{0};
    // Highest revision captured by this snapshot (the applied watermark at
    // snapshot time). Restore installs data up to and including it.
    uint64_t snapshotRevision = 0;
    // 32-hex-char (128-bit) whole-snapshot verification hash and catalog hash.
    std::string verificationHash;
    std::string catalogHash;
    // Data files covering this VShard (fileId + the VShard's revision range in
    // each), ordered by fileId.
    std::vector<VShardExtent> dataExtents;
    // Tombstone object ids included in the snapshot, ascending.
    std::vector<uint64_t> tombstoneObjectIds;

    friend bool operator==(const VShardSnapshotManifest&, const VShardSnapshotManifest&) = default;

    // Structurally well-formed: valid VShard, 32-hex hashes, extents ordered by
    // fileId with max>=min ranges, tombstone ids strictly ascending, and every
    // extent/tombstone revision within [.., snapshotRevision].
    [[nodiscard]] bool valid() const;

    // Deterministic serialisation with a trailing CRC.
    [[nodiscard]] std::string encode() const;
    // Parse; nullopt on any corruption or a structurally invalid manifest.
    [[nodiscard]] static std::optional<VShardSnapshotManifest> decode(std::string_view bytes);
};

}  // namespace timestar
