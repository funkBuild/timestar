#pragma once

#include "../core/series_id.hpp"
#include "../core/vshard.hpp"
#include "vshard_extent_map.hpp"
#include "vshard_snapshot_manifest.hpp"
#include "vshard_verification_hash.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar {

// Assembles a VShard snapshot (Task 4d). The caller streams the VShard's RESOLVED
// logical view -- one LWW winner per (series, timestamp), in canonical
// (series, timestamp) order -- through the add* methods (which fold the
// whole-snapshot verification hash), then calls build() with the VShard's
// collected extents (from addTsmFileExtents), the catalog hash, and any tombstone
// object ids. build() derives snapshotRevision from the extents' max revision and
// emits a validated VShardSnapshotManifest.
//
// It composes the pieces already built (verification hash + extent map + manifest)
// into the snapshot descriptor; the actual reading of the resolved view is the
// caller's job (the read path), keeping this unit pure and testable.
class VShardSnapshotBuilder {
public:
    explicit VShardSnapshotBuilder(VShardId vshard) : vshard_(vshard) {}

    // Fold one resolved point into the verification hash (canonical order; see
    // VShardVerificationHash for the ordering contract).
    void addFloat(const SeriesId128& series, uint64_t ts, double value) { hash_.addFloat(series, ts, value); }
    void addInteger(const SeriesId128& series, uint64_t ts, int64_t value) { hash_.addInteger(series, ts, value); }
    void addBoolean(const SeriesId128& series, uint64_t ts, bool value) { hash_.addBoolean(series, ts, value); }
    void addString(const SeriesId128& series, uint64_t ts, std::string_view value) {
        hash_.addString(series, ts, value);
    }

    // Build the manifest. snapshotRevision = the VShard's max revision across its
    // extents (0 if none). Returns the assembled manifest; it satisfies valid().
    [[nodiscard]] VShardSnapshotManifest build(const VShardExtentMap& extents, std::string catalogHash,
                                               std::vector<uint64_t> tombstoneObjectIds = {}) const;

private:
    VShardId vshard_;
    VShardVerificationHash hash_;
};

}  // namespace timestar
