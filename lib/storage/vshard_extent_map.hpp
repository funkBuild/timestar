#pragma once

#include "../core/vshard.hpp"
#include "point_revision.hpp"

#include <cstdint>
#include <map>
#include <vector>

namespace timestar {

// One VShard's data footprint in a single TSM file: which file, and the
// revision range of that VShard's points within it (ADR 0002/0003).
struct VShardExtent {
    uint64_t fileId = 0;  // TSM file sequence id
    RevisionRange revRange;

    friend bool operator==(const VShardExtent&, const VShardExtent&) = default;
};

// Transient snapshot-builder index: the set of pinned TSM files covering each
// VShard and their revision ranges. This is never a separate durable format;
// VShardSnapshotManifest serializes the selected VShard's extents inside TSP1.
class VShardExtentMap {
public:
    // Record that `file` (with the given per-VShard revision range) covers
    // `vshard`. Adding the same (vshard, fileId) twice merges the revision range.
    void add(VShardId vshard, const VShardExtent& extent);

    // Extents for a VShard, ordered by fileId (empty if none).
    [[nodiscard]] std::vector<VShardExtent> extents(VShardId vshard) const;
    // Union revision range of a VShard across all its extents (empty if none).
    [[nodiscard]] RevisionRange revRange(VShardId vshard) const;

private:
    // vshard -> (fileId -> revRange), ordered for deterministic snapshot manifests.
    std::map<uint16_t, std::map<uint64_t, RevisionRange>> byVShard_;
};

}  // namespace timestar
