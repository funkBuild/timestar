#pragma once

#include "tsm.hpp"
#include "vshard_extent_map.hpp"

#include <seastar/core/future.hh>

namespace timestar {

// Snapshot extent collection (Task 4d, read-only). Populates `map` with the
// extents a single (already-open, sparse-indexed) TSM file `tsm`, identified by
// `fileId`, contributes: for every VShard whose series live in the file, it adds
// one extent carrying that VShard's revision range within the file (the union of
// the block ranges of the VShard's series). A VShard's series are identified by
// virtualShard(seriesId), so extraction is derived and stable across core counts.
//
// This composes the transient extent index with the v1 per-block revision
// ranges so it can feed a VShardSnapshotManifest. It changes no on-disk format
// and touches no hot path; it is the offline read side.
seastar::future<> addTsmFileExtents(VShardExtentMap& map, uint64_t fileId, ::TSM& tsm);

}  // namespace timestar
