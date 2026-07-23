#pragma once

#include "tsm.hpp"
#include "vshard_snapshot_builder.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace timestar {

// Reads a VShard's RESOLVED logical view from a fixed set of TSM files and folds
// it into `builder`'s verification hash (Task 4d snapshot creation, read side).
//
// It collects the VShard's series (virtualShard == vshard) across `files`, and
// for each series -- in canonical SeriesId128 order -- resolves last-write-wins
// across files by dataRank (the newer generation's value wins at a shared
// timestamp, matching the query path), then feeds the resolved (timestamp, value)
// points in ascending-timestamp order. `files` may be given in any order; this
// sorts a copy by dataRank descending.
//
// Additive and read-only: it changes no format and no hot path. The caller
// afterwards calls builder.build(extents, ...) to emit the manifest.
seastar::future<> feedVShardResolvedView(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> files,
                                         VShardSnapshotBuilder& builder);

}  // namespace timestar
