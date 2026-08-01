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
// sorts a copy by dataRank ascending (oldest first, overwrite with newer).
//
// Additive and read-only: it changes no format and no hot path. The caller
// afterwards calls builder.build(extents, ...) to emit the manifest.
//
// PRECONDITIONS the caller must satisfy (a snapshot is over the flushed TSM view):
//   - Flush the memory store first: this reads TSM FILES ONLY, so any un-flushed
//     data would be missing from the verification hash.
//   - Keep the input files and their tombstone views pinned and stable for the
//     read. This applies each file's current tombstone sidecar; snapshot creation
//     additionally checks mutation generations to reject a concurrent delete.
//   - Pass builder.build() the SAME files' extents (from addTsmFileExtents), so
//     the manifest's snapshotRevision matches the hashed data; feeding one file
//     set here and a different extent map to build() yields a valid-looking
//     manifest whose watermark contradicts its hash.
seastar::future<> feedVShardResolvedView(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> files,
                                         VShardSnapshotBuilder& builder);

// Restore verification gate (Task 4d): re-derive the verification hash from the
// (just-installed) `files` and compare it to `manifest.verificationHash`. Returns
// true iff they match -- i.e. the installed data reproduces the snapshot's
// resolved logical view exactly. A false result means the install is corrupt or
// incomplete and must be rejected/quarantined, never served. Same stability
// precondition as feedVShardResolvedView; shipped snapshot objects themselves
// are required to have no tombstone sidecars.
seastar::future<bool> verifyVShardSnapshot(const VShardSnapshotManifest& manifest,
                                           std::vector<seastar::shared_ptr<::TSM>> files);

}  // namespace timestar
