#pragma once

#include "tsm.hpp"
#include "vshard_snapshot_manifest.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <vector>

namespace timestar {

// Restore a VShard snapshot (Task 4d): install-not-rebuild with verification.
// First re-derives the verification hash from `sourceFiles` and compares it to
// `manifest`; ONLY if it matches are the files installed by copying each
// sourceFiles[i]'s backing file to targetPaths[i]. A mismatch installs NOTHING
// and returns false -- corrupt/incomplete snapshots are rejected, never made live
// (the parent plan's restore-verifies-the-hash contract). `sourceFiles` must be
// open+sparse-indexed; `targetPaths` are valid TSM filenames in the shard's tsm
// dir (one per source file). Returns true iff installed.
//
// Install is ALL-OR-NOTHING for the common failure: every file is copied to a
// unique temp (data-fsync'd) BEFORE any is published, so a copy failure (e.g.
// disk full) rolls back the temps and installs nothing. Parent dirs are fsync'd
// after the rename-publish. A source file whose maxRevision() exceeds
// manifest.snapshotRevision is rejected (it is not the data the manifest
// describes). A verify/watermark mismatch returns false; an I/O failure during
// install throws (after rolling back staged temps).
seastar::future<bool> restoreVShardSnapshot(const VShardSnapshotManifest& manifest,
                                            std::vector<seastar::shared_ptr<::TSM>> sourceFiles,
                                            std::vector<std::string> targetPaths);

}  // namespace timestar
