#pragma once

#include "../core/vshard.hpp"
#include "tsm.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <vector>

namespace timestar {

// One VShard's data movement in the offline shard_N -> VShard migration (Task 6).
// Reads the VShard's series (virtualShard == vshard) from `sourceFiles`, resolves
// last-write-wins across them (keep-last, matching the query path), and writes
// them into a single VShard-PURE TSM file at `outputPath`. No revisions are
// stamped, so every block's [minRev,maxRev] is the migrated-floor [0,0] -- any
// later replicated write (revision >= 1) beats imported data (ADR 0003).
//
// Additive: it only reads existing files and writes a NEW file; it does not
// mutate or delete the sources (the caller sequences delete-after-verify for
// crash-atomicity). Returns the number of series written.
//
// Offline / one-VShard-at-a-time: it streams output (writeSeriesStreaming +
// closeDMA) to bound memory and drain between blocks, so a large VShard does not
// pin its whole encoded size in RAM. A series whose value type differs across
// files (a corrupt type-flip) keeps only its NEWEST-type data; older,
// different-typed generations are skipped rather than aborting the migration.
seastar::future<size_t> migrateVShardToFile(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> sourceFiles,
                                            std::string outputPath);

}  // namespace timestar
