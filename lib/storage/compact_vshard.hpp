#pragma once

#include "../core/vshard.hpp"
#include "tsm.hpp"

#include <functional>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <utility>
#include <vector>

namespace timestar {

// VShard-partitioned compaction (Task 4c): reads a single VShard's series from
// `inputs`, resolves last-write-wins across them (keep-last, matching the query
// path), and writes them into ONE VShard-PURE TSM file at `outputPath`,
// PRESERVING each series' revision range (the union of its input blocks'
// [minRev,maxRev]). This is the steady-state repartition of already-revisioned data into
// VShard-pure tier>=1 files, so cross-file LWW-by-range and the recovery counter
// stay correct (ADR 0003). Streamed (bounded memory). Returns series written.
seastar::future<size_t> compactVShardToFile(VShardId vshard, std::vector<seastar::shared_ptr<::TSM>> inputs,
                                            std::string outputPath);

// Partition `inputs` into per-VShard pure files: for every VShard whose series
// appear in `inputs`, produce one file `<outputDir>/<pathForVShard(vshard)>`.
// Returns (vshard, path) for each non-empty VShard file written, ascending by
// vshard. `pathForVShard` maps a VShard to its output filename (caller controls
// naming / sequence allocation).
seastar::future<std::vector<std::pair<VShardId, std::string>>> partitionByVShard(
    std::vector<seastar::shared_ptr<::TSM>> inputs, std::function<std::string(VShardId)> pathForVShard);

}  // namespace timestar
