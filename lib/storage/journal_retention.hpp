#pragma once

#include "../core/vshard.hpp"
#include "journal_record.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace timestar {

// Per-VShard journal retention watermarks and the segment-GC decision they
// drive (ADR 0001 sec 4/5). Journals are multiplexed per core, so a single
// laggard VShard would otherwise pin a whole shared segment; GC reclaims a
// segment only once every record in it is released, and any still-live records
// of a laggard are COPIED FORWARD into the current segment before the old one is
// deleted. This class computes those decisions; the I/O (delete/copy-forward)
// lives in the journal writer.
//
// A record is RELEASED (reclaimable) once its per-VShard sequence is at or below
// that VShard's released watermark -- i.e. its query-visible effects are durable
// in TSM/index/catalog and it is not needed by any live learner. released()
// advances monotonically; a lower value is ignored.
class JournalRetention {
public:
    // Advance a VShard's released watermark (monotonic; a regression is ignored).
    void setReleased(VShardId vshard, uint64_t releasedSeq);

    // The released watermark for a VShard (0 = nothing released yet).
    [[nodiscard]] uint64_t released(VShardId vshard) const;

    // FORGET a VShard entirely, so its next watermark starts from 0 again.
    //
    // MOVEMENT MUST CALL THIS when a VShard is torn down on this node, and the monotonic
    // rule above is exactly why (debt D-40). `setReleased` ignores a regression, which is
    // right while one group's sequence keeps climbing and wrong the moment the sequence
    // RESTARTS: a VShard removed and later re-added gets a FRESH journal whose vshard_seq
    // begins at 1, and a stale watermark of, say, 500 would mark every record of the new
    // journal released -- the first GC pass then deletes the new group's sealed segments,
    // including its snapshot boundary and its only HardState record. That is true in BOTH
    // layouts (the per-VShard journal is a fresh directory; the shared journal is fresh
    // records in a live one), and it is the mirror image of the leak D-40 describes: a
    // watermark left too LOW pins segments forever, one left too HIGH deletes live ones.
    //
    // Not called from anywhere yet, because VShard teardown/movement is not wired (D-40).
    // It exists now so that wiring is a one-line change at the teardown site rather than a
    // rediscovery of this hazard.
    void clearReleased(VShardId vshard);

    // How many VShards carry a watermark (diagnostics; also the thing that grows without
    // bound if movement forgets to clearReleased).
    [[nodiscard]] size_t trackedVShards() const { return released_.size(); }

    struct SegmentGc {
        // True iff EVERY record in the segment is released: the segment can be
        // deleted with no copy-forward.
        bool reclaimable = false;
        // Indices (into the passed-in segment record vector) of records that are
        // still live (seq > released) and must be copied forward before the
        // segment is deleted. Empty iff reclaimable.
        std::vector<size_t> liveRecordIndices;
    };

    // Decide GC for one segment given its records (in stored order).
    [[nodiscard]] SegmentGc planSegment(const std::vector<JournalRecord>& segmentRecords) const;

private:
    std::unordered_map<uint16_t, uint64_t> released_;
};

}  // namespace timestar
