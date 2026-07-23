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
