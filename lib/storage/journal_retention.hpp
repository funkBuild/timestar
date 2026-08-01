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
    // ================== READ THE ORDERING BEFORE CALLING THIS (debt D-40) ==============
    //
    // This is HALF of a protocol, and on its own it is the wrong half. VShard teardown has
    // two hazards that pull in OPPOSITE directions, and each single-line "fix" causes the
    // other one:
    //
    //   * FORGETTING ALONE LEAKS THE DEPARTED GROUP. `released()` drops to 0, so every
    //     record that VShard ever wrote reads as LIVE forever. In a private journal that
    //     pins its whole directory; in a shared journal each segment containing one of
    //     those records stays pinned (D-39 lets unrelated later segments reclaim, but it
    //     cannot make the departed group's records dead without this protocol).
    //   * PUBLISHING "EVERYTHING RELEASED" ALONE LEAKS THE ENTRY. It reclaims correctly
    //     now, but `setReleased` is MONOTONIC, so the watermark survives; a later re-add
    //     over a fresh journal -- whose `vshard_seq` restarts at 1 -- inherits it and the
    //     first GC pass deletes the NEW group's sealed segments, snapshot boundary and
    //     only HardState record included.
    //
    // The safe shape is therefore ORDERED and STATE-AWARE, not a call at a single site:
    //   (a) at teardown, publish a released watermark covering the departed VShard's
    //       records (they are dead -- the replica is gone), and leave the entry in place;
    //   (b) let a GC pass actually collect against it;
    //   (c) only THEN clearReleased() -- or, equivalently and more simply, clear at
    //       RE-ADD time, before the new group's first append, so no stale value can ever
    //       reach the new journal.
    // Whichever of (c)'s two placements is chosen, it must hang off the MOVEMENT decision
    // and not off `vshards_.count() == 0`: a group that is merely momentarily absent -- an
    // `addVShard` that has not run yet, a restart mid-move -- would otherwise be declared
    // fully released and have its LIVE log deleted.
    //
    // Not called from anywhere yet, because VShard teardown/movement is not wired (D-40).
    // It exists so the protocol above is written down where its implementer will meet it.
    // ==================================================================================
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
